"""
outreach/trades_demo.py
Production-minded trades demo workflow.

Flow:
demo inbox -> inquiry persisted -> deterministic qualification ->
response draft/send -> booking job dispatch -> durable audit trail
"""
from __future__ import annotations

import email
import imaplib
import os
import re
from datetime import datetime, timedelta, timezone
from email.header import decode_header
from email.message import Message
from email.utils import parsedate_to_datetime
from pathlib import Path

from dotenv import load_dotenv

from .email_sender import infer_timezone, render_outreach_body, send_email
from .google_calendar import pick_demo_slot
from .n8n_bridge import dispatch_workflow
from .runtime import assert_outbound_allowed, safe_mode_enabled
from .senders import canonical_sender, internal_sender_addresses
from .storage.db import (
    connect,
    get_trades_demo_inquiry,
    get_trades_demo_stats,
    init_outreach_tables,
    list_trades_demo_inquiries,
    log_trades_demo_inquiry,
    recent_workflow_jobs,
    update_trades_demo_inquiry,
)
from .telegram_notifier import notify_trades_demo_approval, notify_trades_demo_result

load_dotenv(Path(__file__).parent.parent / ".env")

IMAP_HOST = "imap.gmail.com"
IMAP_PORT = 993

_SERVICE_KEYWORDS = {
    "plumber": ["plumber", "plumbing", "drain", "blocked drain", "hot water", "leak", "pipe"],
    "electrician": ["electrician", "electrical", "switchboard", "power", "lighting", "rewire"],
    "hvac": ["hvac", "air conditioning", "aircon", "heating", "cooling", "ventilation"],
    "roofer": ["roof", "roofing", "gutter", "leak in roof"],
}
_URGENT_MARKERS = ("urgent", "asap", "today", "tonight", "immediately", "straight away", "emergency")
_QUALIFYING_MARKERS = ("quote", "address", "available", "phone", "tomorrow", "this week", "inspection", "callback")


def _log_activity(title: str, detail: str, *, status: str = "info", entity_id: str = "") -> None:
    try:
        from reporting.core import log_activity_event

        log_activity_event(
            "trades_demo",
            title,
            detail,
            entity_type="trades_demo",
            entity_id=entity_id,
            status=status,
            page="operations",
        )
    except Exception:
        pass


def demo_account() -> dict[str, str | int]:
    address = os.environ.get("DEMO_EMAIL_ADDRESS", "").strip().lower()
    password = os.environ.get("DEMO_EMAIL_PASSWORD", "").replace(" ", "").strip()
    name = os.environ.get("DEMO_EMAIL_NAME", "").strip()
    if not address or not password:
        raise RuntimeError("Demo inbox is not configured. Missing DEMO_EMAIL_ADDRESS or DEMO_EMAIL_PASSWORD.")
    canonical = canonical_sender(address, name)
    return {
        "address": address,
        "password": password,
        "name": canonical["name"],
        "daily_limit": int(os.environ.get("DEMO_EMAIL_DAILY_LIMIT", "50")),
    }


def _decode_header_value(value: str) -> str:
    parts = decode_header(value or "")
    output: list[str] = []
    for part, enc in parts:
        if isinstance(part, bytes):
            output.append(part.decode(enc or "utf-8", errors="replace"))
        else:
            output.append(part)
    return "".join(output)


def _extract_body(msg: Message) -> str:
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                payload = part.get_payload(decode=True)
                if payload:
                    return payload.decode(part.get_content_charset() or "utf-8", errors="replace")
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            return payload.decode(msg.get_content_charset() or "utf-8", errors="replace")
    return ""


def _extract_email(from_raw: str) -> str:
    match = re.search(r"[\w.+-]+@[\w-]+\.[a-z.]+", from_raw or "", re.IGNORECASE)
    return match.group(0).lower() if match else (from_raw or "").strip().lower()


def _classify_inquiry(subject: str, body: str) -> dict[str, object]:
    text = f"{subject}\n{body}".lower()
    job_type = ""
    for candidate, markers in _SERVICE_KEYWORDS.items():
        if any(marker in text for marker in markers):
            job_type = candidate
            break
    urgency = "urgent" if any(marker in text for marker in _URGENT_MARKERS) else "normal"
    location = ""
    address_match = re.search(r"\b(?:street|st|road|rd|avenue|ave|drive|dr|lane|ln|close|cres|court|ct)\b", text)
    if address_match:
        location = "address_provided"
    score = 0.2
    reasons: list[str] = []
    if job_type:
        score += 0.35
        reasons.append(f"service={job_type}")
    if urgency == "urgent":
        score += 0.2
        reasons.append("urgent_language")
    if any(marker in text for marker in _QUALIFYING_MARKERS):
        score += 0.2
        reasons.append("booking_context")
    if re.search(r"\b\d{3,}\b", text):
        score += 0.1
        reasons.append("numeric_detail")
    if location:
        score += 0.15
        reasons.append(location)
    status = "qualified" if score >= 0.6 else "needs_info"
    if not reasons:
        reasons.append("insufficient_job_detail")
    return {
        "status": status,
        "job_type": job_type or "general",
        "urgency": urgency,
        "location_hint": location,
        "qualification_score": min(score, 1.0),
        "qualification_reason": ", ".join(reasons),
    }


def _response_for_inquiry(
    inquiry: dict,
    *,
    slot: dict[str, str] | None = None,
) -> tuple[str, str]:
    first_name = (inquiry.get("from_name") or "").strip().split(" ")[0] or "there"
    if slot:
        subject = f"Re: {inquiry.get('subject') or 'your enquiry'}"
        body = (
            f"Hi {first_name},\n\n"
            f"Thanks for the enquiry.\n\n"
            f"This looks like something we can help with. I've pencilled in a callback window for {slot['local_label']} "
            f"so the job details do not sit in the inbox.\n\n"
            f"If that time does not work, reply with a better window and I will adjust it.\n\n"
            f"For the callback, please have the property address and a quick photo ready if possible."
        )
        return subject, body

    subject = f"Re: {inquiry.get('subject') or 'your enquiry'}"
    body = (
        f"Hi {first_name},\n\n"
        f"Thanks for the enquiry.\n\n"
        f"I can help move this forward, but I need 3 quick details first:\n"
        f"1. What exactly needs fixing?\n"
        f"2. What is the property address or suburb?\n"
        f"3. What time window suits you best for a callback?\n\n"
        f"Once I have that, I can line up the next step."
    )
    return subject, body


def simulate_demo_inquiry(
    *,
    from_email: str,
    subject: str,
    body: str,
    from_name: str = "Demo Prospect",
    source: str = "simulation",
) -> int:
    conn = connect()
    init_outreach_tables(conn)
    inquiry_id = log_trades_demo_inquiry(
        conn,
        message_id=f"sim-{datetime.now(timezone.utc).timestamp()}-{from_email}",
        source=source,
        from_name=from_name,
        from_address=from_email,
        subject=subject,
        body=body,
        received_at=datetime.now(timezone.utc).isoformat(),
    )
    if inquiry_id is None:
        raise RuntimeError("Failed to create simulated trades demo inquiry.")
    return inquiry_id


def poll_demo_inbox(*, since_days: int = 14, limit: int = 20) -> int:
    account = demo_account()
    internal = set(internal_sender_addresses()) | {account["address"]}
    conn = connect()
    init_outreach_tables(conn)
    mail = imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT)
    added = 0
    try:
        mail.login(account["address"], str(account["password"]))
        mail.select("INBOX")
        since = (datetime.now() - timedelta(days=since_days)).strftime("%d-%b-%Y")
        status, data = mail.search(None, f'(SINCE "{since}")')
        if status != "OK":
            return 0
        ids = list(data[0].split())[-limit:]
        for num in ids:
            status, msg_data = mail.fetch(num, "(RFC822)")
            if status != "OK" or not msg_data or not msg_data[0]:
                continue
            raw = msg_data[0][1]
            msg = email.message_from_bytes(raw)
            from_raw = _decode_header_value(msg.get("From", ""))
            from_addr = _extract_email(from_raw)
            if from_addr in internal:
                continue
            subject = _decode_header_value(msg.get("Subject", ""))
            body = _extract_body(msg).strip()
            if not body:
                continue
            try:
                received_at = parsedate_to_datetime(msg.get("Date", "")).astimezone(timezone.utc).isoformat()
            except Exception:
                received_at = datetime.now(timezone.utc).isoformat()
            inquiry_id = log_trades_demo_inquiry(
                conn,
                message_id=(msg.get("Message-ID") or "").strip(),
                source="imap",
                from_name=from_raw,
                from_address=from_addr,
                subject=subject,
                body=body,
                received_at=received_at,
            )
            if inquiry_id:
                added += 1
        if added:
            _log_activity("Trades demo inbox polled", f"Added {added} new inquiry row(s).")
        return added
    finally:
        try:
            mail.logout()
        except Exception:
            pass


def process_demo_inquiry(inquiry_id: int, *, send_response: bool = True, require_approval: bool = True) -> dict[str, object]:
    conn = connect()
    init_outreach_tables(conn)
    row = get_trades_demo_inquiry(conn, inquiry_id)
    if row is None:
        raise RuntimeError(f"Trades demo inquiry {inquiry_id} not found.")

    inquiry = dict(row)
    classification = _classify_inquiry(inquiry.get("subject", ""), inquiry.get("body", ""))
    tz_name = infer_timezone(f"{inquiry.get('body', '')} {inquiry.get('subject', '')}")
    slot = None
    booking_result: dict[str, object] = {"status": "not_needed", "mode": ""}
    if classification["status"] == "qualified":
        slot = pick_demo_slot(received_at=inquiry["received_at"], timezone_hint=tz_name)

    response_subject, response_body = _response_for_inquiry(inquiry, slot=slot)
    prepared_status = "ready_for_approval" if require_approval else str(classification["status"])
    update_trades_demo_inquiry(
        conn,
        inquiry_id,
        status=prepared_status,
        approval_status="pending",
        job_type=classification["job_type"],
        urgency=classification["urgency"],
        location_hint=str(classification["location_hint"] or ""),
        qualification_score=float(classification["qualification_score"]),
        qualification_reason=str(classification["qualification_reason"]),
        response_subject=response_subject,
        response_body=response_body,
        booking_status="pending" if slot else "not_needed",
        booking_slot_start=(slot or {}).get("start"),
        booking_slot_end=(slot or {}).get("end"),
        calendar_timezone=(slot or {}).get("timezone"),
        execution_mode="awaiting_approval" if require_approval else "",
        error_note="",
    )
    if require_approval:
        notify_trades_demo_approval(
            inquiry_id,
            {
                "from_name": inquiry.get("from_name", ""),
                "from_address": inquiry.get("from_address", ""),
                "subject": inquiry.get("subject", ""),
                "qualification_status": classification["status"],
                "qualification_score": classification["qualification_score"],
                "qualification_reason": classification["qualification_reason"],
                "slot": slot or {},
            },
        )
        detail = (
            f"inquiry={inquiry_id} staged_for_approval qualification={classification['qualification_reason']} "
            f"slot={(slot or {}).get('local_label', 'n/a')}"
        )
        _log_activity("Trades demo inquiry staged", detail, status="info", entity_id=str(inquiry_id))
        return {
            "inquiry_id": inquiry_id,
            "status": "ready_for_approval",
            "classification": classification,
            "booking": {"status": "pending_approval", "mode": "approval_gate"},
            "response_sent_at": "",
        }

    return approve_demo_inquiry(inquiry_id, send_response=send_response)


def approve_demo_inquiry(inquiry_id: int, *, send_response: bool = True, approved_by: str = "operator") -> dict[str, object]:
    conn = connect()
    init_outreach_tables(conn)
    row = get_trades_demo_inquiry(conn, inquiry_id)
    if row is None:
        raise RuntimeError(f"Trades demo inquiry {inquiry_id} not found.")

    inquiry = dict(row)
    slot = None
    if inquiry.get("booking_slot_start") and inquiry.get("booking_slot_end"):
        slot = {
            "start": inquiry["booking_slot_start"],
            "end": inquiry["booking_slot_end"],
            "timezone": inquiry.get("calendar_timezone") or "UTC",
            "local_label": inquiry.get("calendar_timezone") or "UTC",
        }
    booking_result: dict[str, object] = {"status": "not_needed", "mode": ""}
    if inquiry.get("booking_status") == "pending" and slot:
        booking_payload = {
            "summary": f"Trades demo callback: {inquiry['from_address']}",
            "description": inquiry["body"][:1500],
            "start_iso": slot["start"],
            "end_iso": slot["end"],
            "timezone": slot["timezone"],
            "attendee_email": inquiry["from_address"],
            "source": inquiry.get("source", ""),
        }
        booking_result = dispatch_workflow(
            conn,
            "trades_demo_booking",
            booking_payload,
            entity_type="trades_demo_inquiry",
            entity_id=str(inquiry_id),
        )

    sent_at = ""
    if send_response:
        if safe_mode_enabled():
            raise RuntimeError("Trades demo response send blocked by safe mode.")
        assert_outbound_allowed("trades_demo_send_response")
        account = demo_account()
        final_body = render_outreach_body(str(inquiry.get("response_body") or ""), account, "en")
        send_email(inquiry["from_address"], str(inquiry.get("response_subject") or ""), final_body, account)
        sent_at = datetime.now(timezone.utc).isoformat()

    fallback_status = "qualified" if inquiry.get("booking_status") == "pending" else "needs_info"
    status = "booked" if booking_result.get("status") == "completed" else fallback_status
    update_trades_demo_inquiry(
        conn,
        inquiry_id,
        status=status,
        approval_status="approved",
        approved_at=datetime.now(timezone.utc).isoformat(),
        approved_by=approved_by,
        response_sent_at=sent_at or None,
        booking_status=str(booking_result.get("status", "")),
        calendar_event_id=str(booking_result.get("id", "") or booking_result.get("external_ref", "")),
        execution_mode=str(booking_result.get("mode", "")),
        last_job_id=booking_result.get("job_id"),
        error_note=str(booking_result.get("error", "") or ""),
    )
    detail = (
        f"inquiry={inquiry_id} status={status} approval=approved "
        f"booking={booking_result.get('status', 'n/a')} mode={booking_result.get('mode', '')}"
    )
    _log_activity("Trades demo inquiry processed", detail, status="ok" if status != "failed" else "warn", entity_id=str(inquiry_id))
    notify_trades_demo_result(
        inquiry_id,
        {
            "status": status,
            "booking_status": str(booking_result.get("status", "")),
            "execution_mode": str(booking_result.get("mode", "")),
            "calendar_event_id": str(booking_result.get("id", "") or booking_result.get("external_ref", "")),
            "error_note": str(booking_result.get("error", "") or ""),
        },
    )
    return {
        "inquiry_id": inquiry_id,
        "status": status,
        "classification": {
            "status": inquiry.get("status"),
            "job_type": inquiry.get("job_type"),
            "urgency": inquiry.get("urgency"),
            "qualification_reason": inquiry.get("qualification_reason"),
            "qualification_score": inquiry.get("qualification_score"),
        },
        "booking": booking_result,
        "response_sent_at": sent_at,
    }


def reject_demo_inquiry(inquiry_id: int, *, rejected_by: str = "operator", reason: str = "") -> dict[str, object]:
    conn = connect()
    init_outreach_tables(conn)
    row = get_trades_demo_inquiry(conn, inquiry_id)
    if row is None:
        raise RuntimeError(f"Trades demo inquiry {inquiry_id} not found.")
    update_trades_demo_inquiry(
        conn,
        inquiry_id,
        status="rejected",
        approval_status="rejected",
        rejected_at=datetime.now(timezone.utc).isoformat(),
        approved_by=rejected_by,
        error_note=reason or "Rejected by operator.",
        execution_mode="approval_gate",
    )
    _log_activity("Trades demo inquiry rejected", f"inquiry={inquiry_id} by={rejected_by} reason={reason or 'n/a'}", status="warn", entity_id=str(inquiry_id))
    return {"inquiry_id": inquiry_id, "status": "rejected"}


def approve_all_demo_inquiries(*, limit: int = 20, send_response: bool = True, approved_by: str = "operator") -> dict[str, object]:
    conn = connect()
    init_outreach_tables(conn)
    rows = conn.execute(
        """
        SELECT id
        FROM trades_demo_inquiries
        WHERE approval_status='pending'
          AND status='ready_for_approval'
        ORDER BY received_at ASC, id ASC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    processed: list[int] = []
    failed: list[str] = []
    for row in rows:
        try:
            approve_demo_inquiry(int(row["id"]), send_response=send_response, approved_by=approved_by)
            processed.append(int(row["id"]))
        except Exception as exc:
            failed.append(f"{row['id']}: {exc}")
    return {"approved": processed, "failed": failed}


def run_trades_demo_cycle(*, limit: int = 10, since_days: int = 14, send_response: bool = True, require_approval: bool = True) -> dict[str, int]:
    added = poll_demo_inbox(since_days=since_days, limit=limit)
    conn = connect()
    init_outreach_tables(conn)
    pending = [dict(row) for row in list_trades_demo_inquiries(conn, status="new", limit=limit)]
    processed = 0
    failed = 0
    for row in pending:
        try:
            process_demo_inquiry(int(row["id"]), send_response=send_response, require_approval=require_approval)
            processed += 1
        except Exception as exc:
            update_trades_demo_inquiry(conn, int(row["id"]), status="failed", error_note=str(exc))
            _log_activity("Trades demo inquiry failed", f"inquiry={row['id']} error={exc}", status="warn", entity_id=str(row["id"]))
            failed += 1
    return {"added": added, "processed": processed, "failed": failed}


def trades_demo_status(*, limit: int = 10) -> dict[str, object]:
    conn = connect()
    init_outreach_tables(conn)
    stats = get_trades_demo_stats(conn)
    inquiries = [dict(row) for row in list_trades_demo_inquiries(conn, limit=limit)]
    jobs = [dict(row) for row in recent_workflow_jobs(conn, limit=limit)]
    return {"stats": stats, "inquiries": inquiries, "jobs": jobs}
