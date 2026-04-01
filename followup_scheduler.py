"""
outreach/followup_scheduler.py

Automatically schedules follow-up emails for leads that have been contacted
but haven't replied. Follow-ups are created as review batches for approval
before being scheduled.

Touch sequence:
  Touch 1: initial outreach (already sent)
  Touch 2: +2 days after Touch 1 sent
  Touch 3: +3 days after Touch 2 sent
  Touch 4: +4 days after Touch 3 sent
  Touch 5: +5 days after Touch 4 sent
  Touch 6-10: +30 days after previous touch (monthly nurture)

Maximum: 10 touches total, then stop to protect domain reputation.

Business hours: Follow-ups are scheduled to send between 9:00-17:00 in the
recipient's local time zone (based on address country inference or default UTC).
"""

import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

from .generator import generate_followup
from .storage.db import connect, init_outreach_tables, get_qualified_leads, get_due_scheduled_drafts
from .email_sender import _load_accounts

LEADS_DB = Path(__file__).parent.parent / "leadgen" / "data" / "leads.db"


def _infer_timezone(lead: dict) -> str:
    """
    Infer timezone from lead address or website.
    Returns IANA timezone name (e.g., 'Europe/Bratislava').
    Default: 'UTC' if cannot determine.
    """
    address = (lead.get("address") or "").lower()
    website = (lead.get("website") or "").lower()

    # Simple country-based mapping (expand as needed)
    if any(city in address for city in ["bratislava", "košice", "žilina", "prešov", "slovakia"]):
        return "Europe/Bratislava"
    if any(city in address for city in ["prague", "praha", "české budějovice", "czech", "česko"]):
        return "Europe/Prague"
    if any(city in address for city in ["vienna", "wien", "austria"]):
        return "Europe/Vienna"
    if any(city in address for city in ["berlin", "hamburg", "munich", "germany", "deutschland"]):
        return "Europe/Berlin"
    if ".au" in website or "australia" in address:
        return "Australia/Sydney"
    if ".uk" in website or "uk" in address or "united kingdom" in address:
        return "Europe/London"
    if ".us" in website or "usa" in address or "united states" in address:
        return "America/New_York"

    # Default fallback
    return "UTC"


def _next_business_hour(dt: datetime, timezone_str: str) -> datetime:
    """
    Adjust datetime to next business hour (9:00-17:00) in given timezone.
    If dt is outside business hours, move to 9:00 next business day.
    """
    try:
        tz = ZoneInfo(timezone_str)
        dt_local = dt.astimezone(tz)

        # Define business hours
        business_start = 9
        business_end = 17

        # If dt is before 9:00, set to 9:00 same day
        if dt_local.hour < business_start:
            dt_local = dt_local.replace(hour=business_start, minute=0, second=0, microsecond=0)
        # If dt is after 17:00 or on weekend, move to 9:00 next business day
        elif dt_local.hour >= business_end or dt_local.weekday() >= 5:
            # Add days until weekday
            days_ahead = 1
            while (dt_local + timedelta(days=days_ahead)).weekday() >= 5:
                days_ahead += 1
            dt_local = (dt_local + timedelta(days=days_ahead)).replace(hour=business_start, minute=0, second=0, microsecond=0)
        # Otherwise keep within business hours (already in range)
        else:
            # Ensure within business hours, cap at 17:00
            if dt_local.hour >= business_end:
                dt_local = dt_local.replace(hour=business_end, minute=0, second=0, microsecond=0)

        return dt_local.astimezone(timezone.utc)
    except Exception:
        # Timezone error or other issue, return as-is
        return dt


def _calculate_next_send_time(last_sent_at: str, touch_number: int) -> datetime:
    """
    Calculate the next send time based on touch number.
    Returns UTC datetime.
    """
    last_dt = datetime.fromisoformat(last_sent_at.rstrip("Z"))
    now = datetime.now(timezone.utc)

    if touch_number == 1:
        delay_days = 2
    elif touch_number == 2:
        delay_days = 3
    elif touch_number == 3:
        delay_days = 4
    elif touch_number == 4:
        delay_days = 5
    else:  # touch_number >= 5
        delay_days = 30  # monthly nurture

    next_dt = last_dt + timedelta(days=delay_days)

    # Don't schedule in the past
    if next_dt < now:
        next_dt = now + timedelta(minutes=30)  # default 30 min from now

    return next_dt


def _create_review_batch_for_followups(conn: sqlite3.Connection, drafts: List[dict]) -> int:
    """
    Create a review batch containing multiple follow-up drafts.
    Returns batch ID.
    """
    if not drafts:
        return 0

    # Use first draft as representative for batch metadata
    first = drafts[0]
    batch_key = f"followup_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

    # Insert batch record
    now = datetime.now(timezone.utc).isoformat()
    cur = conn.execute(
        """
        INSERT INTO review_batches
            (batch_key, recipient, sender_name, sender_address, subject, body, draft_count, status, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?)
        """,
        (
            batch_key,
            "followup_scheduler",  # recipient indicates auto-generated
            first.get("sender_name", ""),
            first.get("sender_address", ""),
            first.get("subject", ""),
            first.get("body", ""),
            len(drafts),
            now,
        ),
    )

    # Update all drafts to be in review
    draft_ids = [d["id"] for d in drafts]
    conn.executemany(
        """
        UPDATE outreach_log
        SET approval_state = 'in_review',
            review_batch_key = ?,
            error_note = NULL
        WHERE id = ?
        """,
        [(batch_key, did) for did in draft_ids],
    )

    conn.commit()
    return cur.lastrowid


def schedule_followups(limit: int = 50) -> dict:
    """
    Main entry point: Scan for leads that need follow-ups and create review batches.

    Args:
        limit: Maximum number of leads to process in one run

    Returns:
        dict with counts: leads_processed, drafts_created, batches_created
    """
    conn = connect()
    init_outreach_tables(conn)

    # Find leads that had Touch 1-4 sent but no subsequent touch yet
    # Also include Touch 5+ that are due for monthly follow-up (up to max 10)
    query = """
        SELECT o.*, b.name, b.website, w.language, b.outreach_angle, b.top_gap, b.target_niche,
               b.address, b.contact_name
        FROM outreach_log o
        JOIN businesses b ON b.id = o.lead_id
        LEFT JOIN website_data w ON w.business_id = b.id
        LEFT JOIN replies r ON r.lead_id = b.id
        WHERE o.status = 'sent'
          AND o.touch_number IS NOT NULL
          AND r.id IS NULL  -- no replies yet
          AND NOT EXISTS (
              SELECT 1 FROM outreach_log o2
              WHERE o2.lead_id = o.lead_id
                AND o2.touch_number = o.touch_number + 1
          )
          AND o.touch_number < 10  -- cap at touch 10
        ORDER BY o.sent_at ASC
        LIMIT ?
    """

    rows = conn.execute(query, (limit,)).fetchall()
    leads_processed = len(rows)
    drafts_created = 0
    batches_created = 0
    batch_drafts = []  # accumulate drafts for batch creation

    for row in rows:
        lead = dict(row)
        # Skip if no address (email) to send to
        if not lead.get("address"):
            continue
        current_touch = lead["touch_number"]
        next_touch = current_touch + 1

        # Check if this follow-up is due (based on send_after or sent_at)
        last_sent_at = lead.get("sent_at") or lead.get("created_at")
        if not last_sent_at:
            continue

        next_send_dt = _calculate_next_send_time(last_sent_at, current_touch)

        # If next_send_dt is in the future, we'll schedule it
        # For now, we create drafts with send_after set appropriately
        # They'll be picked up by the existing send queue processor

        # Generate follow-up email
        try:
            # Build lead dict with required fields for generate_followup
            followup_lead = {
                "id": lead["lead_id"],
                "name": lead["name"],
                "website": lead["website"],
                "language": lead.get("language", "en"),
                "outreach_angle": lead.get("outreach_angle", ""),
                "top_gap": lead.get("top_gap", ""),
                "target_niche": lead.get("target_niche", ""),
                "contact_name": lead.get("contact_name", ""),
                "brand_summary": "",  # not needed for follow-up
            }
            draft = generate_followup(followup_lead, touch=next_touch)

            # Determine send_after with business hours adjustment
            tz = _infer_timezone(lead)
            send_after_dt = _next_business_hour(next_send_dt, tz)
            send_after = send_after_dt.isoformat()

            # Insert draft with status 'scheduled'
            now = datetime.now(timezone.utc).isoformat()
            cur = conn.execute(
                """
                INSERT INTO outreach_log
                    (lead_id, channel, address, sender_name, sender_address,
                     subject, message, status, touch_number, send_after, created_at,
                     last_subject, message_variant_fingerprint)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'scheduled', ?, ?, ?, ?, ?)
                """,
                (
                    lead["lead_id"],
                    "email",
                    lead["address"],  # from original outreach
                    lead.get("sender_name"),
                    lead.get("sender_address"),
                    draft["subject"],
                    draft["body"],
                    next_touch,
                    send_after,
                    now,
                    lead.get("subject"),  # last_subject for Re: threading
                    draft["fingerprint"],
                ),
            )
            drafts_created += 1
            batch_drafts.append({
                "id": cur.lastrowid,
                "sender_name": lead.get("sender_name"),
                "sender_address": lead.get("sender_address"),
                "subject": draft["subject"],
                "body": draft["body"],
            })

            # Create batch every 10 drafts or at end
            if len(batch_drafts) >= 10:
                _create_review_batch_for_followups(conn, batch_drafts)
                batches_created += 1
                batch_drafts = []

        except Exception as e:
            # Log error but continue
            print(f"Error scheduling follow-up for lead {lead['lead_id']}: {e}")
            continue

    # Create final batch if any remain
    if batch_drafts:
        _create_review_batch_for_followups(conn, batch_drafts)
        batches_created += 1

    conn.commit()
    conn.close()

    return {
        "leads_processed": leads_processed,
        "drafts_created": drafts_created,
        "batches_created": batches_created,
    }


def get_followup_candidates(conn: sqlite3.Connection, limit: int = 50) -> List[sqlite3.Row]:
    """
    Get leads that are candidates for follow-up scheduling.
    Used for manual review or debugging.
    """
    query = """
        SELECT o.*, b.name, b.email_maps, b.phone, b.address,
               w.language, b.outreach_angle, b.top_gap
        FROM outreach_log o
        JOIN businesses b ON b.id = o.lead_id
        LEFT JOIN website_data w ON w.business_id = b.id
        LEFT JOIN replies r ON r.lead_id = b.id
        WHERE o.status = 'sent'
          AND o.touch_number IS NOT NULL
          AND r.id IS NULL
          AND NOT EXISTS (
              SELECT 1 FROM outreach_log o2
              WHERE o2.lead_id = o.lead_id
                AND o2.touch_number = o.touch_number + 1
          )
          AND o.touch_number < 10
        ORDER BY o.sent_at ASC
        LIMIT ?
    """
    return conn.execute(query, (limit,)).fetchall()


if __name__ == "__main__":
    # Run as standalone script: python -m outreach.followup_scheduler
    import sys
    result = schedule_followups(limit=100)
    print(f"Follow-up scheduler: {result['drafts_created']} drafts created in {result['batches_created']} batches")
    sys.exit(0)
