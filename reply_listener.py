"""
outreach/reply_listener.py
Polls Gmail IMAP for replies to outreach emails.
Matches replies to leads via outreach_log, logs to replies table.
"""
import email
import importlib.util
import imaplib
import os
import sqlite3
from datetime import datetime, timezone
from email.header import decode_header
from email.message import Message
from email.utils import parsedate_to_datetime
from pathlib import Path

from dotenv import load_dotenv

_DB_PATH = Path(__file__).parent / "storage" / "db.py"
_DB_SPEC = importlib.util.spec_from_file_location("outreach_storage_db_reply_listener", _DB_PATH)
if _DB_SPEC is None or _DB_SPEC.loader is None:
    raise ImportError(f"Unable to load outreach storage module from {_DB_PATH}")
_DB_MODULE = importlib.util.module_from_spec(_DB_SPEC)
_DB_SPEC.loader.exec_module(_DB_MODULE)

connect = _DB_MODULE.connect
init_outreach_tables = _DB_MODULE.init_outreach_tables
log_reply = _DB_MODULE.log_reply

load_dotenv(Path(__file__).parent.parent / ".env")

IMAP_HOST = "imap.gmail.com"
IMAP_PORT = 993
_AUTH_BACKOFF: dict[str, float] = {}
AUTH_BACKOFF_SECONDS = 3600


def _decode_header_value(value: str) -> str:
    parts = decode_header(value or "")
    result = []
    for part, enc in parts:
        if isinstance(part, bytes):
            result.append(part.decode(enc or "utf-8", errors="replace"))
        else:
            result.append(part)
    return "".join(result)


def _get_body(msg: Message) -> str:
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                payload = part.get_payload(decode=True)
                return payload.decode(part.get_content_charset() or "utf-8", errors="replace")
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            return payload.decode(msg.get_content_charset() or "utf-8", errors="replace")
    return ""


def _load_accounts() -> list[dict]:
    accounts = []
    i = 1
    while True:
        addr = os.environ.get(f"EMAIL_{i}_ADDRESS")
        if not addr:
            break
        accounts.append({
            "address": addr,
            "password": os.environ.get(f"EMAIL_{i}_PASSWORD", ""),
        })
        i += 1
    return accounts


def _match_lead(conn: sqlite3.Connection, from_addr: str,
                subject: str) -> tuple[int | None, int | None]:
    """Try to match a reply to a lead_id and outreach_id."""
    # Match by recipient address in outreach_log
    row = conn.execute(
        """
        SELECT o.id, o.lead_id FROM outreach_log o
        JOIN businesses b ON b.id = o.lead_id
        WHERE o.status = 'sent' AND o.channel = 'email'
          AND (
            -- reply from the email we sent to
            LOWER(o.address) = LOWER(?)
            OR
            -- or business name appears in subject
            LOWER(?) LIKE '%' || LOWER(b.name) || '%'
          )
        ORDER BY o.sent_at DESC LIMIT 1
        """,
        (from_addr, subject),
    ).fetchone()
    if row:
        return row["lead_id"], row["id"]
    return None, None


def poll_replies(since_days: int = 7) -> int:
    """Poll all configured Gmail accounts for new replies. Returns count logged."""
    conn = connect()
    init_outreach_tables(conn)
    total = 0

    for acc in _load_accounts():
        now_ts = datetime.now(timezone.utc).timestamp()
        if _AUTH_BACKOFF.get(acc["address"], 0) > now_ts:
            continue
        mail = None
        try:
            mail = imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT)
            mail.login(acc["address"], acc["password"])
            mail.select("INBOX")

            # Search all recent messages and rely on Message-ID dedupe.
            # Gmail/read state is not a safe source of truth for reply monitoring.
            from datetime import timedelta
            since = (datetime.now() - timedelta(days=since_days)).strftime("%d-%b-%Y")
            _, data = mail.search(None, f'(SINCE "{since}")')

            for num in data[0].split():
                _, msg_data = mail.fetch(num, "(RFC822)")
                raw = msg_data[0][1]
                msg = email.message_from_bytes(raw)

                from_raw = _decode_header_value(msg.get("From", ""))
                subject = _decode_header_value(msg.get("Subject", ""))
                date_str = msg.get("Date", "")
                message_id = (msg.get("Message-ID") or "").strip()
                body = _get_body(msg)

                # extract email address from From header
                import re
                match = re.search(r"[\w.+-]+@[\w-]+\.[a-z]+", from_raw)
                from_addr = match.group(0) if match else from_raw

                lead_id, outreach_id = _match_lead(conn, from_addr, subject)
                if lead_id is None:
                    continue  # not one of our leads

                try:
                    received_at = parsedate_to_datetime(date_str).astimezone(timezone.utc).isoformat()
                except Exception:
                    received_at = datetime.now(timezone.utc).isoformat()

                reply_id = log_reply(
                    conn,
                    lead_id,
                    "email",
                    body,
                    received_at,
                    outreach_id,
                    raw=raw.decode("utf-8", errors="replace"),
                    message_id=message_id,
                    from_name=from_raw,
                    from_address=from_addr,
                    subject=subject,
                )
                if reply_id is not None:
                    total += 1

            mail.logout()
        except Exception as e:
            if "AUTHENTICATIONFAILED" in str(e).upper() or "INVALID CREDENTIALS" in str(e).upper():
                _AUTH_BACKOFF[acc["address"]] = now_ts + AUTH_BACKOFF_SECONDS
            print(f"[reply_listener] Error polling {acc['address']}: {e}")
            try:
                if mail is not None:
                    mail.logout()
            except Exception:
                try:
                    if mail is not None:
                        mail.shutdown()
                except Exception:
                    pass

    return total
