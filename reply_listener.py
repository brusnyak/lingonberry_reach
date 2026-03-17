"""
outreach/reply_listener.py
Polls Gmail IMAP for replies to outreach emails.
Matches replies to leads via outreach_log, logs to replies table.
"""
import email
import imaplib
import os
import sqlite3
from datetime import datetime, timezone
from email.header import decode_header

from storage.db import connect, log_reply

IMAP_HOST = "imap.gmail.com"
IMAP_PORT = 993


def _decode_header_value(value: str) -> str:
    parts = decode_header(value or "")
    result = []
    for part, enc in parts:
        if isinstance(part, bytes):
            result.append(part.decode(enc or "utf-8", errors="replace"))
        else:
            result.append(part)
    return "".join(result)


def _get_body(msg: email.message.Message) -> str:
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
    total = 0

    for acc in _load_accounts():
        try:
            mail = imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT)
            mail.login(acc["address"], acc["password"])
            mail.select("INBOX")

            # Search unseen messages from last N days
            from datetime import timedelta
            since = (datetime.now() - timedelta(days=since_days)).strftime("%d-%b-%Y")
            _, data = mail.search(None, f'(UNSEEN SINCE "{since}")')

            for num in data[0].split():
                _, msg_data = mail.fetch(num, "(RFC822)")
                raw = msg_data[0][1]
                msg = email.message_from_bytes(raw)

                from_raw = _decode_header_value(msg.get("From", ""))
                subject = _decode_header_value(msg.get("Subject", ""))
                date_str = msg.get("Date", "")
                body = _get_body(msg)

                # extract email address from From header
                import re
                match = re.search(r"[\w.+-]+@[\w-]+\.[a-z]+", from_raw)
                from_addr = match.group(0) if match else from_raw

                lead_id, outreach_id = _match_lead(conn, from_addr, subject)
                if lead_id is None:
                    continue  # not one of our leads

                try:
                    received_at = datetime.strptime(
                        date_str[:31].strip(), "%a, %d %b %Y %H:%M:%S %z"
                    ).isoformat()
                except Exception:
                    received_at = datetime.now(timezone.utc).isoformat()

                log_reply(conn, lead_id, "email", body,
                          received_at, outreach_id, raw=raw.decode("utf-8", errors="replace"))
                total += 1

            mail.logout()
        except Exception as e:
            print(f"[reply_listener] Error polling {acc['address']}: {e}")

    return total
