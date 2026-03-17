"""
outreach/email_sender.py
Gmail SMTP sender with account rotation and daily limit tracking.
Plain text only — no HTML, no tracking pixels.
"""
import os
import smtplib
import sqlite3
from datetime import date
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587


def _load_accounts() -> list[dict]:
    """Load all configured email accounts from env."""
    accounts = []
    i = 1
    while True:
        addr = os.environ.get(f"EMAIL_{i}_ADDRESS")
        if not addr:
            break
        # Google displays app passwords with spaces (e.g. "abcd efgh ijkl mnop")
        # Strip all spaces — both formats work but dotenv may preserve them
        raw_pw = os.environ.get(f"EMAIL_{i}_PASSWORD", "")
        accounts.append({
            "index": i,
            "address": addr.strip(),
            "password": raw_pw.replace(" ", ""),
            "name": os.environ.get(f"EMAIL_{i}_NAME", addr.split("@")[0]),
            "daily_limit": int(os.environ.get(f"EMAIL_{i}_DAILY_LIMIT", "30")),
        })
        i += 1
    return accounts


def _sent_today(conn: sqlite3.Connection, address: str) -> int:
    today = date.today().isoformat()
    row = conn.execute(
        """
        SELECT COUNT(*) AS cnt FROM outreach_log
        WHERE channel='email' AND address=? AND status='sent'
          AND DATE(sent_at) = ?
        """,
        (address, today),
    ).fetchone()
    return row["cnt"] if row else 0


def pick_account(conn: sqlite3.Connection) -> Optional[dict]:
    """Return the first account that hasn't hit its daily limit."""
    for acc in _load_accounts():
        if _sent_today(conn, acc["address"]) < acc["daily_limit"]:
            return acc
    return None


def send_email(to_address: str, subject: str, body: str,
               account: dict) -> None:
    """Send a plain-text email via Gmail SMTP."""
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = f"{account['name']} <{account['address']}>"
    msg["To"] = to_address
    msg.attach(MIMEText(body, "plain", "utf-8"))

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.ehlo()
        server.starttls()
        server.login(account["address"], account["password"])
        server.sendmail(account["address"], to_address, msg.as_string())
