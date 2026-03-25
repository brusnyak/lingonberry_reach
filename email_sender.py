"""
outreach/email_sender.py
Gmail SMTP sender with account rotation and daily limit tracking.
Plain text only — no HTML, no tracking pixels.
"""
import os
import random
import smtplib
import sqlite3
from datetime import date, datetime, time, timedelta, timezone
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

# Ensure outreach/ is on sys.path so bare imports work regardless of cwd
import sys as _sys
_sys.path.insert(0, str(Path(__file__).parent))
from senders import env_sender_name

load_dotenv(Path(__file__).parent.parent / ".env")

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587

# Send windows by detected timezone (UTC times)
# Tradesmen: morning before first job, maybe midday, evening after work
_WINDOWS_BY_TZ: dict[str, list[tuple[time, time]]] = {
    "AU":  [(time(20, 0), time(22, 0)), (time(8, 0), time(11, 0))],   # AEST UTC+10: 6-8am + 6-9pm local
    "EU":  [(time(7, 0),  time(9, 0)),  (time(12, 0), time(15, 0))],  # CET UTC+1: 8-10am + 1-4pm local
    "default": [(time(8, 0), time(10, 0)), (time(17, 0), time(19, 0))],  # safe global fallback
}

_AU_INDICATORS = {
    "nsw", "vic", "qld", "wa", "sa", "nt", "act", "tas",
    "australia", "sydney", "melbourne", "brisbane", "perth",
    "adelaide", "canberra", "darwin", "hobart",
}
_EU_INDICATORS = {
    ".sk", ".cz", ".at", ".de", "bratislava", "praha", "brno",
    "wien", "vienna", "berlin", "münchen", "munich", "prague",
    "slovakia", "czech", "austria", "germany",
}


def _detect_tz_key(address: str) -> str:
    """Infer timezone bucket from lead address string."""
    addr = (address or "").lower()
    for token in _AU_INDICATORS:
        if token in addr:
            return "AU"
    for token in _EU_INDICATORS:
        if token in addr:
            return "EU"
    return "default"


COOLDOWN_MIN_MINUTES = 18
COOLDOWN_MAX_MINUTES = 43
def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


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
        address = addr.strip()
        accounts.append({
            "index": i,
            "address": address,
            "password": raw_pw.replace(" ", ""),
            "name": env_sender_name(i, address),
            "daily_limit": int(os.environ.get(f"EMAIL_{i}_DAILY_LIMIT", "30")),
        })
        i += 1
    return accounts


def _sent_today(conn: sqlite3.Connection, address: str) -> int:
    today = _utc_now().date().isoformat()
    row = conn.execute(
        """
        SELECT COUNT(*) AS cnt FROM outreach_log
        WHERE channel='email' AND sender_address=? AND status='sent'
          AND DATE(sent_at) = ?
        """,
        (address, today),
    ).fetchone()
    return row["cnt"] if row else 0


def _last_sent_at(conn: sqlite3.Connection, address: str) -> str:
    row = conn.execute(
        """
        SELECT COALESCE(MAX(COALESCE(sent_at, send_after)), '') AS last_sent
        FROM outreach_log
        WHERE channel='email' AND sender_address=? AND status IN ('sent', 'scheduled')
        """,
        (address,),
    ).fetchone()
    return row["last_sent"] if row else ""


def pick_account(conn: sqlite3.Connection) -> Optional[dict]:
    """Return the least-used eligible account for even warmup-aware distribution."""
    eligible = []
    for acc in _load_accounts():
        sent_today = _sent_today(conn, acc["address"])
        if sent_today >= acc["daily_limit"]:
            continue
        eligible.append((sent_today, _last_sent_at(conn, acc["address"]), acc["index"], acc))
    if not eligible:
        return None
    eligible.sort(key=lambda item: (item[0], item[1], item[2]))
    return eligible[0][3]


def _within_windows(now: datetime, windows: list[tuple[time, time]]) -> bool:
    if now.weekday() >= 5:
        return False
    current = now.time()
    return any(start <= current <= end for start, end in windows)


def _next_window_start(now: datetime, windows: list[tuple[time, time]]) -> datetime:
    candidate = now
    for _ in range(14):
        if candidate.weekday() < 5:
            for start, _end in windows:
                dt = datetime.combine(candidate.date(), start, tzinfo=timezone.utc)
                if dt >= now:
                    return dt
        candidate = datetime.combine(candidate.date() + timedelta(days=1), time(0, 0), tzinfo=timezone.utc)
    return datetime.combine((now + timedelta(days=1)).date(), windows[0][0], tzinfo=timezone.utc)


def _clip_to_window(ts: datetime, windows: list[tuple[time, time]]) -> datetime:
    if ts.weekday() >= 5:
        return _next_window_start(ts, windows)
    for start, end in windows:
        start_dt = datetime.combine(ts.date(), start, tzinfo=timezone.utc)
        end_dt = datetime.combine(ts.date(), end, tzinfo=timezone.utc)
        if start_dt <= ts <= end_dt:
            return ts
        if ts < start_dt:
            return start_dt
    return _next_window_start(datetime.combine(ts.date() + timedelta(days=1), time(0, 0), tzinfo=timezone.utc), windows)


def next_send_after(conn: sqlite3.Connection, address: str, jitter_seed: int | None = None,
                    now: datetime | None = None, lead_address: str = "") -> datetime:
    now = now or _utc_now()
    windows = _WINDOWS_BY_TZ[_detect_tz_key(lead_address)]
    last = _last_sent_at(conn, address)
    baseline = now
    if last:
        try:
            baseline = max(baseline, datetime.fromisoformat(last))
        except ValueError:
            pass
    rng = random.Random(jitter_seed or int(now.timestamp()))
    cooldown = timedelta(minutes=rng.randint(COOLDOWN_MIN_MINUTES, COOLDOWN_MAX_MINUTES))
    candidate = _clip_to_window(baseline if _within_windows(baseline, windows) else _next_window_start(baseline, windows), windows)
    if candidate == baseline and not _within_windows(candidate, windows):
        candidate = _next_window_start(candidate, windows)
    if last:
        candidate = max(candidate, baseline + cooldown)
    candidate = _clip_to_window(candidate, windows)
    if candidate < now:
        candidate = _clip_to_window(now + timedelta(minutes=rng.randint(3, 12)), windows)
    return candidate


def send_email(to_address: str, subject: str, body: str,
               account: dict) -> None:
    """Send a plain-text email via Gmail SMTP."""
    normalized_body = (body or "").replace("\\r\\n", "\n").replace("\\n", "\n")
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = f"{account['name']} <{account['address']}>"
    msg["To"] = to_address
    msg.attach(MIMEText(normalized_body, "plain", "utf-8"))

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.ehlo()
        server.starttls()
        server.login(account["address"], account["password"])
        server.sendmail(account["address"], to_address, msg.as_string())
