"""
outreach/email_sender.py
Gmail SMTP sender with account rotation and daily limit tracking.
Plain text only — no HTML, no tracking pixels.
"""
import os
import random
import re
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

import pytz
from geopy.geocoders import Nominatim
from timezonefinder import TimezoneFinder

_tf = TimezoneFinder()
_geolocator = Nominatim(user_agent="biz_system_outreach")
_TZ_CACHE: dict[str, str] = {}

LOCAL_WINDOWS = [
    (time(6, 30), time(8, 45)),   # Local morning (before job/early day)
    (time(15, 30), time(18, 15))  # Local afternoon (wrapping up)
]

def infer_timezone(address: str) -> str:
    addr = (address or "").strip()
    if not addr:
        return "UTC"
    if addr in _TZ_CACHE:
        return _TZ_CACHE[addr]
    
    try:
        addr_lower = addr.lower()
        if any(token in addr_lower for token in ["sydney", "melbourne", "brisbane", "nsw", "vic", "qld", "australia", "perth", "adelaide"]):
            _TZ_CACHE[addr] = "Australia/Sydney"
            return "Australia/Sydney"
        if any(token in addr_lower for token in ["bratislava", "praha", "brno", "wien", "vienna", ".sk", ".cz", ".at", "munich", "berlin"]):
            _TZ_CACHE[addr] = "Europe/Bratislava"
            return "Europe/Bratislava"

        location = _geolocator.geocode(addr)
        if location:
            tz_str = _tf.timezone_at(lat=location.latitude, lng=location.longitude)
            if tz_str:
                _TZ_CACHE[addr] = tz_str
                return tz_str
    except Exception:
        pass
    
    _TZ_CACHE[addr] = "UTC"
    return "UTC"


COOLDOWN_MIN_MINUTES = 18
COOLDOWN_MAX_MINUTES = 43
def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


_SIGNOFF_PATTERNS = (
    "cheers",
    "thanks",
    "let me know",
    "curious either way",
    "dajte vedieť",
    "vďaka",
    "budem rád za odpoveď",
    "ďakujem",
    "dejte vědět",
    "díky",
    "budu rád za odpověď",
    "děkuji",
    "geben sie gern kurz bescheid",
    "geben sie gern bescheid",
    "danke",
    "ich bin gespannt auf ihre rückmeldung",
    "viele grüße",
)


def signature_block(account: dict, language: str = "en") -> str:
    name = (account.get("name") or "").strip()
    signer = name.split()[0] if name else "Team"
    lang = (language or "en").lower()
    if lang.startswith("sk"):
        options = ["Dajte vedieť", "Vďaka", "Budem rád za odpoveď", "Ďakujem"]
    elif lang.startswith("cs"):
        options = ["Dejte vědět", "Díky", "Budu rád za odpověď", "Děkuji"]
    elif lang.startswith("de"):
        options = ["Geben Sie gern kurz Bescheid", "Danke", "Ich bin gespannt auf Ihre Rückmeldung", "Viele Grüße"]
    else:
        options = ["Cheers", "Thanks", "Let me know", "Curious either way"]
    closing = options[sum(ord(ch) for ch in ((account.get("address") or "") + lang)) % len(options)]
    return f"{closing},\n{signer}"


def strip_known_signature(body: str, signer_name: str = "") -> str:
    text = (body or "").replace("\r\n", "\n").rstrip()
    if not text:
        return ""
    signer = (signer_name or "").strip().split()[0]
    if signer:
        signer_pattern = re.escape(signer)
    else:
        signer_pattern = r"(?:Team|Victor|Yegor|Max)"
    closers = "|".join(re.escape(item) for item in _SIGNOFF_PATTERNS)
    pattern = re.compile(
        rf"(?:\n\s*){{1,2}}(?:{closers}),?\s*\n\s*{signer_pattern}\s*$",
        re.IGNORECASE,
    )
    return re.sub(pattern, "", text).rstrip()


def render_outreach_body(body: str, account: dict, language: str = "en") -> str:
    """Render final email body with signature block matching body language."""
    clean = strip_known_signature(body, account.get("name", ""))
    
    # Detect body language from content to ensure sign-off matches
    # If body contains primarily English text but language param is non-English,
    # force English sign-off to avoid mismatches
    detected_lang = language
    body_lower = (body or "").lower()
    
    # Check if body is actually English (common English words present)
    english_markers = [" i ", " you ", " your ", " quick ", " help ", " build ", " think ", " could ", " would ", " interested"]
    foreign_markers = {
        "sk": [" chcel ", " myslím ", " pomôcť ", " záujem ", " krátka "],
        "cs": [" chtěl ", " myslím ", " pomoct ", " zájem ", " krátký "],
        "de": [" ich ", " sie ", " kurze ", " hilfe ", " interessant"]
    }
    
    english_score = sum(1 for m in english_markers if m in body_lower)
    foreign_scores = {lang: sum(1 for m in markers if m in body_lower) 
                     for lang, markers in foreign_markers.items()}
    
    # If strong English markers and weak foreign markers, use English sign-off
    if english_score >= 2 and max(foreign_scores.values(), default=0) == 0:
        detected_lang = "en"
    
    return f"{clean}\n\n{signature_block(account, detected_lang)}"


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


def _within_local_windows(now_utc: datetime, tz_str: str) -> bool:
    local_tz = pytz.timezone(tz_str)
    now_local = now_utc.astimezone(local_tz)
    if now_local.weekday() >= 5:
        return False
    current = now_local.time()
    return any(start <= current <= end for start, end in LOCAL_WINDOWS)


def _next_local_window_start(now_utc: datetime, tz_str: str) -> datetime:
    local_tz = pytz.timezone(tz_str)
    candidate_local = now_utc.astimezone(local_tz)
    
    for _ in range(14):
        if candidate_local.weekday() < 5:
            for start, _end in LOCAL_WINDOWS:
                dt_local = local_tz.localize(datetime.combine(candidate_local.date(), start), is_dst=None)
                if dt_local > now_utc:
                    return dt_local.astimezone(timezone.utc)
        candidate_local = local_tz.localize(datetime.combine(candidate_local.date() + timedelta(days=1), time(0, 0)), is_dst=None)
    
    fallback_local = local_tz.localize(datetime.combine((now_utc + timedelta(days=1)).astimezone(local_tz).date(), LOCAL_WINDOWS[0][0]), is_dst=None)
    return fallback_local.astimezone(timezone.utc)


def _clip_to_local_window(ts_utc: datetime, tz_str: str) -> datetime:
    local_tz = pytz.timezone(tz_str)
    ts_local = ts_utc.astimezone(local_tz)
    
    if ts_local.weekday() >= 5:
        return _next_local_window_start(ts_utc, tz_str)
        
    for start, end in LOCAL_WINDOWS:
        start_dt = local_tz.localize(datetime.combine(ts_local.date(), start), is_dst=None)
        end_dt = local_tz.localize(datetime.combine(ts_local.date(), end), is_dst=None)
        if start_dt <= ts_utc <= end_dt:
            return ts_utc
        if ts_utc < start_dt:
            return start_dt.astimezone(timezone.utc)
            
    # Exhausted today's windows, get next available
    next_day_local = local_tz.localize(datetime.combine(ts_local.date() + timedelta(days=1), time(0, 0)), is_dst=None)
    return _next_local_window_start(next_day_local.astimezone(timezone.utc), tz_str)


def next_send_after(conn: sqlite3.Connection, address: str, jitter_seed: int | None = None,
                    now: datetime | None = None, lead_address: str = "") -> datetime:
    now = now or _utc_now()
    tz_str = infer_timezone(lead_address)
    
    last = _last_sent_at(conn, address)
    baseline = now
    if last:
        try:
            baseline = max(baseline, datetime.fromisoformat(last))
        except ValueError:
            pass
            
    rng = random.Random(jitter_seed or int(now.timestamp()))
    cooldown = timedelta(minutes=rng.randint(COOLDOWN_MIN_MINUTES, COOLDOWN_MAX_MINUTES))
    
    candidate = _clip_to_local_window(baseline if _within_local_windows(baseline, tz_str) else _next_local_window_start(baseline, tz_str), tz_str)
    
    if candidate == baseline and not _within_local_windows(candidate, tz_str):
        candidate = _next_local_window_start(candidate, tz_str)
        
    if last:
        candidate = max(candidate, baseline + cooldown)
        
    candidate = _clip_to_local_window(candidate, tz_str)
    
    if candidate < now:
        candidate = _clip_to_local_window(now + timedelta(minutes=rng.randint(3, 12)), tz_str)
        
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
