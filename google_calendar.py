"""
outreach/google_calendar.py
Direct Google Calendar integration for the trades demo workflow.

This module stays deliberately narrow:
- deterministic slot picking
- optional direct event creation when access token + calendar id are present
- no scheduling intelligence beyond a fixed callback window
"""
from __future__ import annotations

import json
import os
from datetime import datetime, time, timedelta, timezone
from urllib import parse, request

import pytz

from .email_sender import infer_timezone


def calendar_configured() -> bool:
    calendar_id = os.environ.get("GOOGLE_CALENDAR_ID", "").strip()
    direct_token = os.environ.get("GOOGLE_CALENDAR_ACCESS_TOKEN", "").strip()
    refresh_token = os.environ.get("GOOGLE_OAUTH_REFRESH_TOKEN", "").strip()
    client_id = os.environ.get("GOOGLE_OAUTH_CLIENT_ID", "").strip()
    client_secret = os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET", "").strip()
    return bool(calendar_id) and (bool(direct_token) or bool(refresh_token and client_id and client_secret))


def _resolve_access_token() -> str:
    direct_token = os.environ.get("GOOGLE_CALENDAR_ACCESS_TOKEN", "").strip()
    if direct_token:
        return direct_token

    refresh_token = os.environ.get("GOOGLE_OAUTH_REFRESH_TOKEN", "").strip()
    client_id = os.environ.get("GOOGLE_OAUTH_CLIENT_ID", "").strip()
    client_secret = os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET", "").strip()
    if not (refresh_token and client_id and client_secret):
        raise RuntimeError(
            "Google Calendar is not configured for write access. "
            "Provide GOOGLE_CALENDAR_ACCESS_TOKEN or GOOGLE_OAUTH_CLIENT_ID + GOOGLE_OAUTH_CLIENT_SECRET + GOOGLE_OAUTH_REFRESH_TOKEN."
        )

    payload = parse.urlencode(
        {
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        }
    ).encode("utf-8")
    req = request.Request(
        "https://oauth2.googleapis.com/token",
        data=payload,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    with request.urlopen(req, timeout=20) as resp:
        body = json.loads(resp.read().decode("utf-8"))
    token = body.get("access_token", "")
    if not token:
        raise RuntimeError(f"Failed to refresh Google OAuth access token: {body}")
    return token


def pick_demo_slot(
    *,
    received_at: str = "",
    timezone_hint: str = "",
) -> dict[str, str]:
    """Pick a deterministic callback slot in the lead's local timezone."""
    tz_name = timezone_hint if "/" in timezone_hint else (infer_timezone(timezone_hint) if timezone_hint else "UTC")
    local_tz = pytz.timezone(tz_name)
    base = datetime.now(timezone.utc)
    if received_at:
        try:
            base = max(base, datetime.fromisoformat(received_at.replace("Z", "+00:00")))
        except ValueError:
            pass

    candidate_local = base.astimezone(local_tz)
    slot_options = [time(9, 30), time(13, 30), time(16, 0)]
    for day_offset in range(1, 10):
        day = candidate_local.date() + timedelta(days=day_offset)
        if day.weekday() >= 5:
            continue
        slot_local = local_tz.localize(datetime.combine(day, slot_options[0]), is_dst=None)
        slot_end = slot_local + timedelta(minutes=30)
        return {
            "timezone": tz_name,
            "start": slot_local.astimezone(timezone.utc).isoformat(),
            "end": slot_end.astimezone(timezone.utc).isoformat(),
            "local_label": f"{slot_local.strftime('%A %H:%M')} {tz_name}",
        }
    raise RuntimeError("Unable to find a demo booking slot in the next 10 days.")


def create_calendar_event(
    *,
    summary: str,
    description: str,
    start_iso: str,
    end_iso: str,
    timezone_name: str,
    attendee_email: str = "",
) -> dict[str, str]:
    """Create a Google Calendar event using a pre-provisioned access token."""
    calendar_id = os.environ.get("GOOGLE_CALENDAR_ID", "").strip()
    if not calendar_id:
        raise RuntimeError("Google Calendar is not configured. Missing GOOGLE_CALENDAR_ID.")
    access_token = _resolve_access_token()

    payload: dict[str, object] = {
        "summary": summary,
        "description": description,
        "start": {"dateTime": start_iso, "timeZone": timezone_name},
        "end": {"dateTime": end_iso, "timeZone": timezone_name},
    }
    if attendee_email:
        payload["attendees"] = [{"email": attendee_email}]

    url = f"https://www.googleapis.com/calendar/v3/calendars/{calendar_id}/events"
    req = request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with request.urlopen(req, timeout=20) as resp:
        body = json.loads(resp.read().decode("utf-8"))
    return {
        "id": body.get("id", ""),
        "html_link": body.get("htmlLink", ""),
        "status": body.get("status", ""),
    }
