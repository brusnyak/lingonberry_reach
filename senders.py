"""
outreach/senders.py
Canonical sender identities. One mailbox, one person.
"""
from __future__ import annotations

import os


SENDER_REGISTRY = {
    "maxberryme68@gmail.com": {"name": "Max Berry", "short": "Max B."},
    "brusnyak.f@gmail.com": {"name": "Victor Brusnyak", "short": "Victor B."},
    "victor.brusnyak@gmail.com": {"name": "Victor Brusnyak", "short": "Victor B."},
    "brusnyakyegor@gmail.com": {"name": "Yegor Brusnyak", "short": "Yegor B."},
}


def canonical_sender(address: str, fallback_name: str = "") -> dict:
    addr = (address or "").strip().lower()
    if addr in SENDER_REGISTRY:
        return {"address": addr, **SENDER_REGISTRY[addr]}

    clean_name = (fallback_name or addr.split("@")[0] or "Team").strip()
    parts = [part for part in clean_name.split() if part]
    if len(parts) >= 2:
        short = f"{parts[0]} {parts[-1][0]}."
    else:
        short = clean_name
    return {"address": addr, "name": clean_name, "short": short}


def env_sender_name(index: int, address: str) -> str:
    configured = os.environ.get(f"EMAIL_{index}_NAME", "").strip()
    canonical = canonical_sender(address, configured)
    return canonical["name"]
