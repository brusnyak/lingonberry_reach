"""
outreach/senders.py
Canonical sender identities. One mailbox, one person.
"""
from __future__ import annotations

import os


SENDER_REGISTRY = {
    "lingonberry.max@gmail.com": {"name": "Max Lingonberry", "short": "Max L."},
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


def internal_sender_addresses() -> tuple[str, ...]:
    """Return all known internal mailboxes, preferring configured env accounts."""
    addresses: list[str] = []
    i = 1
    while True:
        addr = os.environ.get(f"EMAIL_{i}_ADDRESS", "").strip().lower()
        if not addr:
            break
        if addr not in addresses:
            addresses.append(addr)
        i += 1
    for addr in SENDER_REGISTRY:
        normalized = addr.strip().lower()
        if normalized and normalized not in addresses:
            addresses.append(normalized)
    return tuple(addresses)


def is_internal_address(address: str) -> bool:
    return (address or "").strip().lower() in set(internal_sender_addresses())
