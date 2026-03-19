"""
outreach/runtime.py
Runtime safety toggles for outreach operations.
"""
from __future__ import annotations

import os


def safe_mode_enabled() -> bool:
    raw = os.environ.get("BIZ_SAFE_MODE", "1").strip().lower()
    return raw not in {"0", "false", "off", "no"}


def assert_outbound_allowed(action: str) -> None:
    if safe_mode_enabled():
        raise RuntimeError(
            f"Outbound action blocked by safe mode: {action}. "
            "Set BIZ_SAFE_MODE=0 only when identities, formatting, and workflow are production-safe."
        )
