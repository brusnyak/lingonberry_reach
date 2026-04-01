"""
outreach/smoke_tests.py
Crash-test core outreach behavior.
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import tempfile
from pathlib import Path

from email_sender import _load_accounts
from reply_drafter import build_reply_draft
from runtime import safe_mode_enabled
from senders import canonical_sender


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def test_sender_registry() -> None:
    _assert(canonical_sender("brusnyak.f@gmail.com")["name"] == "Victor Brusnyak", "brusnyak.f identity mismatch")
    _assert(canonical_sender("lingonberry.max@gmail.com")["short"] == "Max L.", "lingonberry short signer mismatch")


def test_safe_mode_default() -> None:
    expected = os.environ.get("BIZ_SAFE_MODE", "1").strip().lower() not in {"0", "false", "off", "no"}
    _assert(safe_mode_enabled() == expected, "safe mode runtime mismatch with BIZ_SAFE_MODE env")


def test_reply_drafter_identity_and_language() -> None:
    row = {
        "from_address": "kosice@schill.sk",
        "reply_subject": "RE: Quick question about enquiries",
        "content": "Hallo, thank you for your interest. What exactly do you mean here?",
        "target_niche": "dental_medical",
        "label": "question",
        "pain_points": "[]",
        "original_sender_address": "brusnyakyegor@gmail.com",
        "original_sender_name": "Yegor Brusnyak",
    }
    draft = build_reply_draft(row)
    _assert(draft.sender_name == "Yegor Brusnyak", "reply drafter sender mismatch")
    _assert("Hi," in draft.body, "reply drafter should use English for this sample")


def test_account_names() -> None:
    accounts = {acc["address"]: acc for acc in _load_accounts()}
    if not accounts:
        return
    if "brusnyak.f@gmail.com" in accounts:
        _assert(accounts["brusnyak.f@gmail.com"]["name"] == "Victor Brusnyak", "env account name mismatch for brusnyak.f")


def run_basic() -> None:
    test_sender_registry()
    test_safe_mode_default()
    test_reply_drafter_identity_and_language()
    test_account_names()
    print("basic smoke tests passed")


def main() -> int:
    parser = argparse.ArgumentParser(description="Outreach smoke tests")
    parser.add_argument("--live-internal-email", action="store_true", help="Also trigger a live internal email smoke test")
    args = parser.parse_args()

    run_basic()
    if args.live_internal_email:
        from cli import main as cli_main  # lazy import
        os.system(f"{Path(__file__).parent / '.venv/bin/python'} {Path(__file__).parent / 'cli.py'} internal-reply-test")
        print("live internal email smoke test triggered")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
