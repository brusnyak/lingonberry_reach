"""
outreach/cli.py
Convenience CLI for recurring outreach review actions.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from agent.tools import outreach  # type: ignore


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Outreach review/send helper")
    sub = parser.add_subparsers(dest="command", required=True)

    g = sub.add_parser("generate", help="Generate drafts for a niche")
    g.add_argument("-n", "--limit", type=int, default=5)
    g.add_argument("--niche", default="")

    p = sub.add_parser("preview", help="Preview active drafts")
    p.add_argument("-n", "--limit", type=int, default=5)

    r = sub.add_parser("review", help="Send active drafts to Gmail review")
    r.add_argument("-n", "--limit", type=int, default=5)
    r.add_argument("--to", default="egorbrusnyak@gmail.com")

    s = sub.add_parser("review-status", help="Show open review batches")
    s.add_argument("-n", "--limit", type=int, default=10)

    pr = sub.add_parser("poll-reviews", help="Poll Gmail for review replies and unlock approved batches")
    pr.add_argument("-n", "--limit", type=int, default=10)

    sch = sub.add_parser("schedule", help="Schedule approved drafts into the paced send queue")
    sch.add_argument("-n", "--limit", type=int, default=5)

    proc = sub.add_parser("process-send-queue", help="Send scheduled drafts that are due right now")
    proc.add_argument("-n", "--limit", type=int, default=5)

    q = sub.add_parser("queue", help="Show pending/approved/scheduled queue")
    q.add_argument("-n", "--limit", type=int, default=10)

    rq = sub.add_parser("reply-queue", help="Show replies that need action")
    rq.add_argument("-n", "--limit", type=int, default=10)

    prep = sub.add_parser("prepare-replies", help="Draft suggested replies for inbound messages")
    prep.add_argument("-n", "--limit", type=int, default=10)

    prev = sub.add_parser("preview-replies", help="Preview drafted reply emails")
    prev.add_argument("-n", "--limit", type=int, default=5)

    sendr = sub.add_parser("send-replies", help="Send drafted reply emails")
    sendr.add_argument("-n", "--limit", type=int, default=5)

    it = sub.add_parser("internal-reply-test", help="Send a controlled internal reply-workflow test email")

    its = sub.add_parser("internal-reply-test-status", help="Poll and preview internal reply-test drafts")
    its.add_argument("-n", "--limit", type=int, default=5)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "generate":
        print(outreach.generate_drafts(args.limit, args.niche))
        return 0
    if args.command == "preview":
        print(outreach.preview_drafts(args.limit))
        return 0
    if args.command == "review":
        print(outreach.send_review_batch(args.limit, args.to))
        return 0
    if args.command == "review-status":
        print(outreach.review_batch_status(args.limit))
        return 0
    if args.command == "poll-reviews":
        print(outreach.poll_review_gate(args.limit))
        return 0
    if args.command == "schedule":
        print(outreach.schedule_approved(args.limit))
        return 0
    if args.command == "process-send-queue":
        print(outreach.process_send_queue(args.limit))
        return 0
    if args.command == "queue":
        print(outreach.pending_drafts(args.limit))
        return 0
    if args.command == "reply-queue":
        print(outreach.reply_queue(args.limit))
        return 0
    if args.command == "prepare-replies":
        print(outreach.prepare_reply_drafts(args.limit))
        return 0
    if args.command == "preview-replies":
        print(outreach.preview_reply_drafts(args.limit))
        return 0
    if args.command == "send-replies":
        print(outreach.send_reply_drafts(args.limit))
        return 0
    if args.command == "internal-reply-test":
        print(outreach.internal_reply_test())
        return 0
    if args.command == "internal-reply-test-status":
        print(outreach.internal_reply_test_status(args.limit))
        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
