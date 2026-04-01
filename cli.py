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

    tf = sub.add_parser("deterministic-test-flow", help="Reset a single lead and create a fresh deterministic outreach draft")
    tf.add_argument("--lead-id", type=int, default=302)
    tf.add_argument("--recipient", default="")
    tf.add_argument("--send", action="store_true", help="Actually send the freshly created outreach draft")
    tf.add_argument("--no-clear-history", dest="clear_history", action="store_false", help="Keep previous outreach/reply history")
    tf.set_defaults(clear_history=True)

    demo_poll = sub.add_parser("demo-poll", help="Poll the trades demo inbox and store new enquiries")
    demo_poll.add_argument("-n", "--limit", type=int, default=10)
    demo_poll.add_argument("--since-days", type=int, default=14)

    demo_run = sub.add_parser("demo-run", help="Run the full trades demo cycle")
    demo_run.add_argument("-n", "--limit", type=int, default=10)
    demo_run.add_argument("--since-days", type=int, default=14)
    demo_run.add_argument("--no-send", action="store_true", help="Process demo enquiries without sending replies")
    demo_run.add_argument("--no-approval", action="store_true", help="Process demo enquiries immediately without approval gate")

    demo_status = sub.add_parser("demo-status", help="Show trades demo workflow status")
    demo_status.add_argument("-n", "--limit", type=int, default=10)

    demo_sim = sub.add_parser("demo-simulate", help="Create a simulated trades demo enquiry")
    demo_sim.add_argument("--from-email", required=True)
    demo_sim.add_argument("--from-name", default="Demo Prospect")
    demo_sim.add_argument("--subject", required=True)
    demo_sim.add_argument("--body", required=True)

    demo_approve = sub.add_parser("demo-approve", help="Approve and execute one trades demo inquiry")
    demo_approve.add_argument("inquiry_id", type=int)
    demo_approve.add_argument("--no-send", action="store_true", help="Approve without sending the reply email")

    demo_reject = sub.add_parser("demo-reject", help="Reject one trades demo inquiry")
    demo_reject.add_argument("inquiry_id", type=int)
    demo_reject.add_argument("--reason", default="")

    demo_approve_all = sub.add_parser("demo-approve-all", help="Approve all staged trades demo inquiries")
    demo_approve_all.add_argument("-n", "--limit", type=int, default=20)
    demo_approve_all.add_argument("--no-send", action="store_true", help="Approve without sending reply emails")

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
    if args.command == "deterministic-test-flow":
        print(outreach.deterministic_test_lead_flow(args.lead_id, args.recipient, args.clear_history, args.send))
        return 0
    if args.command == "demo-poll":
        from outreach.trades_demo import poll_demo_inbox

        print(poll_demo_inbox(since_days=args.since_days, limit=args.limit))
        return 0
    if args.command == "demo-run":
        from outreach.trades_demo import run_trades_demo_cycle

        print(
            run_trades_demo_cycle(
                limit=args.limit,
                since_days=args.since_days,
                send_response=not args.no_send,
                require_approval=not args.no_approval,
            )
        )
        return 0
    if args.command == "demo-status":
        from outreach.trades_demo import trades_demo_status

        print(trades_demo_status(limit=args.limit))
        return 0
    if args.command == "demo-simulate":
        from outreach.trades_demo import simulate_demo_inquiry

        print(
            simulate_demo_inquiry(
                from_email=args.from_email,
                from_name=args.from_name,
                subject=args.subject,
                body=args.body,
            )
        )
        return 0
    if args.command == "demo-approve":
        from outreach.trades_demo import approve_demo_inquiry

        print(approve_demo_inquiry(args.inquiry_id, send_response=not args.no_send))
        return 0
    if args.command == "demo-reject":
        from outreach.trades_demo import reject_demo_inquiry

        print(reject_demo_inquiry(args.inquiry_id, reason=args.reason))
        return 0
    if args.command == "demo-approve-all":
        from outreach.trades_demo import approve_all_demo_inquiries

        print(approve_all_demo_inquiries(limit=args.limit, send_response=not args.no_send))
        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
