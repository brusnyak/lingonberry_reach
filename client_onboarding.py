"""
outreach/client_onboarding.py
Client onboarding email templates and checklist system.
Agent-facing: generates sendable emails and tracks delivery progress.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional


# Email template variations by niche
ONBOARDING_TEMPLATES = {
    "trades": {
        "subject": "Next steps — getting your AI system live",
        "body": """Hey {name},

Great to connect. To build this in under a day I just need:

1. Access to the inbox where leads arrive (Gmail/Outlook shared inbox — read-only is enough)
2. Your calendar sharing rights (Google/Outlook)
3. 5–10 recent real job request examples (copy-paste is perfect)
4. Your usual quote/reply style (even a couple old emails)
5. Which platforms you use most (Checkatrade, MyBuilder, Bark, hipages, etc.)

That's it. I'll build a test version, run it live on your real leads for 7 days free, then we flip the switch.

Sound good?

Best,
{agent_name}
""",
    },
    "real_estate": {
        "subject": "Next steps — your lead qualification system",
        "body": """Hey {name},

Great to connect. To set this up fast I need:

1. Access to your enquiry inbox (Gmail/Outlook — read-only works)
2. Your CRM or calendar (for lead handoff)
3. 5–10 recent buyer enquiries (copy-paste examples)
4. How you currently reply (a few example emails)
5. Which portals feed leads in (Rightmove, Zoopla, your website, etc.)

I'll build a draft system, test it on your real enquiries for 7 days free, then go live.

Ready to start?

Best,
{agent_name}
""",
    },
    "accounting": {
        "subject": "Next steps — your document intake system",
        "body": """Hey {name},

Great to connect. To build this securely I need:

1. Access to your client document inbox (Gmail/Outlook — read-only)
2. Your practice management system or shared drive (for handoff)
3. 5–10 recent document requests or client follow-ups (examples)
4. Your reminder/follow-up tone (a few past emails)
5. Which intake channels you use (email, portal, website forms, etc.)

Everything stays in your systems — no external storage, Article 28 compliant.

I'll build a test version, run 7 days on your real workflow free, then activate.

Sound good?

Best,
{agent_name}
""",
    },
}


# Generic testimonial request template
TESTIMONIAL_TEMPLATE = """Hey {name},

Quick check-in — how's the system working for you so far?

If it's saving you time, would you mind sharing a quick testimonial? Just 2–3 sentences about what it does for you.

Happy to draft something based on our check-ins if easier — just say yes and I'll send a draft for your approval.

Thanks,
{agent_name}
"""


@dataclass
class OnboardingChecklist:
    """Track client onboarding progress."""
    client_id: str
    client_name: str
    niche: str
    offer: str
    setup_fee: int
    monthly_fee: int
    start_date: str
    
    # Checklist items
    inbox_access: bool = False
    calendar_access: bool = False
    example_leads_received: bool = False
    template_examples_received: bool = False
    platforms_mapped: bool = False
    
    # Build phases
    workflow_built: bool = False
    test_deployed: bool = False
    test_running: bool = False
    live_activated: bool = False
    testimonial_received: bool = False
    
    # Timeline
    test_start_date: Optional[str] = None
    test_end_date: Optional[str] = None
    go_live_date: Optional[str] = None
    
    # Notes
    hours_spent: float = 0.0
    notes: str = ""


def init_onboarding_table(conn: sqlite3.Connection) -> None:
    """Create onboarding tracking table."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS client_onboarding (
            client_id TEXT PRIMARY KEY,
            client_name TEXT NOT NULL,
            niche TEXT NOT NULL,
            offer TEXT NOT NULL,
            setup_fee INTEGER,
            monthly_fee INTEGER,
            start_date TEXT,
            inbox_access BOOLEAN DEFAULT 0,
            calendar_access BOOLEAN DEFAULT 0,
            example_leads_received BOOLEAN DEFAULT 0,
            template_examples_received BOOLEAN DEFAULT 0,
            platforms_mapped BOOLEAN DEFAULT 0,
            workflow_built BOOLEAN DEFAULT 0,
            test_deployed BOOLEAN DEFAULT 0,
            test_running BOOLEAN DEFAULT 0,
            live_activated BOOLEAN DEFAULT 0,
            testimonial_received BOOLEAN DEFAULT 0,
            test_start_date TEXT,
            test_end_date TEXT,
            go_live_date TEXT,
            hours_spent REAL DEFAULT 0,
            notes TEXT DEFAULT ''
        )
        """
    )
    conn.commit()


def create_client_onboarding(
    conn: sqlite3.Connection,
    client_id: str,
    client_name: str,
    niche: str,
    offer: str,
    setup_fee: int,
    monthly_fee: int,
) -> None:
    """Initialize onboarding for new client."""
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT INTO client_onboarding (
            client_id, client_name, niche, offer, setup_fee, monthly_fee, start_date
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(client_id) DO NOTHING
        """,
        (client_id, client_name, niche, offer, setup_fee, monthly_fee, now),
    )
    conn.commit()


def get_onboarding_email(niche: str, client_name: str, agent_name: str) -> tuple[str, str]:
    """Generate onboarding email for niche. Returns (subject, body)."""
    template = ONBOARDING_TEMPLATES.get(niche, ONBOARDING_TEMPLATES["trades"])
    subject = template["subject"]
    body = template["body"].format(name=client_name, agent_name=agent_name)
    return subject, body


def print_onboarding_checklist(client_id: str = "CLIENT-001") -> None:
    """Print formatted checklist for agent use."""
    print(f"""
=== CLIENT ONBOARDING CHECKLIST ===
Client: {client_id}

INTAKE (Day 1):
[ ] Inbox access granted
[ ] Calendar access granted  
[ ] 5–10 example leads received
[ ] Template/examples received
[ ] Platforms mapped

BUILD (Days 2–3):
[ ] Workflow built in n8n (3–4 hrs)
[ ] Calendar integration configured
[ ] Test with dummy leads
[ ] 3-min Loom recorded

TEST (Days 4–10):
[ ] Deploy on real leads
[ ] Monitor daily
[ ] Fix issues
[ ] Capture metrics (time saved, leads filtered, etc.)

LIVE (Day 11+):
[ ] Client approves activation
[ ] Switch to live mode
[ ] Invoice setup fee
[ ] Schedule monthly check-in
[ ] Request testimonial (week 2–3)

TARGET METRICS TO CAPTURE:
- Time saved per day: ___ hours
- Leads filtered/week: ___
- Quotes sent/day: ___
- Jobs booked/week: ___
- Response time: ___ min vs ___ before

=== LOG HOURS ===
Build time: ___ hours
Total project: ___ hours
""")


def update_checklist_item(
    conn: sqlite3.Connection,
    client_id: str,
    item: str,
    value: bool = True,
) -> None:
    """Update a checklist item."""
    valid_items = [
        "inbox_access", "calendar_access", "example_leads_received",
        "template_examples_received", "platforms_mapped", "workflow_built",
        "test_deployed", "test_running", "live_activated", "testimonial_received"
    ]
    if item not in valid_items:
        raise ValueError(f"Invalid item: {item}")
    
    conn.execute(
        f"UPDATE client_onboarding SET {item} = ? WHERE client_id = ?",
        (value, client_id),
    )
    conn.commit()


def get_active_onboardings(conn: sqlite3.Connection) -> list[dict]:
    """Get all clients not yet live."""
    rows = conn.execute(
        """
        SELECT * FROM client_onboarding
        WHERE live_activated = 0
        ORDER BY start_date DESC
        """
    ).fetchall()
    return [dict(r) for r in rows]


def print_delivery_status(conn: sqlite3.Connection) -> None:
    """Print status of all active deliveries."""
    clients = get_active_onboardings(conn)
    
    if not clients:
        print("No active onboardings.")
        return
    
    print("\n=== ACTIVE DELIVERIES ===")
    for c in clients:
        progress = sum([
            c["inbox_access"], c["calendar_access"], c["example_leads_received"],
            c["template_examples_received"], c["platforms_mapped"],
            c["workflow_built"], c["test_deployed"], c["test_running"], c["live_activated"]
        ])
        print(f"\n{c['client_name']} ({c['niche']})")
        print(f"  Progress: {progress}/9 | Hours: {c['hours_spent']}")
        print(f"  Status: {'LIVE' if c['live_activated'] else 'TEST' if c['test_running'] else 'BUILD'}")
        if c["test_end_date"]:
            print(f"  Test ends: {c['test_end_date']}")


# CLI entry point
if __name__ == "__main__":
    import sys
    
    db_path = "../leads.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    init_onboarding_table(conn)
    
    if len(sys.argv) < 2:
        print("Usage: python client_onboarding.py [new|email|checklist|status|update]")
        print("\nExamples:")
        print("  python client_onboarding.py new trades_client_001 'Joe Plumber' trades 'job_filter' 497 197")
        print("  python client_onboarding.py email trades_client_001 trades 'John' 'Your Name'")
        print("  python client_onboarding.py checklist trades_client_001")
        print("  python client_onboarding.py status")
        print("  python client_onboarding.py update trades_client_001 workflow_built")
        sys.exit(0)
    
    cmd = sys.argv[1]
    
    if cmd == "new" and len(sys.argv) >= 7:
        create_client_onboarding(
            conn, sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5],
            int(sys.argv[6]), int(sys.argv[7]) if len(sys.argv) > 7 else 0
        )
        print(f"Created onboarding for {sys.argv[3]}")
    
    elif cmd == "email" and len(sys.argv) >= 5:
        niche = sys.argv[3]
        subject, body = get_onboarding_email(niche, sys.argv[4], sys.argv[5] if len(sys.argv) > 5 else "Agent")
        print(f"\nSubject: {subject}\n")
        print(body)
    
    elif cmd == "checklist":
        client_id = sys.argv[2] if len(sys.argv) > 2 else "CLIENT-001"
        print_onboarding_checklist(client_id)
    
    elif cmd == "status":
        print_delivery_status(conn)
    
    elif cmd == "update" and len(sys.argv) >= 4:
        update_checklist_item(conn, sys.argv[2], sys.argv[3])
        print(f"Updated {sys.argv[2]}: {sys.argv[3]} = True")
    
    conn.close()
