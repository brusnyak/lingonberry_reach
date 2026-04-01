#!/usr/bin/env python3
"""
Reply monitoring CLI dashboard.
Shows new replies, classifications, and draft status.
"""
import sqlite3
from pathlib import Path
from datetime import datetime, timezone

LEADS_DB = Path(__file__).parent.parent / "leadgen" / "data" / "leads.db"


def show_reply_dashboard():
    """Display reply monitoring dashboard."""
    conn = sqlite3.connect(LEADS_DB)
    conn.row_factory = sqlite3.Row
    
    print("\n" + "=" * 80)
    print("📊 REPLY MONITORING DASHBOARD")
    print("=" * 80)
    
    # 1. New replies (not yet classified)
    print("\n🔔 NEW REPLIES (awaiting classification)")
    print("-" * 80)
    cursor = conn.execute("""
        SELECT r.id, r.from_name, r.from_address, r.subject, r.received_at,
               b.name as business_name
        FROM replies r
        LEFT JOIN businesses b ON b.id = r.lead_id
        WHERE NOT EXISTS (
            SELECT 1 FROM reply_classification rc WHERE rc.reply_id = r.id
        )
        ORDER BY r.received_at DESC
        LIMIT 10
    """)
    
    rows = cursor.fetchall()
    if rows:
        for row in rows:
            received = row['received_at'][:16] if row['received_at'] else 'unknown'
            business_name = row['business_name'] or 'UNMATCHED'
            print(f"  [{row['id']}] {row['from_name'][:20]:20} | {business_name[:25]:25} | {received}")
            print(f"       Subject: {row['subject'][:50]}")
    else:
        print("  (no new replies)")
    
    # 2. Classified replies (awaiting draft)
    print("\n🏷️ CLASSIFIED REPLIES (awaiting response)")
    print("-" * 80)
    cursor = conn.execute("""
        SELECT r.id, r.from_name, rc.label, rc.confidence, r.received_at,
               b.name as business_name
        FROM replies r
        JOIN reply_classification rc ON rc.reply_id = r.id
        LEFT JOIN businesses b ON b.id = r.lead_id
        WHERE NOT EXISTS (
            SELECT 1 FROM reply_drafts rd WHERE rd.reply_id = r.id
        )
        AND rc.label IN ('interested', 'question')
        ORDER BY r.received_at DESC
        LIMIT 10
    """)
    
    rows = cursor.fetchall()
    if rows:
        for row in rows:
            emoji = {"interested": "✅", "question": "❓", "not_interested": "❌", "ignore": "🚫"}.get(row['label'], "📧")
            received = row['received_at'][:16] if row['received_at'] else 'unknown'
            print(f"  {emoji} [{row['id']}] {row['from_name'][:20]:20} | {row['label']:12} ({row['confidence']:.0%}) | {received}")
    else:
        print("  (no classified replies awaiting response)")
    
    # 3. Drafts awaiting approval
    print("\n✍️ RESPONSE DRAFTS (awaiting approval)")
    print("-" * 80)
    cursor = conn.execute("""
        SELECT rd.id, rd.reply_id, r.from_name, rd.subject, rd.status, rd.created_at,
               b.name as business_name
        FROM reply_drafts rd
        JOIN replies r ON r.id = rd.reply_id
        LEFT JOIN businesses b ON b.id = r.lead_id
        WHERE rd.status = 'draft'
        ORDER BY rd.created_at DESC
        LIMIT 10
    """)
    
    rows = cursor.fetchall()
    if rows:
        for row in rows:
            created = row['created_at'][:16] if row['created_at'] else 'unknown'
            print(f"  📝 [{row['reply_id']}] {row['from_name'][:20]:20} | {row['subject'][:35]:35} | {created}")
    else:
        print("  (no drafts awaiting approval)")
    
    # 4. Stats summary
    print("\n📈 SUMMARY")
    print("-" * 80)
    
    cursor = conn.execute("SELECT COUNT(*) FROM replies")
    total_replies = cursor.fetchone()[0]
    
    cursor = conn.execute("SELECT COUNT(*) FROM reply_classification")
    classified = cursor.fetchone()[0]
    
    cursor = conn.execute("SELECT COUNT(*) FROM reply_drafts WHERE status = 'draft'")
    drafts_pending = cursor.fetchone()[0]
    
    cursor = conn.execute("SELECT COUNT(*) FROM reply_drafts WHERE status = 'sent'")
    drafts_sent = cursor.fetchone()[0]
    
    print(f"  Total replies received:     {total_replies}")
    print(f"  Classified:                 {classified}")
    print(f"  Drafts pending approval:    {drafts_pending}")
    print(f"  Drafts sent:                {drafts_sent}")
    
    # 5. Scheduled sends
    print("\n📤 SCHEDULED OUTREACH (tomorrow 9:30am AEST)")
    print("-" * 80)
    cursor = conn.execute("""
        SELECT COUNT(*) FROM outreach_log 
        WHERE status = 'scheduled' 
        AND send_after >= datetime('now')
    """)
    scheduled = cursor.fetchone()[0]
    print(f"  Emails scheduled: {scheduled}")
    
    print("\n" + "=" * 80)
    print("Commands:")
    print("  python -m outreach.reply_poller     - Check for new replies")
    print("  python -m outreach.classify_replies - Classify new replies")
    print("  python -m outreach.reply_dashboard  - Refresh this dashboard")
    print("=" * 80 + "\n")
    
    conn.close()


if __name__ == "__main__":
    show_reply_dashboard()
