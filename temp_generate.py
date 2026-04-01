#!/usr/bin/env python
"""
Custom draft generator for specific leads (IDs [330,329,328,325,323,321,320,326,324,483,331,327,318]).
Assumes these leads have validation_status='qualified' and essential data present.
Generates emails and logs them as pending outreach, then sends review batch via send_review_batch.
"""
import sqlite3
from pathlib import Path
import sys

# Add parent directory to path to ensure imports resolve when run as script
sys.path.insert(0, str(Path(__file__).parent))

from storage.db import connect, init_outreach_tables, log_outreach
from generator import generate_email
from email_sender import pick_account
from outreach import send_review_batch  # if available; else we can call its function

LEAD_IDS = [330,329,328,325,323,321,320,326,324,483,331,327,318]

conn = connect()
init_outreach_tables(conn)

# Pick an account for all drafts (consistent)
account = pick_account(conn)
if not account:
    raise RuntimeError("No sender account available")

# Fetch lead data with website_data join
placeholders = ','.join('?' for _ in LEAD_IDS)
query = f'''
SELECT b.id, b.name, b.category, b.address, b.website,
       b.phone, b.email_maps, b.outreach_angle, b.top_gap, b.top_opportunity,
       b.gap_profile, b.opportunity_profile, b.brand_summary, b.pain_point_guess,
       b.apparent_size, b.digital_maturity,
       COALESCE(b.target_niche, '') AS target_niche,
       w.emails AS site_emails, w.socials,
       COALESCE(NULLIF(TRIM(w.language), ''), 'en') AS language
FROM businesses b
LEFT JOIN website_data w ON w.id = (
    SELECT MAX(w2.id) FROM website_data w2 WHERE w2.business_id = b.id
)
WHERE b.id IN ({placeholders})
'''
cur = conn.execute(query, LEAD_IDS)
rows = cur.fetchall()
leads = [dict(row) for row in rows]

if len(leads) < len(LEAD_IDS):
    print(f"Warning: only fetched {len(leads)} out of {len(LEAD_IDS)} leads", file=sys.stderr)

for lead in leads:
    # Determine recipient email
    to_addr = None
    site_emails = lead.get('site_emails') or ''
    for email in site_emails.split(','):
        email = email.strip()
        if email and '@' in email:
            to_addr = email
            break
    if not to_addr:
        fallback = lead.get('email_maps') or ''
        if '@' in fallback:
            to_addr = fallback.strip()
    if not to_addr:
        print(f"Skipping lead {lead['id']} ({lead.get('name')}): no email address found", file=sys.stderr)
        continue

    # Generate email subject and body
    try:
        subject, body = generate_email(lead, account=account)
    except Exception as e:
        print(f"Error generating email for lead {lead['id']}: {e}", file=sys.stderr)
        continue

    # Log as pending outreach
    log_outreach(
        conn,
        lead_id=lead['id'],
        channel='email',
        address=to_addr,
        message=body,
        subject=subject,
        status='pending',
        message_variant_fingerprint=''
    )
    print(f"Drafted lead {lead['id']} ({lead.get('name')}) -> {to_addr}")

conn.commit()
print("\n--- Drafts created. Sending review batch ---")

# Send review batch to Yegor
from outreach import send_review_batch  # import here to avoid circular?
send_review_batch(conn, limit=13, recipient='egorbrusnyak@gmail.com')
print("Review batch sent.")

conn.close()
