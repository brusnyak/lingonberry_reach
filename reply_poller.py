#!/usr/bin/env python3
"""
Reply poller for 4 internal email inboxes.
Polls Gmail via IMAP for new replies to outreach emails.
"""
import imaplib
import email
import sqlite3
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Dict, Optional

# Internal email accounts to poll
EMAIL_ACCOUNTS = [
    {"email": "lingonberry.max@gmail.com", "name": "Max"},
    {"email": "victor.brusnyak@gmail.com", "name": "Victor"},
    {"email": "brusnyakyegor@gmail.com", "name": "Yegor"},
    {"email": "brusnyak.f@gmail.com", "name": "F"}
]

LEADS_DB = Path(__file__).parent.parent / "leadgen" / "data" / "leads.db"


def connect_to_imap(email_addr: str, password: str) -> Optional[imaplib.IMAP4_SSL]:
    """Connect to Gmail IMAP."""
    try:
        mail = imaplib.IMAP4_SSL("imap.gmail.com", 993)
        mail.login(email_addr, password)
        return mail
    except Exception as e:
        print(f"IMAP connection failed for {email_addr}: {e}")
        return None


def fetch_new_replies(mail: imaplib.IMAP4_SSL, since_date: datetime) -> List[Dict]:
    """Fetch emails received since given date."""
    replies = []
    
    try:
        mail.select("inbox")
        
        # Search for emails since date
        date_str = since_date.strftime("%d-%b-%Y")
        _, message_ids = mail.search(None, f'(SINCE "{date_str}")')
        
        if not message_ids[0]:
            return replies
        
        for msg_id in message_ids[0].split():
            try:
                _, msg_data = mail.fetch(msg_id, "(RFC822)")
                raw_email = msg_data[0][1]
                msg = email.message_from_bytes(raw_email)
                
                # Extract fields
                from_addr = msg.get("From", "")
                subject = msg.get("Subject", "")
                date_str = msg.get("Date", "")
                message_id = msg.get("Message-ID", "")
                
                # Get body
                body = ""
                if msg.is_multipart():
                    for part in msg.walk():
                        if part.get_content_type() == "text/plain":
                            body = part.get_payload(decode=True).decode("utf-8", errors="ignore")
                            break
                else:
                    body = msg.get_payload(decode=True).decode("utf-8", errors="ignore")
                
                # Only include if it's a reply (has Re: in subject or references our outreach)
                if "re:" in subject.lower() or is_reply_to_outreach(subject, body):
                    replies.append({
                        "message_id": message_id,
                        "from_name": extract_name(from_addr),
                        "from_address": extract_email(from_addr),
                        "subject": subject,
                        "body": body[:2000],  # Limit size
                        "received_at": datetime.now(timezone.utc).isoformat(),
                        "raw": raw_email.decode("utf-8", errors="ignore")[:5000]
                    })
            except Exception as e:
                print(f"Error processing message {msg_id}: {e}")
                continue
                
    except Exception as e:
        print(f"Error fetching replies: {e}")
    
    return replies


def is_reply_to_outreach(subject: str, body: str) -> bool:
    """Check if this email is likely a reply to our outreach."""
    # Check for common reply indicators
    reply_indicators = [
        "re:", "fwd:", "follow up", "following up",
        "job enquiry", "hipages", "quote", "plumbing", "electrician"
    ]
    subject_lower = subject.lower()
    return any(ind in subject_lower for ind in reply_indicators)


def extract_name(from_field: str) -> str:
    """Extract name from From field."""
    if "<" in from_field:
        return from_field.split("<")[0].strip().strip('"')
    return from_field


def extract_email(from_field: str) -> str:
    """Extract email from From field."""
    if "<" in from_field and ">" in from_field:
        return from_field.split("<")[1].split(">")[0]
    return from_field


def store_reply(reply: Dict, account_email: str) -> Optional[int]:
    """Store reply in database. Returns reply_id if new, None if duplicate."""
    conn = sqlite3.connect(LEADS_DB)
    conn.row_factory = sqlite3.Row
    
    try:
        # Check for duplicate by message_id
        cursor = conn.execute(
            "SELECT id FROM replies WHERE message_id = ?",
            (reply["message_id"],)
        )
        if cursor.fetchone():
            return None  # Already stored
        
        # Find lead_id by matching from_address to email_maps
        cursor = conn.execute(
            "SELECT id FROM businesses WHERE email_maps LIKE ?",
            (f"%{reply['from_address']}%",)
        )
        lead_row = cursor.fetchone()
        lead_id = lead_row["id"] if lead_row else -1  # Use -1 for unmatched replies
        
        # Find related outreach_id
        outreach_id = None
        if lead_id > 0:
            cursor = conn.execute(
                "SELECT id FROM outreach_log WHERE lead_id = ? AND status = 'sent' ORDER BY sent_at DESC LIMIT 1",
                (lead_id,)
            )
            outreach_row = cursor.fetchone()
            if outreach_row:
                outreach_id = outreach_row["id"]
        
        # Insert reply (allow lead_id=-1 for unmatched)
        cursor = conn.execute(
            """INSERT INTO replies 
               (lead_id, outreach_id, channel, message_id, from_name, from_address,
                subject, content, received_at, raw)
               VALUES (?, ?, 'email', ?, ?, ?, ?, ?, ?, ?)""",
            (lead_id if lead_id > 0 else -1, outreach_id, reply["message_id"], reply["from_name"],
             reply["from_address"], reply["subject"], reply["body"],
             reply["received_at"], reply["raw"])
        )
        reply_id = cursor.lastrowid
        conn.commit()
        
        if lead_id == -1:
            print(f"  → Stored reply from {reply['from_address']} (reply_id={reply_id}) [UNMATCHED - no lead found]")
        else:
            print(f"  → Stored reply from {reply['from_address']} (reply_id={reply_id})")
        return reply_id
        
    except Exception as e:
        print(f"  → Error storing reply: {e}")
        conn.rollback()
        return None
    finally:
        conn.close()


def poll_all_accounts(since_hours: int = 24) -> Dict:
    """Poll all 4 email accounts for new replies."""
    since_date = datetime.now(timezone.utc) - timedelta(hours=since_hours)
    results = {
        "accounts_polled": 0,
        "total_replies": 0,
        "new_replies": 0,
        "errors": []
    }
    
    # Load accounts using the same pattern as email_sender
    from outreach.email_sender import _load_accounts
    accounts = _load_accounts()
    
    for acc in accounts:
        email_addr = acc["address"]
        password = acc["password"]
        
        if not password:
            print(f"⚠ No password for {email_addr}, skipping")
            results["errors"].append(f"No password for {email_addr}")
            continue
        
        print(f"\n📧 Polling {email_addr}...")
        mail = connect_to_imap(email_addr, password)
        
        if not mail:
            results["errors"].append(f"Failed to connect {email_addr}")
            continue
        
        try:
            replies = fetch_new_replies(mail, since_date)
            results["accounts_polled"] += 1
            results["total_replies"] += len(replies)
            
            print(f"  Found {len(replies)} potential replies")
            
            for reply in replies:
                reply_id = store_reply(reply, email_addr)
                if reply_id:
                    results["new_replies"] += 1
                    # Send Telegram notification
                    try:
                        from outreach.telegram_notifier import notify_new_reply
                        notify_new_reply(reply_id)
                    except Exception as e:
                        print(f"  Telegram notify failed: {e}")
                    
        finally:
            mail.logout()
    
    return results


if __name__ == "__main__":
    print("=" * 60)
    print("Reply Poller - Checking 4 internal inboxes")
    print("=" * 60)
    
    results = poll_all_accounts(since_hours=24)
    
    print("\n" + "=" * 60)
    print("SUMMARY:")
    print(f"  Accounts polled: {results['accounts_polled']}")
    print(f"  Total replies found: {results['total_replies']}")
    print(f"  New replies stored: {results['new_replies']}")
    if results['errors']:
        print(f"  Errors: {len(results['errors'])}")
    print("=" * 60)
