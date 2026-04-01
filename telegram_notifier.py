#!/usr/bin/env python3
"""
Telegram notifier for reply events.
Sends notifications to Telegram when new replies are detected.
"""
import os
import sqlite3
from pathlib import Path
from typing import Dict, Optional, Any
import urllib.request
import urllib.parse
import json

LEADS_DB = Path(__file__).parent.parent / "leadgen" / "data" / "leads.db"

# Telegram bot configuration
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BRIDGE_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")  # Your personal chat ID


def send_telegram_message(message: str, reply_markup: Optional[Dict] = None) -> bool:
    """Send message via Telegram bot with optional inline keyboard."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠ Telegram not configured (missing TELEGRAM_BRIDGE_TOKEN or TELEGRAM_CHAT_ID)")
        return False
    
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data: Dict[str, Any] = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True
        }
        
        if reply_markup:
            data["reply_markup"] = json.dumps(reply_markup)
        
        req = urllib.request.Request(
            url,
            data=urllib.parse.urlencode(data).encode(),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST"
        )
        
        with urllib.request.urlopen(req, timeout=10) as response:
            result = json.loads(response.read().decode())
            return result.get("ok", False)
            
    except Exception as e:
        print(f"⚠ Failed to send Telegram notification: {e}")
        return False


def notify_new_reply(reply_id: int) -> bool:
    """Send Telegram notification with inline buttons for quick actions."""
    conn = sqlite3.connect(LEADS_DB)
    conn.row_factory = sqlite3.Row
    
    try:
        # Get reply details
        cursor = conn.execute(
            """SELECT r.*, b.name as business_name
               FROM replies r
               LEFT JOIN businesses b ON b.id = r.lead_id
               WHERE r.id = ?""",
            (reply_id,)
        )
        reply = cursor.fetchone()
        
        if not reply:
            return False
        
        # Build notification message
        from_name = reply["from_name"] or reply["from_address"].split("@")[0]
        from_email = reply["from_address"]
        subject = reply["subject"] or "(no subject)"
        content = reply["content"] or ""
        
        # Smart summary - if content is long, add a summary marker
        content_display = content[:800] if len(content) <= 800 else content[:800] + "\n\n[Message continues...]"
        
        message = f"""📧 **New Reply from {from_name}**

**From:** {from_email}
**Subject:** {subject}

**Their message:**
{content_display}

---
*Reply ID: {reply_id}*
"""
        
        # Inline keyboard for quick actions
        reply_markup = {
            "inline_keyboard": [
                [
                    {"text": "✅ Accept", "callback_data": f"accept:{reply_id}"},
                    {"text": "✏️ Edit", "callback_data": f"edit:{reply_id}"},
                    {"text": "🚫 Reject", "callback_data": f"reject:{reply_id}"}
                ],
                [
                    {"text": "📝 View Full Message", "callback_data": f"view:{reply_id}"}
                ]
            ]
        }
        
        return send_telegram_message(message, reply_markup)
        
    finally:
        conn.close()


def notify_classification(reply_id: int, classification: str, confidence: float) -> bool:
    """Notify about reply classification with inline buttons."""
    emoji = {
        "interested": "✅",
        "not_interested": "❌",
        "question": "❓",
        "ignore": "🚫"
    }.get(classification, "📧")
    
    message = f"""{emoji} **Reply Classified**

Reply ID: `{reply_id}`
Classification: **{classification}**
Confidence: {confidence:.1%}
"""
    
    reply_markup = {
        "inline_keyboard": [
            [
                {"text": "✍️ Generate Draft", "callback_data": f"draft:{reply_id}"}
            ]
        ]
    }
    
    return send_telegram_message(message, reply_markup)


def notify_draft_created(reply_id: int, draft_subject: str, draft_body: str = "") -> bool:
    """Notify that a response draft has been created with preview."""
    preview = draft_body[:300] if len(draft_body) <= 300 else draft_body[:300] + "..."
    
    message = f"""✍️ **Response Draft Ready**

Reply ID: `{reply_id}`
Subject: {draft_subject}

**Draft Preview:**
```
{preview}
```
"""
    
    reply_markup = {
        "inline_keyboard": [
            [
                {"text": "✅ Send Now", "callback_data": f"send:{reply_id}"},
                {"text": "📝 Edit First", "callback_data": f"edit:{reply_id}"}
            ],
            [
                {"text": "🚫 Discard", "callback_data": f"discard:{reply_id}"}
            ]
        ]
    }
    
    return send_telegram_message(message, reply_markup)


if __name__ == "__main__":
    # Test notification
    print("Testing Telegram notification...")
    test_msg = "🧪 **Test Notification**\n\nReply monitoring system is active."
    if send_telegram_message(test_msg):
        print("✓ Telegram notification sent successfully")
    else:
        print("✗ Failed to send Telegram notification")
        print("  Make sure TELEGRAM_BRIDGE_TOKEN and TELEGRAM_CHAT_ID are set")
