#!/usr/bin/env python3
"""
Tracking pixel utilities for email open tracking.
"""
import secrets
from datetime import datetime, timezone


def generate_tracking_id() -> str:
    """Generate a unique tracking pixel ID."""
    return secrets.token_urlsafe(16)


def get_tracking_pixel_html(tracking_id: str, base_url: str = "https://api.yourdomain.com") -> str:
    """
    Generate HTML for a 1x1 tracking pixel.
    
    Args:
        tracking_id: Unique tracking identifier
        base_url: Base URL for the tracking endpoint
        
    Returns:
        HTML img tag for the tracking pixel
    """
    pixel_url = f"{base_url}/api/track/{tracking_id}.gif"
    return f'<img src="{pixel_url}" width="1" height="1" alt="" style="display:block;width:1px;height:1px;" />'


def append_tracking_to_email(body_html: str, tracking_id: str, base_url: str = "https://api.yourdomain.com") -> str:
    """
    Append tracking pixel to email body.
    
    Args:
        body_html: Email HTML body
        tracking_id: Unique tracking identifier
        base_url: Base URL for tracking endpoint
        
    Returns:
        Modified HTML with tracking pixel
    """
    pixel = get_tracking_pixel_html(tracking_id, base_url)
    
    # If body ends with </html>, insert before that
    if "</html>" in body_html.lower():
        return body_html.replace("</html>", f"{pixel}</html>", 1)
    elif "</body>" in body_html.lower():
        return body_html.replace("</body>", f"{pixel}</body>", 1)
    else:
        # Just append at the end
        return body_html + pixel


def store_tracking_id(outreach_id: int, tracking_id: str) -> None:
    """Store tracking pixel ID in database."""
    import sqlite3
    from pathlib import Path
    
    LEADS_DB = Path(__file__).parent.parent / "leadgen" / "data" / "leads.db"
    
    conn = sqlite3.connect(LEADS_DB)
    conn.execute(
        "UPDATE outreach_log SET tracking_pixel_id = ? WHERE id = ?",
        (tracking_id, outreach_id)
    )
    conn.commit()
    conn.close()
