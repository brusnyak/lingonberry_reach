"""
outreach/storage/db.py
Shared DB layer — reads from leadgen's businesses table,
writes to outreach tables in the same DB.
"""
import sqlite3
from pathlib import Path

LEADS_DB = Path(__file__).parent.parent.parent / "leadgen" / "data" / "leads.db"

OUTREACH_SCHEMA = """
CREATE TABLE IF NOT EXISTS outreach_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    lead_id     INTEGER NOT NULL,
    channel     TEXT NOT NULL,          -- email | instagram | facebook | form
    address     TEXT,                   -- email address or IG/FB handle used
    subject     TEXT,                   -- email only
    message     TEXT NOT NULL,
    status      TEXT DEFAULT 'pending', -- pending | sent | failed | skipped
    sent_at     TEXT,
    created_at  TEXT NOT NULL,
    FOREIGN KEY (lead_id) REFERENCES businesses(id)
);

CREATE TABLE IF NOT EXISTS replies (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    lead_id         INTEGER NOT NULL,
    outreach_id     INTEGER,
    channel         TEXT NOT NULL,
    content         TEXT NOT NULL,
    received_at     TEXT NOT NULL,
    raw             TEXT,               -- full raw message/payload
    FOREIGN KEY (lead_id) REFERENCES businesses(id),
    FOREIGN KEY (outreach_id) REFERENCES outreach_log(id)
);

CREATE TABLE IF NOT EXISTS reply_classification (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    reply_id        INTEGER NOT NULL UNIQUE,
    label           TEXT NOT NULL,      -- interested | not_interested | question | ignore
    pain_points     TEXT,               -- extracted pain points (JSON array)
    confidence      REAL,
    model           TEXT,
    classified_at   TEXT NOT NULL,
    FOREIGN KEY (reply_id) REFERENCES replies(id)
);
"""


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(LEADS_DB)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_outreach_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(OUTREACH_SCHEMA)
    conn.commit()


def get_qualified_leads(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    """Pull all qualified leads that haven't been contacted yet."""
    return conn.execute(
        """
        SELECT b.id, b.name, b.category, b.address, b.website,
               b.phone, b.email_maps,
               w.emails AS site_emails,
               w.socials,
               e.outreach_message,
               b.outreach_angle,
               b.brand_summary,
               b.pain_point_guess,
               b.apparent_size,
               b.digital_maturity
        FROM businesses b
        LEFT JOIN website_data w ON w.business_id = b.id
        LEFT JOIN enrichment e ON e.business_id = b.id
        WHERE b.validation_status = 'qualified'
          AND b.id NOT IN (
              SELECT DISTINCT lead_id FROM outreach_log
              WHERE status IN ('sent', 'pending')
          )
        ORDER BY b.score DESC NULLS LAST
        """
    ).fetchall()


def get_pending_drafts(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    """Drafts awaiting human review."""
    return conn.execute(
        """
        SELECT o.*, b.name, b.website, b.outreach_angle
        FROM outreach_log o
        JOIN businesses b ON b.id = o.lead_id
        WHERE o.status = 'pending'
        ORDER BY o.created_at DESC
        """
    ).fetchall()


def log_outreach(conn: sqlite3.Connection, lead_id: int, channel: str,
                 address: str, message: str, subject: str = "",
                 status: str = "pending") -> int:
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()
    cur = conn.execute(
        """
        INSERT INTO outreach_log (lead_id, channel, address, subject, message, status, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (lead_id, channel, address, subject, message, status, now),
    )
    conn.commit()
    return cur.lastrowid


def mark_sent(conn: sqlite3.Connection, outreach_id: int) -> None:
    from datetime import datetime, timezone
    conn.execute(
        "UPDATE outreach_log SET status='sent', sent_at=? WHERE id=?",
        (datetime.now(timezone.utc).isoformat(), outreach_id),
    )
    conn.commit()


def mark_failed(conn: sqlite3.Connection, outreach_id: int, reason: str = "") -> None:
    conn.execute(
        "UPDATE outreach_log SET status='failed', message=message||? WHERE id=?",
        (f"\n[FAILED: {reason}]", outreach_id),
    )
    conn.commit()


def mark_skipped(conn: sqlite3.Connection, outreach_id: int) -> None:
    conn.execute("UPDATE outreach_log SET status='skipped' WHERE id=?", (outreach_id,))
    conn.commit()


def log_reply(conn: sqlite3.Connection, lead_id: int, channel: str,
              content: str, received_at: str, outreach_id: int = None,
              raw: str = "") -> int:
    cur = conn.execute(
        """
        INSERT INTO replies (lead_id, outreach_id, channel, content, received_at, raw)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (lead_id, outreach_id, channel, content, received_at, raw),
    )
    conn.commit()
    return cur.lastrowid


def log_classification(conn: sqlite3.Connection, reply_id: int, label: str,
                       pain_points: str, confidence: float, model: str) -> None:
    from datetime import datetime, timezone
    conn.execute(
        """
        INSERT OR REPLACE INTO reply_classification
            (reply_id, label, pain_points, confidence, model, classified_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (reply_id, label, pain_points, confidence, model,
         datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()


def get_stats(conn: sqlite3.Connection) -> dict:
    row = conn.execute(
        """
        SELECT
            COUNT(*) FILTER (WHERE status='sent')    AS sent,
            COUNT(*) FILTER (WHERE status='pending') AS pending,
            COUNT(*) FILTER (WHERE status='failed')  AS failed,
            COUNT(*) FILTER (WHERE status='skipped') AS skipped
        FROM outreach_log
        """
    ).fetchone()
    replies = conn.execute("SELECT COUNT(*) AS cnt FROM replies").fetchone()["cnt"]
    return {**dict(row), "replies": replies}
