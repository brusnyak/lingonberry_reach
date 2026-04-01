"""
outreach/storage/db.py
Shared DB layer — reads from leadgen's businesses table,
writes to outreach tables in the same DB.
"""
import os
import sqlite3
from pathlib import Path
from typing import Optional

LEADS_DB = Path(__file__).parent.parent.parent / "leadgen" / "data" / "leads.db"

def _get_encryption_key() -> Optional[str]:
    """Get database encryption key from environment variable."""
    key = os.environ.get("DB_ENCRYPTION_KEY", "").strip()
    if not key:
        return None
    # If key is not hex, derive a 256-bit key using a simple hash
    # In production, use proper KDF like PBKDF2
    if len(key) == 64 and all(c in "0123456789abcdefABCDEF" for c in key):
        return key
    # Simple derivation: SHA256 of the key, then hex
    import hashlib
    return hashlib.sha256(key.encode()).hexdigest()[:64]

def connect_encrypted(db_path: str | Path) -> sqlite3.Connection:
    """Connect to SQLite database with optional SQLCipher encryption."""
    # Try to import pysqlcipher3, fall back to standard sqlite3 if not available
    try:
        import sqlcipher3 as sqlite3_enc
        connector = sqlite3_enc
    except ImportError:
        # pysqlcipher3 not available, use standard sqlite3 (no encryption)
        connector = sqlite3
    
    conn = connector.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    
    # Apply encryption if key is available
    key = _get_encryption_key()
    if key and connector is sqlite3_enc:
        try:
            # Set cipher parameters for better security
            conn.execute("PRAGMA cipher_page_size = 4096")
            conn.execute("PRAGMA kdf_iter = 256000")
            conn.execute(f"PRAGMA key = 'x'{key}")
            # Verify the key worked by trying to read something
            conn.execute("SELECT count(*) FROM sqlite_master")
        except Exception as e:
            raise RuntimeError(f"Failed to unlock database with encryption key: {e}")
    else:
        # No encryption or no pysqlcipher3
        conn.execute("PRAGMA journal_mode=WAL")
    
    return conn

OUTREACH_SCHEMA = """
CREATE TABLE IF NOT EXISTS outreach_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    lead_id     INTEGER NOT NULL,
    channel     TEXT NOT NULL,          -- email | instagram | facebook | form
    address     TEXT,                   -- email address or IG/FB handle used
    sender_name TEXT,
    sender_address TEXT,
    subject     TEXT,                   -- email only
    message     TEXT NOT NULL,
    status      TEXT DEFAULT 'pending', -- pending | approved | scheduled | sent | failed | skipped
    approval_state TEXT DEFAULT 'pending',
    review_batch_key TEXT,
    signature_name TEXT,
    scheduled_at TEXT,
    send_after TEXT,
    jitter_seed INTEGER,
    message_variant_fingerprint TEXT,
    error_note  TEXT,
    sent_at     TEXT,
    created_at  TEXT NOT NULL,
    FOREIGN KEY (lead_id) REFERENCES businesses(id)
);

CREATE TABLE IF NOT EXISTS replies (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    lead_id         INTEGER NOT NULL,
    outreach_id     INTEGER,
    channel         TEXT NOT NULL,
    message_id      TEXT,
    from_name       TEXT,
    from_address    TEXT,
    subject         TEXT,
    content         TEXT NOT NULL,
    received_at     TEXT NOT NULL,
    notified_at     TEXT,
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

CREATE TABLE IF NOT EXISTS reply_drafts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    reply_id        INTEGER NOT NULL UNIQUE,
    subject         TEXT NOT NULL,
    body            TEXT NOT NULL,
    sender_name     TEXT,
    sender_address  TEXT,
    status          TEXT NOT NULL DEFAULT 'draft', -- draft | approved | sent | skipped | failed
    rationale       TEXT,
    error_note      TEXT,
    created_at      TEXT NOT NULL,
    approved_at     TEXT,
    sent_at         TEXT,
    FOREIGN KEY (reply_id) REFERENCES replies(id)
);

CREATE TABLE IF NOT EXISTS review_batches (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_key       TEXT NOT NULL UNIQUE,
    recipient       TEXT NOT NULL,
    sender_name     TEXT,
    sender_address  TEXT,
    subject         TEXT NOT NULL,
    body            TEXT NOT NULL,
    draft_count     INTEGER NOT NULL DEFAULT 0,
    status          TEXT NOT NULL DEFAULT 'pending', -- pending | sent | replied | approved | rejected
    reply_content   TEXT,
    sent_at         TEXT,
    replied_at      TEXT,
    approved_at     TEXT,
    created_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS workflow_jobs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_key    TEXT NOT NULL,
    entity_type     TEXT DEFAULT '',
    entity_id       TEXT DEFAULT '',
    payload         TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending', -- pending | dispatched | completed | failed | manual
    attempts        INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT,
    external_ref    TEXT,
    result_payload  TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    executed_at     TEXT
);

CREATE TABLE IF NOT EXISTS trades_demo_inquiries (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id          TEXT UNIQUE,
    source              TEXT NOT NULL DEFAULT 'imap',
    from_name           TEXT,
    from_address        TEXT NOT NULL,
    subject             TEXT,
    body                TEXT NOT NULL,
    received_at         TEXT NOT NULL,
    status              TEXT NOT NULL DEFAULT 'new', -- new | needs_info | qualified | responded | booked | duplicate | failed
    job_type            TEXT,
    urgency             TEXT,
    location_hint       TEXT,
    qualification_score REAL DEFAULT 0,
    qualification_reason TEXT,
    response_subject    TEXT,
    response_body       TEXT,
    response_sent_at    TEXT,
    booking_status      TEXT DEFAULT '', -- pending | completed | failed | not_needed
    booking_slot_start  TEXT,
    booking_slot_end    TEXT,
    calendar_timezone   TEXT,
    calendar_event_id   TEXT,
    approval_status     TEXT NOT NULL DEFAULT 'pending', -- pending | approved | rejected
    approved_at         TEXT,
    approved_by         TEXT,
    rejected_at         TEXT,
    execution_mode      TEXT,
    last_job_id         INTEGER,
    error_note          TEXT,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    FOREIGN KEY (last_job_id) REFERENCES workflow_jobs(id)
);
"""


def connect() -> sqlite3.Connection:
    """Connect to the leads database with encryption if available."""
    return connect_encrypted(LEADS_DB)


def init_outreach_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(OUTREACH_SCHEMA)
    ocols = {row["name"] for row in conn.execute("PRAGMA table_info(outreach_log)").fetchall()}
    if "sender_name" not in ocols:
        conn.execute("ALTER TABLE outreach_log ADD COLUMN sender_name TEXT")
    if "sender_address" not in ocols:
        conn.execute("ALTER TABLE outreach_log ADD COLUMN sender_address TEXT")
    if "error_note" not in ocols:
        conn.execute("ALTER TABLE outreach_log ADD COLUMN error_note TEXT")
    if "approval_state" not in ocols:
        conn.execute("ALTER TABLE outreach_log ADD COLUMN approval_state TEXT DEFAULT 'pending'")
    if "review_batch_key" not in ocols:
        conn.execute("ALTER TABLE outreach_log ADD COLUMN review_batch_key TEXT")
    if "signature_name" not in ocols:
        conn.execute("ALTER TABLE outreach_log ADD COLUMN signature_name TEXT")
    if "scheduled_at" not in ocols:
        conn.execute("ALTER TABLE outreach_log ADD COLUMN scheduled_at TEXT")
    if "send_after" not in ocols:
        conn.execute("ALTER TABLE outreach_log ADD COLUMN send_after TEXT")
    if "jitter_seed" not in ocols:
        conn.execute("ALTER TABLE outreach_log ADD COLUMN jitter_seed INTEGER")
    if "message_variant_fingerprint" not in ocols:
        conn.execute("ALTER TABLE outreach_log ADD COLUMN message_variant_fingerprint TEXT")
    if "touch_number" not in ocols:
        conn.execute("ALTER TABLE outreach_log ADD COLUMN touch_number INTEGER DEFAULT 1")
    if "last_subject" not in ocols:
        conn.execute("ALTER TABLE outreach_log ADD COLUMN last_subject TEXT")
    if "tracking_pixel_id" not in ocols:
        conn.execute("ALTER TABLE outreach_log ADD COLUMN tracking_pixel_id TEXT")
    if "opened_at" not in ocols:
        conn.execute("ALTER TABLE outreach_log ADD COLUMN opened_at TEXT")
    if "tracking_ua" not in ocols:
        conn.execute("ALTER TABLE outreach_log ADD COLUMN tracking_ua TEXT")
    if "tracking_ip" not in ocols:
        conn.execute("ALTER TABLE outreach_log ADD COLUMN tracking_ip TEXT")
    job_cols = {row["name"] for row in conn.execute("PRAGMA table_info(workflow_jobs)").fetchall()}
    for col, typedef in [
        ("entity_type", "TEXT DEFAULT ''"),
        ("entity_id", "TEXT DEFAULT ''"),
        ("payload", "TEXT NOT NULL DEFAULT '{}'"),
        ("status", "TEXT NOT NULL DEFAULT 'pending'"),
        ("attempts", "INTEGER NOT NULL DEFAULT 0"),
        ("last_error", "TEXT"),
        ("external_ref", "TEXT"),
        ("result_payload", "TEXT"),
        ("created_at", "TEXT"),
        ("updated_at", "TEXT"),
        ("executed_at", "TEXT"),
    ]:
        if col not in job_cols:
            conn.execute(f"ALTER TABLE workflow_jobs ADD COLUMN {col} {typedef}")
    demo_cols = {row["name"] for row in conn.execute("PRAGMA table_info(trades_demo_inquiries)").fetchall()}
    for col, typedef in [
        ("message_id", "TEXT UNIQUE"),
        ("source", "TEXT NOT NULL DEFAULT 'imap'"),
        ("from_name", "TEXT"),
        ("from_address", "TEXT NOT NULL DEFAULT ''"),
        ("subject", "TEXT"),
        ("body", "TEXT NOT NULL DEFAULT ''"),
        ("received_at", "TEXT"),
        ("status", "TEXT NOT NULL DEFAULT 'new'"),
        ("job_type", "TEXT"),
        ("urgency", "TEXT"),
        ("location_hint", "TEXT"),
        ("qualification_score", "REAL DEFAULT 0"),
        ("qualification_reason", "TEXT"),
        ("response_subject", "TEXT"),
        ("response_body", "TEXT"),
        ("response_sent_at", "TEXT"),
        ("booking_status", "TEXT DEFAULT ''"),
        ("booking_slot_start", "TEXT"),
        ("booking_slot_end", "TEXT"),
        ("calendar_timezone", "TEXT"),
        ("calendar_event_id", "TEXT"),
        ("approval_status", "TEXT NOT NULL DEFAULT 'pending'"),
        ("approved_at", "TEXT"),
        ("approved_by", "TEXT"),
        ("rejected_at", "TEXT"),
        ("execution_mode", "TEXT"),
        ("last_job_id", "INTEGER"),
        ("error_note", "TEXT"),
        ("created_at", "TEXT"),
        ("updated_at", "TEXT"),
    ]:
        if col not in demo_cols:
            conn.execute(f"ALTER TABLE trades_demo_inquiries ADD COLUMN {col} {typedef}")
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_trades_demo_message_id ON trades_demo_inquiries(message_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_demo_status_received ON trades_demo_inquiries(status, received_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_workflow_jobs_key_status ON workflow_jobs(workflow_key, status)")
    cols = {row["name"] for row in conn.execute("PRAGMA table_info(replies)").fetchall()}
    if "message_id" not in cols:
        conn.execute("ALTER TABLE replies ADD COLUMN message_id TEXT")
    if "notified_at" not in cols:
        conn.execute("ALTER TABLE replies ADD COLUMN notified_at TEXT")
    if "from_name" not in cols:
        conn.execute("ALTER TABLE replies ADD COLUMN from_name TEXT")
    if "from_address" not in cols:
        conn.execute("ALTER TABLE replies ADD COLUMN from_address TEXT")
    if "subject" not in cols:
        conn.execute("ALTER TABLE replies ADD COLUMN subject TEXT")
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_replies_message_id ON replies(message_id)"
    )
    conn.commit()


def create_review_batch(
    conn: sqlite3.Connection,
    batch_key: str,
    recipient: str,
    sender_name: str,
    sender_address: str,
    subject: str,
    body: str,
    draft_ids: list[int],
) -> int:
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc).isoformat()
    cur = conn.execute(
        """
        INSERT INTO review_batches
            (batch_key, recipient, sender_name, sender_address, subject, body, draft_count, status, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?)
        """,
        (batch_key, recipient, sender_name, sender_address, subject, body, len(draft_ids), now),
    )
    if draft_ids:
        conn.executemany(
            """
            UPDATE outreach_log
            SET approval_state='in_review',
                review_batch_key=?,
                error_note=NULL
            WHERE id=?
            """,
            [(batch_key, draft_id) for draft_id in draft_ids],
        )
    conn.commit()
    return cur.lastrowid


def mark_review_batch_sent(conn: sqlite3.Connection, batch_key: str) -> None:
    from datetime import datetime, timezone

    conn.execute(
        """
        UPDATE review_batches
        SET status='sent',
            sent_at=?
        WHERE batch_key=?
        """,
        (datetime.now(timezone.utc).isoformat(), batch_key),
    )
    conn.commit()


def get_review_batch(conn: sqlite3.Connection, batch_key: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM review_batches WHERE batch_key=?",
        (batch_key,),
    ).fetchone()


def get_open_review_batches(conn: sqlite3.Connection, limit: int = 20) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT * FROM review_batches
        WHERE status IN ('pending', 'sent', 'replied')
        ORDER BY created_at ASC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def get_review_batch_drafts(conn: sqlite3.Connection, batch_key: str) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT o.*, b.name, b.target_niche, w.language
        FROM outreach_log o
        JOIN businesses b ON b.id = o.lead_id
        LEFT JOIN website_data w ON w.business_id = b.id
        WHERE o.review_batch_key=?
        ORDER BY o.created_at ASC, o.id ASC
        """,
        (batch_key,),
    ).fetchall()


def mark_review_batch_replied(conn: sqlite3.Connection, batch_key: str, reply_content: str) -> None:
    from datetime import datetime, timezone

    conn.execute(
        """
        UPDATE review_batches
        SET status='replied',
            reply_content=?,
            replied_at=?
        WHERE batch_key=?
        """,
        (reply_content, datetime.now(timezone.utc).isoformat(), batch_key),
    )
    conn.commit()


def approve_review_batch(conn: sqlite3.Connection, batch_key: str, reply_content: str = "") -> int:
    from datetime import datetime, timezone

    drafts = get_review_batch_drafts(conn, batch_key)
    for row in drafts:
        conn.execute(
            """
            UPDATE outreach_log
            SET status='approved',
                approval_state='approved',
                error_note=NULL
            WHERE id=?
            """,
            (row["id"],),
        )
    conn.execute(
        """
        UPDATE review_batches
        SET status='approved',
            reply_content=?,
            approved_at=?
        WHERE batch_key=?
        """,
        (reply_content, datetime.now(timezone.utc).isoformat(), batch_key),
    )
    conn.commit()
    return len(drafts)


def reject_review_batch(conn: sqlite3.Connection, batch_key: str, reply_content: str = "") -> int:
    drafts = get_review_batch_drafts(conn, batch_key)
    for row in drafts:
        conn.execute(
            """
            UPDATE outreach_log
            SET approval_state='rejected',
                error_note=?
            WHERE id=?
            """,
            (reply_content or "Review reply marked this batch as rejected.", row["id"]),
        )
    conn.execute(
        """
        UPDATE review_batches
        SET status='rejected',
            reply_content=?
        WHERE batch_key=?
        """,
        (reply_content, batch_key),
    )
    conn.commit()
    return len(drafts)


def get_qualified_leads(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    """Pull all qualified leads that haven't been contacted yet."""
    return conn.execute(
        """
        SELECT b.id, b.name, b.category, b.address, b.website,
               b.phone, b.email_maps,
               w.emails AS site_emails,
               w.socials,
               w.language,
               e.outreach_message,
               b.outreach_angle,
               b.top_gap,
               b.top_opportunity,
               b.opportunity_profile,
               b.gap_profile,
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
              WHERE status IN ('sent', 'pending', 'approved', 'scheduled')
          )
        ORDER BY b.score DESC NULLS LAST
        """
    ).fetchall()


def get_pending_drafts(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    """Drafts awaiting human review."""
    return conn.execute(
        """
        SELECT o.*, b.name, b.website, b.address AS business_address, b.outreach_angle
             , w.language
        FROM outreach_log o
        JOIN businesses b ON b.id = o.lead_id
        LEFT JOIN website_data w ON w.id = (
            SELECT MAX(w2.id) FROM website_data w2 WHERE w2.business_id = b.id
        )
        WHERE o.status = 'pending'
          AND COALESCE(o.approval_state, 'pending') != 'rejected'
        ORDER BY o.created_at DESC
        """
    ).fetchall()


def get_approved_drafts(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT o.*, b.name, b.website, b.address AS business_address, b.outreach_angle
             , w.language
        FROM outreach_log o
        JOIN businesses b ON b.id = o.lead_id
        LEFT JOIN website_data w ON w.id = (
            SELECT MAX(w2.id) FROM website_data w2 WHERE w2.business_id = b.id
        )
        WHERE o.status = 'approved'
        ORDER BY COALESCE(o.touch_number, 1) DESC, o.created_at ASC
        """
    ).fetchall()


def get_scheduled_drafts(conn: sqlite3.Connection, limit: int = 50) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT o.*, b.name, b.website, b.address AS business_address, b.outreach_angle, w.language
        FROM outreach_log o
        JOIN businesses b ON b.id = o.lead_id
        LEFT JOIN website_data w ON w.id = (
            SELECT MAX(w2.id) FROM website_data w2 WHERE w2.business_id = b.id
        )
        WHERE o.status = 'scheduled'
        ORDER BY COALESCE(o.send_after, o.created_at) ASC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def get_due_scheduled_drafts(conn: sqlite3.Connection, now_iso: str, limit: int = 20) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT o.*, b.name, b.website, b.address AS business_address, b.outreach_angle, w.language
        FROM outreach_log o
        JOIN businesses b ON b.id = o.lead_id
        LEFT JOIN website_data w ON w.id = (
            SELECT MAX(w2.id) FROM website_data w2 WHERE w2.business_id = b.id
        )
        WHERE o.status = 'scheduled'
          AND COALESCE(o.send_after, '') != ''
          AND o.send_after <= ?
        ORDER BY o.send_after ASC, o.id ASC
        LIMIT ?
        """,
        (now_iso, limit),
    ).fetchall()


def log_outreach(conn: sqlite3.Connection, lead_id: int, channel: str,
                 address: str, message: str, subject: str = "",
                 status: str = "pending", message_variant_fingerprint: str = "") -> int:
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()
    cur = conn.execute(
        """
        INSERT INTO outreach_log (
            lead_id, channel, address, subject, message, status, approval_state, created_at, message_variant_fingerprint
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (lead_id, channel, address, subject, message, status, status, now, message_variant_fingerprint or None),
    )
    conn.commit()
    return cur.lastrowid


def mark_sent(
    conn: sqlite3.Connection,
    outreach_id: int,
    sender_name: str = "",
    sender_address: str = "",
) -> None:
    from datetime import datetime, timezone
    conn.execute(
        """
        UPDATE outreach_log
        SET status='sent',
            approval_state='approved',
            sent_at=?,
            sender_name=?,
            sender_address=?,
            signature_name=?,
            error_note=NULL
        WHERE id=?
        """,
        (
            datetime.now(timezone.utc).isoformat(),
            sender_name or None,
            sender_address or None,
            (sender_name or "").split()[0] or None,
            outreach_id,
        ),
    )
    conn.commit()


def mark_approved(conn: sqlite3.Connection, outreach_id: int) -> None:
    conn.execute(
        "UPDATE outreach_log SET status='approved', approval_state='approved', error_note=NULL WHERE id=?",
        (outreach_id,),
    )
    conn.commit()


def mark_scheduled(
    conn: sqlite3.Connection,
    outreach_id: int,
    sender_name: str,
    sender_address: str,
    send_after: str,
    jitter_seed: int,
) -> None:
    from datetime import datetime, timezone

    conn.execute(
        """
        UPDATE outreach_log
        SET status='scheduled',
            approval_state='approved',
            scheduled_at=?,
            send_after=?,
            sender_name=?,
            sender_address=?,
            signature_name=?,
            jitter_seed=?,
            error_note=NULL
        WHERE id=?
        """,
        (
            datetime.now(timezone.utc).isoformat(),
            send_after,
            sender_name or None,
            sender_address or None,
            (sender_name or "").split()[0] or None,
            jitter_seed,
            outreach_id,
        ),
    )
    conn.commit()


def mark_failed(conn: sqlite3.Connection, outreach_id: int, reason: str = "") -> None:
    conn.execute(
        "UPDATE outreach_log SET status='failed', error_note=? WHERE id=?",
        (reason, outreach_id),
    )
    conn.commit()


def mark_skipped(conn: sqlite3.Connection, outreach_id: int) -> None:
    conn.execute("UPDATE outreach_log SET status='skipped' WHERE id=?", (outreach_id,))
    conn.commit()


def clean_failed_suffixes(conn: sqlite3.Connection) -> int:
    rows = conn.execute(
        """
        SELECT id, message
        FROM outreach_log
        WHERE message LIKE '%[FAILED:%'
        """
    ).fetchall()
    cleaned = 0
    for row in rows:
        message = row["message"] or ""
        marker = "\n[FAILED:"
        idx = message.find(marker)
        if idx == -1:
            idx = message.find("[FAILED:")
        if idx == -1:
            continue
        conn.execute(
            "UPDATE outreach_log SET message=? WHERE id=?",
            (message[:idx].rstrip(), row["id"]),
        )
        cleaned += 1
    conn.commit()
    return cleaned


def log_reply(conn: sqlite3.Connection, lead_id: int, channel: str,
              content: str, received_at: str, outreach_id: int = None,
              raw: str = "", message_id: str = "",
              from_name: str = "", from_address: str = "", subject: str = "") -> int | None:
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO replies
            (lead_id, outreach_id, channel, message_id, from_name, from_address, subject, content, received_at, raw)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            lead_id,
            outreach_id,
            channel,
            message_id or None,
            from_name or None,
            from_address or None,
            subject or None,
            content,
            received_at,
            raw,
        ),
    )
    conn.commit()
    return cur.lastrowid if cur.rowcount else None


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
            COUNT(*) FILTER (WHERE status='approved') AS approved,
            COUNT(*) FILTER (WHERE status='scheduled') AS scheduled,
            COUNT(*) FILTER (WHERE status='pending') AS pending,
            COUNT(*) FILTER (WHERE status='failed')  AS failed,
            COUNT(*) FILTER (WHERE status='skipped') AS skipped
        FROM outreach_log
        """
    ).fetchone()
    replies = conn.execute("SELECT COUNT(*) AS cnt FROM replies").fetchone()["cnt"]
    return {**dict(row), "replies": replies}


def sender_utilization(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT
            COALESCE(sender_address, '(unassigned)') AS sender_address,
            COUNT(*) FILTER (WHERE status='sent') AS sent_count,
            COUNT(*) FILTER (WHERE status='scheduled') AS scheduled_count,
            MAX(sent_at) AS last_sent_at
        FROM outreach_log
        WHERE channel='email'
        GROUP BY COALESCE(sender_address, '(unassigned)')
        ORDER BY sent_count DESC, scheduled_count DESC, sender_address ASC
        """
    ).fetchall()


def get_unnotified_replies(conn: sqlite3.Connection, limit: int = 10) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT
            r.id,
            r.channel,
            r.content,
            r.received_at,
            r.from_address,
            r.subject,
            b.name,
            b.target_niche,
            b.category,
            b.outreach_angle,
            rc.label,
            rc.confidence,
            rc.pain_points
        FROM replies r
        JOIN businesses b ON b.id = r.lead_id
        LEFT JOIN reply_classification rc ON rc.reply_id = r.id
        WHERE r.notified_at IS NULL
        ORDER BY r.received_at ASC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def mark_reply_notified(conn: sqlite3.Connection, reply_id: int) -> None:
    from datetime import datetime, timezone
    conn.execute(
        "UPDATE replies SET notified_at=? WHERE id=?",
        (datetime.now(timezone.utc).isoformat(), reply_id),
    )
    conn.commit()


def upsert_reply_draft(
    conn: sqlite3.Connection,
    reply_id: int,
    subject: str,
    body: str,
    sender_name: str = "",
    sender_address: str = "",
    rationale: str = "",
) -> int:
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc).isoformat()
    existing = conn.execute(
        "SELECT id, status FROM reply_drafts WHERE reply_id=?",
        (reply_id,),
    ).fetchone()
    if existing:
        conn.execute(
            """
            UPDATE reply_drafts
            SET subject=?, body=?, sender_name=?, sender_address=?, rationale=?, error_note=NULL,
                status=CASE WHEN status='sent' THEN status ELSE 'draft' END
            WHERE reply_id=?
            """,
            (subject, body, sender_name or None, sender_address or None, rationale or None, reply_id),
        )
        conn.commit()
        return existing["id"]

    cur = conn.execute(
        """
        INSERT INTO reply_drafts
            (reply_id, subject, body, sender_name, sender_address, rationale, status, created_at)
        VALUES (?, ?, ?, ?, ?, ?, 'draft', ?)
        """,
        (reply_id, subject, body, sender_name or None, sender_address or None, rationale or None, now),
    )
    conn.commit()
    return cur.lastrowid


def mark_reply_draft_approved(conn: sqlite3.Connection, reply_id: int) -> None:
    from datetime import datetime, timezone

    conn.execute(
        """
        UPDATE reply_drafts
        SET status='approved',
            approved_at=?
        WHERE reply_id=?
        """,
        (datetime.now(timezone.utc).isoformat(), reply_id),
    )
    conn.commit()


def mark_reply_draft_sent(conn: sqlite3.Connection, reply_id: int, sender_name: str = "", sender_address: str = "") -> None:
    from datetime import datetime, timezone

    conn.execute(
        """
        UPDATE reply_drafts
        SET status='sent',
            sent_at=?,
            sender_name=COALESCE(NULLIF(?, ''), sender_name),
            sender_address=COALESCE(NULLIF(?, ''), sender_address),
            error_note=NULL
        WHERE reply_id=?
        """,
        (datetime.now(timezone.utc).isoformat(), sender_name, sender_address, reply_id),
    )
    conn.commit()


def mark_reply_draft_failed(conn: sqlite3.Connection, reply_id: int, reason: str) -> None:
    conn.execute(
        "UPDATE reply_drafts SET status='failed', error_note=? WHERE reply_id=?",
        (reason, reply_id),
    )
    conn.commit()


def get_reply_queue(conn: sqlite3.Connection, limit: int = 20) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT
            r.id AS reply_id,
            r.received_at,
            r.from_address,
            r.subject AS reply_subject,
            r.content,
            b.name,
            b.target_niche,
            b.category,
            o.sender_name AS original_sender_name,
            o.sender_address AS original_sender_address,
            o.subject AS original_subject,
            rc.label,
            rc.confidence,
            rc.pain_points,
            rd.subject AS draft_subject,
            rd.body AS draft_body,
            rd.status AS draft_status,
            rd.sender_name,
            rd.sender_address,
            rd.rationale,
            rd.error_note,
            COALESCE(NULLIF(TRIM(w.language), ''), 'en') AS lead_language
        FROM replies r
        JOIN businesses b ON b.id = r.lead_id
        LEFT JOIN website_data w ON w.id = (
            SELECT MAX(w2.id) FROM website_data w2 WHERE w2.business_id = b.id
        )
        LEFT JOIN outreach_log o ON o.id = r.outreach_id
        LEFT JOIN reply_classification rc ON rc.reply_id = r.id
        LEFT JOIN reply_drafts rd ON rd.reply_id = r.id
        ORDER BY r.received_at DESC, r.id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def get_reply_queue_needing_action(conn: sqlite3.Connection, limit: int = 20) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT
            r.id AS reply_id,
            r.received_at,
            r.from_address,
            r.subject AS reply_subject,
            r.content,
            b.name,
            b.target_niche,
            o.sender_name AS original_sender_name,
            o.sender_address AS original_sender_address,
            o.subject AS original_subject,
            rc.label,
            rc.confidence,
            rc.pain_points,
            rd.subject AS draft_subject,
            rd.body AS draft_body,
            rd.status AS draft_status,
            rd.sender_name,
            rd.sender_address,
            rd.rationale,
            rd.error_note,
            COALESCE(NULLIF(TRIM(w.language), ''), 'en') AS lead_language
        FROM replies r
        JOIN businesses b ON b.id = r.lead_id
        LEFT JOIN website_data w ON w.id = (
            SELECT MAX(w2.id) FROM website_data w2 WHERE w2.business_id = b.id
        )
        LEFT JOIN outreach_log o ON o.id = r.outreach_id
        LEFT JOIN reply_classification rc ON rc.reply_id = r.id
        LEFT JOIN reply_drafts rd ON rd.reply_id = r.id
        WHERE COALESCE(rd.status, 'draft') NOT IN ('sent', 'skipped')
        ORDER BY
            CASE COALESCE(rc.label, 'unclassified')
                WHEN 'interested' THEN 0
                WHEN 'question' THEN 1
                WHEN 'not_interested' THEN 2
                ELSE 3
            END,
            r.received_at DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def create_workflow_job(
    conn: sqlite3.Connection,
    workflow_key: str,
    payload: str,
    *,
    entity_type: str = "",
    entity_id: str = "",
    status: str = "pending",
) -> int:
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc).isoformat()
    cur = conn.execute(
        """
        INSERT INTO workflow_jobs (
            workflow_key, entity_type, entity_id, payload, status, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (workflow_key, entity_type, entity_id, payload, status, now, now),
    )
    conn.commit()
    return int(cur.lastrowid)


def update_workflow_job(
    conn: sqlite3.Connection,
    job_id: int,
    *,
    status: str,
    attempts: int | None = None,
    last_error: str = "",
    external_ref: str = "",
    result_payload: str = "",
) -> None:
    from datetime import datetime, timezone

    current = conn.execute("SELECT attempts FROM workflow_jobs WHERE id=?", (job_id,)).fetchone()
    next_attempts = attempts if attempts is not None else ((current["attempts"] if current else 0) + 1)
    executed_at = datetime.now(timezone.utc).isoformat() if status in {"completed", "failed", "manual"} else None
    conn.execute(
        """
        UPDATE workflow_jobs
        SET status=?,
            attempts=?,
            last_error=CASE WHEN ?='' THEN last_error ELSE ? END,
            external_ref=CASE WHEN ?='' THEN external_ref ELSE ? END,
            result_payload=CASE WHEN ?='' THEN result_payload ELSE ? END,
            updated_at=?,
            executed_at=COALESCE(?, executed_at)
        WHERE id=?
        """,
        (
            status,
            next_attempts,
            last_error,
            last_error,
            external_ref,
            external_ref,
            result_payload,
            result_payload,
            datetime.now(timezone.utc).isoformat(),
            executed_at,
            job_id,
        ),
    )
    conn.commit()


def recent_workflow_jobs(conn: sqlite3.Connection, limit: int = 20) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT *
        FROM workflow_jobs
        ORDER BY created_at DESC, id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def log_trades_demo_inquiry(
    conn: sqlite3.Connection,
    *,
    message_id: str = "",
    source: str,
    from_name: str,
    from_address: str,
    subject: str,
    body: str,
    received_at: str,
) -> int | None:
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc).isoformat()
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO trades_demo_inquiries (
            message_id, source, from_name, from_address, subject, body, received_at, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            message_id or None,
            source,
            from_name or None,
            from_address,
            subject or None,
            body,
            received_at,
            now,
            now,
        ),
    )
    conn.commit()
    return int(cur.lastrowid) if cur.rowcount else None


def update_trades_demo_inquiry(conn: sqlite3.Connection, inquiry_id: int, **fields: object) -> None:
    from datetime import datetime, timezone

    if not fields:
        return
    fields["updated_at"] = datetime.now(timezone.utc).isoformat()
    keys = ", ".join(f"{key}=?" for key in fields.keys())
    values = list(fields.values()) + [inquiry_id]
    conn.execute(f"UPDATE trades_demo_inquiries SET {keys} WHERE id=?", values)
    conn.commit()


def get_trades_demo_inquiry(conn: sqlite3.Connection, inquiry_id: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM trades_demo_inquiries WHERE id=?",
        (inquiry_id,),
    ).fetchone()


def list_trades_demo_inquiries(
    conn: sqlite3.Connection,
    *,
    status: str = "",
    limit: int = 20,
) -> list[sqlite3.Row]:
    if status:
        return conn.execute(
            """
            SELECT *
            FROM trades_demo_inquiries
            WHERE status=?
            ORDER BY received_at DESC, id DESC
            LIMIT ?
            """,
            (status, limit),
        ).fetchall()
    return conn.execute(
        """
        SELECT *
        FROM trades_demo_inquiries
        ORDER BY received_at DESC, id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def get_trades_demo_stats(conn: sqlite3.Connection) -> dict:
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE status='new') AS new_count,
            COUNT(*) FILTER (WHERE approval_status='pending') AS pending_approval_count,
            COUNT(*) FILTER (WHERE status='needs_info') AS needs_info_count,
            COUNT(*) FILTER (WHERE status='qualified') AS qualified_count,
            COUNT(*) FILTER (WHERE status='booked') AS booked_count,
            COUNT(*) FILTER (WHERE status='failed') AS failed_count,
            COUNT(*) FILTER (WHERE response_sent_at IS NOT NULL) AS responded_count,
            MAX(received_at) AS last_received_at,
            MAX(response_sent_at) AS last_response_sent_at
        FROM trades_demo_inquiries
        """
    ).fetchone()
    jobs = conn.execute(
        """
        SELECT
            COUNT(*) AS total_jobs,
            COUNT(*) FILTER (WHERE status='completed') AS completed_jobs,
            COUNT(*) FILTER (WHERE status='failed') AS failed_jobs,
            COUNT(*) FILTER (WHERE status='manual') AS manual_jobs,
            MAX(updated_at) AS last_job_update
        FROM workflow_jobs
        """
    ).fetchone()
    return {**dict(row), **dict(jobs)}
