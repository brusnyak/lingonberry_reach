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
"""


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(LEADS_DB)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


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
        ORDER BY o.created_at ASC
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
        WHERE COALESCE(rd.status, 'draft') != 'sent'
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
