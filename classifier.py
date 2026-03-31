"""
outreach/classifier.py
LLM-based reply classifier.
Labels: interested | not_interested | question | ignore
Extracts pain points when label is 'interested' or 'question'.
"""
import importlib.util
import json
import os
import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from agent.remote_models import complete_text

_DB_PATH = Path(__file__).parent / "storage" / "db.py"
_DB_SPEC = importlib.util.spec_from_file_location("outreach_storage_db_classifier", _DB_PATH)
if _DB_SPEC is None or _DB_SPEC.loader is None:
    raise ImportError(f"Unable to load outreach storage module from {_DB_PATH}")
_DB_MODULE = importlib.util.module_from_spec(_DB_SPEC)
_DB_SPEC.loader.exec_module(_DB_MODULE)


PROMPT = """Classify this reply to a cold outreach email from a freelance web/digital agency.

Reply:
\"\"\"
{reply}
\"\"\"

Labels:
- interested: positive, wants to talk, asks for more info, open to a call
- not_interested: politely declines, unsubscribes, says not relevant
- question: asks a clarifying question without clear positive/negative signal
- ignore: out-of-office, bounce, spam, unrelated

Also extract any pain points mentioned (website issues, lack of clients, tech problems, etc.).

Return JSON only:
{{"label": "...", "pain_points": ["...", "..."], "confidence": 0.0-1.0}}"""


def _heuristic_classify(reply_text: str) -> dict:
    text = (reply_text or "").strip()
    lower = text.lower()

    if any(token in lower for token in ["out of office", "automatic reply", "mailer-daemon", "delivery has failed", "undeliverable"]):
        return {"label": "ignore", "pain_points": [], "confidence": 0.85}
    if any(token in lower for token in ["not interested", "no thanks", "stop", "unsubscribe", "not relevant", "nesúhlas", "nemáme záujem", "nezáujem"]):
        return {"label": "not_interested", "pain_points": [], "confidence": 0.8}
    pains = []
    for needle in [
        "manual", "manually", "ručn", "manualne", "slow", "3 days", "follow-up", "follow up",
        "documents", "doklad", "intake", "onboarding", "appointment", "booking"
    ]:
        if needle in lower:
            pains.append(needle)
    if "?" in text or any(token in lower for token in ["what", "how", "which", "čo", "ako", "kolko", "koľko", "prečo"]):
        return {"label": "question", "pain_points": pains[:3], "confidence": 0.7}
    if any(token in lower for token in ["yes", "sure", "sounds good", "open to", "send more", "please send", "zaujíma", "pošlite", "môžeme"]):
        return {"label": "interested", "pain_points": pains[:3], "confidence": 0.68}
    return {"label": "ignore", "pain_points": pains[:2], "confidence": 0.4}


def classify_reply(reply_text: str) -> dict:
    try:
        if not any(
            os.environ.get(name)
            for name in ("OPENROUTER_API_KEY", "GROQ_API_KEY", "GOOGLE_AI_STUDIO_API_KEY", "GOOGLE_AI_VICTOR_API_KEY")
        ):
            return _heuristic_classify(reply_text)
        raw = complete_text(
            user_prompt=PROMPT.format(reply=reply_text[:2000]),
            temperature=0.2,
            max_tokens=200,
        ).strip()
        raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.MULTILINE).strip()
        result = json.loads(raw)
        return {
            "label": result.get("label", "ignore"),
            "pain_points": result.get("pain_points", []),
            "confidence": float(result.get("confidence", 0.5)),
        }
    except Exception:
        return _heuristic_classify(reply_text)


def run_classifier(conn: sqlite3.Connection) -> int:
    """Classify all unclassified replies. Returns count processed."""
    log_classification = _DB_MODULE.log_classification
    rows = conn.execute(
        """
        SELECT r.id, r.content FROM replies r
        LEFT JOIN reply_classification rc ON rc.reply_id = r.id
        WHERE rc.id IS NULL
        """
    ).fetchall()

    for row in rows:
        result = classify_reply(row["content"])
        log_classification(
            conn,
            reply_id=row["id"],
            label=result["label"],
            pain_points=json.dumps(result["pain_points"]),
            confidence=result["confidence"],
            model="mistralai/mistral-small-3.1-24b-instruct:free",
        )

    return len(rows)
