"""
outreach/classifier.py
LLM-based reply classifier.
Labels: interested | not_interested | question | ignore
Extracts pain points when label is 'interested' or 'question'.
"""
import json
import os
import re
import sqlite3
from datetime import datetime, timezone

from openai import OpenAI

_client = None

def _llm() -> OpenAI:
    global _client
    if _client is None:
        _client = OpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=os.environ["OPENROUTER_API_KEY"],
        )
    return _client


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


def classify_reply(reply_text: str) -> dict:
    resp = _llm().chat.completions.create(
        model="google/gemini-2.0-flash-exp:free",
        messages=[{"role": "user", "content": PROMPT.format(reply=reply_text[:2000])}],
        temperature=0.2,
        max_tokens=200,
    )
    raw = resp.choices[0].message.content.strip()
    raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.MULTILINE).strip()
    try:
        result = json.loads(raw)
        return {
            "label": result.get("label", "ignore"),
            "pain_points": result.get("pain_points", []),
            "confidence": float(result.get("confidence", 0.5)),
        }
    except Exception:
        return {"label": "ignore", "pain_points": [], "confidence": 0.0}


def run_classifier(conn: sqlite3.Connection) -> int:
    """Classify all unclassified replies. Returns count processed."""
    from storage.db import log_classification
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
            model="gemini-2.0-flash-exp",
        )

    return len(rows)
