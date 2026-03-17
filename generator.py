"""
outreach/generator.py
Generates channel-appropriate outreach messages using LLM.
Combines outreach_angle (site intel) + outreach_message (enrichment draft).
"""
import json
import os
import re
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


def extract_name(about_text: str, email: str = "") -> str:
    """Best-effort name extraction: try email prefix, skip generic addresses."""
    if email:
        prefix = email.split("@")[0].lower()
        # skip generic: info, contact, hello, admin, support, office, mail
        generic = {"info", "contact", "hello", "admin", "support", "office",
                   "mail", "team", "sales", "enquiries", "enquiry", "noreply"}
        if prefix not in generic and re.match(r"^[a-z]+\.[a-z]+$", prefix):
            parts = prefix.split(".")
            return parts[0].capitalize()
    return ""


def generate_email(lead: dict) -> dict:
    """
    Returns {"subject": str, "body": str}
    lead keys used: name, outreach_angle, outreach_message, brand_summary,
                    pain_point_guess, site_emails, email_maps
    """
    contact_name = extract_name(
        lead.get("brand_summary", ""),
        (lead.get("site_emails") or lead.get("email_maps") or "").split(",")[0].strip(),
    )
    greeting = f"Hi {contact_name}," if contact_name else "Hi,"

    prompt = f"""You write cold outreach emails for a freelance web/digital services agency.
Rules:
- Plain text only, no HTML, no bullet points, no buzzwords
- 3-4 sentences max in the body
- Casual, direct, human — like a message from a real person
- One specific observation about their business (from outreach_angle)
- One soft ask (a quick call or reply, not a pitch)
- No "I hope this email finds you well", no "I wanted to reach out"
- Subject: casual, specific, not salesy (max 8 words)

Business: {lead.get("name")}
Category: {lead.get("category", "")}
Brand summary: {lead.get("brand_summary", "")}
Pain point guess: {lead.get("pain_point_guess", "")}
Outreach angle: {lead.get("outreach_angle", "")}
Enrichment draft: {lead.get("outreach_message", "")}
Greeting to use: {greeting}

Return JSON only: {{"subject": "...", "body": "..."}}"""

    resp = _llm().chat.completions.create(
        model="mistralai/mistral-small-3.1-24b-instruct:free",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.7,
        max_tokens=400,
    )
    raw = resp.choices[0].message.content.strip()
    # strip markdown code fences if present
    raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.MULTILINE).strip()
    try:
        return json.loads(raw)
    except Exception:
        # fallback: return raw as body
        return {"subject": f"Quick question about {lead.get('name', 'your site')}", "body": raw}


def generate_dm(lead: dict, channel: str = "instagram") -> str:
    """
    Returns a short DM string for Instagram or Facebook.
    channel: 'instagram' | 'facebook'
    """
    prompt = f"""You write cold outreach DMs for a freelance web/digital services agency.
Rules:
- 2-3 sentences max
- Very casual, like a real person sliding into DMs
- One specific observation about their business
- One soft question, not a pitch
- No emojis unless it feels very natural
- No "I came across your profile" opener

Business: {lead.get("name")}
Category: {lead.get("category", "")}
Outreach angle: {lead.get("outreach_angle", "")}
Channel: {channel}

Return the message text only, no JSON, no quotes."""

    resp = _llm().chat.completions.create(
        model="mistralai/mistral-small-3.1-24b-instruct:free",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.8,
        max_tokens=150,
    )
    return resp.choices[0].message.content.strip()
