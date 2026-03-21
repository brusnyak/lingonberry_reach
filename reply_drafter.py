"""
outreach/reply_drafter.py
Deterministic first-pass reply drafting for outreach responses.
"""
from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from senders import canonical_sender


@dataclass
class ReplyDraft:
    subject: str
    body: str
    sender_name: str
    sender_address: str
    rationale: str


def _clean_text(value: str) -> str:
    text = " ".join((value or "").split())
    return text.strip()


def _signer(name: str) -> str:
    parts = [part for part in (name or "").split() if part]
    if len(parts) >= 2:
        return f"{parts[0]} {parts[-1][0]}."
    return parts[0] if parts else "Team"


def _language_for_address(address: str, niche: str, content: str = "", lead_language: str = "") -> str:
    addr = (address or "").lower()
    niche = (niche or "").lower()
    body = (content or "").lower()
    if any(token in body for token in ["thank you", "what exactly", "follow-up", "manual processes", "happy to", "hello", "hi,"]):
        return "en"
    if any(token in body for token in ["guten tag", "danke", "bitte", "rückmeldung"]):
        return "de"
        
    lead_lang = (lead_language or "").strip().lower()
    if lead_lang in {"en", "sk", "cs", "de"}:
        return lead_lang
        
    if addr.endswith("@gmail.com"):
        return "en"
    if addr.endswith(".sk") or niche in {"real_estate", "accounting_tax", "dental_medical"}:
        return "sk"
    return "en"


def _safe_json_list(raw: str) -> list[str]:
    if not raw:
        return []
    try:
        data = json.loads(raw)
        if isinstance(data, list):
            generic = {"manual", "manually", "ručn", "manualne", "follow-up", "follow up", "booking", "appointment", "intake"}
            cleaned = []
            for item in data:
                text = str(item).strip()
                if not text:
                    continue
                if text.lower() in generic:
                    continue
                cleaned.append(text)
            return cleaned
    except Exception:
        pass
    return []


def _niche_scope(niche: str, language: str) -> str:
    niche = (niche or "").lower()
    if language == "sk":
        if niche == "real_estate":
            return "prvá reakcia na dopyt, follow-up a to, aby lead nezostal visieť bez odpovede"
        if niche == "accounting_tax":
            return "onboarding klienta, pripomínanie podkladov a menej ručného doháňania dokumentov"
        if niche == "dental_medical":
            return "prvú reakciu na nový dopyt, follow-up a menej výpadkov pri objednávkach"
        return "prvú reakciu, follow-up a jednoduchšie spracovanie nových dopytov"
    if niche == "real_estate":
        return "first response on new enquiries, follow-up, and making sure leads do not sit untouched"
    if niche == "accounting_tax":
        return "client onboarding, reminders for missing documents, and less manual chasing"
    if niche == "dental_medical":
        return "first response on new enquiries, follow-up, and reducing avoidable drop-off"
    return "first response, follow-up, and a cleaner intake flow"


def _question_type(text: str) -> str:
    lower = (text or "").lower()
    if any(token in lower for token in ["price", "cost", "pricing", "koľko", "cena", "ceny"]):
        return "pricing"
    if any(token in lower for token in ["who are you", "who is", "reference", "portfolio", "previous work",
                                         "past work", "example", "case study", "proof", "track record",
                                         "kto ste", "referencie", "ukážka", "príklad"]):
        return "credibility"
    if any(token in lower for token in ["call", "meet", "chat", "hovor", "callu", "stretn", "telefon"]):
        return "call"
    if any(token in lower for token in ["what", "how", "čo", "ako", "co ", "jak", "?"]):
        return "clarify"
    return "generic"


def _subject(original_subject: str, reply_subject: str, language: str) -> str:
    base = _clean_text(reply_subject) or _clean_text(original_subject)
    if base:
        if base.lower().startswith("re:"):
            return base
        return f"Re: {base}"
    return "Re: quick follow-up" if language != "sk" else "Re: krátky follow-up"


def _not_interested_body(language: str, signer: str) -> tuple[str, str]:
    if language == "sk":
        return (
            f"Dobrý deň,\n\nďakujem za odpoveď, beriem na vedomie.\nNebudem to ďalej tlačiť.\n\nS pozdravom,\n{signer}",
            "Close the loop politely and stop the thread.",
        )
    return (
        f"Hi,\n\nthanks for the reply, understood.\nI will leave it there.\n\nBest,\n{signer}",
        "Close the loop politely and stop the thread.",
    )


def _clarify_body(language: str, signer: str, niche: str, qtype: str, pains: list[str]) -> tuple[str, str]:
    scope = _niche_scope(niche, language)
    pain_hint = ""
    if pains:
        joined = ", ".join(pains[:2])
        pain_hint = f" Hlavne okolo {joined}." if language == "sk" else f" Mainly around {joined}."
    if language == "sk":
        if qtype == "credibility":
            body = (
                f"Dobrý deň,\n\njasné. Pracujem s malými firmami na {scope}.\n"
                f"Nie som agentúra — robím to sám, takže viem byť konkrétny a rýchly.\n\n"
                f"Ak chcete, pošlem vám krátky outline toho, čo by som sa pozrel ako prvé u vás, "
                f"a môžete sami posúdiť, či to dáva zmysel.\n\n"
                f"S pozdravom,\n{signer}"
            )
            return body, "Answer credibility question: explain who you are, offer a concrete next step."
        if qtype == "pricing":
            body = (
                f"Dobrý deň,\n\njasné. Cena závisí od toho, aký kus procesu treba riešiť, "
                f"takže najprv si vždy rýchlo pozriem aktuálny stav.\n"
                f"To, čo mám na mysli, je {scope}.{pain_hint}\n\n"
                f"Ak chcete, pošlem krátky konkrétny outline a uvidíte, či to vôbec dáva zmysel.\n\n"
                f"S pozdravom,\n{signer}"
            )
            return body, "Answer pricing softly, avoid hard numbers, offer a short outline."
        if qtype == "call":
            body = (
                f"Dobrý deň,\n\njasné. Mám na mysli hlavne {scope}.{pain_hint}\n"
                f"Kľudne si môžeme dať krátky call a prejsť to.\n\n"
                f"Ak vám to vyhovuje, pošlite mi prosím 2-3 časy, ktoré vám sedia.\n\n"
                f"S pozdravom,\n{signer}"
            )
            return body, "Confirm interest and move toward a short call."
        body = (
            f"Dobrý deň,\n\njasné. Mám na mysli hlavne {scope}.{pain_hint}\n"
            f"Nie veľký projekt na slepo, skôr krátke upratanie toho, kde sa dopyty strácajú alebo zbytočne stoja.\n\n"
            f"Ak chcete, pošlem vám to v 3 bodoch na konkrétnom príklade.\n\n"
            f"S pozdravom,\n{signer}"
        )
        return body, "Answer the question directly, then offer a small concrete next step."
    if qtype == "credibility":
        body = (
            f"Hi,\n\nfair question. I work with small businesses on {scope}.\n"
            f"I do this solo, not through an agency, so I can be specific and move fast.\n\n"
            f"Rather than a portfolio, I find it more useful to send a short outline of what I would look at "
            f"in your specific setup — that way you can judge whether it is relevant before committing to anything.\n\n"
            f"Want me to send that?\n\nBest,\n{signer}"
        )
        return body, "Answer credibility question: explain who you are, offer a concrete next step."
    if qtype == "pricing":
        body = (
            f"Hi,\n\nsure. Pricing depends on how much of the process needs attention, so I usually start by looking at the current setup first.\n"
            f"What I mean here is {scope}.{pain_hint}\n\n"
            f"If useful, I can send a short outline first so you can see whether it is relevant.\n\n"
            f"Best,\n{signer}"
        )
        return body, "Answer pricing softly, avoid hard numbers, offer a short outline."
    if qtype == "call":
        body = (
            f"Hi,\n\nsure. What I mean here is {scope}.{pain_hint}\n"
            f"Happy to jump on a short call and walk through it.\n\n"
            f"If that is easier, send over 2-3 times that work for you.\n\n"
            f"Best,\n{signer}"
        )
        return body, "Confirm interest and move toward a short call."
    body = (
        f"Hi,\n\nsure. What I mean here is {scope}.{pain_hint}\n"
        f"Not a big vague project, more a small cleanup around where replies, follow-up, or intake get stuck.\n\n"
        f"If useful, I can send a 3-point outline on what I would look at first.\n\n"
        f"Best,\n{signer}"
    )
    return body, "Answer the question directly, then offer a small concrete next step."


def _interested_body(language: str, signer: str, niche: str, pains: list[str]) -> tuple[str, str]:
    scope = _niche_scope(niche, language)
    pain_hint = ""
    if pains:
        joined = ", ".join(pains[:2])
        pain_hint = f" Konkrétne okolo {joined}." if language == "sk" else f" Specifically around {joined}."
    if language == "sk":
        body = (
            f"Dobrý deň,\n\nvďaka za odpoveď.\n"
            f"Mám na mysli hlavne {scope}.{pain_hint}\n\n"
            f"Ak chcete, pošlem krátky outline, čo by som sa pozrel ako prvé, a keď to bude dávať zmysel, môžeme sa potom krátko spojiť.\n\n"
            f"S pozdravom,\n{signer}"
        )
        return body, "Positive follow-up: reinforce scope and offer a short outline first."
    body = (
        f"Hi,\n\nthanks for getting back to me.\n"
        f"What I mean is mainly {scope}.{pain_hint}\n\n"
        f"If useful, I can send a short outline of what I would look at first, and if it feels relevant we can jump on a quick call after that.\n\n"
        f"Best,\n{signer}"
    )
    return body, "Positive follow-up: reinforce scope and offer a short outline first."


def build_reply_draft(row: dict) -> ReplyDraft:
    # Always use the address that sent the original outreach — never fall back to a hardcoded default
    sender_address = (
        row.get("original_sender_address")
        or row.get("sender_address")
        or ""
    ).strip().lower()

    # If we still have no address, log a warning and use the first account from env
    if not sender_address:
        import os
        sender_address = os.environ.get("EMAIL_1_ADDRESS", "").strip().lower()

    sender_profile = canonical_sender(
        sender_address,
        row.get("original_sender_name") or row.get("sender_name") or "",
    )
    sender_name = sender_profile["name"]
    niche = row.get("target_niche") or ""
    language = _language_for_address(row.get("from_address") or "", niche, row.get("content") or "", row.get("lead_language") or "")
    signer = _signer(sender_name)
    label = (row.get("label") or "question").lower()
    qtype = _question_type(row.get("content") or "")
    pains = _safe_json_list(row.get("pain_points") or "")
    subject = _subject(row.get("original_subject") or "", row.get("reply_subject") or "", language)

    if label == "not_interested":
        body, rationale = _not_interested_body(language, signer)
    elif label == "interested":
        body, rationale = _interested_body(language, signer, niche, pains)
    else:
        body, rationale = _clarify_body(language, signer, niche, qtype, pains)

    return ReplyDraft(
        subject=subject,
        body=re.sub(r"\n{3,}", "\n\n", body).strip(),
        sender_name=sender_name,
        sender_address=sender_profile["address"],
        rationale=rationale,
    )
