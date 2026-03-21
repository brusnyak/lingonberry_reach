"""
outreach/frameworks.py
Cold email copywriting frameworks mapped from `material/` best practices.
These frameworks (PAS, AIDA, Minimal Question, Value Offer) provide a structure
for both deterministic template generation and LLM-based drafting.
"""

FRAMEWORKS = {
    "PAS": {
        "name": "Problem - Agitation - Solution",
        "description": "Identify a pain point, agitate it, and offer our unique solution.",
        "structure": [
            "Greeting: Simple and professional.",
            "Problem (Icebreaker): Compliment or observation leading into a specific problem.",
            "Agitation: Highlight the cost of not solving it (e.g., losing 74% of traffic, wasting 5 hours).",
            "Solution: Present our offer as the unique, low-risk fix.",
            "CTA: Single, low-pressure question (e.g., open for a 5-min call?)."
        ],
        "example": "Hey {name},\n\nI was going through your articles on {website}...\n\nProofreading is fundamental... This can be achieved without wasting 5 hours using {tool}.\n\nCan I interest you in a call for a brief walkthrough?"
    },
    "AIDA": {
        "name": "Attention - Interest - Desire - Action",
        "description": "Classic marketing framework applied to cold email. Great for middle-stage funnels.",
        "structure": [
            "Attention: Highly personalized icebreaker (recent news, promotion).",
            "Interest: Relevant problem statement.",
            "Desire: Introduce a competitor case study or relevant testimonial.",
            "Action: Low-friction soft close."
        ],
        "example": "Just read your story on Bloomberg... You recently showed interest in {problem}. We teamed up with {competitor} who brought in 35k MRR using {product}. Watch the 2-min breakdown here: {link}."
    },
    "MINIMAL_QUESTION": {
        "name": "Minimal Question",
        "description": "Very short, direct question designed solely to spark a reply.",
        "structure": [
            "Greeting.",
            "Context (Optional): Where I found them or what I was looking at.",
            "Question: A direct internal-process question about their current workflow.",
            "Close: 'Happy to keep it short' or similar soft close."
        ],
        "example": "Hi {name},\n\nI was looking at properties recently and a few agents took 3+ days to reply.\n\nMade me curious: do you handle new enquiries manually too?\n\nHappy to keep it short."
    },
    "VALUE_OFFER": {
        "name": "Value Offer (Giveaway)",
        "description": "Offer upfront value (lead magnet, loom, outline) without asking for a meeting.",
        "structure": [
            "Greeting.",
            "Observation: Noticed a specific gap in their workflow.",
            "Offer: Offer to send a short outline or video showing how to fix it.",
            "CTA: 'If useful, I can send a 3-point outline. Let me know.'"
        ],
        "example": "Hi {name},\n\nI noticed you don't have a direct booking flow on the site.\n\nWe usually help clinics fix this to reduce drop-off. If useful, I can send a quick outline of how we'd approach it for you.\n\nLet me know."
    }
}

def get_framework(name: str) -> dict | None:
    """Retrieve a specific cold email framework by name."""
    return FRAMEWORKS.get(name.upper())

def list_frameworks() -> list[str]:
    """List all available framework names."""
    return list(FRAMEWORKS.keys())
