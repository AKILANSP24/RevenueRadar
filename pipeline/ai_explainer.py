import os
import logging
import anthropic
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

_client = None

def get_client():
    global _client
    if _client is None:
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            logger.error("ANTHROPIC_API_KEY not set in .env")
            return None
        _client = anthropic.Anthropic(api_key=api_key)
    return _client


def explain_anomaly(event: dict, zscore: float, baseline: dict) -> str:
    """
    Uses Claude to generate a 2-sentence plain English explanation
    for a flagged anomaly event.
    """
    client = get_client()
    if not client:
        return "AI explanation unavailable — API key missing."

    prompt = (
        f"A {event.get('severity', 'warning').upper()} revenue anomaly was detected.\n"
        f"Payment Source: {event.get('source', 'unknown').capitalize()}\n"
        f"Transaction Amount: ${event.get('amount', 0):.2f}\n"
        f"Z-Score: {zscore:.2f} standard deviations from baseline\n"
        f"Baseline Mean: ${baseline.get('mean', 0):.2f}\n"
        f"Baseline Std Dev: ${baseline.get('std', 0):.2f}\n\n"
        f"In exactly 2 sentences, explain why this transaction was flagged "
        f"and what it could indicate for a fraud analyst. Be specific and concise."
    )

    try:
        message = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=120,
            messages=[{"role": "user", "content": prompt}]
        )
        return message.content[0].text.strip()
    except Exception as e:
        logger.error(f"Claude API error: {e}")
        return f"AI explanation unavailable — {str(e)}"