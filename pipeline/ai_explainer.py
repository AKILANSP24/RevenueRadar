import os
import time
import logging
from groq import Groq
from dotenv import load_dotenv
from datetime import datetime, timezone
from typing import List, Dict

load_dotenv()
logger = logging.getLogger(__name__)

_client = None

# Rolling memory of recent anomalies
_recent_anomalies: List[Dict] = []
MAX_HISTORY = 50

# Per-source risk tracking
_source_risk: Dict[str, Dict] = {
    "stripe":  {"flags": 0, "critical": 0, "warning": 0, "total_zscore": 0.0, "last_flag": None},
    "paypal":  {"flags": 0, "critical": 0, "warning": 0, "total_zscore": 0.0, "last_flag": None},
    "shopify": {"flags": 0, "critical": 0, "warning": 0, "total_zscore": 0.0, "last_flag": None},
}


def get_client():
    global _client
    if _client is None:
        api_key = os.getenv("GROQ_API_KEY")
        if not api_key:
            logger.error("GROQ_API_KEY not set in .env")
            return None
        _client = Groq(api_key=api_key)
    return _client


def get_risk_level(flags: int, critical: int) -> str:
    """Compute risk level from flag count and critical count."""
    if critical >= 2 or flags >= 8:
        return "CRITICAL"
    elif critical >= 1 or flags >= 4:
        return "HIGH"
    elif flags >= 2:
        return "MEDIUM"
    elif flags >= 1:
        return "LOW"
    return "CLEAR"


def get_source_risk_summary() -> Dict[str, Dict]:
    """Returns risk summary for all sources."""
    summary = {}
    for src, data in _source_risk.items():
        summary[src] = {
            "flags": data["flags"],
            "critical": data["critical"],
            "warning": data["warning"],
            "risk_level": get_risk_level(data["flags"], data["critical"]),
            "avg_zscore": round(data["total_zscore"] / data["flags"], 2) if data["flags"] > 0 else 0.0,
            "last_flag": data["last_flag"],
        }
    return summary


def _build_context_summary() -> str:
    """Builds a brief summary of recent anomaly history."""
    if not _recent_anomalies:
        return "No prior anomalies in this session."

    source_counts: Dict[str, int] = {}
    severity_counts: Dict[str, int] = {}
    for a in _recent_anomalies[-10:]:  # last 10 only
        src = a.get("source", "unknown")
        sev = a.get("severity", "warning")
        source_counts[src] = source_counts.get(src, 0) + 1
        severity_counts[sev] = severity_counts.get(sev, 0) + 1

    parts = [f"{c} {s.capitalize()}" for s, c in source_counts.items()]
    critical = severity_counts.get("critical", 0)
    warning = severity_counts.get("warning", 0)

    return (
        f"Last 10 anomalies: {', '.join(parts)}. "
        f"{critical} critical, {warning} warning."
    )


def _detect_burst() -> str:
    """Check if 3+ anomalies occurred in last 5 minutes."""
    now = datetime.now(timezone.utc).timestamp()
    recent = [
        a for a in _recent_anomalies
        if a.get("ts") and (now - a["ts"]) <= 300
    ]
    if len(recent) >= 5:
        return f"BURST ALERT — {len(recent)} anomalies in last 5 min."
    elif len(recent) >= 3:
        return f"Elevated activity — {len(recent)} anomalies in last 5 min."
    return ""


def explain_anomaly(event: dict, zscore: float, baseline: dict) -> str:
    """
    Uses Groq Llama-3.3 to generate a short, actionable, risk-aware
    explanation for a flagged anomaly event.
    """
    global _recent_anomalies, _source_risk

    client = get_client()
    if not client:
        return "AI explanation unavailable — GROQ_API_KEY missing."

    source = event.get("source", "unknown")
    severity = event.get("severity", "warning")

    # Update rolling history
    _recent_anomalies.append({
        "source": source,
        "severity": severity,
        "amount": event.get("amount"),
        "zscore": zscore,
        "ts": datetime.now(timezone.utc).timestamp(),
    })
    if len(_recent_anomalies) > MAX_HISTORY:
        _recent_anomalies.pop(0)

    # Update per-source risk tracker
    if source in _source_risk:
        _source_risk[source]["flags"] += 1
        _source_risk[source]["total_zscore"] += abs(zscore)
        _source_risk[source]["last_flag"] = datetime.now(timezone.utc).strftime("%H:%M")
        if severity == "critical":
            _source_risk[source]["critical"] += 1
        else:
            _source_risk[source]["warning"] += 1

    # Build context
    src_data = _source_risk.get(source, {})
    flags = src_data.get("flags", 1)
    risk_level = get_risk_level(flags, src_data.get("critical", 0))
    source_label = source.capitalize()
    context = _build_context_summary()
    burst = _detect_burst()

    ordinal = (
        f"{flags}{'st' if flags == 1 else 'nd' if flags == 2 else 'rd' if flags == 3 else 'th'}"
    )

    prompt = f"""You are a concise fraud analyst. A {severity.upper()} anomaly was just detected.

Source: {source_label} | Severity: {severity.upper()} | Session risk level: {risk_level}
This is the {ordinal} {source_label} flag this session.
{f"BURST WARNING: {burst}" if burst else ""}
Session context: {context}

Write exactly 2 SHORT sentences (max 20 words each):
1. What makes this {source_label} transaction suspicious right now (mention risk level and session pattern)
2. The single most important action the fraud analyst must take

Rules:
- Do NOT repeat the Z-score number or exact dollar amount
- Reference the session risk level ({risk_level}) naturally
- If burst detected, mention it urgently
- Start sentence 1 with "{source_label}"
- Be direct and actionable, no generic phrases"""

    try:
        response = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=80,
            temperature=0.35
        )
        time.sleep(1)
        return response.choices[0].message.content.strip()
    except Exception as e:
        logger.error(f"Groq API error: {e}")
        return f"AI explanation unavailable — {str(e)}"