import uuid
from dataclasses import dataclass
from typing import Dict, Any
from datetime import datetime

@dataclass
class FinancialEvent:
    """Unified financial event schema across all sources."""
    event_id: str
    source: str
    event_type: str
    amount: float
    currency: str
    timestamp: str
    customer_id: str
    plan_tier: str
    region: str
    metadata: Dict[str, Any]

def validate_event(data: Dict[str, Any]) -> FinancialEvent:
    """
    Validates a dictionary against the Unified Event Schema.
    Raises ValueError if validation fails.
    """
    allowed_sources = {"stripe", "shopify", "paypal"}
    allowed_types = {"charge", "subscription", "refund", "order", "invoice"}
    allowed_tiers = {"basic", "pro", "enterprise", "one_time"}

    if "event_id" not in data or not isinstance(data["event_id"], str):
        raise ValueError("event_id must be a string UUID")
        
    if data.get("source") not in allowed_sources:
        raise ValueError(f"source must be one of {allowed_sources}")
        
    if data.get("event_type") not in allowed_types:
        raise ValueError(f"event_type must be one of {allowed_types}")
        
    if not isinstance(data.get("amount"), (int, float)):
        raise ValueError("amount must be a float")
        
    if data.get("currency") != "INR":
        raise ValueError("currency must be 'INR'")
        
    try:
        # Simple ISO format validation
        datetime.fromisoformat(data["timestamp"].replace('Z', '+00:00'))
    except (KeyError, ValueError, TypeError):
        raise ValueError("timestamp must be a valid ISO8601 string")
        
    if not isinstance(data.get("customer_id"), str):
        raise ValueError("customer_id must be a string")
        
    if data.get("plan_tier") not in allowed_tiers:
        raise ValueError(f"plan_tier must be one of {allowed_tiers}")
        
    if not isinstance(data.get("region"), str):
        raise ValueError("region must be a string")
        
    if not isinstance(data.get("metadata"), dict):
        raise ValueError("metadata must be a dictionary")
        
    return FinancialEvent(
        event_id=data["event_id"],
        source=data["source"],
        event_type=data["event_type"],
        amount=float(data["amount"]),
        currency=data["currency"],
        timestamp=data["timestamp"],
        customer_id=data["customer_id"],
        plan_tier=data["plan_tier"],
        region=data["region"],
        metadata=data["metadata"]
    )
