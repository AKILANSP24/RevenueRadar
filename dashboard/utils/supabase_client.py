import os
import logging
from datetime import datetime, date
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

try:
    from supabase import create_client
except ImportError:
    create_client = None

_client = None

def get_client():
    """Returns singleton Supabase client."""
    global _client
    if _client:
        return _client
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key or not create_client:
        logger.error("Supabase credentials missing.")
        return None
    try:
        _client = create_client(url, key)
        return _client
    except Exception as e:
        logger.error(f"Supabase init failed: {e}")
        return None

def get_recent_events(limit=50):
    """Returns most recent anomaly events."""
    client = get_client()
    if not client:
        return []
    try:
        res = client.table("anomaly_events").select("*").order(
            "created_at", desc=True).limit(limit).execute()
        return res.data or []
    except Exception as e:
        logger.error(f"get_recent_events error: {e}")
        return []

def get_daily_health_scores(days=30):
    """Returns daily health scores for last N days."""
    client = get_client()
    if not client:
        return []
    try:
        res = client.table("daily_health_scores").select("*").order(
            "date", desc=True).limit(days).execute()
        return res.data or []
    except Exception as e:
        logger.error(f"get_daily_health_scores error: {e}")
        return []

def get_anomaly_counts_by_hour_day():
    """Returns anomaly counts grouped by hour and day."""
    client = get_client()
    if not client:
        return []
    try:
        res = client.table("anomaly_events").select(
            "timestamp, severity").execute()
        return res.data or []
    except Exception as e:
        logger.error(f"get_anomaly_counts_by_hour_day error: {e}")
        return []

def get_source_breakdown():
    """Returns anomaly counts per source."""
    client = get_client()
    if not client:
        return []
    try:
        res = client.table("anomaly_events").select(
            "source").execute()
        return res.data or []
    except Exception as e:
        logger.error(f"get_source_breakdown error: {e}")
        return []

def get_today_stats():
    """Returns today's aggregated stats."""
    client = get_client()
    if not client:
        return {}
    try:
        today = date.today().isoformat()
        res = client.table("daily_health_scores").select(
            "*").eq("date", today).execute()
        if res.data:
            return res.data[0]
        return {
            "health_score": 100.0,
            "total_events": 0,
            "anomaly_count": 0,
            "critical_count": 0
        }
    except Exception as e:
        logger.error(f"get_today_stats error: {e}")
        return {}