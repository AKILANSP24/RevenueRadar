import os
import logging
from typing import Dict, Any
from dotenv import load_dotenv

try:
    from supabase import create_client, Client
except ImportError:
    create_client, Client = None, None

logger = logging.getLogger(__name__)

# Initialize client safely as a global cache
_supabase_client = None

def get_supabase_client():
    """Returns the Supabase Client, initializing it if necessary."""
    global _supabase_client
    if _supabase_client is not None:
        return _supabase_client
        
    if not create_client:
        logger.error("supabase Python library is not installed.")
        return None
        
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    
    if not url or not key:
        logger.error("Missing SUPABASE_URL or SUPABASE_KEY in environment variables.")
        return None
        
    try:
        _supabase_client = create_client(url, key)
        return _supabase_client
    except Exception as e:
        logger.error(f"Failed to initialize Supabase client: {e}")
        return None

def calculate_health_score(critical: int, warning: int, avg_zscore: float) -> float:
    """
    Calculates the system's daily Revenue Health Score using the weighted formula.
    
    Formula used:
    score = 100 - ((critical_count x 10) + (warning_count x 3) + (avg_z_score x 2))
    
    Args:
        critical (int): Number of total critical anomalies detected in the day.
        warning (int): Number of warning-level anomalies detected.
        avg_zscore (float): The average absolute Z-Score of the daily anomalies.
        
    Returns:
        float: Computed Health Score mathematically clamped between 0.0 and 100.0.
    """
    score = 100.0 - ((critical * 10.0) + (warning * 3.0) + (avg_zscore * 2.0))
    return max(0.0, min(100.0, float(score)))

def insert_anomaly_event(event_data: Dict[str, Any]) -> None:
    """
    Inserts a newly detected anomaly event directly into Supabase 'anomaly_events'.
    This should sequentially execute ONLY for 'warning' and 'critical' severities.
    
    Gracefully catches and logs insertion issues to prevent crashing the root pipeline process.
    
    Args:
        event_data (dict): Validated dictionary mapping to the schema of anomaly_events.
    """
    severity = event_data.get("severity", "normal")
    if severity == "normal":
        logger.debug(f"Event {event_data.get('event_id')} is 'normal'. Insertion skipped.")
        return

    client = get_supabase_client()
    if not client:
        return
        
    try:
        # Pushing a single record using synchronous insert
        client.table("anomaly_events").insert(event_data).execute()
        logger.info(f"Anomaly Event {event_data.get('event_id')} securely inserted into DB")
    except Exception as e:
        logger.error(f"Supabase DB Insert Failed for event {event_data.get('event_id')}: {e}")

def upsert_daily_health_score(date: str, stats: Dict[str, Any]) -> None:
    """
    Upserts (Updates or Inserts if missing) the global aggregate scores for 
    the current localized date into 'daily_health_scores'.
    
    Expected keys inside stats dictionary:
    - critical_count: Count of critical events
    - warning_count: Count of warning events 
    - avg_zscore: Average absolute magnitude of anomalies
    - total_events: Sum total of financial actions processed today
    - anomaly_count: Count of warning + critical events
    
    Args:
        date (str): Unique identifying date string 'YYYY-MM-DD'.
        stats (dict): Dictionary containing counts to trigger computation mapping.
    """
    client = get_supabase_client()
    if not client:
        return
        
    critical = stats.get("critical_count", 0)
    warning = stats.get("warning_count", 0)
    avg_zscore = stats.get("avg_zscore", 0.0)
    
    # Process dynamically the mathematical bounding rule
    score = calculate_health_score(critical, warning, avg_zscore)
    
    record = {
        "date": date,
        "health_score": score,
        "total_events": stats.get("total_events", 0),
        "anomaly_count": stats.get("anomaly_count", 0),
        "critical_count": critical
    }
    
    try:
        # Utilize on_conflict argument targeting the unique DATE column
        client.table("daily_health_scores").upsert(record, on_conflict="date").execute()
        logger.debug(f"Daily aggregate score for {date} upserted safely.")
    except Exception as e:
        logger.error(f"Supabase DB Upsert failed for Health Score {date}: {e}")
