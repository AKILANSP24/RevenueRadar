import os
import sys
import time
import logging
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pipeline.schema import validate_event
from pipeline.anomaly_engine import AnomalyEngine
from pipeline.storage import EventBuffer
from pipeline.database import insert_anomaly_event, upsert_daily_health_score
from pipeline.ai_explainer import explain_anomaly

try:
    from supabase import create_client
except ImportError:
    create_client = None

engine = AnomalyEngine()
buffer = EventBuffer(max_size=10, flush_interval=30)

daily_stats = {
    "total_events": 0,
    "anomaly_count": 0,
    "critical_count": 0,
    "warning_count": 0,
    "zscore_sum": 0.0
}

# Deduplication: track already-processed event_ids in memory
processed_event_ids = set()


def process_event(raw: dict) -> None:
    global daily_stats

    # --- Deduplication check ---
    event_id = raw.get("event_id")
    if event_id and event_id in processed_event_ids:
        return
    if event_id:
        processed_event_ids.add(event_id)

    try:
        event = validate_event(raw)
    except ValueError as e:
        logger.warning(f"Validation failed: {e}")
        return

    ts = datetime.fromisoformat(event.timestamp.replace("Z", "+00:00"))
    hour = ts.hour
    day = ts.weekday()
    date_str = ts.strftime("%Y-%m-%d")

    engine.update_baseline(hour, day, event.amount)
    zscore = engine.compute_zscore(hour, day, event.amount)
    severity = engine.classify_severity(zscore)
    baseline_stats = engine.get_baseline_stats(hour, day)

    logger.info(
        f"[{severity.upper()}] {event.source} | "
        f"${event.amount:.2f} | Z={zscore:.2f} | "
        f"Baseline=${baseline_stats['mean']:.2f}"
    )

    enriched = {
        "event_id": event.event_id,
        "source": event.source,
        "amount": event.amount,
        "timestamp": event.timestamp,
        "severity": severity,
        "z_score": zscore,
        "baseline_mean": baseline_stats["mean"],
        "baseline_std": baseline_stats["std"],
        "ai_explanation": None
    }

    buffer.add_event(enriched)
    daily_stats["total_events"] += 1

    if severity in ("warning", "critical"):
        daily_stats["anomaly_count"] += 1
        daily_stats["zscore_sum"] += abs(zscore)

        if severity == "critical":
            daily_stats["critical_count"] += 1
        else:
            daily_stats["warning_count"] += 1

        # AI explanation for BOTH warning and critical
        explanation = explain_anomaly(enriched, zscore, baseline_stats)
        enriched["ai_explanation"] = explanation
        logger.info(f"AI: {explanation}")

        insert_anomaly_event(enriched)

        avg_zscore = (
            daily_stats["zscore_sum"] / daily_stats["anomaly_count"]
            if daily_stats["anomaly_count"] > 0
            else 0.0
        )

        upsert_daily_health_score(date_str, {
            "critical_count": daily_stats["critical_count"],
            "warning_count": daily_stats["warning_count"],
            "avg_zscore": avg_zscore,
            "total_events": daily_stats["total_events"],
            "anomaly_count": daily_stats["anomaly_count"]
        })


def main():
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY")

    if not create_client or not supabase_url or not supabase_key:
        logger.error("Supabase credentials missing. Check .env file.")
        return

    client = create_client(supabase_url, supabase_key)
    logger.info("RevenueRadar Pipeline started. Listening for events...")

    last_processed_id = None

    while True:
        try:
            query = client.table("raw_events").select("*").order("created_at")

            if last_processed_id:
                query = query.gt("id", last_processed_id)

            response = query.limit(50).execute()
            events = response.data

            if events:
                for event in events:
                    process_event(event)
                    last_processed_id = event["id"]

            time.sleep(3)

        except KeyboardInterrupt:
            logger.info("Pipeline stopped.")
            buffer.flush_to_parquet()
            break
        except Exception as e:
            logger.error(f"Pipeline error: {e}")
            time.sleep(5)


if __name__ == "__main__":
    main()