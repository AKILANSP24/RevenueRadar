import os
import json
import time
import uuid
import random
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv

import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from pipeline.schema import validate_event

try:
    from supabase import create_client
except ImportError:
    create_client = None

logger = logging.getLogger(__name__)

# Realistic hourly multipliers (low at night, peak business hours)
HOURLY_MULTIPLIERS = {
    0: 0.1, 1: 0.1, 2: 0.05, 3: 0.05, 4: 0.05,
    5: 0.1, 6: 0.2, 7: 0.4, 8: 0.7, 9: 1.0,
    10: 1.2, 11: 1.3, 12: 1.4, 13: 1.3, 14: 1.2,
    15: 1.1, 16: 1.0, 17: 0.9, 18: 0.8, 19: 0.7,
    20: 0.5, 21: 0.4, 22: 0.3, 23: 0.2
}

def get_realistic_amount():
    """Stripe: mix of small charges, subscriptions, and occasional large enterprise."""
    tier = random.choices(
        ['micro', 'small', 'medium', 'large', 'enterprise'],
        weights=[20, 40, 25, 12, 3]
    )[0]
    if tier == 'micro':
        return round(random.uniform(50, 500), 2)
    elif tier == 'small':
        return round(random.uniform(500, 3000), 2)
    elif tier == 'medium':
        return round(random.uniform(3000, 8000), 2)
    elif tier == 'large':
        return round(random.uniform(8000, 25000), 2)
    else:
        return round(random.uniform(25000, 80000), 2)

def run_stripe_sim():
    load_dotenv()
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY")
    anomaly_rate = float(os.getenv("SIM_ANOMALY_INJECTION_RATE", 0.04))

    client = None
    if create_client and supabase_url and supabase_key:
        try:
            client = create_client(supabase_url, supabase_key)
            logger.info("Stripe Sim: Connected to Supabase.")
        except Exception as e:
            logger.error(f"Stripe Sim: Supabase connection failed: {e}")

    if not client:
        logger.warning("Stripe Sim: Running in Dry-Run Mode.")

    event_types = ["charge", "subscription", "refund"]
    plan_tiers = ["basic", "pro", "enterprise", "one_time"]
    regions = ["US", "IN", "EU", "UK", "CA"]
    logger.info("Stripe Simulator started.")

    while True:
        try:
            hour = datetime.now().hour
            multiplier = HOURLY_MULTIPLIERS.get(hour, 1.0)
            # Sleep inversely proportional to multiplier (busier = faster events)
            sleep_time = round(random.uniform(2.5, 4.5) / max(multiplier, 0.1), 2)
            sleep_time = max(1.0, min(sleep_time, 15.0))

            amount = get_realistic_amount()

            if random.random() < anomaly_rate:
                original = amount
                # Anomaly: spike between 4x and 8x
                amount = round(amount * random.uniform(4.0, 8.0), 2)
                logger.warning(f"STRIPE ANOMALY injected: {original} -> {amount}")

            data = {
                "event_id": str(uuid.uuid4()),
                "source": "stripe",
                "event_type": random.choice(event_types),
                "amount": amount,
                "currency": "INR",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "customer_id": f"cus_str_{random.randint(1000, 9999)}",
                "plan_tier": random.choice(plan_tiers),
                "region": random.choice(regions),
                "metadata": {
                    "user_agent": "stripe_sim_script",
                    "hour_multiplier": multiplier
                }
            }

            validate_event(data)

            if client:
                client.table("raw_events").insert(data).execute()
                logger.info(f"[LIVE - Stripe] {data['event_id'][:8]}... | ₹{data['amount']:,.2f} | {data['region']}")
            else:
                logger.info(f"[DRY RUN - Stripe] {json.dumps(data)}")

            time.sleep(sleep_time)

        except KeyboardInterrupt:
            logger.info("Stripe Simulator stopped.")
            break
        except Exception as e:
            logger.error(f"Stripe Sim Error: {e}")
            time.sleep(2)