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

HOURLY_MULTIPLIERS = {
    0: 0.15, 1: 0.1, 2: 0.05, 3: 0.05, 4: 0.08,
    5: 0.15, 6: 0.3, 7: 0.5, 8: 0.8, 9: 1.1,
    10: 1.3, 11: 1.4, 12: 1.5, 13: 1.3, 14: 1.2,
    15: 1.0, 16: 0.9, 17: 0.85, 18: 0.7, 19: 0.6,
    20: 0.45, 21: 0.35, 22: 0.25, 23: 0.2
}

def get_realistic_amount():
    """PayPal: higher value invoices, B2B focus."""
    tier = random.choices(
        ['small', 'medium', 'large', 'enterprise'],
        weights=[25, 40, 25, 10]
    )[0]
    if tier == 'small':
        return round(random.uniform(1000, 5000), 2)
    elif tier == 'medium':
        return round(random.uniform(5000, 20000), 2)
    elif tier == 'large':
        return round(random.uniform(20000, 60000), 2)
    else:
        return round(random.uniform(60000, 150000), 2)

def run_paypal_sim():
    load_dotenv()
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY")
    anomaly_rate = float(os.getenv("SIM_ANOMALY_INJECTION_RATE", 0.04))

    client = None
    if create_client and supabase_url and supabase_key:
        try:
            client = create_client(supabase_url, supabase_key)
            logger.info("PayPal Sim: Connected to Supabase.")
        except Exception as e:
            logger.error(f"PayPal Sim: Supabase connection failed: {e}")

    if not client:
        logger.warning("PayPal Sim: Running in Dry-Run Mode.")

    plan_tiers = ["basic", "pro", "enterprise", "one_time"]
    regions = ["US", "IN", "EU", "UK", "CA"]
    logger.info("PayPal Simulator started.")

    while True:
        try:
            hour = datetime.now().hour
            multiplier = HOURLY_MULTIPLIERS.get(hour, 1.0)
            sleep_time = round(random.uniform(3.0, 5.5) / max(multiplier, 0.1), 2)
            sleep_time = max(1.5, min(sleep_time, 18.0))

            amount = get_realistic_amount()

            if random.random() < anomaly_rate:
                original = amount
                amount = round(amount * random.uniform(3.5, 7.0), 2)
                logger.warning(f"PAYPAL ANOMALY injected: {original} -> {amount}")

            data = {
                "event_id": str(uuid.uuid4()),
                "source": "paypal",
                "event_type": "invoice",
                "amount": amount,
                "currency": "INR",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "customer_id": f"cus_ppl_{random.randint(1000, 9999)}",
                "plan_tier": random.choice(plan_tiers),
                "region": random.choice(regions),
                "metadata": {
                    "user_agent": "paypal_sim_script",
                    "hour_multiplier": multiplier
                }
            }

            validate_event(data)

            if client:
                client.table("raw_events").insert(data).execute()
                logger.info(f"[LIVE - PayPal] {data['event_id'][:8]}... | ₹{data['amount']:,.2f} | {data['region']}")
            else:
                logger.info(f"[DRY RUN - PayPal] {json.dumps(data)}")

            time.sleep(sleep_time)

        except KeyboardInterrupt:
            logger.info("PayPal Simulator stopped.")
            break
        except Exception as e:
            logger.error(f"PayPal Sim Error: {e}")
            time.sleep(2)