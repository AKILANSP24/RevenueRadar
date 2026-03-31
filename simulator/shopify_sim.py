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

# Shopify retail: peaks at lunch and evening shopping
HOURLY_MULTIPLIERS = {
    0: 0.1, 1: 0.05, 2: 0.05, 3: 0.05, 4: 0.05,
    5: 0.1, 6: 0.2, 7: 0.3, 8: 0.5, 9: 0.7,
    10: 0.9, 11: 1.1, 12: 1.4, 13: 1.2, 14: 1.0,
    15: 1.0, 16: 1.1, 17: 1.2, 18: 1.5, 19: 1.6,
    20: 1.4, 21: 1.1, 22: 0.7, 23: 0.3
}

def get_realistic_amount():
    """Shopify: mostly small retail orders, occasional bulk."""
    tier = random.choices(
        ['tiny', 'small', 'medium', 'large', 'bulk'],
        weights=[30, 35, 20, 12, 3]
    )[0]
    if tier == 'tiny':
        return round(random.uniform(100, 500), 2)
    elif tier == 'small':
        return round(random.uniform(500, 2000), 2)
    elif tier == 'medium':
        return round(random.uniform(2000, 6000), 2)
    elif tier == 'large':
        return round(random.uniform(6000, 15000), 2)
    else:
        return round(random.uniform(15000, 40000), 2)

def run_shopify_sim():
    load_dotenv()
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY")
    anomaly_rate = float(os.getenv("SIM_ANOMALY_INJECTION_RATE", 0.06))

    client = None
    if create_client and supabase_url and supabase_key:
        try:
            client = create_client(supabase_url, supabase_key)
            logger.info("Shopify Sim: Connected to Supabase.")
        except Exception as e:
            logger.error(f"Shopify Sim: Supabase connection failed: {e}")

    if not client:
        logger.warning("Shopify Sim: Running in Dry-Run Mode.")

    plan_tiers = ["basic", "pro", "enterprise", "one_time"]
    regions = ["US", "IN", "EU", "UK", "CA"]
    logger.info("Shopify Simulator started.")

    while True:
        try:
            hour = datetime.now().hour
            multiplier = HOURLY_MULTIPLIERS.get(hour, 1.0)
            # Shopify fires faster (more orders than invoices)
            sleep_time = round(random.uniform(1.5, 3.5) / max(multiplier, 0.1), 2)
            sleep_time = max(0.8, min(sleep_time, 12.0))

            amount = get_realistic_amount()

            if random.random() < anomaly_rate:
                original = amount
                amount = round(amount * random.uniform(4.0, 9.0), 2)
                logger.warning(f"SHOPIFY ANOMALY injected: {original} -> {amount}")

            data = {
                "event_id": str(uuid.uuid4()),
                "source": "shopify",
                "event_type": "order",
                "amount": amount,
                "currency": "INR",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "customer_id": f"cus_shp_{random.randint(1000, 9999)}",
                "plan_tier": random.choice(plan_tiers),
                "region": random.choice(regions),
                "metadata": {
                    "user_agent": "shopify_sim_script",
                    "hour_multiplier": multiplier
                }
            }

            validate_event(data)

            if client:
                client.table("raw_events").insert(data).execute()
                logger.info(f"[LIVE - Shopify] {data['event_id'][:8]}... | ₹{data['amount']:,.2f} | {data['region']}")
            else:
                logger.info(f"[DRY RUN - Shopify] {json.dumps(data)}")

            time.sleep(sleep_time)

        except KeyboardInterrupt:
            logger.info("Shopify Simulator stopped.")
            break
        except Exception as e:
            logger.error(f"Shopify Sim Error: {e}")
            time.sleep(2)