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

def run_paypal_sim():
    """Publishes mock PayPal invoice transactions to Supabase raw_events table."""
    load_dotenv()
    
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY")
    events_per_min = int(os.getenv("SIM_EVENTS_PER_MINUTE", 20))
    anomaly_rate = float(os.getenv("SIM_ANOMALY_INJECTION_RATE", 0.05))
    
    client = None
    if create_client and supabase_url and supabase_key:
        try:
            client = create_client(supabase_url, supabase_key)
            logger.info("PayPal Sim: Connected to Supabase.")
        except Exception as e:
            logger.error(f"PayPal Sim: Supabase connection failed: {e}")
    
    if not client:
        logger.warning("PayPal Sim: Running in Dry-Run Mode.")
    
    sleep_time = 60.0 / events_per_min
    plan_tiers = ["basic", "pro", "enterprise", "one_time"]
    regions = ["US", "IN", "EU", "UK", "CA"]
    
    logger.info("PayPal Simulator started.")
    
    while True:
        try:
            amount = round(random.uniform(1000.0, 50000.0), 2)
            
            if random.random() < anomaly_rate:
                original = amount
                amount = round(amount * 5.0, 2)
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
                "metadata": {"user_agent": "paypal_sim_script"}
            }
            
            validate_event(data)
            
            if client:
                client.table("raw_events").insert(data).execute()
                logger.info(f"[LIVE - PayPal] {data['event_id']} | ₹{data['amount']}")
            else:
                logger.info(f"[DRY RUN - PayPal] {json.dumps(data)}")
            
            time.sleep(sleep_time)
            
        except KeyboardInterrupt:
            logger.info("PayPal Simulator stopped.")
            break
        except Exception as e:
            logger.error(f"PayPal Sim Error: {e}")
            time.sleep(1)