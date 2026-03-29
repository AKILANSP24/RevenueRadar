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

def run_shopify_sim():
    """Publishes mock Shopify order transactions to Supabase raw_events table."""
    load_dotenv()
    
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY")
    events_per_min = int(os.getenv("SIM_EVENTS_PER_MINUTE", 20))
    anomaly_rate = float(os.getenv("SIM_ANOMALY_INJECTION_RATE", 0.05))
    
    client = None
    if create_client and supabase_url and supabase_key:
        try:
            client = create_client(supabase_url, supabase_key)
            logger.info("Shopify Sim: Connected to Supabase.")
        except Exception as e:
            logger.error(f"Shopify Sim: Supabase connection failed: {e}")
    
    if not client:
        logger.warning("Shopify Sim: Running in Dry-Run Mode.")
    
    sleep_time = 60.0 / events_per_min
    plan_tiers = ["basic", "pro", "enterprise", "one_time"]
    regions = ["US", "IN", "EU", "UK", "CA"]
    
    logger.info("Shopify Simulator started.")
    
    while True:
        try:
            amount = round(random.uniform(200.0, 5000.0), 2)
            
            if random.random() < anomaly_rate:
                original = amount
                amount = round(amount * 5.0, 2)
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
                "metadata": {"user_agent": "shopify_sim_script"}
            }
            
            validate_event(data)
            
            if client:
                client.table("raw_events").insert(data).execute()
                logger.info(f"[LIVE - Shopify] {data['event_id']} | ₹{data['amount']}")
            else:
                logger.info(f"[DRY RUN - Shopify] {json.dumps(data)}")
            
            time.sleep(sleep_time)
            
        except KeyboardInterrupt:
            logger.info("Shopify Simulator stopped.")
            break
        except Exception as e:
            logger.error(f"Shopify Sim Error: {e}")
            time.sleep(1)