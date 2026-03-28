import os
import json
import time
import uuid
import random
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv

try:
    from google.cloud import pubsub_v1
except ImportError:
    pubsub_v1 = None

import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from pipeline.schema import validate_event

logger = logging.getLogger(__name__)

def run_shopify_sim():
    """Publishes mock Shopify order transactions."""
    load_dotenv()
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
    topic_id = os.getenv("PUBSUB_TOPIC")
    events_per_min = int(os.getenv("SIM_EVENTS_PER_MINUTE", 20))
    anomaly_rate = float(os.getenv("SIM_ANOMALY_INJECTION_RATE", 0.05))
    
    auth_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    has_creds = auth_path and os.path.exists(auth_path)
    
    publisher = None
    topic_path = None
    if pubsub_v1 and has_creds:
        try:
            publisher = pubsub_v1.PublisherClient()
            topic_path = publisher.topic_path(project_id, topic_id)
            logger.info(f"Shopify Sim: Connected to Pub/Sub topic {topic_path}")
        except Exception as e:
            logger.error(f"Shopify Sim: Failed to connect to Pub/Sub: {e}")
            publisher = None
            
    if not publisher:
        logger.warning("Shopify Sim: Running in Dry-Run Mode. No GCP credentials found.")
        
    sleep_time = 60.0 / events_per_min
    event_types = ["order"]
    plan_tiers = ["basic", "pro", "enterprise", "one_time"]
    regions = ["US", "IN", "EU", "UK", "CA"]
    
    logger.info("Shopify Simulator started.")
    
    while True:
        try:
            amount = round(random.uniform(200.0, 5000.0), 2)
            
            # Inject anomaly (random spike)
            if random.random() < anomaly_rate:
                original = amount
                amount = round(amount * 5.0, 2)
                logger.warning(f"SHOPIFY ANOMALY injected: {original} -> {amount}")
                
            data = {
                "event_id": str(uuid.uuid4()),
                "source": "shopify",
                "event_type": random.choice(event_types),
                "amount": amount,
                "currency": "INR",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "customer_id": f"cus_shp_{random.randint(1000, 9999)}",
                "plan_tier": random.choice(plan_tiers),
                "region": random.choice(regions),
                "metadata": {"user_agent": "shopify_sim_script"}
            }
            
            # Validate generated schema before publishing
            _ = validate_event(data)
            payload = json.dumps(data).encode("utf-8")
            
            if publisher and topic_path:
                future = publisher.publish(topic_path, payload)
                future.result()
                logger.debug(f"Shopify published: {data['event_id']}")
            else:
                logger.info(f"[DRY RUN - Shopify] {json.dumps(data)}")
                
            time.sleep(sleep_time)
            
        except KeyboardInterrupt:
            logger.info("Shopify Simulator stopped.")
            break
        except Exception as e:
            logger.error(f"Shopify Sim Error: {e}")
            time.sleep(1)
