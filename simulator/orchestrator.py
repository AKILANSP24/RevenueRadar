import sys
import os
import threading
import logging
import time

# Configure root logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Update path to avoid issues with absolute imports in simulators
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from simulator.stripe_sim import run_stripe_sim
from simulator.shopify_sim import run_shopify_sim
from simulator.paypal_sim import run_paypal_sim

def main():
    logger.info("Starting RevenueRadar Simulation Orchestrator...")
    
    # Create threads
    t_stripe = threading.Thread(target=run_stripe_sim, name="StripeThread", daemon=True)
    t_shopify = threading.Thread(target=run_shopify_sim, name="ShopifyThread", daemon=True)
    t_paypal = threading.Thread(target=run_paypal_sim, name="PayPalThread", daemon=True)
    
    # Start threads
    t_stripe.start()
    time.sleep(0.5) # staggering start slightly
    t_shopify.start()
    time.sleep(0.5)
    t_paypal.start()
    
    # Keep main thread alive to handle keyboard interrupt
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("\nCaught KeyboardInterrupt. Shutting down all simulators...")
        sys.exit(0)

if __name__ == "__main__":
    main()
