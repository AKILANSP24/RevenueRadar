import os
import logging
from dotenv import load_dotenv

try:
    from groq import Groq
except ImportError:
    Groq = None

logger = logging.getLogger(__name__)

def explain_anomaly(event_data: dict, z_score: float) -> str:
    """
    Generates a 2-sentence plain-English explanation for a critical
    revenue anomaly using Groq's llama3-8b-8192 model.
    Fails gracefully returning a default string on error.
    """
    default_explanation = f"Critical anomaly detected natively (Severity factor {z_score:.2f}). Further automated context currently unavailable."
    
    if not Groq:
        logger.warning("Groq not installed. Returning default explanation.")
        return default_explanation
        
    load_dotenv()
    api_key = os.getenv("GROQ_API_KEY")
    
    if not api_key or api_key == "your-groq-key":
        logger.warning("GROQ_API_KEY not found or invalid. Returning default explanation.")
        return default_explanation
        
    try:
        client = Groq(api_key=api_key)
        
        prompt = f"""
You are a fast financial operations analyst monitoring a real-time anomaly stream.
An anomaly flagged critical severity (Anomaly Index: {z_score:.2f}).
Event JSON Profile: {event_data}

Provide an exactly 2-sentence, plain-English summary explaining why this transaction is statistically unusual and its potential business impact. Avoid internal jargon.
"""
        response = client.chat.completions.create(
            messages=[
                {"role": "system", "content": "You are a direct, concise financial data assistant."},
                {"role": "user", "content": prompt}
            ],
            model="llama3-8b-8192",
            max_tokens=200,
            temperature=0.4
        )
        
        explanation = response.choices[0].message.content.strip()
        return explanation if explanation else default_explanation
        
    except Exception as e:
        logger.error(f"Groq API error during explanation generation: {e}")
        return default_explanation

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_event = {"amount": 50000.0, "source": "stripe", "event_type": "charge"}
    print(explain_anomaly(test_event, 4.5))
