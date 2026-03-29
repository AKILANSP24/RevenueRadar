import os
import logging
import time
from datetime import datetime
from typing import List, Dict, Any

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    pa, pq = None, None

logger = logging.getLogger(__name__)

class EventBuffer:
    """
    Buffers processed events and flushes them to local
    Parquet files under the data/events/ directory.
    Auto-flushes when buffer hits 10 events or 30 seconds pass.
    """
    
    def __init__(self, max_size: int = 10, flush_interval: int = 30):
        """Initializes the buffer with size and time limits."""
        self.max_size = max_size
        self.flush_interval = flush_interval
        self.buffer: List[Dict[str, Any]] = []
        self.last_flush_time = time.time()
    
    def add_event(self, event_dict: Dict[str, Any]) -> None:
        """
        Adds a processed event to the buffer.
        Triggers flush if buffer is full or time limit exceeded.
        
        Args:
            event_dict: Processed event as a dictionary.
        """
        self.buffer.append(event_dict)
        
        time_elapsed = time.time() - self.last_flush_time
        if len(self.buffer) >= self.max_size or time_elapsed >= self.flush_interval:
            self.flush_to_parquet()
    
    def flush_to_parquet(self) -> None:
        """
        Writes all buffered events to a Parquet file.
        Path: data/events/YYYY-MM-DD/HH-MM-SS.parquet
        Clears buffer after writing.
        """
        if not self.buffer:
            return
        
        if not pa or not pq:
            logger.warning("pyarrow not installed. Skipping parquet flush.")
            self.buffer.clear()
            return
        
        try:
            now = datetime.utcnow()
            date_str = now.strftime("%Y-%m-%d")
            time_str = now.strftime("%H-%M-%S")
            
            folder = os.path.join("data", "events", date_str)
            os.makedirs(folder, exist_ok=True)
            
            filepath = os.path.join(folder, f"{time_str}.parquet")
            
            # Convert list of dicts to pyarrow table
            # Flatten metadata to string for parquet compatibility
            clean_buffer = []
            for event in self.buffer:
                clean = dict(event)
                if isinstance(clean.get("metadata"), dict):
                    import json
                    clean["metadata"] = json.dumps(clean["metadata"])
                clean_buffer.append(clean)
            
            table = pa.Table.from_pylist(clean_buffer)
            pq.write_table(table, filepath)
            
            logger.info(f"Flushed {len(self.buffer)} events to {filepath}")
            self.buffer.clear()
            self.last_flush_time = time.time()
            
        except Exception as e:
            logger.error(f"Parquet flush failed: {e}")
            self.buffer.clear()