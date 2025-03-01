from typing import Dict, Any
import redis
import time
import logging

class VisitCounterService:
    _instance = None
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        # Dictionary to keep track of in-memory visit counts
        self._visit_counts: Dict[str, int] = {}
        
        # Establish connection to Redis database
        self.redis_client = redis.Redis(
            host='redis1',
            port=6379,
            decode_responses=True
        )
        
        # Cache storage to reduce redundant queries
        self._cache: Dict[str, Dict[str, Any]] = {}
        self.cache_ttl = 5  # Cache expiration time (in seconds)
        
        # Buffer for batch writing visit counts to Redis
        self._write_buffer: Dict[str, int] = {}
        self.flush_interval = 30  # Buffer flush interval (in seconds)
        self.last_flush_time = time.time()
        
        self._initialized = True

    async def increment_visit(self, page_id: str) -> Dict[str, Any]:
        """Increment the visit count for a given page."""
        try:
            # Check if it's time to flush the buffer to Redis
            current_time = time.time()
            if current_time - self.last_flush_time >= self.flush_interval:
                await self._flush_buffer()
                self.last_flush_time = current_time
            
            # Add page visit count to the buffer
            if page_id not in self._write_buffer:
                self._write_buffer[page_id] = 0
            self._write_buffer[page_id] += 1
            
            # Retrieve existing count from Redis
            try:
                redis_count = int(self.redis_client.get(f"page:{page_id}") or 0)
            except Exception as e:
                logging.error(f"Redis error: {str(e)}")
                redis_count = 0
                
            # Compute total visit count (Redis + buffer)
            buffer_count = self._write_buffer.get(page_id, 0)
            total_count = redis_count + buffer_count
            
            # Store updated count in cache
            self._cache[page_id] = {
                "count": total_count,
                "timestamp": time.time()
            }
            
            return {
                "visits": total_count,
                "served_via": "redis"
            }
            
        except Exception as e:
            logging.error(f"Error in increment_visit: {str(e)}")
            return {"visits": 0, "served_via": "in_memory"}

    async def get_visit_count(self, page_id: str) -> Dict[str, Any]:
        """Retrieve the visit count for a given page."""
        try:
            # Check if page data exists in cache
            if page_id in self._cache:
                cache_entry = self._cache[page_id]
                current_time = time.time()
                
                # Return cached value if it's still valid
                if current_time - cache_entry["timestamp"] < self.cache_ttl:
                    return {
                        "visits": cache_entry["count"],
                        "served_via": "in_memory"
                    }
            
            # Cache miss or expired; check if buffer flush is needed
            current_time = time.time()
            if current_time - self.last_flush_time >= self.flush_interval:
                await self._flush_buffer()
                self.last_flush_time = current_time
                
            # Retrieve visit count from Redis
            try:
                redis_count = int(self.redis_client.get(f"page:{page_id}") or 0)
            except Exception as e:
                logging.error(f"Redis error: {str(e)}")
                redis_count = 0
            
            # Combine Redis value with buffer count
            buffer_count = self._write_buffer.get(page_id, 0)
            total_count = redis_count + buffer_count
            
            # Store updated value in cache
            self._cache[page_id] = {
                "count": total_count,
                "timestamp": time.time()
            }
            
            return {
                "visits": total_count,
                "served_via": "redis"
            }
            
        except Exception as e:
            logging.error(f"Error in get_visit_count: {str(e)}")
            return {"visits": 0, "served_via": "in_memory"}

    async def _flush_buffer(self) -> None:
        """Write buffered visit counts to Redis."""
        if not self._write_buffer:
            return  # Skip if buffer is empty
            
        try:
            # Copy buffer data and clear the buffer
            buffer_to_flush = self._write_buffer.copy()
            self._write_buffer.clear()
            
            # Use Redis pipeline to batch update counts
            pipeline = self.redis_client.pipeline()
            for page_id, count in buffer_to_flush.items():
                pipeline.incrby(f"page:{page_id}", count)
            pipeline.execute()
            
            # Update cached values with new counts from Redis
            for page_id in buffer_to_flush:
                if page_id in self._cache:
                    redis_count = int(self.redis_client.get(f"page:{page_id}") or 0)
                    self._cache[page_id]["count"] = redis_count
                    self._cache[page_id]["timestamp"] = time.time()
            
        except Exception as e:
            logging.error(f"Error flushing buffer: {str(e)}")
            # Restore buffer contents if flush fails
            for page_id, count in buffer_to_flush.items():
                if page_id not in self._write_buffer:
                    self._write_buffer[page_id] = 0
                self._write_buffer[page_id] += count