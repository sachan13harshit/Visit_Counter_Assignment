from typing import Dict, List, Any
import asyncio
from datetime import datetime
from ..core.redis_manager import RedisManager

class VisitCounterService:
    def __init__(self):
        """Initialize the visit counter service with Redis manager"""
        # self.redis_manager = RedisManager()
        self._visit_counts: Dict[str, int] = {}

        self.redis_client = redis.Redis(
            host="redis1",
            port=6379,
            decode_responses=True
        )

        self.use_redis = True

    async def increment_visit(self, page_id: str) -> Dict[str, Any]:
        """
        Increment visit count for a page
        
        Args:
            page_id: Unique identifier for the page
        """
        # TODO: Implement visit count increment
        # pass
        if self.use_redis:
            count = self.redis_client.incr(f"page:{page_id}")
            return {
                "visits": count,
                "served_via": "redis"
            }
            else:
                if page_id not in self._visit_counts:
                    self._visit_counts[page_id] = 0
                self._visit_counts[page_id] += 1
                return {
                    "visits": self._visit_counts[page_id],
                    "served_via": "in_memory"
                }

    async def get_visit_count(self, page_id: str) -> Dict[str, Any]:
        """
        Get current visit count for a page
        
        Args:
            page_id: Unique identifier for the page
            
        Returns:
            Current visit count
        """
        # TODO: Implement getting visit count
        if self.use_redis:
            count = self.redis_client.get(f"page:{page_id}")
            return {
                "visits": int(count) if count else 0,
                "served_via": "redis"
            }
        else:
            count = self._visit_counts.get(page_id, 0)
            return {
                "visits": count,
                "served_via": "in_memory"
            }
