"""Async Redis client singleton for capacity limiting."""
import logging
import os

import redis.asyncio as redis

logger = logging.getLogger(__name__)

_redis_client: redis.Redis | None = None


async def get_redis_client() -> redis.Redis | None:
    """Get or create async Redis client for capacity limiting."""
    global _redis_client
    if _redis_client is None:
        redis_url = os.getenv("REDIS_URL")
        if not redis_url:
            logger.warning("REDIS_URL not set, capacity limiting disabled")
            return None
        _redis_client = redis.from_url(
            redis_url,
            socket_connect_timeout=2,
            socket_timeout=2,
        )
    return _redis_client


async def close_redis_client():
    """Close the Redis client and reset the singleton."""
    global _redis_client
    if _redis_client is not None:
        await _redis_client.aclose()
        _redis_client = None
