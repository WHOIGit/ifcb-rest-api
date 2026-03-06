"""Capacity limiting and response utilities for the IFCB raw data service."""
import logging

from fastapi import Depends, HTTPException, Request, Response
import redis.asyncio as redis

from .redis_client import get_redis_client

logger = logging.getLogger(__name__)

CAPACITY_FAST = "fast"
CAPACITY_SLOW = "slow"


def render_bytes(payload, media_type: str, headers=None) -> Response:
    return Response(content=bytes(payload), media_type=media_type, headers=headers)


def capacity_limited(group: str):
    """FastAPI dependency factory for per-group capacity limiting via Redis."""
    async def dep(request: Request):
        state = request.app.state
        max_concurrent = state.capacity_groups.get(group)
        if max_concurrent is None:
            raise ValueError(f"Unknown capacity group: {group}")

        redis_client = await get_redis_client()
        if redis_client is None:
            yield
            return

        redis_key = f"ifcb_raw:capacity:{group}"
        acquired = False

        try:
            new_count = await redis_client.incr(redis_key)
            await redis_client.expire(redis_key, state.capacity_key_ttl)

            if new_count > max_concurrent:
                await redis_client.decr(redis_key)
                logger.warning(f"[CAPACITY] Group '{group}' exceeded: {new_count}/{max_concurrent} - returning 429")
                retry_after = state.capacity_retry_after_groups.get(group, state.capacity_retry_after)
                raise HTTPException(
                    status_code=429,
                    detail={"error": "Too many concurrent requests", "group": group, "limit": max_concurrent},
                    headers={"Retry-After": str(retry_after)},
                )

            acquired = True
        except redis.RedisError as e:
            logger.error(f"[CAPACITY] Redis error: {e}")
            raise HTTPException(
                status_code=503,
                detail={"error": "Service temporarily unavailable"},
                headers={"Retry-After": "5"},
            )

        try:
            yield
        finally:
            if acquired and redis_client:
                try:
                    await redis_client.decr(redis_key)
                except redis.RedisError as e:
                    logger.error(f"[CAPACITY] Failed to decrement counter for group '{group}': {e}")

    return Depends(dep)
