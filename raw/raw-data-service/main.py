"""FastAPI entrypoint for the IFCB raw data service."""
import logging
import os
import traceback

import redis.asyncio as redis

from stateless_microservice import ServiceConfig, create_app, AuthClient

from .processor import RawProcessor
from .redis_client import get_redis_client

# Configure logging from environment
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)

logger = logging.getLogger(__name__)


class GlobalCapacityMiddleware:
    """ASGI middleware for global capacity limiting. Runs before auth."""

    def __init__(self, app, max_concurrent: int = 100, retry_after: int = 1):
        self.app = app
        self.max_concurrent = max_concurrent
        self.retry_after = retry_after
        self.redis_key = "ifcb_raw:capacity:global"

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        # Extract request info for logging
        path = scope.get("path", "unknown")
        method = scope.get("method", "unknown")
        request_id = f"{method} {path}"

        redis_client = await get_redis_client()
        if redis_client is None:
            logger.debug(f"[GLOBAL CAPACITY] Redis unavailable, passing through: {request_id}")
            await self.app(scope, receive, send)
            return

        acquired = False
        current_count = 0
        response_started = False

        try:
            current_count = await redis_client.incr(self.redis_key)
            await redis_client.expire(self.redis_key, 30)

            logger.debug(f"[GLOBAL CAPACITY] {request_id} - count: {current_count}/{self.max_concurrent}")

            if current_count > self.max_concurrent:
                await redis_client.decr(self.redis_key)
                logger.warning(f"[GLOBAL CAPACITY] EXCEEDED: {current_count}/{self.max_concurrent} - returning 429 for {request_id}")
                await self._send_429(send)
                return

            acquired = True

            # Wrap send to track if response has started
            async def tracked_send(message):
                nonlocal response_started
                if message.get("type") == "http.response.start":
                    response_started = True
                await send(message)

            await self.app(scope, receive, tracked_send)

        except redis.RedisError as e:
            logger.error(f"[GLOBAL CAPACITY] Redis error for {request_id}: {e}")
            if not response_started:
                await self._send_503(send)
            return

        except Exception as e:
            # Catch-all for any unexpected exceptions to prevent connection drops
            logger.error(
                f"[GLOBAL CAPACITY] Unexpected error for {request_id} "
                f"(count was {current_count}/{self.max_concurrent}): {type(e).__name__}: {e}\n"
                f"{traceback.format_exc()}"
            )
            if not response_started:
                await self._send_500(send, str(e))
            # Don't re-raise - we've handled it by sending an error response

        finally:
            if acquired and redis_client:
                try:
                    new_count = await redis_client.decr(self.redis_key)
                    logger.debug(f"[GLOBAL CAPACITY] {request_id} complete - count now: {new_count}")
                except redis.RedisError as e:
                    logger.error(f"[GLOBAL CAPACITY] Failed to decrement for {request_id}: {e}")

    async def _send_429(self, send):
        await send({
            "type": "http.response.start",
            "status": 429,
            "headers": [
                (b"content-type", b"application/json"),
                (b"retry-after", str(self.retry_after).encode()),
            ],
        })
        await send({
            "type": "http.response.body",
            "body": b'{"error":"Server at capacity","detail":"global limit exceeded"}',
        })

    async def _send_503(self, send):
        await send({
            "type": "http.response.start",
            "status": 503,
            "headers": [
                (b"content-type", b"application/json"),
                (b"retry-after", b"5"),
            ],
        })
        await send({
            "type": "http.response.body",
            "body": b'{"error":"Service temporarily unavailable"}',
        })

    async def _send_500(self, send, detail: str = "Internal server error"):
        import json
        body = json.dumps({"error": "Internal server error", "detail": detail}).encode()
        await send({
            "type": "http.response.start",
            "status": 500,
            "headers": [
                (b"content-type", b"application/json"),
            ],
        })
        await send({
            "type": "http.response.body",
            "body": body,
        })


config = ServiceConfig(description="IFCB raw data service.")

auth_service_url = os.getenv("AUTH_SERVICE_URL")
if not auth_service_url:
    raise ValueError("AUTH_SERVICE_URL environment variable is required")

auth_client = AuthClient(auth_service_url=auth_service_url)

app = create_app(RawProcessor(), config, auth_client=auth_client)

# Global capacity ceiling - runs before auth, prevents connection errors
global_capacity = int(os.getenv("GLOBAL_CAPACITY_LIMIT", "100"))
retry_after = int(os.getenv("CAPACITY_RETRY_AFTER", "1"))
app = GlobalCapacityMiddleware(app, max_concurrent=global_capacity, retry_after=retry_after)
