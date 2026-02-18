"""FastAPI entrypoint for the IFCB raw data service."""
from contextlib import asynccontextmanager
import logging
import os
from fastapi import FastAPI
import redis.asyncio as redis

import botocore
import boto3

from stateless_microservice import ServiceConfig, create_app, AuthClient
from storage.s3 import BucketStore
from storage.redis import AsyncRedisStore

from .roistores import AsyncS3RoiStore, AsyncFilesystemRoiStore, CachingRoiStore
from .binstores import AsyncFilesystemBinStore, AsyncS3BinStore, CachingBinStore
from .processor import RawProcessor
from .redis_client import get_redis_client, close_redis_client

logger = logging.getLogger(__name__)


class GlobalCapacityMiddleware:
    """ASGI middleware for global capacity limiting. Runs before auth."""

    def __init__(self, app, max_concurrent: int = 100, retry_after: int = 1, key_ttl: int = 30):
        self.app = app
        self.max_concurrent = max_concurrent
        self.retry_after = retry_after
        self.key_ttl = key_ttl
        self.redis_key = "ifcb_raw:capacity:global"

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        redis_client = await get_redis_client()
        if redis_client is None:
            await self.app(scope, receive, send)
            return

        acquired = False
        response_started = False

        try:
            new_count = await redis_client.incr(self.redis_key)
            await redis_client.expire(self.redis_key, self.key_ttl)

            if new_count > self.max_concurrent:
                await redis_client.decr(self.redis_key)
                logger.warning(f"[GLOBAL CAPACITY] EXCEEDED: {new_count}/{self.max_concurrent} - returning 429")
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
            logger.error(f"[GLOBAL CAPACITY] Redis error: {e}")
            if not response_started:
                await self._send_503(send)
            return

        except Exception as e:
            # Catch-all for unexpected exceptions to prevent connection drops
            logger.exception(f"[GLOBAL CAPACITY] Unexpected error: {e}")
            if not response_started:
                await self._send_500(send, str(e))

        finally:
            if acquired and redis_client:
                try:
                    await redis_client.decr(self.redis_key)
                except redis.RedisError as e:
                    logger.error(f"[GLOBAL CAPACITY] Failed to decrement: {e}")

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

@asynccontextmanager
async def lifespan(app: FastAPI):
    print('initializing RawProcessor...')
    raw_data_dir = "/data/raw"  # Always mounted here in container
    # Redis client
    app.state.redis_client = await get_redis_client()

    s3_bucket = os.getenv("S3_BUCKET_NAME")
    s3_endpoint = os.getenv("S3_ENDPOINT_URL")
    s3_access_key = os.getenv("S3_ACCESS_KEY")
    s3_secret_key = os.getenv("S3_SECRET_KEY")
    s3_prefix = os.getenv("S3_PREFIX", "")
    s3_raw_prefix = os.getenv("S3_RAW_PREFIX", "")
    s3_concurrent_requests = int(os.getenv("S3_CONCURRENT_REQUESTS", "50"))

    s3_configured = all([s3_bucket, s3_access_key, s3_secret_key])
    fs_configured = raw_data_dir is not None

    app.state.s3_roi_store = None
    app.state.fs_roi_store = None

    if s3_configured:
        app.state.s3_session = boto3.session.Session()
        app.state.s3_client = app.state.s3_session.client(
            's3',
            endpoint_url=s3_endpoint,
            aws_access_key_id=s3_access_key,
            aws_secret_access_key=s3_secret_key,
            config = botocore.config.Config(
                max_pool_connections=s3_concurrent_requests
            )
        )

        app.state.bucket_store = BucketStore(s3_bucket, app.state.s3_client)
        app.state.s3_roi_store = AsyncS3RoiStore(
            s3_bucket=s3_bucket,
            s3_client=app.state.s3_client,
            s3_prefix=s3_prefix,
        )

    if fs_configured:
        app.state.fs_roi_store = AsyncFilesystemRoiStore(raw_data_dir, file_type="png")

    fs_bin_store = AsyncFilesystemBinStore(raw_data_dir) if fs_configured else None
    s3_bin_store = AsyncS3BinStore(
        s3_bucket=s3_bucket,
        s3_client=app.state.s3_client,
        s3_prefix=s3_raw_prefix,
    ) if s3_configured else None
    app.state.bin_store = CachingBinStore(fs=fs_bin_store, s3=s3_bin_store) if (fs_bin_store or s3_bin_store) else None

    app.state.roi_store = CachingRoiStore(
        cache=AsyncRedisStore(app.state.redis_client),
        s3=app.state.s3_roi_store,
        fs=app.state.fs_roi_store,
    )

    # init complete.
    yield
    # cleanup
    await close_redis_client()

app = create_app(RawProcessor(), config, auth_client=auth_client, lifespan=lifespan)

# Global capacity ceiling - runs before auth, prevents connection errors
global_capacity = int(os.getenv("GLOBAL_CAPACITY_LIMIT", "100"))
retry_after = int(os.getenv("CAPACITY_RETRY_AFTER", "1"))
key_ttl = int(os.getenv("CAPACITY_KEY_TTL", "30"))
app = GlobalCapacityMiddleware(app, max_concurrent=global_capacity, retry_after=retry_after, key_ttl=key_ttl)
