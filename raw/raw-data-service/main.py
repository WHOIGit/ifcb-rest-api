"""FastAPI entrypoint for the IFCB raw data service."""
from contextlib import asynccontextmanager
import asyncio
from io import BytesIO
import json
import logging
import os
from typing import Annotated

import botocore
import boto3
from fastapi import Depends, FastAPI, HTTPException, Request
from PIL import Image
import redis.asyncio as redis

from amplify_auth import AuthClient
from storage.s3 import BucketStore
from storage.redis import AsyncRedisStore

from .roistores import AsyncS3RoiStore, AsyncFilesystemRoiStore, CachingRoiStore
from .binstores import AsyncFilesystemBinStore, AsyncS3BinStore, CachingBinStore, build_bin_archive, build_roi_archive
from .models import RawBinParams, RawBinArchiveParams, BinIDParams, ROIImageParams, ROIArchiveParams
from .processor import capacity_limited, CAPACITY_FAST, CAPACITY_SLOW, render_bytes
from .ifcbhdr import parse_hdr_bytes
from .redis_client import get_redis_client, close_redis_client

logger = logging.getLogger(__name__)


class GlobalCapacityMiddleware:
    """ASGI middleware for global capacity limiting. Runs before auth."""

    def __init__(self, app):
        self.app = app
        self.redis_key = "ifcb_raw:capacity:global"

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        redis_client = await get_redis_client()
        if redis_client is None:
            await self.app(scope, receive, send)
            return

        state = self.app.state
        max_concurrent = getattr(state, 'global_capacity', 100)
        retry_after = getattr(state, 'capacity_retry_after', 1)
        key_ttl = getattr(state, 'capacity_key_ttl', 30)

        acquired = False
        response_started = False

        try:
            new_count = await redis_client.incr(self.redis_key)
            await redis_client.expire(self.redis_key, key_ttl)

            if new_count > max_concurrent:
                await redis_client.decr(self.redis_key)
                logger.warning(f"[GLOBAL CAPACITY] EXCEEDED: {new_count}/{max_concurrent} - returning 429")
                await self._send_429(send, retry_after)
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

    async def _send_429(self, send, retry_after: int):
        await send({
            "type": "http.response.start",
            "status": 429,
            "headers": [
                (b"content-type", b"application/json"),
                (b"retry-after", str(retry_after).encode()),
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


auth_service_url = os.getenv("AUTH_SERVICE_URL")
if not auth_service_url:
    raise ValueError("AUTH_SERVICE_URL environment variable is required")

auth_client = AuthClient(auth_service_url=auth_service_url)

@asynccontextmanager
async def lifespan(app: FastAPI):
    raw_data_dir = "/data/raw"  # Always mounted here in container
    # Redis client
    app.state.redis_client = await get_redis_client()

    # Capacity config (shared by GlobalCapacityMiddleware and capacity_limited dep)
    groups_json = os.getenv("CAPACITY_GROUPS")
    app.state.capacity_groups = json.loads(groups_json) if groups_json else {"fast": 40, "slow": 5}
    app.state.capacity_retry_after = int(os.getenv("CAPACITY_RETRY_AFTER", "1"))
    app.state.capacity_key_ttl = int(os.getenv("CAPACITY_KEY_TTL", "30"))
    app.state.global_capacity = int(os.getenv("GLOBAL_CAPACITY_LIMIT", "100"))
    retry_after_groups_json = os.getenv("CAPACITY_RETRY_AFTER_GROUPS")
    app.state.capacity_retry_after_groups = (
        {k: int(v) for k, v in json.loads(retry_after_groups_json).items()}
        if retry_after_groups_json else {}
    )

    s3_bucket = os.getenv("S3_BUCKET_NAME")
    s3_endpoint = os.getenv("S3_ENDPOINT_URL")
    s3_access_key = os.getenv("S3_ACCESS_KEY")
    s3_secret_key = os.getenv("S3_SECRET_KEY")
    s3_prefix = os.getenv("S3_PREFIX", "")
    s3_raw_prefix = os.getenv("S3_RAW_PREFIX", "")
    s3_concurrent_requests = int(os.getenv("S3_CONCURRENT_REQUESTS", "50"))

    s3_configured = all([s3_bucket, s3_access_key, s3_secret_key])
    fs_configured = os.path.exists(raw_data_dir)

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
    if fs_bin_store and s3_bin_store:
        app.state.bin_store = CachingBinStore(fs=fs_bin_store, s3=s3_bin_store)
    elif fs_bin_store:
        app.state.bin_store = fs_bin_store
    elif s3_bin_store:
        app.state.bin_store = s3_bin_store
    else:
        app.state.bin_store = None

    redis_cache = AsyncRedisStore(app.state.redis_client) if app.state.redis_client else None
    app.state.roi_store = CachingRoiStore(
        cache=redis_cache,
        s3=app.state.s3_roi_store,
        fs=app.state.fs_roi_store,
    )

    # init complete.
    yield
    # cleanup
    await close_redis_client()


_inner_app = FastAPI(
    title="raw-data-server",
    description="IFCB raw data service.",
    lifespan=lifespan,
)

_auth = Depends(auth_client.require_scopes(["ifcb:raw:read"]))

_ROI_IMAGE_EXPIRES = 'Fri, 01 Jan 2038 00:00:00 GMT'
_RAW_FILE_MEDIA_TYPES = {"hdr": "text/plain", "adc": "text/csv", "roi": "application/octet-stream"}
_BIN_ARCHIVE_MEDIA_TYPES = {"zip": "application/zip", "tgz": "application/gzip"}
_ROI_IMAGE_MEDIA_TYPES = {"png": "image/png", "jpg": "image/jpeg"}
_ROI_ARCHIVE_MEDIA_TYPES = {"zip": "application/zip", "tar": "application/x-tar"}


async def _jpeg_from_png(png_bytes: bytes) -> bytes:
    buf = BytesIO()
    image = Image.open(BytesIO(png_bytes))
    await asyncio.to_thread(image.convert("RGB").save, buf, format="JPEG")
    buf.seek(0)
    return buf.getvalue()


@_inner_app.get("/health")
async def health():
    return {"status": "ok"}


@_inner_app.get("/data/raw/{bin_id}.{extension}", tags=["IFCB"], summary="Serve a raw IFCB file.")
async def raw_file(
    request: Request,
    path_params: Annotated[RawBinParams, Depends()],
    token_info=_auth,
    _cap=capacity_limited(CAPACITY_FAST),
):
    if request.app.state.bin_store is None:
        raise HTTPException(status_code=503, detail="Raw data store not configured.")
    try:
        content = await request.app.state.bin_store.get(f"{path_params.bin_id}.{path_params.extension}")
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Bin ID {path_params.bin_id} not found.")
    return render_bytes(content, _RAW_FILE_MEDIA_TYPES[path_params.extension])


@_inner_app.get("/data/archive/{bin_id}.{extension}", tags=["IFCB"], summary="Serve raw IFCB bin files in an archive.")
async def raw_archive_file(
    request: Request,
    path_params: Annotated[RawBinArchiveParams, Depends()],
    token_info=_auth,
    _cap=capacity_limited(CAPACITY_SLOW),
):
    if request.app.state.bin_store is None:
        raise HTTPException(status_code=503, detail="Raw data store not configured.")
    try:
        buf = await build_bin_archive(request.app.state.bin_store, path_params.bin_id, path_params.extension)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Bin ID {path_params.bin_id} not found.")
    return render_bytes(buf, _BIN_ARCHIVE_MEDIA_TYPES[path_params.extension])


@_inner_app.get("/data/rois/{bin_id}.json", tags=["IFCB"], summary="Serve list of ROI IDs associated with the bin.")
async def roi_ids(
    request: Request,
    path_params: Annotated[BinIDParams, Depends()],
    token_info=_auth,
    _cap=capacity_limited(CAPACITY_FAST),
):
    if request.app.state.bin_store is None:
        raise HTTPException(status_code=503, detail="Raw data store not configured.")
    try:
        return await request.app.state.bin_store.list_images(path_params.bin_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Bin ID {path_params.bin_id} not found.")


@_inner_app.get("/metadata/raw/{bin_id}.json", tags=["IFCB"], summary="Serve metadata from the header file.")
async def metadata(
    request: Request,
    path_params: Annotated[BinIDParams, Depends()],
    token_info=_auth,
    _cap=capacity_limited(CAPACITY_FAST),
):
    if request.app.state.bin_store is None:
        raise HTTPException(status_code=503, detail="Raw data store not configured.")
    try:
        hdr_bytes = await request.app.state.bin_store.get(f"{path_params.bin_id}.hdr")
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Bin ID {path_params.bin_id} not found.")
    return await asyncio.to_thread(parse_hdr_bytes, hdr_bytes)


@_inner_app.get("/image/roi/{roi_id}.{extension}", tags=["IFCB"], summary="Serve a specified ROI.")
async def roi_image(
    request: Request,
    path_params: Annotated[ROIImageParams, Depends()],
    token_info=_auth,
    _cap=capacity_limited(CAPACITY_FAST),
):
    try:
        png_bytes = await request.app.state.roi_store.get(path_params.roi_id)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=f"ROI ID {path_params.roi_id} not found.") from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving ROI ID {path_params.roi_id}: {e}")
    image_bytes = png_bytes if path_params.extension == "png" else await _jpeg_from_png(png_bytes)
    return render_bytes(image_bytes, _ROI_IMAGE_MEDIA_TYPES[path_params.extension],
                        headers={'Expires': _ROI_IMAGE_EXPIRES})


@_inner_app.get("/image/rois/{bin_id}.{extension}", tags=["IFCB"], summary="Serve ROI images in a tar/zip archive.")
async def roi_archive(
    request: Request,
    path_params: Annotated[ROIArchiveParams, Depends()],
    token_info=_auth,
    _cap=capacity_limited(CAPACITY_SLOW),
):
    store = request.app.state.bin_store
    if store is None:
        raise HTTPException(status_code=503, detail="Raw data store not configured.")
    try:
        buf = await build_roi_archive(store, path_params.bin_id, path_params.extension)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Bin ID {path_params.bin_id} not found.")
    return render_bytes(buf, _ROI_ARCHIVE_MEDIA_TYPES[path_params.extension])


# Global capacity ceiling - runs before auth, prevents connection errors.
# Config is read from _inner_app.state at request time (set in lifespan above).
app = GlobalCapacityMiddleware(_inner_app)
