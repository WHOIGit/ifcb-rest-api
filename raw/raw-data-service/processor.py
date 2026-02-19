"""Stateless template processor."""
import asyncio
from contextlib import asynccontextmanager
import functools
from io import BytesIO
import json
import logging
import os
import time

from typing import List, Literal

import boto3
import botocore
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import redis.asyncio as redis

from stateless_microservice import BaseProcessor, StatelessAction, render_bytes

from .binstores import AsyncBinStore
from .ifcbhdr import parse_hdr_bytes
from .s3utils import IfcbPidTransformer, list_roi_ids_from_s3
from .redis_client import get_redis_client

from .roistores import AsyncFilesystemRoiStore, AsyncS3RoiStore

from storage.s3 import BucketStore

from PIL import Image

logger = logging.getLogger(__name__)

CAPACITY_FAST = "fast"
CAPACITY_SLOW = "slow"

def capacity_limited(group: str):
    """
    Decorate an async handler method to run under `self.capacity_limit(...)`.
    """
    def decorator(fn):
        @functools.wraps(fn)
        async def wrapper(self, *args, **kwargs):
            async with self.capacity_limit(group):
                return await fn(self, *args, **kwargs)

        return wrapper

    return decorator

class RawBinParams(BaseModel):
    """ Path parameters for the raw_file endpoint. """

    bin_id: str = Field(..., description="ID of bin to retrieve.")
    extension: Literal["hdr", "adc", "roi"]


class RawBinArchiveParams(BaseModel):
    """ Path parameters for the raw_zip_file endpoint. """

    bin_id: str = Field(..., description="ID of bin to retrieve.")
    extension: Literal["zip", "tgz"]


class BinIDParams(BaseModel):
    """ Path parameter model with only bin_id. """

    bin_id: str = Field(..., description="ID of bin")


class ROIImageParams(BaseModel):
    """ Path parameters for the roi-image endpoint. """
    
    roi_id: str = Field(..., description="ID of ROI")
    extension: Literal["png", "jpg"]


class ROIArchiveParams(BaseModel):
    """ Path parameters for the roi-archive endpoint. """

    bin_id: str = Field(..., description="ID of bin")
    extension: Literal["zip", "tar"]


class RawProcessor(BaseProcessor):
    """Processor for raw data requests."""

    def __init__(self):
        # Capacity groups configuration
        # Can be overridden via CAPACITY_GROUPS env var as JSON
        # e.g. '{"fast": 40, "slow": 5}'
        default_groups = {"fast": 40, "slow": 5}
        groups_json = os.getenv("CAPACITY_GROUPS")
        if groups_json:
            self.capacity_groups = json.loads(groups_json)
        else:
            self.capacity_groups = default_groups

        self.capacity_retry_after = int(os.getenv("CAPACITY_RETRY_AFTER", "1"))
        self.capacity_key_ttl = int(os.getenv("CAPACITY_KEY_TTL", "30"))

    @property
    def name(self) -> str:
        return "raw-data-server"

    def get_stateless_actions(self) -> List[StatelessAction]:
        return [
            StatelessAction(
                name="raw-file",
                path="/data/raw/{bin_id}.{extension}",
                path_params_model=RawBinParams,
                handler=self.handle_raw_file_request,
                summary="Serve a raw IFCB file.",
                description="Serve a raw IFCB file.",
                tags=("IFCB",),
                methods=("GET",),
                required_scopes=["ifcb:raw:read"],
            ),
            StatelessAction(
                name="raw-archive-file",
                path="/data/archive/{bin_id}.{extension}",
                path_params_model=RawBinArchiveParams,
                handler=self.handle_raw_archive_file_request,
                summary="Serve raw IFCB bin files in an archive.",
                description="Serve raw IFCB bin files in an archive.",
                tags=("IFCB",),
                methods=("GET",),
                required_scopes=["ifcb:raw:read"],
            ),
            StatelessAction(
                name="roi-ids",
                path="/data/rois/{bin_id}.json",
                path_params_model=BinIDParams,
                handler=self.handle_roi_list_request,
                summary="Serve list of ROI IDs associated with the bin.",
                description="Serve list of ROI IDs associated with the bin.",
                tags=("IFCB",),
                methods=("GET",),
                required_scopes=["ifcb:raw:read"],
            ),
            StatelessAction(
                name="metadata",
                path="/metadata/raw/{bin_id}.json",
                path_params_model=BinIDParams,
                handler=self.handle_metadata_request,
                summary="Serve metadata from the header file.",
                description="Serve metadata from the header file.",
                tags=("IFCB",),
                methods=("GET",),
                required_scopes=["ifcb:raw:read"],
            ),
            StatelessAction(
                name="roi-image",
                path="/image/roi/{roi_id}.{extension}",
                path_params_model=ROIImageParams,
                handler=self.handle_roi_image_request,
                summary="Serve a specified ROI.",
                description="Serve a specified ROI.",
                tags=("IFCB",),
                methods=("GET",),
                required_scopes=["ifcb:raw:read"],
            ),
           StatelessAction(
                name="roi-archive",
                path="/image/rois/{bin_id}.{extension}",
                path_params_model=ROIArchiveParams,
                handler=self.handle_roi_archive_request,
                summary="Serve ROI images in a tar/zip archive.",
                description="Serve ROI images in a tar/zip archive.",
                tags=("IFCB",),
                methods=("GET",),
                required_scopes=["ifcb:raw:read"],
            ),

        ]

    def bin_store(self) -> AsyncBinStore:
        return self.app.state.bin_store

    @asynccontextmanager
    async def capacity_limit(self, group: str):
        """Async context manager for per-group capacity limiting via Redis."""
        max_concurrent = self.capacity_groups.get(group)
        if max_concurrent is None:
            raise ValueError(f"Unknown capacity group: {group}")

        redis_client = await get_redis_client()
        if redis_client is None:
            # No Redis configured, pass through without limiting
            yield
            return

        redis_key = f"ifcb_raw:capacity:{group}"
        acquired = False

        try:
            # Atomically increment counter
            new_count = await redis_client.incr(redis_key)
            # Set TTL as safety net (in case of crashes)
            await redis_client.expire(redis_key, self.capacity_key_ttl)

            if new_count > max_concurrent:
                # Over capacity - rollback and reject
                await redis_client.decr(redis_key)
                logger.warning(f"[CAPACITY] Group '{group}' exceeded: {new_count}/{max_concurrent} - returning 429")
                raise HTTPException(
                    status_code=429,
                    detail={"error": "Too many concurrent requests", "group": group, "limit": max_concurrent},
                    headers={"Retry-After": str(self.capacity_retry_after)},
                )

            acquired = True
            yield

        except redis.RedisError as e:
            logger.error(f"[CAPACITY] Redis error: {e}")
            raise HTTPException(
                status_code=503,
                detail={"error": "Service temporarily unavailable"},
                headers={"Retry-After": "5"},
            )

        finally:
            if acquired and redis_client:
                try:
                    await redis_client.decr(redis_key)
                except redis.RedisError as e:
                    logger.error(f"[CAPACITY] Failed to decrement counter for group '{group}': {e}")

    @capacity_limited(CAPACITY_FAST)
    async def handle_raw_file_request(self, path_params: RawBinParams, token_info=None):
        """ Retrieve raw IFCB files. """
        key = f"{path_params.bin_id}.{path_params.extension}"
        try:
            content = await self.bin_store().get(key)
        except KeyError:
            raise HTTPException(status_code=404, detail=f"Bin ID {path_params.bin_id} not found.")
        media_type = {
            "hdr": "text/plain",
            "adc": "text/csv",
            "roi": "application/octet-stream",
        }[path_params.extension]
        return render_bytes(content, media_type)

    @capacity_limited(CAPACITY_SLOW)
    async def handle_raw_archive_file_request(self, path_params: RawBinArchiveParams, token_info=None):
        """ Retrieve raw IFCB files in a zip or tar/gzip archive. """
        import zipfile
        import tarfile

        bin_id = path_params.bin_id
        store = self.bin_store()
        buffer = BytesIO()

        hdr_path, adc_path, roi_path = await asyncio.gather(
            store.get_path(f"{bin_id}.hdr"),
            store.get_path(f"{bin_id}.adc"),
            store.get_path(f"{bin_id}.roi"),
        )

        if all([hdr_path, adc_path, roi_path]):
            # Path-based writes: zipfile/tarfile reads each file in chunks,
            # so the full .roi bytes are never resident in RAM simultaneously
            # with the compressed output.
            paths = {"hdr": hdr_path, "adc": adc_path, "roi": roi_path}
            if path_params.extension == "zip":
                media_type = "application/zip"
                def write_zip():
                    with zipfile.ZipFile(buffer, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
                        for ext, path in paths.items():
                            zipf.write(path, arcname=f"{bin_id}.{ext}")
                await asyncio.to_thread(write_zip)
            elif path_params.extension == "tgz":
                media_type = "application/gzip"
                def write_tgz():
                    with tarfile.open(fileobj=buffer, mode='w:gz') as tarf:
                        for ext, path in paths.items():
                            tarf.add(path, arcname=f"{bin_id}.{ext}")
                await asyncio.to_thread(write_tgz)
        else:
            # Byte-based fallback for S3-backed stores (no filesystem paths available).
            try:
                hdr_bytes, adc_bytes, roi_bytes = await asyncio.gather(
                    store.get(f"{bin_id}.hdr"),
                    store.get(f"{bin_id}.adc"),
                    store.get(f"{bin_id}.roi"),
                )
            except KeyError:
                raise HTTPException(status_code=404, detail=f"Bin ID {bin_id} not found.")
            raw_files = {"hdr": hdr_bytes, "adc": adc_bytes, "roi": roi_bytes}
            if path_params.extension == "zip":
                media_type = "application/zip"
                def write_zip():
                    with zipfile.ZipFile(buffer, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
                        for ext, data in raw_files.items():
                            zipf.writestr(f"{bin_id}.{ext}", data)
                await asyncio.to_thread(write_zip)
            elif path_params.extension == "tgz":
                media_type = "application/gzip"
                def write_tgz():
                    with tarfile.open(fileobj=buffer, mode='w:gz') as tarf:
                        for ext, data in raw_files.items():
                            info = tarfile.TarInfo(name=f"{bin_id}.{ext}")
                            info.size = len(data)
                            tarf.addfile(tarinfo=info, fileobj=BytesIO(data))
                await asyncio.to_thread(write_tgz)

        buffer.seek(0)
        return render_bytes(buffer.getvalue(), media_type)

    @capacity_limited(CAPACITY_FAST)
    async def handle_roi_list_request(self, path_params: BinIDParams, token_info=None):
        """ Retrieve list of ROI IDs associated with the bin. """
        pid = path_params.bin_id
        try:
            image_list = await self.bin_store().list_images(pid)
        except KeyError:
            raise HTTPException(status_code=404, detail=f"Bin ID {pid} not found.")
        return image_list

    @capacity_limited(CAPACITY_FAST)
    async def handle_roi_image_request(self, path_params: ROIImageParams, token_info=None):
        """ Retrieve a specific ROI image. """
        roi_id = path_params.roi_id
        media_type = {
            "png": "image/png",
            "jpg": "image/jpeg",
        }[path_params.extension]

        try:
            png_bytes = await self.app.state.roi_store.get(roi_id)
        except KeyError as e:
            raise HTTPException(status_code=404, detail=f"ROI ID {roi_id} not found.") from e
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error retrieving ROI ID {roi_id}: {e}")

        if path_params.extension == "png":
            return render_bytes(png_bytes, media_type, headers={'Expires': 'Fri, 01 Jan 2038 00:00:00 GMT'})

        img_buffer = BytesIO()
        image = Image.open(BytesIO(png_bytes))
        await asyncio.to_thread(image.convert("RGB").save, img_buffer, format="JPEG")
        img_buffer.seek(0)
        return render_bytes(img_buffer.getvalue(), media_type, headers={'Expires': 'Fri, 01 Jan 2038 00:00:00 GMT'})

    @capacity_limited(CAPACITY_FAST)
    async def handle_metadata_request(self, path_params: BinIDParams, token_info=None):
        """ Retrieve metadata from the header file. """
        try:
            hdr_bytes = await self.bin_store().get(f"{path_params.bin_id}.hdr")
        except KeyError:
            raise HTTPException(status_code=404, detail=f"Bin ID {path_params.bin_id} not found.")
        props = await asyncio.to_thread(parse_hdr_bytes, hdr_bytes)
        return props

    @capacity_limited(CAPACITY_SLOW)
    async def handle_roi_archive_request(self, path_params: ROIArchiveParams, token_info=None):
        """ Retrieve a tar/zip archive of ROI images for a given bin. """
        import zipfile
        import tarfile

        store = self.bin_store()
        if store is None:
            raise HTTPException(status_code=503, detail="Raw data store not configured, cannot serve ROI archives.")
        pid = path_params.bin_id

        def encode_png(image) -> bytes:
            buf = BytesIO()
            image.save(buf, format="PNG")
            return buf.getvalue()

        buffer = BytesIO()
        try:
            if path_params.extension == "zip":
                with zipfile.ZipFile(buffer, 'w') as zipf:
                    async for roi_id, image in store.iter_images(pid):
                        image_bytes = await asyncio.to_thread(encode_png, image)
                        zipf.writestr(f"{roi_id}.png", image_bytes)
            elif path_params.extension == "tar":
                with tarfile.open(fileobj=buffer, mode='w') as tarf:
                    async for roi_id, image in store.iter_images(pid):
                        image_bytes = await asyncio.to_thread(encode_png, image)
                        info = tarfile.TarInfo(name=f"{roi_id}.png")
                        info.size = len(image_bytes)
                        tarf.addfile(tarinfo=info, fileobj=BytesIO(image_bytes))
        except KeyError:
            raise HTTPException(status_code=404, detail=f"Bin ID {pid} not found.")

        buffer.seek(0)
        media_type = {
            "zip": "application/zip",
            "tar": "application/x-tar",
        }[path_params.extension]
        return render_bytes(buffer.getvalue(), media_type)
