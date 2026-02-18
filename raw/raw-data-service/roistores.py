from abc import ABC, abstractmethod
import asyncio
from io import BytesIO

from storage.object import DictStore
from storage.s3 import BucketStore
from storage.utils import KeyTransformingStore, PrefixKeyTransformer

from .ifcb import SyncIfcbDataDirectory, AsyncIfcbDataDirectory
from .s3utils import IfcbPidTransformer


class SyncRoiStore(ABC):
    """A store for IFCB ROI images."""

    @abstractmethod
    def get(self, roi_id: str) -> bytes:
        """Get the image data for the given ROI ID."""
        raise NotImplementedError

    def put(self, roi_id: str, image_data: bytes):
        raise NotImplementedError("ROI store is read-only")
    

class AsyncRoiStore(ABC):
    """An asynchronous store for IFCB ROI images."""

    @abstractmethod
    async def get(self, roi_id: str) -> bytes:
        """Get the image data for the given ROI ID."""
        raise NotImplementedError

    async def put(self, roi_id: str, image_data: bytes):
        raise NotImplementedError("ROI store is read-only")
    

class SyncFilesystemRoiStore(SyncRoiStore):
    """A filesystem-based synchronous ROI store."""

    dd: SyncIfcbDataDirectory
    file_type: str

    def __init__(self, base_path: str, file_type: str = "png"):
        self.dd = SyncIfcbDataDirectory(base_path, require_roi=True)
        file_type = file_type.lower().lstrip(".")
        if file_type == "jpeg":
            file_type = "jpg"
        if file_type not in {"png", "jpg"}:
            raise ValueError(f"Unsupported file_type: {file_type!r} (expected 'png' or 'jpg')")
        self.file_type = file_type

    def get(self, roi_id: str) -> bytes:
        image = self.dd.read_image(roi_id)
        image_data = BytesIO()
        if self.file_type == "jpg":
            image.save(image_data, format="JPEG")
        else:  # png
            image.save(image_data, format="PNG")
        return image_data.getvalue()
    

class AsyncFilesystemRoiStore(AsyncRoiStore):

    dd: AsyncIfcbDataDirectory
    file_type: str

    def __init__(self, base_path: str, file_type: str = "png"):
        self.dd = AsyncIfcbDataDirectory(base_path, require_roi=True)
        file_type = file_type.lower().lstrip(".")
        if file_type == "jpeg":
            file_type = "jpg"
        if file_type not in {"png", "jpg"}:
            raise ValueError(f"Unsupported file_type: {file_type!r} (expected 'png' or 'jpg')")
        self.file_type = file_type

    async def exists(self, roi_id: str) -> bool:
        return await self.dd.image_exists(roi_id)

    async def get(self, roi_id: str) -> bytes:
        image = await self.dd.read_image(roi_id)
        image_data = BytesIO()
        if self.file_type == "jpg":
            image.save(image_data, format="JPEG")
        else:  # png
            image.save(image_data, format="PNG")
        return image_data.getvalue()


class S3RoiStore(SyncRoiStore):
    """An S3-based asynchronous ROI store."""
    def __init__(self, s3_bucket: str, s3_client=None, s3_prefix: str | None = None):
        bucket_store = BucketStore(s3_bucket, s3_client)

        # Compose transformers: first apply prefix, then IFCB-specific path structure
        # Inner layer: add S3 prefix (e.g., "ifcb_data/")
        if s3_prefix:
            prefix_transformer = PrefixKeyTransformer(prefix=s3_prefix.rstrip("/") + "/")
            prefix_store = KeyTransformingStore(bucket_store, prefix_transformer)
        else:
            prefix_store = bucket_store

        # Outer layer: apply IFCB PID -> S3 path transformation
        ifcb_transformer = IfcbPidTransformer()
        self.store = KeyTransformingStore(prefix_store, ifcb_transformer)

    def exists(self, roi_id: str) -> bool:
        return self.store.exists(roi_id)
    
    def get(self, roi_id: str) -> bytes:
        return self.store.get(roi_id)
    
    def put(self, roi_id: str, image_data: bytes):
        return self.store.put(roi_id, image_data)

class AsyncS3RoiStore(AsyncRoiStore):
    """An S3-based asynchronous ROI store."""
    def __init__(self, s3_bucket: str, s3_client=None, s3_prefix: str | None = None):
        self.store = S3RoiStore(s3_bucket, s3_client, s3_prefix)

    async def exists(self, roi_id: str) -> bool:
        return await asyncio.to_thread(self.store.exists, roi_id)
    
    async def get(self, roi_id: str) -> bytes:
        return await asyncio.to_thread(self.store.get, roi_id)
    
    async def put(self, roi_id: str, image_data: bytes):
        return await asyncio.to_thread(self.store.put, roi_id, image_data)
    

class AsyncDictRoiStore(AsyncRoiStore):
    """An in-memory asynchronous ROI store using a dictionary."""
    def __init__(self):
        self.store = DictStore()

    async def exists(self, roi_id: str) -> bool:
        return self.store.exists(roi_id)

    async def get(self, roi_id: str) -> bytes:
        return self.store.get(roi_id)

    async def put(self, roi_id: str, image_data: bytes):
        self.store.put(roi_id, image_data)

class CachingRoiStore(AsyncRoiStore):
    """semantics for a caching ROI store:
    
    read from cache first; if miss, read from S3; if miss, read from filesystem.
    write to both cache and S3.
    cache should be bounded in size and evict old items as needed (redis?)
    """
    def __init__(self, cache: AsyncRoiStore | None = None,
                 s3: AsyncS3RoiStore | None = None,
                 fs: AsyncFilesystemRoiStore | None = None):
        self.cache_store = cache if cache is not None else AsyncDictRoiStore()
        self.s3_store = s3
        self.fs_store = fs

    async def get(self, roi_id: str) -> bytes:
        # try cache
        if await self.cache_store.exists(roi_id):
            return await self.cache_store.get(roi_id)
        # try S3
        if self.s3_store and await self.s3_store.exists(roi_id):
            data = await self.s3_store.get(roi_id)
            # populate cache
            await self.cache_store.put(roi_id, data)
            return data
        # try filesystem
        if self.fs_store and await self.fs_store.exists(roi_id):
            data = await self.fs_store.get(roi_id)
            # populate S3 and cache
            if self.s3_store:
                await self.s3_store.put(roi_id, data)
            await self.cache_store.put(roi_id, data)
            return data
        raise KeyError(f"ROI ID {roi_id} not found in any store")

    async def put(self, roi_id: str, image_data: bytes):
        await self.cache_store.put(roi_id, image_data)
        if self.s3_store:
            await self.s3_store.put(roi_id, image_data)