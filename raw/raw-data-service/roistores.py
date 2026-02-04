from abc import ABC, abstractmethod
from io import BytesIO

from storage.s3 import BucketStore
from storage.utils import KeyTransformingStore
from storage.utils import PrefixKeyTransformer

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

    def get(self, roi_id: str) -> bytes:
        return self.store.get(roi_id)
