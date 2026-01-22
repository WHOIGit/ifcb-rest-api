from abc import ABC, abstractmethod
from io import BytesIO

from .ifcb import SyncIfcbDataDirectory, AsyncIfcbDataDirectory
from .ifcb_parsing import parse_roi_id


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
        bin_id, target = parse_roi_id(roi_id)
        image = self.dd.get_roi(bin_id, target)
        image_data = BytesIO()
        if self.file_type == "jpg":
            image.save(image_data, format="JPEG")
        else:  # png
            image.save(image_data, format="PNG")
        return image_data.getvalue()
    

class AsyncFilesystemRoiStore(AsyncRoiStore):

    dd = AsyncIfcbDataDirectory
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
        bin_id, target = parse_roi_id(roi_id)
        image = await self.dd.read_image(bin_id, target)
        image_data = BytesIO()
        if self.file_type == "jpg":
            image.save(image_data, format="JPEG")
        else:  # png
            image.save(image_data, format="PNG")
        return image_data.getvalue()
