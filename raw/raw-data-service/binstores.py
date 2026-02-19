"""Async stores for IFCB raw bin data files (.hdr, .adc, .roi)."""
from abc import ABC, abstractmethod
import asyncio
from io import BytesIO

import aiofiles
from PIL import Image

from storage.s3 import BucketStore
from storage.utils import KeyTransformingStore, PrefixKeyTransformer

from .ifcb import async_find_fileset, DEFAULT_INCLUDE, DEFAULT_EXCLUDE
from .ifcb_parsing import parse_roi_id, add_target
from .s3utils import IfcbBinKeyTransformer


def _parse_adc_bytes(bin_id: str, adc_bytes: bytes) -> dict:
    """Parse .adc CSV bytes into ROI metadata dict."""
    if bin_id.startswith('I'):
        x_col, y_col, w_col, h_col = 9, 10, 11, 12
    else:
        x_col, y_col, w_col, h_col = 13, 14, 15, 16
    images = {}
    for i, line in enumerate(adc_bytes.decode('utf-8', errors='replace').splitlines()):
        fields = line.strip().split(',')
        try:
            x = int(fields[x_col])
            y = int(fields[y_col])
            width = int(fields[w_col])
            height = int(fields[h_col])
        except (ValueError, IndexError):
            continue
        if width == 0 or height == 0:
            continue
        images[i + 1] = {
            'roi_id': add_target(bin_id, i + 1),
            'x': x,
            'y': y,
            'width': width,
            'height': height,
        }
    return images


def _extract_roi_images(bin_id: str, adc_bytes: bytes, roi_bytes: bytes, rois=None) -> dict:
    """Extract PIL Images from .adc and .roi bytes."""
    if bin_id.startswith('I'):
        w_col, h_col, offset_col = 11, 12, 13
    else:
        w_col, h_col, offset_col = 15, 16, 17
    images = {}
    roi_buffer = BytesIO(roi_bytes)
    for i, line in enumerate(adc_bytes.decode('utf-8', errors='replace').splitlines()):
        if rois is not None and (i + 1) not in rois:
            continue
        fields = line.strip().split(',')
        try:
            width = int(fields[w_col])
            height = int(fields[h_col])
            offset = int(fields[offset_col])
        except (ValueError, IndexError):
            continue
        if width == 0 or height == 0:
            continue
        roi_buffer.seek(offset)
        data = roi_buffer.read(width * height)
        images[i + 1] = Image.frombuffer('L', (width, height), data, 'raw', 'L', 0, 1)
    return images


class AsyncBinStore(ABC):
    """Async store for IFCB raw bin data files (.hdr, .adc, .roi).

    Keys are bin file basenames: '{bin_id}.{ext}',
    e.g. 'D20221227T093138_IFCB127.hdr'.
    """

    @abstractmethod
    async def exists(self, key: str) -> bool:
        """Return True if the given bin file exists."""
        ...

    @abstractmethod
    async def get(self, key: str) -> bytes:
        """Return the full content of the given bin file."""
        ...

    async def list_images(self, bin_id: str) -> dict:
        """Parse .adc to return ROI metadata dict."""
        adc_bytes = await self.get(f"{bin_id}.adc")
        return await asyncio.to_thread(_parse_adc_bytes, bin_id, adc_bytes)

    async def read_images(self, bin_id: str, rois=None) -> dict:
        """Extract PIL Images from .roi for this bin."""
        adc_bytes = await self.get(f"{bin_id}.adc")
        roi_bytes = await self.get(f"{bin_id}.roi")
        return await asyncio.to_thread(_extract_roi_images, bin_id, adc_bytes, roi_bytes, rois)

    async def put(self, key: str, data: bytes) -> None:
        """Store the given bin file. Default raises NotImplementedError."""
        raise NotImplementedError("bin store is read-only")

    async def read_image(self, roi_id: str):
        """Extract a single PIL Image by ROI ID."""
        bin_id, target_num = parse_roi_id(roi_id)
        images = await self.read_images(bin_id, rois={target_num})
        if target_num not in images:
            raise KeyError(roi_id)
        return images[target_num]

    async def get_path(self, key: str) -> str | None:
        """Return the filesystem path for the given key, or None if unavailable.

        Only overridden by filesystem-backed stores; all others return None.
        """
        return None

    async def iter_images(self, bin_id: str, rois=None):
        """Async generator yielding (roi_id, PIL.Image) pairs.

        Default loads all images via read_images first; the filesystem store
        overrides this to stream one image at a time without holding them all
        in memory simultaneously.
        """
        images = await self.read_images(bin_id, rois=rois)
        for target_num, image in images.items():
            yield add_target(bin_id, target_num), image


class AsyncFilesystemBinStore(AsyncBinStore):
    """Filesystem-backed async bin store.

    Searches for filesets under root_path using the same include/exclude
    logic as AsyncIfcbDataDirectory.
    """

    def __init__(
        self,
        root_path: str,
        include=DEFAULT_INCLUDE,
        exclude=DEFAULT_EXCLUDE,
    ):
        self.root_path = root_path
        self.include = include
        self.exclude = exclude

    async def _find_path(self, bin_id: str, ext: str) -> str | None:
        """Return absolute path to the given bin file, or None if not found."""
        basepath = await async_find_fileset(
            self.root_path, bin_id,
            include=self.include, exclude=self.exclude,
            require_adc=(ext in ('adc', 'roi')),
            require_roi=(ext == 'roi'),
        )
        if basepath is None:
            return None
        return f"{basepath}.{ext}"

    async def exists(self, key: str) -> bool:
        bin_id, ext = key.rsplit('.', 1)
        return await self._find_path(bin_id, ext) is not None

    async def get(self, key: str) -> bytes:
        bin_id, ext = key.rsplit('.', 1)
        path = await self._find_path(bin_id, ext)
        if path is None:
            raise KeyError(key)
        async with aiofiles.open(path, 'rb') as f:
            return await f.read()

    async def read_images(self, bin_id: str, rois=None) -> dict:
        """Override to keep the .roi file open across all seeks (more efficient than loading into memory)."""
        adc_path = await self._find_path(bin_id, 'adc')
        roi_path = await self._find_path(bin_id, 'roi')
        if adc_path is None or roi_path is None:
            raise KeyError(bin_id)
        if bin_id.startswith('I'):
            w_col, h_col, offset_col = 11, 12, 13
        else:
            w_col, h_col, offset_col = 15, 16, 17
        images = {}
        async with aiofiles.open(roi_path, 'rb') as roi_file:
            async with aiofiles.open(adc_path, 'r') as adc_file:
                i = 0
                async for line in adc_file:
                    fields = line.strip().split(',')
                    try:
                        width = int(fields[w_col])
                        height = int(fields[h_col])
                    except (ValueError, IndexError):
                        i += 1
                        continue
                    if rois is not None and (i + 1) not in rois:
                        pass
                    elif width == 0 or height == 0:
                        pass
                    else:
                        offset = int(fields[offset_col])
                        await roi_file.seek(offset)
                        data = await roi_file.read(width * height)
                        image = await asyncio.to_thread(
                            Image.frombuffer, 'L', (width, height), data, 'raw', 'L', 0, 1
                        )
                        images[i + 1] = image
                    i += 1
        return images

    async def get_path(self, key: str) -> str | None:
        bin_id, ext = key.rsplit('.', 1)
        return await self._find_path(bin_id, ext)

    async def iter_images(self, bin_id: str, rois=None):
        """Stream ROI images one at a time, keeping both files open across seeks."""
        adc_path = await self._find_path(bin_id, 'adc')
        roi_path = await self._find_path(bin_id, 'roi')
        if adc_path is None or roi_path is None:
            raise KeyError(bin_id)
        if bin_id.startswith('I'):
            w_col, h_col, offset_col = 11, 12, 13
        else:
            w_col, h_col, offset_col = 15, 16, 17
        async with aiofiles.open(roi_path, 'rb') as roi_file:
            async with aiofiles.open(adc_path, 'r') as adc_file:
                i = 0
                async for line in adc_file:
                    fields = line.strip().split(',')
                    try:
                        width = int(fields[w_col])
                        height = int(fields[h_col])
                    except (ValueError, IndexError):
                        i += 1
                        continue
                    if rois is not None and (i + 1) not in rois:
                        i += 1
                        continue
                    if width == 0 or height == 0:
                        i += 1
                        continue
                    offset = int(fields[offset_col])
                    await roi_file.seek(offset)
                    data = await roi_file.read(width * height)
                    image = await asyncio.to_thread(
                        Image.frombuffer, 'L', (width, height), data, 'raw', 'L', 0, 1
                    )
                    yield add_target(bin_id, i + 1), image
                    i += 1


class AsyncS3BinStore(AsyncBinStore):
    """S3-backed async bin store.

    Uses KeyTransformingStore with IfcbBinKeyTransformer to map bin file
    basenames to S3 keys of the form '{year}/{bin_id}.{ext}'.
    """

    def __init__(
        self,
        s3_bucket: str,
        s3_client=None,
        s3_prefix: str | None = None,
    ):
        bucket_store = BucketStore(s3_bucket, s3_client)
        if s3_prefix:
            prefix_transformer = PrefixKeyTransformer(prefix=s3_prefix.rstrip("/") + "/")
            inner_store = KeyTransformingStore(bucket_store, prefix_transformer)
        else:
            inner_store = bucket_store
        self._store = KeyTransformingStore(inner_store, IfcbBinKeyTransformer())

    async def exists(self, key: str) -> bool:
        return await asyncio.to_thread(self._store.exists, key)

    async def get(self, key: str) -> bytes:
        return await asyncio.to_thread(self._store.get, key)

    async def put(self, key: str, data: bytes) -> None:
        await asyncio.to_thread(self._store.put, key, data)


class CachingBinStore(AsyncBinStore):
    """Two-tier bin store: S3 as cache, filesystem as source of truth.

    On get: tries S3 first; on miss falls back to filesystem and promotes
    the file to S3 so subsequent reads are served from S3.
    """

    def __init__(
        self,
        fs: AsyncBinStore | None = None,
        s3: AsyncS3BinStore | None = None,
    ):
        self.fs = fs
        self.s3 = s3

    async def exists(self, key: str) -> bool:
        if self.s3 and await self.s3.exists(key):
            return True
        if self.fs and await self.fs.exists(key):
            return True
        return False

    async def get(self, key: str) -> bytes:
        # Try S3 first (already promoted)
        if self.s3 and await self.s3.exists(key):
            return await self.s3.get(key)
        # Fall back to filesystem; promote to S3
        if self.fs:
            data = await self.fs.get(key)
            if self.s3:
                try:
                    await self.s3.put(key, data)
                except Exception:
                    pass  # best-effort
            return data
        raise KeyError(key)

    async def read_images(self, bin_id: str, rois=None) -> dict:
        """Delegate to the most efficient available store.

        Prefers the filesystem store when present because AsyncFilesystemBinStore
        keeps the .roi file open across all seeks rather than loading the full
        file into memory. Falls back to S3 (full byte load) only when no
        filesystem store is configured.
        """
        if self.fs:
            return await self.fs.read_images(bin_id, rois=rois)
        if self.s3:
            return await self.s3.read_images(bin_id, rois=rois)
        raise KeyError(bin_id)

    async def get_path(self, key: str) -> str | None:
        if self.fs:
            return await self.fs.get_path(key)
        return None

    async def iter_images(self, bin_id: str, rois=None):
        # Prefer filesystem for memory-efficient seek-based reads, but fall
        # back to S3 on a miss (e.g. bins that were rotated off the filesystem
        # after being promoted to S3).  KeyError from the filesystem store is
        # only ever raised before any yields, so catching it here is safe.
        if self.fs:
            try:
                async for item in self.fs.iter_images(bin_id, rois=rois):
                    yield item
                return
            except KeyError:
                pass
        if self.s3:
            async for item in self.s3.iter_images(bin_id, rois=rois):
                yield item
            return
        raise KeyError(bin_id)

    async def put(self, key: str, data: bytes) -> None:
        if self.s3:
            await self.s3.put(key, data)
            return
        raise NotImplementedError("CachingBinStore requires an S3 store to support put")
