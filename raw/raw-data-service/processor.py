"""Stateless template processor."""
import asyncio
from io import BytesIO
import os

from typing import List, Literal

import aiofiles
from fastapi import HTTPException
from pydantic import BaseModel, Field

from stateless_microservice import BaseProcessor, StatelessAction, render_bytes

from .ifcb import IfcbDataDirectory, add_target, parse_target
from .ifcbhdr import parse_hdr_file
from .roistore import IfcbRoiStore
from PIL import Image


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
        self.raw_data_dir = os.getenv("IFCB_RAW_DATA_DIR", "/data/raw")
        self._data_dir = IfcbDataDirectory(self.raw_data_dir)
        # Metadata lookup does not require local .roi files
        self._roi_meta_dir = IfcbDataDirectory(self.raw_data_dir, require_roi=False)

        self.roi_backend = os.getenv("ROI_BACKEND", "s3").lower()

        if self.roi_backend not in ("s3", "fs"):
            raise ValueError("ROI_BACKEND must be 's3' or 'fs'")

        self.s3_bucket = os.getenv("S3_BUCKET_NAME")
        self.s3_endpoint = os.getenv("S3_ENDPOINT_URL")
        self.s3_access_key = os.getenv("S3_ACCESS_KEY")
        self.s3_secret_key = os.getenv("S3_SECRET_KEY")
        self.s3_prefix = os.getenv("S3_PREFIX", "")

        if self.roi_backend == "s3":
            if not self.s3_bucket:
                raise ValueError("S3_BUCKET_NAME environment variable is required when ROI_BACKEND=s3")
            if not self.s3_access_key:
                raise ValueError("S3_ACCESS_KEY environment variable is required when ROI_BACKEND=s3")
            if not self.s3_secret_key:
                raise ValueError("S3_SECRET_KEY environment variable is required when ROI_BACKEND=s3")

            self.roi_store = IfcbRoiStore(
                bucket_name=self.s3_bucket,
                endpoint_url=self.s3_endpoint,
                s3_access_key=self.s3_access_key,
                s3_secret_key=self.s3_secret_key,
                prefix=self.s3_prefix,
            )
        else:
            # Legacy filesystem ROI access
            self._roi_fs_dir = IfcbDataDirectory(self.raw_data_dir)

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
            ),

        ]

    def data_directory(self) -> IfcbDataDirectory:
        return self._data_dir
    
    async def raw_data_paths(self, bin_id: str):
        dd = self.data_directory()
        return await dd.paths(bin_id)
    
    async def handle_raw_file_request(self, path_params: RawBinParams):
        """ Retrieve raw IFCB files. """
        if path_params.extension == "adc":
            require_adc = True
            require_roi = False
        elif path_params.extension == "roi":
            require_adc = True
            require_roi = True
        else:
            require_adc = False
            require_roi = False
        try:
            paths = await self.raw_data_paths(path_params.bin_id)
        except KeyError:
            raise HTTPException(status_code=404, detail=f"Bin ID {path_params.bin_id} not found.")
        path = paths.get(path_params.extension)
        if path is None:
            raise FileNotFoundError(f"No file with extension {path_params.extension} for bin {path_params.bin_id}")
        # read file contents from path
        async with aiofiles.open(path, mode='rb') as f:
            content = await f.read()
        media_type = {
            "hdr": "text/plain",
            "adc": "text/csv",
            "roi": "application/octet-stream",
        }[path_params.extension]
        return render_bytes(content, media_type)

    async def handle_raw_archive_file_request(self, path_params: RawBinArchiveParams):
        """ Retrieve raw IFCB files in a zip or tar/gzip archive. """

        if path_params.extension == "zip":
            media_type = "application/zip"
        elif path_params.extension == "tgz":
            media_type = "application/gzip"

        paths = await self.raw_data_paths(path_params.bin_id)

        buffer = BytesIO()

        if path_params.extension == "zip":
            import zipfile
            with zipfile.ZipFile(buffer, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
                for ext, path in paths.items():
                    await asyncio.to_thread(zipf.write, path, arcname=f"{path_params.bin_id}.{ext}")
        elif path_params.extension == "tgz":
            import tarfile
            with tarfile.open(fileobj=buffer, mode='w:gz') as tarf:
                for ext, path in paths.items():
                    await asyncio.to_thread(tarf.add, path, arcname=f"{path_params.bin_id}.{ext}")

        buffer.seek(0)
        
        return render_bytes(buffer.getvalue(), media_type)

    async def handle_roi_list_request(self, path_params: BinIDParams):
        """ Retrieve list of ROI IDs associated with the bin. """

        pid = path_params.bin_id
        dd = self._roi_meta_dir

        if self.roi_backend == "s3":
            roi_ids = await asyncio.to_thread(self.roi_store.list_roi_ids, pid)
            if roi_ids is None:
                raise HTTPException(status_code=404, detail=f"Bin ID {pid} not found.")
            return {idx + 1: {"roi_id": roi_id} for idx, roi_id in enumerate(roi_ids)}

        image_list = await dd.list_images(pid)
        return image_list

    async def handle_roi_image_request(self, path_params: ROIImageParams):
        """ Retrieve a specific ROI image. """
        roi_id = path_params.roi_id
        media_type = {
            "png": "image/png",
            "jpg": "image/jpeg",
        }[path_params.extension]

        if self.roi_backend == "s3":
            exists = await asyncio.to_thread(self.roi_store.exists, roi_id)
            if not exists:
                raise HTTPException(status_code=404, detail=f"ROI ID {roi_id} not found.")

            png_bytes = await asyncio.to_thread(self.roi_store.get, roi_id)

            if path_params.extension == "png":
                return render_bytes(png_bytes, media_type)

            img_buffer = BytesIO()
            image = Image.open(BytesIO(png_bytes))
            await asyncio.to_thread(image.convert("RGB").save, img_buffer, format="JPEG")
            img_buffer.seek(0)
            return render_bytes(img_buffer.getvalue(), media_type)

        # Legacy filesystem ROI retrieval
        pid, target = parse_target(roi_id)
        images = await self._roi_fs_dir.read_images(pid, rois=[target])
        image = images.get(target)
        if image is None:
            raise HTTPException(status_code=404, detail=f"ROI ID {roi_id} not found.")
        img_buffer = BytesIO()
        format = {
            "png": "PNG",
            "jpg": "JPEG",
        }[path_params.extension]
        await asyncio.to_thread(image.save, img_buffer, format=format)
        img_buffer.seek(0)
        return render_bytes(img_buffer.getvalue(), media_type)

    async def handle_metadata_request(self, path_params: BinIDParams):
        """ Retrieve metadata from the header file. """
        try:
            paths = await self.raw_data_paths(path_params.bin_id)
        except KeyError:
            raise HTTPException(status_code=404, detail=f"Bin ID {path_params.bin_id} not found.")
        hdr_path = paths.get("hdr")
        props = await asyncio.to_thread(parse_hdr_file, hdr_path)
        return props

    async def handle_roi_archive_request(self, path_params: ROIArchiveParams):
        """ Retrieve a tar/zip archive of ROI images for a given bin. """
        dd = self._roi_meta_dir
        pid = path_params.bin_id
        roi_ids = []
        if self.roi_backend == "s3":
            roi_ids = await asyncio.to_thread(self.roi_store.list_roi_ids, pid)
            if roi_ids is None:
                raise HTTPException(status_code=404, detail=f"Bin ID {pid} not found.")
        else:
            images = await dd.list_images(pid)
            roi_ids = [img["roi_id"] for img in images.values()]
        fs_images = None
        if self.roi_backend == "fs":
            fs_images = await self._roi_fs_dir.read_images(pid)

        def format_image(image):
            img_buffer = BytesIO()
            image.save(img_buffer, format="PNG")
            img_buffer.seek(0)
            return img_buffer.getvalue()

        buffer = BytesIO()
        if path_params.extension == "zip":
            import zipfile
            with zipfile.ZipFile(buffer, 'w') as zipf:
                for roi_id in roi_ids:
                    if self.roi_backend == "s3":
                        image_bytes = await asyncio.to_thread(self.roi_store.get, roi_id)
                    else:
                        _, target = parse_target(roi_id)
                        image = fs_images.get(target) if fs_images else None
                        if image is None:
                            continue
                        image_bytes = await asyncio.to_thread(format_image, image)
                    await asyncio.to_thread(zipf.writestr, f"{roi_id}.png", image_bytes)
        elif path_params.extension == "tar":
            import tarfile
            with tarfile.open(fileobj=buffer, mode='w') as tarf:
                for roi_id in roi_ids:
                    if self.roi_backend == "s3":
                        image_bytes = await asyncio.to_thread(self.roi_store.get, roi_id)
                    else:
                        _, target = parse_target(roi_id)
                        image = fs_images.get(target) if fs_images else None
                        if image is None:
                            continue
                        image_bytes = await asyncio.to_thread(format_image, image)
                    info = tarfile.TarInfo(name=f"{roi_id}.png")
                    info.size = len(image_bytes)
                    await asyncio.to_thread(tarf.addfile, tarinfo=info, fileobj=BytesIO(image_bytes))

        buffer.seek(0)
        media_type = {
            "zip": "application/zip",
            "tar": "application/x-tar",
        }[path_params.extension]
        return render_bytes(buffer.getvalue(), media_type)
