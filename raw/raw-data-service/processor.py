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
                required_scopes=["ifcb:rois:list"],
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
                required_scopes=["ifcb:metadata:read"],
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
                required_scopes=["ifcb:images:read"],
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
                required_scopes=["ifcb:images:read"],
            ),

        ]

    def data_directory(self) -> IfcbDataDirectory:
        return IfcbDataDirectory(os.getenv("IFCB_RAW_DATA_DIR", "/data/raw"))
    
    async def raw_data_paths(self, bin_id: str):
        dd = self.data_directory()
        return await dd.paths(bin_id)
    
    async def handle_raw_file_request(self, path_params: RawBinParams, token_info=None):
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

    async def handle_raw_archive_file_request(self, path_params: RawBinArchiveParams, token_info=None):
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

    async def handle_roi_list_request(self, path_params: BinIDParams, token_info=None):
        """ Retrieve list of ROI IDs associated with the bin. """

        dd = self.data_directory()
        pid = path_params.bin_id
        image_list = await dd.list_images(pid)
        return image_list

    async def handle_roi_image_request(self, path_params: ROIImageParams, token_info=None):
        """ Retrieve a specific ROI image. """
        dd = self.data_directory()
        roi_id = path_params.roi_id
        pid, target = parse_target(roi_id)
        images = await dd.read_images(pid, rois=[target])
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
        media_type = {
            "png": "image/png",
            "jpg": "image/jpeg",
        }[path_params.extension]
        return render_bytes(img_buffer.getvalue(), media_type)

    async def handle_metadata_request(self, path_params: BinIDParams, token_info=None):
        """ Retrieve metadata from the header file. """
        try:
            paths = await self.raw_data_paths(path_params.bin_id)
        except KeyError:
            raise HTTPException(status_code=404, detail=f"Bin ID {path_params.bin_id} not found.")
        hdr_path = paths.get("hdr")
        props = await asyncio.to_thread(parse_hdr_file, hdr_path)
        return props

    async def handle_roi_archive_request(self, path_params: ROIArchiveParams, token_info=None):
        """ Retrieve a tar/zip archive of ROI images for a given bin. """
        dd = self.data_directory()
        pid = path_params.bin_id
        images = await dd.read_images(pid)

        def format_image(image):
            img_buffer = BytesIO()
            image.save(img_buffer, format='PNG')
            img_buffer.seek(0)
            return img_buffer.getvalue()

        buffer = BytesIO()
        if path_params.extension == "zip":
            import zipfile
            with zipfile.ZipFile(buffer, 'w') as zipf:
                for target, image in images.items():
                    roi_id = add_target(pid, target)
                    await asyncio.to_thread(zipf.writestr, f"{roi_id}.png", format_image(image))
        elif path_params.extension == "tar":
            import tarfile
            with tarfile.open(fileobj=buffer, mode='w') as tarf:
                for target, image in images.items():
                    roi_id = add_target(pid, target)
                    info = tarfile.TarInfo(name=f"{roi_id}.png")
                    img_data = await asyncio.to_thread(format_image, image)
                    info.size = len(img_data)
                    await asyncio.to_thread(tarf.addfile, tarinfo=info, fileobj=BytesIO(img_data))

        buffer.seek(0)
        media_type = {
            "zip": "application/zip",
            "tar": "application/x-tar",
        }[path_params.extension]
        return render_bytes(buffer.getvalue(), media_type)
