"""Stateless template processor."""
import os

from typing import List, Literal

import aiofiles
from fastapi import HTTPException
from pydantic import BaseModel, Field

from stateless_microservice import BaseProcessor, StatelessAction, render_bytes

from .ifcb import IfcbDataDirectory


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
            ),
            StatelessAction(
                name="raw-archive-file",
                path="/data/raw/{bin_id}.{extension}",
                path_params_model=RawBinArchiveParams,
                handler=self.handle_raw_file_request,
                summary="Serve raw IFCB bin files in an archive.",
                description="Serve raw IFCB bin files in an archive.",
                tags=("IFCB",),
                methods=("GET",),
            ),
            StatelessAction(
                name="archive-zip",
                path="/data/archive/{bin_id}.zip",
                path_params_model=BinIDParams,
                handler=self.handle_archive_zip_request,
                summary="Serve archival zip formatted data.",
                description="Serve archival zip formatted data",
                tags=("IFCB",),
                methods=("GET",),
            ),
            StatelessAction(
                name="roi-ids",
                path="/data/rois/{bin_id}.json",
                path_params_model=BinIDParams,
                handler=self.handle_roi_id_request,
                summary="Serve list of ROI IDs associated with the bin.",
                description="Serve list of ROI IDs associated with the bin.",
                tags=("IFCB",),
                methods=("GET",),
            ),
            StatelessAction(
                name="metadata",
                path="/data/rois/{bin_id}.json",
                path_params_model=BinIDParams,
                handler=self.handle_roi_id_request,
                summary="Serve metadata from the header file.",
                description="Serve metadata from the header file.",
                tags=("IFCB",),
                methods=("GET",),
            ),
            StatelessAction(
                name="roi-image",
                path="/image/roi/{roi_id}.{extension}",
                path_params_model=ROIImageParams,
                handler=self.handle_roi_id_request,
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
        dd = IfcbDataDirectory(os.getenv("IFCB_RAW_DATA_DIR", "/data/raw"), require_adc=require_adc, require_roi=require_roi)
        try:
            paths = await dd.paths(path_params.bin_id)
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

        if path_params.compression_type == "zip":
            media_type = "application/zip"
        elif path_params.compression_type == "tgz":
            media_type = "application/gzip"

        content = "" #TODO
        return render_bytes(content, media_type)

    async def handle_archive_zip_request(self, path_params: BinIDParams):
        """ Retrieve archival storage in zip format provided by bin2zip_stream. """

        # TODO

    async def handle_roi_id_request(self, path_params: BinIDParams):
        """ Retrieve list of ROI IDs associated with the bin. """

        # TODO

    async def handle_roi_image_request(self, path_params: ROIImageParams):
        """ Retrieve a specific ROI image. """

        # TODO

    async def handle_metadata_request(self, path_params: BinIDParams):
        """ Retrieve metadata from the header file. """

        # TODO

    async def handle_roi_archive_request(self, path_params: ROIArchiveParams):
        """ Retrieve a tar/zip archive of ROI images for a given bin. """

        # TODO
