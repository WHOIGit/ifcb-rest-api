"""Pydantic path parameter models for the IFCB raw data service."""
from typing import Literal

from pydantic import BaseModel, Field


class RawBinParams(BaseModel):
    """Path parameters for the raw_file endpoint."""
    bin_id: str = Field(..., description="ID of bin to retrieve.")
    extension: Literal["hdr", "adc", "roi"]


class RawBinArchiveParams(BaseModel):
    """Path parameters for the raw_archive_file endpoint."""
    bin_id: str = Field(..., description="ID of bin to retrieve.")
    extension: Literal["zip", "tgz"]


class BinIDParams(BaseModel):
    """Path parameter model with only bin_id."""
    bin_id: str = Field(..., description="ID of bin")


class ROIImageParams(BaseModel):
    """Path parameters for the roi_image endpoint."""
    roi_id: str = Field(..., description="ID of ROI")
    extension: Literal["png", "jpg"]


class ROIArchiveParams(BaseModel):
    """Path parameters for the roi_archive endpoint."""
    bin_id: str = Field(..., description="ID of bin")
    extension: Literal["zip", "tar"]
