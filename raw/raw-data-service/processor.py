"""Stateless template processor."""

from typing import List

from pydantic import BaseModel, Field

from stateless_microservice import BaseProcessor, StatelessAction, render_bytes


class RawBinParams(BaseModel):
    """ Path parameters for the raw_file endpoint. """

    bin_id: str = Field(..., description="ID of bin to retrieve.")
    extension: str = Field(..., description="Extension to use (hdr, adc, roi)"


class RawBinArchiveParams(BaseModel):
    """ Path parameters for the raw_zip_file endpoint. """

    bin_id: str = Field(..., description="ID of bin to retrieve.")
    extension: str = Field(..., description="Extension to use (zip, tgz)"


class RawProcessor(BaseProcessor):
    """Processor for raw data requests."""

    @property
    def name(self) -> str:
        return "raw-data-server"

    def get_stateless_actions(self) -> List[StatelessAction]:
        return [
            StatelessAction(
                name="raw-file",
                path="/data/raw/{bin id}.{extension}",
                path_param_model=RawBinParams,
                handler=self.handle_raw_file_request,
                summary="Serve a raw IFCB file.",
                description="Serve a raw IFCB file..",
                tags=("IFCB",),
                methods=("GET",),
            ),
            StatelessAction(
                name="raw-archive-file",
                path="/data/raw/{bin id}.{extension}",
                path_param_model=RawBinArchiveParams,
                handler=self.handle_raw_file_request,
                summary="Serve raw IFCB bin files in an archive.",
                description="Serve raw IFCB bin files in an archive.",
                tags=("IFCB",),
                methods=("GET",),
            ),

        ]

    async def handle_raw_file_request(self, path_params: RawBinParams):
        """ Retrieve raw IFCB files. """

        # TODO
        return "" 

    async def handle_raw_archive_file_request(self, path_params: RawBinArchiveParams):
        """ Retrieve raw IFCB files in a zip or tar/gzip archive. """

        if path_params.compression_type == "zip":
            media_type = "application/zip"
        elif path_params.compression_type == "tgz":
            media_type = "application/gzip"

        content = "" #TODO
        return render_bytes(content, media_type)
