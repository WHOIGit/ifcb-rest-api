"""FastAPI entrypoint for the IFCB raw data service."""

from stateless_microservice import ServiceConfig, create_app

from .processor import RawProcessor

config = ServiceConfig(
            description="IFCB raw data service.",
            )

app = create_app(RawProcessor(), config)
