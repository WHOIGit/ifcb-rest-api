"""FastAPI entrypoint for the IFCB raw data service."""
import os

from stateless_microservice import ServiceConfig, create_app, AuthClient

from .processor import RawProcessor

config = ServiceConfig(
            description="IFCB raw data service.",
            )

auth_service_url = os.getenv("AUTH_SERVICE_URL")
if not auth_service_url:
    raise ValueError("AUTH_SERVICE_URL environment variable is required")

auth_client = AuthClient(auth_service_url=auth_service_url)

app = create_app(RawProcessor(), config, auth_client=auth_client)
