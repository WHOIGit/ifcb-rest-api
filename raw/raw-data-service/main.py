"""FastAPI entrypoint for the IFCB raw data service."""
import os

from anyio import CapacityLimiter, WouldBlock
from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from stateless_microservice import ServiceConfig, create_app, AuthClient

from .processor import RawProcessor


class CapacityLimiterMiddleware(BaseHTTPMiddleware):
    """Limits concurrent requests per worker. Returns 429 when at capacity."""

    def __init__(self, app, max_concurrent: int = 50, path_prefix: str | None = None):
        super().__init__(app)
        self.limiter = CapacityLimiter(max_concurrent)
        self.path_prefix = path_prefix

    async def dispatch(self, request: Request, call_next):
        # Skip limiting if path doesn't match prefix
        if self.path_prefix and not request.url.path.startswith(self.path_prefix):
            return await call_next(request)

        try:
            self.limiter.acquire_nowait()
        except WouldBlock:
            return JSONResponse(
                status_code=429,
                content={"detail": "Server at capacity. Please retry later."},
                headers={"Retry-After": "5"}
            )

        try:
            return await call_next(request)
        finally:
            self.limiter.release()


config = ServiceConfig(description="IFCB raw data service.")

auth_service_url = os.getenv("AUTH_SERVICE_URL")
if not auth_service_url:
    raise ValueError("AUTH_SERVICE_URL environment variable is required")

auth_client = AuthClient(auth_service_url=auth_service_url)

app = create_app(RawProcessor(), config, auth_client=auth_client)

# Per-worker capacity limit for ROI endpoint (total capacity = workers × this value)
max_concurrent = int(os.getenv("MAX_CONCURRENT_REQUESTS_PER_WORKER", "50"))
app.add_middleware(CapacityLimiterMiddleware, max_concurrent=max_concurrent, path_prefix="/image/roi/")
