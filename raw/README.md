## Raw Data Service

REST API for serving IFCB raw data files and ROI images.

### Required Environment Variables

- `HOST_IFCB_RAW_DATA_DIR` - **Required**. Path on host machine to IFCB raw data directory (mounted to `/data/raw` in container)

### ROI Backend Configuration

- `ROI_BACKEND` - Optional, defaults to `s3`. Set to `fs` to use local ROI files instead of S3

**When `ROI_BACKEND=s3` (default):**
- `S3_BUCKET_NAME` - **Required**. S3 bucket name
- `S3_ACCESS_KEY` - **Required**. S3 access key
- `S3_SECRET_KEY` - **Required**. S3 secret key
- `S3_ENDPOINT_URL` - Optional. S3 endpoint URL (for non-AWS S3 services)
- `S3_PREFIX` - Optional. Key prefix for S3 objects (e.g., `ifcb_data`)
- `S3_CONCURRENT_REQUESTS` - Optional, defaults to `50`. Number of concurrent S3 requests when fetching ROI archives

**When `ROI_BACKEND=fs`:**
- ROI images are extracted from local `.roi` files on-the-fly

### Optional Configuration

- `PORT` - Optional, defaults to `8001`. Port for the service (both host and container)

### Capacity Limiting

Rate limits concurrent requests via Redis to prevent overload.

- `REDIS_URL` - Redis connection URL (e.g., `redis://redis:6379/0`). Required for capacity limiting.
- `GLOBAL_CAPACITY_LIMIT` - Max concurrent requests before auth. Default `100`.
- `CAPACITY_GROUPS` - JSON config for per-endpoint group limits. Default `{"fast": 40, "slow": 5}`.
- `CAPACITY_RETRY_AFTER` - Retry-After header value (seconds) on 429. Default `1`.
- `CAPACITY_KEY_TTL` - Redis key TTL in seconds (crash recovery). Default `30`.

- `ENDPOINT_CAPACITY_MAP` - JSON mapping endpoints to groups. Default:
  ```json
  {"raw-file": "fast", "raw-archive-file": "slow", "roi-ids": "fast", "metadata": "fast", "roi-image": "fast", "roi-archive": "slow"}
  ```

Example custom config with three groups:
```bash
CAPACITY_GROUPS='{"critical": 100, "standard": 40, "heavy": 5}'
ENDPOINT_CAPACITY_MAP='{"roi-image": "critical", "raw-file": "standard", "metadata": "standard", "roi-ids": "standard", "raw-archive-file": "heavy", "roi-archive": "heavy"}'
```

### Gunicorn Configuration

- `GUNICORN_WORKERS` - Number of worker processes. Default `50` (docker-compose) / `8` (Dockerfile).
- `GUNICORN_TIMEOUT` - Worker timeout in seconds. Default `120`.
