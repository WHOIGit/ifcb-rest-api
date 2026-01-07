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
