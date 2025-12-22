## Required README for package install

Environment variables for ROI access (S3-backed by default):
- `ROI_BACKEND` (optional, `s3` default; set to `fs` to use local ROI files)
- `S3_BUCKET_NAME` (required when `ROI_BACKEND=s3`)
- `S3_ACCESS_KEY` (required when `ROI_BACKEND=s3`)
- `S3_SECRET_KEY` (required when `ROI_BACKEND=s3`)
- `S3_ENDPOINT_URL` (optional, for non-AWS S3)
- `S3_PREFIX` (optional key prefix)
- `IFCB_RAW_DATA_DIR` (optional, defaults to `/data/raw` for metadata lookups)

Notes:
- When `ROI_BACKEND=s3`, ROI list/archive are derived from S3 keys (no ADC fallback).
