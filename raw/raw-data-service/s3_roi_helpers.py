"""Helper functions for S3-backed ROI operations."""


def list_roi_ids_from_s3(bucket_store, bin_id: str, prefix: str = "") -> list:
    """List ROI IDs for a given bin from S3.

    Args:
        bucket_store: BucketStore instance for S3 access
        bin_id: IFCB bin ID (e.g., 'D20250114T172241_IFCB109')
        prefix: Optional S3 key prefix (e.g., 'ifcb_data')

    Returns:
        List of ROI IDs like ['D20250114T172241_IFCB109_00001', ...]

    S3 structure: {prefix}/{year}/{bin_lid}/{roi_number:05d}.png
    Example: ifcb_data/2025/D20250114T172241_IFCB109/00002.png
    """
    # Extract year from bin_id
    if bin_id.startswith("D") and len(bin_id) >= 5:
        year = bin_id[1:5]  # D20250114... -> 2025
    else:
        year = "legacy"

    # Construct S3 prefix
    prefix = prefix.rstrip("/") if prefix else ""
    if prefix:
        search_prefix = f"{prefix}/{year}/{bin_id}/"
    else:
        search_prefix = f"{year}/{bin_id}/"

    roi_ids = []
    for key in bucket_store.keys(prefix=search_prefix):
        filename = key.split("/")[-1]
        if not filename.endswith(".png"):
            continue

        # Parse filename: {target:05d}.png
        # Example: 00002.png
        try:
            # Remove .png extension and parse as integer
            target_str = filename[:-4]
            target = int(target_str)
            roi_ids.append(f"{bin_id}_{target:05d}")
        except ValueError:
            continue

    roi_ids.sort()
    return roi_ids
