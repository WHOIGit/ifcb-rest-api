"""IFCB PID parsing, S3 key transformation, and S3 ROI utilities.

This module provides:
- PID parsing for IFCB identifiers
- PID to S3 key transformation for use with KeyTransformingStore
- S3 ROI listing utilities

Supports both V1 and V2 IFCB PID formats:
- V2: D20160714T023910_IFCB101_00014
- V1: IFCB3_2008_013_423456_00014
"""

import re


def parse_ifcb_pid(pid: str) -> dict:
    """Parse an IFCB PID to extract bin_lid and target.

    Args:
        pid: IFCB permanent identifier (may include path prefix,
             product suffix, or file extension)

    Returns:
        dict with keys:
            - 'bin_lid': The bin identifier (without target)
            - 'target': The target number as an integer (or None if no target)

    Raises:
        ValueError: If the PID format is invalid

    Examples:
        >>> parse_ifcb_pid('D20160714T023910_IFCB101_00014.png')
        {'bin_lid': 'D20160714T023910_IFCB101', 'target': 14}

        >>> parse_ifcb_pid('IFCB3_2008_013_423456_00014')
        {'bin_lid': 'IFCB3_2008_013_423456', 'target': 14}

        >>> parse_ifcb_pid('/path/to/D20160714T023910_IFCB101.adc')
        {'bin_lid': 'D20160714T023910_IFCB101', 'target': None}
    """
    # Strip Windows directory prefixes and path components
    pid = re.sub(r'^.*\\', '', pid)  # Remove Windows paths
    pid = re.sub(r'^.*/', '', pid)    # Remove Unix paths

    # V2 pattern: D<yyyymmddTHHMMSS>_IFCB<###>[_<target>][_<product>][.<ext>]
    v2_pattern = r'^(D\d{8}T\d{6}_IFCB\d+)(?:_(\d+))?(?:_[a-zA-Z][a-zA-Z0-9_]*)?(?:\.[a-zA-Z][a-zA-Z0-9]*)?$'
    match = re.match(v2_pattern, pid)

    if match:
        bin_lid = match.group(1)
        target = int(match.group(2)) if match.group(2) else None
        return {'bin_lid': bin_lid, 'target': target}

    # V1 pattern: IFCB<#>_<yyyy>_<DDD>_<HHMMSS>[_<target>][_<product>][.<ext>]
    v1_pattern = r'^(IFCB\d+_\d{4}_\d{3}_\d{6})(?:_(\d+))?(?:_[a-zA-Z][a-zA-Z0-9_]*)?(?:\.[a-zA-Z][a-zA-Z0-9]*)?$'
    match = re.match(v1_pattern, pid)

    if match:
        bin_lid = match.group(1)
        target = int(match.group(2)) if match.group(2) else None
        return {'bin_lid': bin_lid, 'target': target}

    raise ValueError(f'Invalid IFCB PID format: {pid}')


class IfcbPidTransformer:
    """Transforms IFCB PIDs to/from S3 keys for use with KeyTransformingStore.

    Transforms PIDs like 'D20250114T172241_IFCB109_00002' to S3 keys like
    '2025/D20250114T172241_IFCB109/00002.png'.

    For adding a prefix, compose this with PrefixKeyTransformer from storage.utils.
    """

    def transform_key(self, pid: str) -> str:
        """Transform IFCB PID to S3 key.

        Args:
            pid: IFCB PID like 'D20250114T172241_IFCB109_00002'

        Returns:
            S3 key like '2025/D20250114T172241_IFCB109/00002.png'
        """
        parsed = parse_ifcb_pid(pid)
        bin_lid = parsed['bin_lid']
        roi_number = parsed['target']

        # Extract year from bin_lid
        if bin_lid.startswith("D") and len(bin_lid) >= 5:
            year = bin_lid[1:5]  # D20250114... -> 2025
        else:
            year = "legacy"

        return f"{year}/{bin_lid}/{roi_number:05d}.png"

    def reverse_transform_key(self, s3_key: str) -> str:
        """Transform S3 key back to IFCB PID.

        Args:
            s3_key: S3 key like '2025/D20250114T172241_IFCB109/00002.png'

        Returns:
            IFCB PID like 'D20250114T172241_IFCB109_00002'
        """
        parts = s3_key.split('/')
        if len(parts) < 3:
            raise ValueError(f"Invalid S3 key format: {s3_key}")

        # parts: ['2025', 'D20250114T172241_IFCB109', '00002.png']
        bin_lid = parts[-2]
        filename = parts[-1]

        # Remove .png extension and parse ROI number
        if not filename.endswith('.png'):
            raise ValueError(f"Expected .png file, got: {filename}")
        roi_number = filename[:-4]

        return f"{bin_lid}_{roi_number}"


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
