"""IFCB PID to S3 key transformer for use with KeyTransformingStore."""

from .ifcb_parsing import parse_ifcb_pid


class IfcbPidTransformer:
    """Transforms IFCB PIDs to/from S3 keys.

    Transforms PIDs like 'D20250114T172241_IFCB109_00002' to S3 keys like
    'ifcb_data/2025/D20250114T172241_IFCB109/00002.png'.
    """

    def __init__(self, prefix=""):
        """Initialize transformer with optional S3 key prefix.

        Args:
            prefix: Optional prefix to prepend to all keys (e.g., 'ifcb_data')
        """
        self.prefix = prefix.rstrip("/") if prefix else ""

    def transform_key(self, pid: str) -> str:
        """Transform IFCB PID to S3 key.

        Args:
            pid: IFCB PID like 'D20250114T172241_IFCB109_00002'

        Returns:
            S3 key like 'ifcb_data/2025/D20250114T172241_IFCB109/00002.png'
        """
        parsed = parse_ifcb_pid(pid)
        bin_lid = parsed['bin_lid']
        roi_number = parsed['target']

        # Extract year from bin_lid
        if bin_lid.startswith("D") and len(bin_lid) >= 5:
            year = bin_lid[1:5]  # D20250114... -> 2025
        else:
            year = "legacy"

        if self.prefix:
            return f"{self.prefix}/{year}/{bin_lid}/{roi_number:05d}.png"
        else:
            return f"{year}/{bin_lid}/{roi_number:05d}.png"

    def reverse_transform_key(self, s3_key: str) -> str:
        """Transform S3 key back to IFCB PID.

        Args:
            s3_key: S3 key like 'ifcb_data/2025/D20250114T172241_IFCB109/00002.png'

        Returns:
            IFCB PID like 'D20250114T172241_IFCB109_00002'
        """
        # Remove prefix if present
        if self.prefix and s3_key.startswith(self.prefix + "/"):
            s3_key = s3_key[len(self.prefix) + 1:]

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
