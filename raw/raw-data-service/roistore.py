"""S3-backed store for IFCB ROI images (ported from ifcb-roi-service)."""

from ifcb import Pid
from storage.object import ObjectStore
from storage.s3 import BucketStore


class IfcbRoiStore(ObjectStore):
    """S3-backed store for accessing IFCB ROI images."""

    @classmethod
    def base64_store(
        cls,
        bucket_name: str,
        endpoint_url: str = None,
        s3_access_key: str = None,
        s3_secret_key: str = None,
        prefix: str = "",
    ):
        """Create a Base64Store for IFCB ROI images stored in S3."""
        from storage.utils import Base64Store

        store = cls(
            bucket_name=bucket_name,
            endpoint_url=endpoint_url,
            s3_access_key=s3_access_key,
            s3_secret_key=s3_secret_key,
            prefix=prefix,
        )
        return Base64Store(store)

    def __init__(
        self,
        bucket_name: str,
        endpoint_url: str = None,
        s3_access_key: str = None,
        s3_secret_key: str = None,
        prefix: str = "",
    ):
        """Initialize S3-backed ROI store."""
        super().__init__()
        self.bucket_store = BucketStore(
            s3_url=endpoint_url,
            s3_access_key=s3_access_key,
            s3_secret_key=s3_secret_key,
            bucket_name=bucket_name,
        )
        self.bucket_store.__enter__()
        self.prefix = prefix.rstrip("/")

    def _make_key(self, pid: str) -> str:
        """Convert PID to S3 key."""
        parsed_pid = Pid(pid)
        bin_lid = parsed_pid.bin_lid
        roi_number = int(parsed_pid.target)

        # Extract year from bin_lid (bins start with 'D' followed by year)
        year = bin_lid[1:5] if bin_lid.startswith("D") else "legacy"

        if self.prefix:
            key = f"{self.prefix}/{year}/{bin_lid}/{roi_number:05d}.png"
        else:
            key = f"{year}/{bin_lid}/{roi_number:05d}.png"

        return key

    def get(self, key: str) -> bytes:
        """Get ROI image data by PID."""
        s3_key = self._make_key(key)
        return self.bucket_store.get(s3_key)

    def exists(self, key: str) -> bool:
        """Check if ROI image exists for given PID."""
        s3_key = self._make_key(key)
        return self.bucket_store.exists(s3_key)

    def put(self, key: str, value: bytes):
        """Store ROI image data (not supported in this service)."""
        raise NotImplementedError("write operations are not supported")

    def delete(self, key: str):
        """Delete ROI image (not supported in this service)."""
        raise NotImplementedError("write operations are not supported")

    def keys(self):
        """List all PID keys in the store."""
        for s3_key in self.bucket_store.keys():
            yield s3_key

    def list_roi_ids(self, bin_id: str):
        """List ROI IDs for a given bin using S3 key patterns."""
        year = bin_id[1:5] if bin_id.startswith("D") else "legacy"
        if self.prefix:
            prefix = f"{self.prefix}/{year}/{bin_id}/"
        else:
            prefix = f"{year}/{bin_id}/"
        roi_ids = []
        for key in self.bucket_store.keys(prefix=prefix):
            filename = key.split("/")[-1]
            if not filename.endswith(".png"):
                continue
            try:
                target = int(filename[:-4])
            except ValueError:
                continue
            roi_ids.append(f"{bin_id}_{target:05d}")
        roi_ids.sort()
        return roi_ids
