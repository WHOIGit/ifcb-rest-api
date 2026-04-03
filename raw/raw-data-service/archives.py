"""Archive builders for IFCB raw bins and ROI images."""

import asyncio
from io import BytesIO

from ifcbkit.stores import AsyncBinStore


async def build_bin_archive(store: AsyncBinStore, bin_id: str, extension: str) -> bytes:
    """Build a zip or tgz archive of the three raw bin files (.hdr, .adc, .roi)."""
    import tarfile
    import zipfile

    buffer = BytesIO()

    hdr_path, adc_path, roi_path = await asyncio.gather(
        store.get_path(f"{bin_id}.hdr"),
        store.get_path(f"{bin_id}.adc"),
        store.get_path(f"{bin_id}.roi"),
    )

    if all([hdr_path, adc_path, roi_path]):
        paths = {"hdr": hdr_path, "adc": adc_path, "roi": roi_path}
        if extension == "zip":
            def write_zip():
                try:
                    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as zipf:
                        for ext, path in paths.items():
                            zipf.write(path, arcname=f"{bin_id}.{ext}")
                except FileNotFoundError:
                    raise KeyError(bin_id)
            await asyncio.to_thread(write_zip)
        elif extension == "tgz":
            def write_tgz():
                try:
                    with tarfile.open(fileobj=buffer, mode="w:gz") as tarf:
                        for ext, path in paths.items():
                            tarf.add(path, arcname=f"{bin_id}.{ext}")
                except FileNotFoundError:
                    raise KeyError(bin_id)
            await asyncio.to_thread(write_tgz)
    else:
        hdr_bytes, adc_bytes, roi_bytes = await asyncio.gather(
            store.get(f"{bin_id}.hdr"),
            store.get(f"{bin_id}.adc"),
            store.get(f"{bin_id}.roi"),
        )
        raw_files = {"hdr": hdr_bytes, "adc": adc_bytes, "roi": roi_bytes}
        if extension == "zip":
            def write_zip():
                with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as zipf:
                    for ext, data in raw_files.items():
                        zipf.writestr(f"{bin_id}.{ext}", data)
            await asyncio.to_thread(write_zip)
        elif extension == "tgz":
            def write_tgz():
                with tarfile.open(fileobj=buffer, mode="w:gz") as tarf:
                    for ext, data in raw_files.items():
                        info = tarfile.TarInfo(name=f"{bin_id}.{ext}")
                        info.size = len(data)
                        tarf.addfile(tarinfo=info, fileobj=BytesIO(data))
            await asyncio.to_thread(write_tgz)

    buffer.seek(0)
    return buffer.getvalue()


async def build_roi_archive(store: AsyncBinStore, bin_id: str, extension: str) -> bytes:
    """Build a zip or tar archive of all ROI images for a given bin as PNGs."""
    import tarfile
    import zipfile

    def encode_png(image) -> bytes:
        buf = BytesIO()
        image.save(buf, format="PNG")
        return buf.getvalue()

    buffer = BytesIO()
    if extension == "zip":
        with zipfile.ZipFile(buffer, "w") as zipf:
            async for roi_id, image in store.iter_images(bin_id):
                image_bytes = await asyncio.to_thread(encode_png, image)
                zipf.writestr(f"{roi_id}.png", image_bytes)
    elif extension == "tar":
        with tarfile.open(fileobj=buffer, mode="w") as tarf:
            async for roi_id, image in store.iter_images(bin_id):
                image_bytes = await asyncio.to_thread(encode_png, image)
                info = tarfile.TarInfo(name=f"{roi_id}.png")
                info.size = len(image_bytes)
                tarf.addfile(tarinfo=info, fileobj=BytesIO(image_bytes))

    buffer.seek(0)
    return buffer.getvalue()
