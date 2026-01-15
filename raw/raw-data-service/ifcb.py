import aiofiles
import aiofiles.os as aios
import aiofiles.ospath as aiopath
import asyncio
import os
import re

from PIL import Image


DEFAULT_EXCLUDE = ['skip', 'beads']
DEFAULT_INCLUDE = ['data']

def validate_path(
    filepath,
    exclude=DEFAULT_EXCLUDE,
    include=DEFAULT_INCLUDE,
):
    """
    Validate an IFCB raw data file path.

    A well-formed raw data file path relative to some root only contains
    path components that are not excluded and are either included or part
    of the file's basename (without extension).

    :param filepath: the pathname of the file
    :param exclude: directory names to ignore
    :param include: directory names to include, even if they do not match
      the path's basename
    :returns bool: if the pathname is valid
    """

    if not set(exclude).isdisjoint(set(include)):
        raise ValueError('include and exclude must be disjoint')

    dirname, basename = os.path.split(filepath)
    pid, ext = os.path.splitext(basename)
    components = dirname.split(os.sep)
    for c in components:
        if c in exclude:
            return False
        if c not in include and c not in pid:
            return False
    return True

async def _async_split_dir_entries(dirpath, *, exclude=DEFAULT_EXCLUDE, sort=True, reverse=False):
    """Return (dirnames, filenames) for dirpath using aiofiles/os.path."""
    names = await aios.listdir(dirpath)
    dirnames, filenames = [], []

    async def _isdir(name):
        return await aiopath.isdir(os.path.join(dirpath, name))

    isdirs = await asyncio.gather(*(_isdir(n) for n in names))
    for name, is_dir in zip(names, isdirs):
        if is_dir:
            if name in exclude:
                continue
            dirnames.append(name)
        else:
            filenames.append(name)

    if sort:
        dirnames.sort(reverse=reverse)
        filenames.sort(reverse=reverse)
    return dirnames, filenames

async def async_list_filesets(
    dirpath,
    exclude=DEFAULT_EXCLUDE,
    include=DEFAULT_INCLUDE,
    sort=True,
    validate=True,
    require_adc=True,
    require_roi=True,
):
    """
    Async version of list_filesets using aiofiles for directory traversal.

    Yields (dp, basename) for each .adc/.hdr/(.roi) fileset found.
    """
    if not set(exclude).isdisjoint(set(include)):
        raise ValueError('include and exclude must be disjoint')

    stack = [dirpath]
    while stack:
        dp = stack.pop()
        dirnames, filenames = await _async_split_dir_entries(dp, exclude=exclude, sort=sort, reverse=True)

        # DFS order roughly matches the sync behavior (reverse sorting retained)
        for d in dirnames:
            stack.append(os.path.join(dp, d))

        fnset = set(filenames)
        for f in filenames:
            basename, extension = f[:-4], f[-3:]
            has_adc = (basename + '.adc') in fnset
            has_roi = (basename + '.roi') in fnset
            if extension == 'hdr' and (has_adc or not require_adc) and (has_roi or not require_roi):
                if validate:
                    if dp == dirpath:
                        reldir = ''
                    else:
                        reldir = dp[len(dirpath) + 1 :]
                    if not validate_path(os.path.join(reldir, basename), include=include, exclude=exclude):
                        continue
                yield dp, basename

async def async_list_data_dirs(dirpath, exclude=DEFAULT_EXCLUDE, sort=True, prune=True):
    """
    Async version of list_data_dirs using aiofiles for directory traversal.

    Yields descendant directories that contain at least one .hdr file.
    """
    dirnames, filenames = await _async_split_dir_entries(dirpath, exclude=exclude, sort=sort, reverse=False)

    for name in filenames:
        if name[-3:] == 'hdr':
            yield dirpath
            if prune:
                return
            break

    for name in dirnames:
        child = os.path.join(dirpath, name)
        async for dd in async_list_data_dirs(child, exclude=exclude, sort=sort, prune=prune):
            yield dd

async def async_find_fileset(
    dirpath,
    pid,
    include=DEFAULT_INCLUDE,
    exclude=DEFAULT_EXCLUDE,
    require_adc=True,
    require_roi=True,
):
    """
    Async version of find_fileset using aiofiles for directory traversal.

    Returns Fileset or None.
    """
    try:
        names = await aios.listdir(dirpath)
    except FileNotFoundError:
        return None

    # check direct match first
    hdr_name = pid + '.hdr'
    if hdr_name in names:
        basepath = os.path.join(dirpath, pid)
        # enforce presence of .adc and .roi files if required
        if require_adc and (pid + '.adc') not in names:
            return None
        if require_roi and (pid + '.roi') not in names:
            return None
        return basepath

    # recurse into plausible subdirectories
    for name in names:
        if name in exclude:
            continue
        if name in include or name in pid:
            child = os.path.join(dirpath, name)
            if await aiopath.isdir(child):
                fs = await async_find_fileset(
                    child,
                    pid,
                    include=include,
                    exclude=exclude,
                    require_adc=require_adc,
                    require_roi=require_roi,
                )
                if fs is not None:
                    return fs
    return None


class IfcbDataDirectory:
    """Representation of an IFCB data directory.

    :param root_path: the root directory containing IFCB filesets
    :param include: list of directory names to include when searching
    :param exclude: list of directory names to exclude when searching
    :param require_adc: if True, only consider filesets with .adc files
    :param require_roi: if True, only consider filesets with .roi files
    """

    def __init__(
        self,
        root_path,
        include=DEFAULT_INCLUDE,
        exclude=DEFAULT_EXCLUDE,
        require_adc=True,
        require_roi=True,
    ):
        self.root_path = root_path
        self.include = include
        self.exclude = exclude
        self.require_adc = require_adc
        self.require_roi = require_roi

        if not set(exclude).isdisjoint(set(include)):
            raise ValueError('include and exclude must be disjoint')
        
        if require_roi and not require_adc:
            raise ValueError('require_roi=True requires require_adc=True')

    async def _exists(self, pid):
        """Check if the fileset for the given PID exists."""
        fs = await async_find_fileset(
            self.root_path,
            pid,
            include=self.include,
            exclude=self.exclude,
            require_adc=self.require_adc,
            require_roi=self.require_roi,
        )
        if fs is None:
            return False, None
        return True, fs
    
    async def exists(self, pid):
        """Return True if the fileset for the given PID exists, False otherwise."""
        exists, _ = await self._exists(pid)
        return exists
    
    async def paths(self, pid):
        """Return the full path to the specified file in the fileset for the given PID."""
        exists, fs = await self._exists(pid)
        if not exists:
            raise KeyError(pid)
        return {
            'hdr': fs + '.hdr',
            'adc': fs + '.adc' if self.require_adc else None,
            'roi': fs + '.roi' if self.require_roi else None,
        }

    async def list(self):
        """Yield all PIDs and associated paths in the store."""
        async for dp, bn in async_list_filesets(
            self.root_path,
            exclude=self.exclude,
            include=self.include,
            require_adc=self.require_adc,
            require_roi=self.require_roi,
        ):
            yield {
                'pid': bn,
                'hdr': os.path.join(dp, bn + '.hdr'),
                'adc': os.path.join(dp, bn + '.adc') if self.require_adc else None,
                'roi': os.path.join(dp, bn + '.roi') if self.require_roi else None,
            }

    async def list_images(self, pid):
        """List ROI image metadata from the fileset for the given PID."""
        paths = await self.paths(pid)
        adc_path = paths.get('adc')
        if pid.startswith('I'):
            x_col, y_col, w_col, h_col = 9, 10, 11, 12
        else:
            x_col, y_col, w_col, h_col = 13, 14, 15, 16
        images = {}
        async with aiofiles.open(adc_path, 'r') as adc_file:
            adc_text = await adc_file.read()
            adc_lines = adc_text.splitlines()
            for i, line in enumerate(adc_lines):
                fields = line.strip().split(',')
                x = int(fields[x_col])
                y = int(fields[y_col])
                width = int(fields[w_col])
                height = int(fields[h_col])
                if width == 0 or height == 0:
                    pass # skip triggers without ROIs
                else:
                    images[i+1] = {
                        'roi_id': add_target(pid, i+1),
                        'x': x,
                        'y': y,
                        'width': width,
                        'height': height,
                    }
        return images

    async def read_images(self, pid, rois=None):
        """Read ROI images from the fileset for the given PID."""
        if not self.require_roi:
            raise ValueError('require_roi must be True to read ROI images')
        paths = await self.paths(pid)
        adc_path = paths.get('adc')
        roi_path = paths.get('roi')
        if pid.startswith('I'):
            w_col, h_col, offset_col = 11, 12, 13
        else:
            w_col, h_col, offset_col = 15, 16, 17
        images = {}
        async with aiofiles.open(roi_path, 'rb') as roi_file:
            async with aiofiles.open(adc_path, 'r') as adc_file:
                i = 0
                async for line in adc_file:
                    fields = line.strip().split(',')
                    width = int(fields[w_col])
                    height = int(fields[h_col])
                    if rois is not None and (i+1) not in rois:
                        pass # skip unwanted ROIs
                    elif width == 0 or height == 0:
                        pass # skip triggers without ROIs
                    else:
                        offset = int(fields[offset_col])
                        await roi_file.seek(offset)
                        data = await roi_file.read(width * height)
                        image = await asyncio.to_thread(Image.frombuffer, 'L', (width, height), data, 'raw', 'L', 0, 1)
                        images[i+1] = image
                    i += 1
        return images

# parse "I" style bin IDs in the form 'IFCB{n}_{yyyy}_{ddd}_{hh}{mm}{ss}'
def parse_ifcb_i_bin_id(bin_id):
    import re
    pattern = r'^IFCB(\d+)_(\d{4})_(\d{3})_(\d{2})(\d{2})(\d{2})$'
    match = re.match(pattern, bin_id)
    if not match:
        raise ValueError(f'Invalid IFCB bin ID format: {bin_id}')
    instrument_id = int(match.group(1))
    year = int(match.group(2))
    day_of_year = int(match.group(3))
    hour = int(match.group(4))
    minute = int(match.group(5))
    second = int(match.group(6))
    return {
        'instrument_id': instrument_id,
        'year': year,
        'day_of_year': day_of_year,
        'hour': hour,
        'minute': minute,
        'second': second,
    }

# parse "D" style bin IDs in the form 'D{yyyy}{mm}{dd}T{hh}{mm}{ss}_IFCB{n}'
def parse_ifcb_d_bin_id(bin_id):
    import re
    pattern = r'^D(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})_IFCB(\d+)$'
    match = re.match(pattern, bin_id)
    if not match:
        raise ValueError(f'Invalid IFCB D-style bin ID format: {bin_id}')
    year = int(match.group(1))
    month = int(match.group(2))
    day = int(match.group(3))
    hour = int(match.group(4))
    minute = int(match.group(5))
    second = int(match.group(6))
    instrument_id = int(match.group(7))
    return {
        'instrument_id': instrument_id,
        'year': year,
        'month': month,
        'day': day,
        'hour': hour,
        'minute': minute,
        'second': second,
    }

def parse_bin_id(bin_id: str):
    """Parse an IFCB bin ID in either "I" or "D" style format."""
    if bin_id.startswith('D'):
        return parse_ifcb_d_bin_id(bin_id)
    else:
        return parse_ifcb_i_bin_id(bin_id)

def bin_timestamp(bin_id: str):
    """Extract a timestamp from an IFCB bin ID."""
    from datetime import datetime, timedelta, timezone

    parsed = parse_bin_id(bin_id)
    if 'day_of_year' in parsed:
        dt = datetime(parsed['year'], 1, 1, tzinfo=timezone.utc) + timedelta(days=parsed['day_of_year'] - 1)
        dt = dt.replace(hour=parsed['hour'], minute=parsed['minute'], second=parsed['second'])
    else:
        dt = datetime(
            parsed['year'],
            parsed['month'],
            parsed['day'],
            parsed['hour'],
            parsed['minute'],
            parsed['second'],
            tzinfo=timezone.utc,
        )
    return dt


def add_target(bin_id: str, target: int):
    """Add a target number to an IFCB bin ID."""
    return f"{bin_id}_{target:05d}"


def parse_target(roi_id: str):
    """Parse an IFCB ROI ID into (bin_id, target)."""
    import re
    pattern = r'^(IFCB\d+_\d{4}_\d{3}_\d{6}|D\d{8}T\d{6}_IFCB\d+)_(\d{5})$'
    match = re.match(pattern, roi_id)
    if not match:
        raise ValueError(f'Invalid IFCB ROI ID format: {roi_id}')
    bin_id = match.group(1)
    target = int(match.group(2))
    return bin_id, target


# product files access

async def find_product_file(directory, filename, exhaustive=False):
    candidate = os.path.join(directory, filename)
    if await aiopath.exists(candidate):
        return candidate

    try:
        names = await aios.listdir(directory)
    except FileNotFoundError:
        return None

    for name in names:
        path = os.path.join(directory, name)
        if await aiopath.isdir(path):
            if not exhaustive and name not in filename:
                continue
            result = await find_product_file(path, filename, exhaustive=exhaustive)
            if result is not None:
                return result
        elif name == filename:
            return path

    return None


async def list_product_files(directory, regex):
    try:
        names = await aios.listdir(directory)
    except FileNotFoundError:
        return

    for name in names:
        path = os.path.join(directory, name)
        if await aiopath.isdir(path):
            async for p in list_product_files(path, regex):
                yield p
        elif re.match(regex, name):
            yield path


async def product_path(directory, filename, exhaustive=False):
    path = await find_product_file(directory, filename, exhaustive=exhaustive)
    if not path:
        raise FileNotFoundError(f'Product file {filename} not found in {directory}')
    return path


async def blob_path(directory, pid, version=4):
    filename = f'{pid}_blobs_v{version}.zip'
    return await product_path(directory, filename)


async def class_scores_path(directory, pid, version=4):
    filename = f'{pid}.csv'
    return await product_path(directory, filename)


async def features_path(directory, pid, version=4):
    filename = f'{pid}_features_v{version}.zip'
    return await product_path(directory, filename)

