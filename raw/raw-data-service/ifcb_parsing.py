"""
IFCB bin ID and ROI ID parsing utilities.

This module provides functions for parsing and transforming IFCB bin IDs (PIDs/LIDs)
and ROI IDs (target IDs) in both "I" style and "D" style formats.

I-style format: IFCB{n}_{yyyy}_{ddd}_{hh}{mm}{ss}
D-style format: D{yyyy}{mm}{dd}T{hh}{mm}{ss}_IFCB{n}

ROI IDs append a 5-digit target number: {bin_id}_{nnnnn}
"""

import re
from datetime import datetime, timedelta, timezone
from typing import Tuple


# Regex patterns for bin ID formats
I_STYLE_PATTERN = re.compile(r'^IFCB(\d+)_(\d{4})_(\d{3})_(\d{2})(\d{2})(\d{2})$')
D_STYLE_PATTERN = re.compile(r'^D(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})_IFCB(\d+)$')
ROI_ID_PATTERN = re.compile(r'^(IFCB\d+_\d{4}_\d{3}_\d{6}|D\d{8}T\d{6}_IFCB\d+)_(\d{5})$')


def parse_i_style_bin_id(bin_id: str) -> dict:
    """
    Parse an I-style bin ID in the form 'IFCB{n}_{yyyy}_{ddd}_{hh}{mm}{ss}'.

    :param bin_id: the bin ID string
    :returns: dict with instrument_id, year, day_of_year, hour, minute, second
    :raises ValueError: if the format is invalid
    """
    match = I_STYLE_PATTERN.match(bin_id)
    if not match:
        raise ValueError(f'Invalid IFCB I-style bin ID format: {bin_id}')
    return {
        'instrument_id': int(match.group(1)),
        'year': int(match.group(2)),
        'day_of_year': int(match.group(3)),
        'hour': int(match.group(4)),
        'minute': int(match.group(5)),
        'second': int(match.group(6)),
    }


def parse_d_style_bin_id(bin_id: str) -> dict:
    """
    Parse a D-style bin ID in the form 'D{yyyy}{mm}{dd}T{hh}{mm}{ss}_IFCB{n}'.

    :param bin_id: the bin ID string
    :returns: dict with instrument_id, year, month, day, hour, minute, second
    :raises ValueError: if the format is invalid
    """
    match = D_STYLE_PATTERN.match(bin_id)
    if not match:
        raise ValueError(f'Invalid IFCB D-style bin ID format: {bin_id}')
    return {
        'instrument_id': int(match.group(7)),
        'year': int(match.group(1)),
        'month': int(match.group(2)),
        'day': int(match.group(3)),
        'hour': int(match.group(4)),
        'minute': int(match.group(5)),
        'second': int(match.group(6)),
    }


def parse_bin_id(bin_id: str) -> dict:
    """
    Parse an IFCB bin ID in either I-style or D-style format.

    :param bin_id: the bin ID string
    :returns: dict with parsed components (format varies by style)
    :raises ValueError: if the format is invalid
    """
    if bin_id.startswith('D'):
        return parse_d_style_bin_id(bin_id)
    elif bin_id.startswith('I'):
        return parse_i_style_bin_id(bin_id)
    else:
        raise ValueError(f'Invalid IFCB bin ID format: {bin_id}')


def bin_timestamp(bin_id: str) -> datetime:
    """
    Extract a UTC timestamp from an IFCB bin ID.

    :param bin_id: the bin ID string
    :returns: datetime object in UTC
    :raises ValueError: if the format is invalid
    """
    parsed = parse_bin_id(bin_id)
    if 'day_of_year' in parsed:
        # I-style: use day of year
        dt = datetime(parsed['year'], 1, 1, tzinfo=timezone.utc) + timedelta(days=parsed['day_of_year'] - 1)
        dt = dt.replace(hour=parsed['hour'], minute=parsed['minute'], second=parsed['second'])
    else:
        # D-style: use month and day
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


def bin_day_dir(bin_id: str) -> str:
    """
    Get the day directory name for a bin ID (e.g., 'D20210101' for D-style).

    :param bin_id: the bin ID string
    :returns: day directory string in the form 'D{yyyymmdd}'
    :raises ValueError: if the format is invalid
    """
    ts = bin_timestamp(bin_id)
    return ts.strftime('D%Y%m%d')


def bin_year(bin_id: str) -> int:
    """
    Extract the year from an IFCB bin ID.

    :param bin_id: the bin ID string
    :returns: year as integer
    :raises ValueError: if the format is invalid
    """
    parsed = parse_bin_id(bin_id)
    return parsed['year']


def bin_instrument_id(bin_id: str) -> int:
    """
    Extract the instrument ID from an IFCB bin ID.

    :param bin_id: the bin ID string
    :returns: instrument ID as integer
    :raises ValueError: if the format is invalid
    """
    parsed = parse_bin_id(bin_id)
    return parsed['instrument_id']


def add_target(bin_id: str, target: int) -> str:
    """
    Add a target number to an IFCB bin ID to create an ROI ID.

    :param bin_id: the bin ID string
    :param target: the target number (1-based)
    :returns: ROI ID string in the form '{bin_id}_{nnnnn}'
    """
    return f"{bin_id}_{target:05d}"


def parse_roi_id(roi_id: str) -> Tuple[str, int]:
    """
    Parse an IFCB ROI ID into (bin_id, target).

    :param roi_id: the ROI ID string
    :returns: tuple of (bin_id, target_number)
    :raises ValueError: if the format is invalid
    """
    match = ROI_ID_PATTERN.match(roi_id)
    if not match:
        raise ValueError(f'Invalid IFCB ROI ID format: {roi_id}')
    bin_id = match.group(1)
    target = int(match.group(2))
    return bin_id, target


# Alias for backward compatibility
parse_target = parse_roi_id


def parse_pid(pid: str) -> dict:
    """
    Parse a PID (bin ID or ROI ID) and return a comprehensive dict.

    This function handles both bin IDs and ROI IDs, returning a dict
    with all parsed components plus derived values like timestamp and day_dir.

    :param pid: the PID string (bin ID or ROI ID)
    :returns: dict with lid, timestamp, year, day_dir, instrument_id, and optionally target
    :raises ValueError: if the format is invalid
    """
    # Check if it's an ROI ID (has target suffix)
    roi_match = ROI_ID_PATTERN.match(pid)
    if roi_match:
        bin_id = roi_match.group(1)
        target = int(roi_match.group(2))
    else:
        bin_id = pid
        target = None

    parsed = parse_bin_id(bin_id)
    ts = bin_timestamp(bin_id)

    result = {
        'lid': bin_id,
        'timestamp': ts,
        'year': parsed['year'],
        'day_dir': bin_day_dir(bin_id),
        'instrument_id': parsed['instrument_id'],
    }

    if target is not None:
        result['target'] = target
        result['roi_id'] = pid

    return result


