"""Lightweight IFCB PID parsing (extracted functionality from pyifcb).

This module provides minimal PID parsing for IFCB identifiers,
extracting only the bin_lid and target fields needed by this service.

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
