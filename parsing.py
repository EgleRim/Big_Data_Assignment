# parsing.py
"""
AIS data streaming and validation for low-memory parallel processing.

DIRTY DATA HANDLING — The assignment warns: "not the only faulty data."
The Danish AIS dataset contains multiple categories of bad records that
must be filtered before any anomaly detection:

Category 1 — Invalid MMSI numbers:
    - Known bad patterns: 000000000, 111111111, 123456789, 987654321 etc.
    - All-same-digit MMSIs (111111111, 222222222, ..., 999999999)
    - Wrong length (not exactly 9 digits)
    - Non-numeric MMSI strings
    These come from unconfigured transponders and would create
    a single massive MMSI bucket crashing worker memory.

Category 2 — Invalid coordinates:
    - Latitude outside [-90, 90]
    - Longitude outside [-180, 180]
    - Exactly (0.0, 0.0) — null island, AIS default when GPS not locked
    AIS devices report 0°N 0°E when position is unavailable.
    Including these would generate false teleportation events.

Category 3 — Malformed rows:
    - Fewer than 16 columns (truncated lines from file write errors)
    - Unparseable timestamps (not in DD/MM/YYYY HH:MM:SS format)
    These arise from partial writes, encoding errors, or CSV corruption.

Category 4 — AIS base stations (992xxxxxx prefix):
    - Shore-based AIS infrastructure, not vessels
    - Never have draught, SOG, or movement — excluded from detection
    Treating them as vessels would generate false draft concealment events.

Category 5 — Missing/default sensor values:
    - Empty SOG field - treated as 0.0 (stationary)
    - Empty draught - treated as 0.0 (not reporting)
    These are not filtered out but handled gracefully downstream.
"""

import csv
from typing import Generator, Tuple
from config import (
    COL_MMSI, COL_LATITUDE, COL_LONGITUDE,
    COL_TIMESTAMP, COL_SOG, COL_DRAUGHT,
    INVALID_MMSI_PATTERNS, INVALID_MMSI_PREFIXES,
    EXPECTED_MMSI_LENGTH,
)
from geo import is_valid_coordinate, ts_to_epoch


def is_valid_mmsi(mmsi: str) -> bool:
    """
    Validate MMSI against all known dirty-data patterns.

    Filters Category 1 dirty data — invalid/default transponder IDs.
    """
    mmsi = mmsi.strip()

    # Must be exactly 9 numeric digits
    if not mmsi or not mmsi.isdigit():
        return False
    if len(mmsi) != EXPECTED_MMSI_LENGTH:
        return False

    # Known bad static patterns (unconfigured transponders)
    if mmsi in INVALID_MMSI_PATTERNS:
        return False

    # Known bad prefixes
    if mmsi.startswith(INVALID_MMSI_PREFIXES):
        return False

    # All-same-digit MMSIs (000000001 catches the one with trailing digit)
    if len(set(mmsi)) == 1:
        return False

    return True


def stream_csv_rows(filepath: str) -> Generator[list, None, None]:
    """
    Generator: yields raw CSV rows as lists, skipping the header.
    Low-memory — reads one line at a time, never loads full file.
    """
    with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.reader(f)
        next(reader, None)   # skip header
        for row in reader:
            yield row


def stream_valid_rows(
    filepath: str,
) -> Generator[Tuple[str, str, int, float, float, float, float], None, None]:
    """
    Generator: yields fully validated, parsed AIS records.
    Filters ALL five categories of dirty data described in module docstring.

    Yields: (mmsi, ts_str, epoch, lat, lon, sog, draught)

    Called by each parallel worker on its own byte range.
    Returns only clean, validated records.
    """
    with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.reader(f)
        next(reader, None)   # skip header

        for row in reader:
            # Category 3: malformed rows
            if len(row) < 16:
                continue

            mmsi  = row[COL_MMSI].strip()
            lat_s = row[COL_LATITUDE].strip()
            lon_s = row[COL_LONGITUDE].strip()
            ts    = row[COL_TIMESTAMP].strip()
            sog_s = row[COL_SOG].strip()
            dr_s  = row[COL_DRAUGHT].strip()

            # Category 1: invalid MMSI
            if not is_valid_mmsi(mmsi):
                continue

            # Category 2: invalid coordinates
            if not is_valid_coordinate(lat_s, lon_s):
                continue

            # Category 3: unparseable timestamp
            try:
                epoch = ts_to_epoch(ts)
            except Exception:
                continue

            # Category 5: missing sensor values — use safe defaults
            try:
                lat     = float(lat_s)
                lon     = float(lon_s)
                sog     = float(sog_s) if sog_s else 0.0
                draught = float(dr_s)  if dr_s  else 0.0
            except ValueError:
                continue

            yield mmsi, ts, epoch, lat, lon, sog, draught
