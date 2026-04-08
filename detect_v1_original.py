# detect.py  — VERSION 1 (strict assignment spec)
"""
Anomaly detection exactly as specified in the assignment.
NO improvements applied — this is the baseline for comparison.

Run this version first to get V1 output, then swap back to the
improved detect.py for V2 output.
"""

from typing import List, Tuple, Dict, Any
from config import (
    GOING_DARK_GAP_HOURS, GOING_DARK_MOVEMENT_KM,
    TELEPORTATION_SPEED_KNOTS,
    DRAFT_CHANGE_GAP_HOURS, DRAFT_CHANGE_PERCENT_THRESHOLD,
)
from geo import haversine_distance


def detect_going_dark_anomalies(
    mmsi: str,
    records: List[Tuple],
    gap_threshold_hours: float = GOING_DARK_GAP_HOURS,
    movement_threshold_km: float = GOING_DARK_MOVEMENT_KM,
) -> List[Dict[str, Any]]:
    """
    Anomaly A: AIS gap > 4h where vessel moved > 5km (not anchored).
    """
    if len(records) < 2:
        return []

    anomalies = []
    gap_threshold_sec = gap_threshold_hours * 3600

    for i in range(1, len(records)):
        prev_ts, prev_epoch, prev_lat, prev_lon, _, _ = records[i - 1]
        curr_ts, curr_epoch, curr_lat, curr_lon, _, _ = records[i]

        gap_sec = curr_epoch - prev_epoch
        if gap_sec <= gap_threshold_sec:
            continue

        dist_km = haversine_distance(prev_lat, prev_lon, curr_lat, curr_lon)
        if dist_km <= movement_threshold_km:
            continue

        anomalies.append({
            'mmsi': mmsi,
            'gap_start': prev_ts,
            'gap_end': curr_ts,
            'gap_hours': round(gap_sec / 3600.0, 2),
            'distance_km': round(dist_km, 2),
            'pos_before': (prev_lat, prev_lon),
            'pos_after': (curr_lat, curr_lon),
            'anomaly_type': 'going_dark',
        })

    return anomalies


def detect_teleportation_anomalies(
    mmsi: str,
    records: List[Tuple],
    speed_threshold_knots: float = TELEPORTATION_SPEED_KNOTS,
) -> List[Dict[str, Any]]:
    """
    Anomaly D: Speed > 60 knots between consecutive pings.
    NO minimum distance filter — strict assignment spec.
    This produces many GPS noise events which is expected for V1.
    """
    if len(records) < 2:
        return []

    anomalies = []

    for i in range(1, len(records)):
        prev_ts, prev_epoch, prev_lat, prev_lon, _, _ = records[i - 1]
        curr_ts, curr_epoch, curr_lat, curr_lon, _, _ = records[i]

        time_sec = curr_epoch - prev_epoch
        if time_sec <= 0:
            continue

        dist_km = haversine_distance(prev_lat, prev_lon, curr_lat, curr_lon)
        speed_kmh = dist_km / (time_sec / 3600.0)
        speed_knots = speed_kmh / 1.852

        if speed_knots > speed_threshold_knots:
            anomalies.append({
                'mmsi': mmsi,
                'gap_start': prev_ts,
                'gap_end': curr_ts,
                'distance_km': round(dist_km, 2),
                'distance_nm': round(dist_km / 1.852, 2),
                'speed_knots': round(speed_knots, 2),
                'pos_prev': (prev_lat, prev_lon),
                'pos_curr': (curr_lat, curr_lon),
                'anomaly_type': 'teleportation',
            })

    return anomalies


def detect_draft_change_anomalies(
    mmsi: str,
    records: List[Tuple],
    gap_threshold_hours: float = DRAFT_CHANGE_GAP_HOURS,
    draft_change_percent_threshold: float = DRAFT_CHANGE_PERCENT_THRESHOLD,
) -> List[Dict[str, Any]]:
    """
    Anomaly C: Draught changes > 5% during AIS blackout > 2h.
    NO concealment strategy — strict assignment spec.
    Returns 0 events because draught is rarely reported in real AIS data.
    """
    anomalies = []
    gap_threshold_sec = gap_threshold_hours * 3600

    for i in range(1, len(records)):
        prev_ts, prev_epoch, prev_lat, prev_lon, _, prev_draft = records[i - 1]
        curr_ts, curr_epoch, curr_lat, curr_lon, _, curr_draft = records[i]

        gap_sec = curr_epoch - prev_epoch
        if gap_sec <= gap_threshold_sec:
            continue

        if prev_draft <= 0 or curr_draft <= 0:
            continue

        change_pct = ((curr_draft - prev_draft) / prev_draft) * 100

        if abs(change_pct) >= draft_change_percent_threshold:
            anomalies.append({
                'mmsi': mmsi,
                'gap_start': prev_ts,
                'gap_end': curr_ts,
                'gap_hours': round(gap_sec / 3600.0, 2),
                'draught_before': round(prev_draft, 2),
                'draught_after': round(curr_draft, 2),
                'draught_change_percent': round(change_pct, 2),
                'pos_before': (prev_lat, prev_lon),
                'pos_after': (curr_lat, curr_lon),
                'detection_strategy': 'gap_and_change',
                'anomaly_type': 'draft_change',
            })

    return anomalies
