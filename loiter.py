# loiter.py
"""
Loitering anomaly detection (Anomaly B).
Module-level functions are required for ProcessPoolExecutor pickling on Windows.
"""

import gc
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List, Tuple, Dict, Any, Set
from collections import defaultdict

from config import (
    LOITERING_PROXIMITY_KM, LOITERING_SOG_KNOTS,
    LOITERING_DURATION_HOURS, NUM_WORKERS,
)
from geo import haversine_distance
from parsing import stream_valid_rows

GRID_CELL_DEG = 0.5


def _check_loitering_pair_final(
    mmsi1: str, mmsi2: str,
    records1: List[Tuple], records2: List[Tuple],
    proximity_threshold_km: float,
    loitering_sec: int,
    sog_threshold_knots: float,
) -> Dict[str, Any]:
    """Verify one candidate pair — all three conditions per ping."""
    min_epoch = max(records1[0][1], records2[0][1])
    max_epoch = min(records1[-1][1], records2[-1][1])
    overlap   = max_epoch - min_epoch

    if overlap < loitering_sec:
        return None

    s1 = max(1, len(records1) // 20)
    s2 = max(1, len(records2) // 20)
    count    = 0
    min_dist = float('inf')
    first_ts = None
    last_ts  = None

    for i in range(0, len(records1), s1):
        ts1, epoch1, lat1, lon1, sog1, _ = records1[i]
        if epoch1 < min_epoch or epoch1 > max_epoch:
            continue
        if sog1 >= sog_threshold_knots:
            continue
        for j in range(0, len(records2), s2):
            ts2, epoch2, lat2, lon2, sog2, _ = records2[j]
            if abs(epoch1 - epoch2) > 900:
                continue
            if sog2 >= sog_threshold_knots:
                continue
            dist = haversine_distance(lat1, lon1, lat2, lon2)
            if dist < proximity_threshold_km:
                count += 1
                min_dist = min(min_dist, dist)
                if first_ts is None:
                    first_ts = ts1
                last_ts = ts1

    if count >= 3 and min_dist < float('inf'):
        return {
            'mmsi_vessel1':     mmsi1,
            'mmsi_vessel2':     mmsi2,
            'anomaly_type':     'loitering',
            'loitering_start':  first_ts,
            'loitering_end':    last_ts,
            'duration_hours':   round(overlap / 3600.0, 2),
            'proximity_events': count,
            'min_distance_km':  round(min_dist, 3),
        }
    return None


def _check_pair_batch(batch: List[Tuple]) -> List[Dict[str, Any]]:
    """Process a batch of candidate pairs in a worker process."""
    results = []
    for (mmsi1, mmsi2, rec1, rec2, prox_km, loit_sec, sog_thresh) in batch:
        r = _check_loitering_pair_final(
            mmsi1, mmsi2, rec1, rec2, prox_km, loit_sec, sog_thresh
        )
        if r:
            results.append(r)
    return results


def _build_candidate_pairs(
    slow_vessels: Dict[str, Tuple],
    vessel_positions: Dict[str, Tuple],
) -> List[Tuple]:
    """Build candidate pairs using fixed 0.5° spatial grid."""
    spatial_grid: Dict[tuple, List[str]] = defaultdict(list)
    for mmsi, (lat, lon) in vessel_positions.items():
        cell = (int(lat / GRID_CELL_DEG), int(lon / GRID_CELL_DEG))
        spatial_grid[cell].append(mmsi)

    sizes = [len(v) for v in spatial_grid.values()]
    if sizes:
        print(f"[Loitering] Grid: {len(spatial_grid)} cells, "
              f"max: {max(sizes)}, "
              f"median: {sorted(sizes)[len(sizes)//2]}")

    checked:    Set[tuple]  = set()
    candidates: List[Tuple] = []

    for cell, mmsis_in_cell in spatial_grid.items():
        lat_c, lon_c = cell
        adjacent: Set[str] = set(mmsis_in_cell)
        for dlat in [-1, 0, 1]:
            for dlon in [-1, 0, 1]:
                if dlat == 0 and dlon == 0:
                    continue
                nb = (lat_c + dlat, lon_c + dlon)
                if nb in spatial_grid:
                    adjacent.update(spatial_grid[nb])

        if len(adjacent) < 2:
            continue

        sorted_mmsis = sorted(adjacent)
        for i, mmsi1 in enumerate(sorted_mmsis):
            for mmsi2 in sorted_mmsis[i + 1:]:
                key = (mmsi1, mmsi2)
                if key in checked:
                    continue
                checked.add(key)

                if mmsi1 not in slow_vessels or mmsi2 not in slow_vessels:
                    continue

                lat1, lon1 = vessel_positions[mmsi1]
                lat2, lon2 = vessel_positions[mmsi2]
                if haversine_distance(lat1, lon1, lat2, lon2) > 50.0:
                    continue

                rec1 = slow_vessels[mmsi1][0]
                rec2 = slow_vessels[mmsi2][0]
                candidates.append((mmsi1, mmsi2, rec1, rec2))

    return candidates


def detect_loitering_anomalies_streaming(
    filepath: str,
    proximity_threshold_km: float = LOITERING_PROXIMITY_KM,
    sog_threshold_knots:    float = LOITERING_SOG_KNOTS,
    loitering_duration_hours: float = LOITERING_DURATION_HOURS,
    num_workers: int = NUM_WORKERS,
) -> List[Dict[str, Any]]:
    """Standalone streaming detection — reads file, then parallel pair checks."""
    print("[Loitering] Phase 1: Collecting slow vessels...")
    loitering_sec = loitering_duration_hours * 3600
    slow_data: Dict[str, List] = defaultdict(list)
    total = 0

    for mmsi, ts_str, epoch, lat, lon, sog, draught in stream_valid_rows(filepath):
        if sog < sog_threshold_knots:
            slow_data[mmsi].append((ts_str, epoch, lat, lon, sog, draught))
        total += 1
        if total % 2_000_000 == 0:
            print(f"[Loitering]   {total:,} records, {len(slow_data)} slow vessels")
            gc.collect()

    print(f"[Loitering] {total:,} records - {len(slow_data)} slow vessels")

    if len(slow_data) < 2:
        return []

    vessel_positions = {
        mmsi: (
            sum(r[2] for r in recs) / len(recs),
            sum(r[3] for r in recs) / len(recs),
        )
        for mmsi, recs in slow_data.items()
    }
    slow_with_pos = {
        mmsi: (recs, *vessel_positions[mmsi])
        for mmsi, recs in slow_data.items()
    }

    candidates = _build_candidate_pairs(slow_with_pos, vessel_positions)
    print(f"[Loitering] {len(candidates):,} candidates")
    if not candidates:
        return []

    batch_size = max(1, len(candidates) // (num_workers * 4))
    batches = [
        [
            (m1, m2, r1, r2,
             proximity_threshold_km, loitering_sec, sog_threshold_knots)
            for m1, m2, r1, r2 in candidates[i:i + batch_size]
        ]
        for i in range(0, len(candidates), batch_size)
    ]

    anomalies = []
    with ProcessPoolExecutor(max_workers=num_workers) as ex:
        for fut in as_completed(
                {ex.submit(_check_pair_batch, b): b for b in batches}):
            try:
                anomalies.extend(fut.result())
            except Exception as e:
                print(f"[Loitering] Batch error: {e}")

    print(f"[Loitering] ok {len(anomalies)} loitering events")
    return anomalies


def detect_loitering_from_slow_vessels(
    slow_vessels_data: Dict[str, List[Tuple]],
    proximity_threshold_km: float = LOITERING_PROXIMITY_KM,
    sog_threshold_knots:    float = LOITERING_SOG_KNOTS,
    loitering_duration_hours: float = LOITERING_DURATION_HOURS,
    num_workers: int = NUM_WORKERS,
) -> List[Dict[str, Any]]:
    """Detection from pre-collected slow vessel records (no file read)."""
    if len(slow_vessels_data) < 2:
        return []

    loitering_sec = loitering_duration_hours * 3600
    vessel_positions = {
        mmsi: (
            sum(r[2] for r in recs) / len(recs),
            sum(r[3] for r in recs) / len(recs),
        )
        for mmsi, recs in slow_vessels_data.items()
    }
    slow_with_pos = {
        mmsi: (recs, *vessel_positions[mmsi])
        for mmsi, recs in slow_vessels_data.items()
    }

    candidates = _build_candidate_pairs(slow_with_pos, vessel_positions)
    if not candidates:
        return []

    batch_size = max(1, len(candidates) // (num_workers * 4))
    batches = [
        [
            (m1, m2, r1, r2,
             proximity_threshold_km, loitering_sec, sog_threshold_knots)
            for m1, m2, r1, r2 in candidates[i:i + batch_size]
        ]
        for i in range(0, len(candidates), batch_size)
    ]

    anomalies = []
    with ProcessPoolExecutor(max_workers=num_workers) as ex:
        for fut in as_completed(
                {ex.submit(_check_pair_batch, b): b for b in batches}):
            try:
                anomalies.extend(fut.result())
            except Exception as e:
                print(f"[Loitering] Batch error: {e}")

    print(f"[Loitering] ok {len(anomalies)} loitering events")
    return anomalies
