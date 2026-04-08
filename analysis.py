# analysis.py
"""
Analysis output handling - writes anomalies and summaries to CSV files.
Supports versioned output directories passed at runtime.
"""

import csv
import os
import json
from typing import List, Dict, Any, Optional
from config import ANALYSIS_DIR, CSV_DELIMITER, OUTPUT_DIRS


def write_anomalies_csv(
    anomalies: List[Dict[str, Any]],
    output_file: str,
    loiter_dir: Optional[str] = None,
) -> None:
    """
    Write anomalies to CSV files split by type.
    All files go into the folder of output_file, except loitering events
    which go into loiter_dir.
    """
    if not anomalies:
        return

    out_dir = os.path.dirname(output_file)
    os.makedirs(out_dir, exist_ok=True)

    ldir = loiter_dir if loiter_dir else OUTPUT_DIRS[2]
    os.makedirs(ldir, exist_ok=True)

    by_type: Dict[str, List] = {}
    for a in anomalies:
        atype = a.get('anomaly_type', 'unknown')
        by_type.setdefault(atype, []).append(a)

    for atype, items in by_type.items():
        if atype == 'going_dark':
            _write_going_dark_csv(items, os.path.join(out_dir, 'going_dark_events.csv'))
        elif atype == 'teleportation':
            _write_teleportation_csv(items, os.path.join(out_dir, 'teleportation_events.csv'))
        elif atype == 'draft_change':
            _write_draft_csv(items, os.path.join(out_dir, 'draft_change_events.csv'))
        elif atype == 'loitering':
            _write_loitering_csv(items, os.path.join(ldir, 'loitering_events.csv'))


def _write_going_dark_csv(anomalies: List[Dict], filepath: str) -> None:
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'mmsi', 'gap_hours', 'distance_km', 'gap_start', 'gap_end'
        ])
        writer.writeheader()
        for a in anomalies:
            writer.writerow({
                'mmsi': a['mmsi'],
                'gap_hours': a['gap_hours'],
                'distance_km': a['distance_km'],
                'gap_start': a['gap_start'],
                'gap_end': a['gap_end'],
            })
    print(f"  ok Wrote {len(anomalies)} going-dark events")


def _write_teleportation_csv(anomalies: List[Dict], filepath: str) -> None:
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'mmsi', 'distance_km', 'distance_nm', 'speed_knots', 'gap_start', 'gap_end'
        ])
        writer.writeheader()
        for a in anomalies:
            writer.writerow({
                'mmsi': a['mmsi'],
                'distance_km': a['distance_km'],
                'distance_nm': a.get('distance_nm', round(a['distance_km'] / 1.852, 2)),
                'speed_knots': a['speed_knots'],
                'gap_start': a['gap_start'],
                'gap_end': a['gap_end'],
            })
    print(f"  ok Wrote {len(anomalies)} teleportation events (≥10km filter applied)")


def _write_draft_csv(anomalies: List[Dict], filepath: str) -> None:
    """
    Write draft change events including detection_strategy column
    so analysts can see which of the 3 strategies caught each event.
    """
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    # Count by strategy for summary
    by_strategy = {}
    for a in anomalies:
        s = a.get('detection_strategy', 'unknown')
        by_strategy[s] = by_strategy.get(s, 0) + 1

    with open(filepath, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'mmsi', 'detection_strategy', 'gap_hours',
            'draught_before', 'draught_after', 'draught_change_percent',
            'total_pings', 'gap_start', 'gap_end',
        ])
        writer.writeheader()
        for a in anomalies:
            writer.writerow({
                'mmsi': a['mmsi'],
                'detection_strategy': a.get('detection_strategy', 'unknown'),
                'gap_hours': a.get('gap_hours', ''),
                'draught_before': a.get('draught_before', ''),
                'draught_after': a.get('draught_after', ''),
                'draught_change_percent': a.get('draught_change_percent', ''),
                'total_pings': a.get('total_pings', ''),
                'gap_start': a.get('gap_start', ''),
                'gap_end': a.get('gap_end', ''),
            })

    print(f"  ok Wrote {len(anomalies)} draft-change events:")
    for s, c in sorted(by_strategy.items()):
        print(f"      {s}: {c}")


def _write_loitering_csv(anomalies: List[Dict], filepath: str) -> None:
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'mmsi_vessel1', 'mmsi_vessel2',
            'duration_hours', 'proximity_events', 'min_distance_km'
        ])
        writer.writeheader()
        for a in anomalies:
            writer.writerow({
                'mmsi_vessel1': a['mmsi_vessel1'],
                'mmsi_vessel2': a['mmsi_vessel2'],
                'duration_hours': a['duration_hours'],
                'proximity_events': a['proximity_events'],
                'min_distance_km': a['min_distance_km'],
            })
    print(f"  ok Wrote {len(anomalies)} loitering events")


def write_vessel_scores_csv(top_vessels: List[Dict], filepath: str) -> None:
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'rank', 'mmsi', 'dfsi', 'total_anomalies',
            'anomaly_a', 'anomaly_d', 'anomaly_c', 'anomaly_b',
            'draft_confirmed', 'draft_concealment',
        ])
        writer.writeheader()
        for rank, vessel in enumerate(top_vessels, 1):
            counts = vessel['anomaly_counts']
            writer.writerow({
                'rank': rank,
                'mmsi': vessel['mmsi'],
                'dfsi': vessel['dfsi'],
                'total_anomalies': vessel['total_anomalies'],
                'anomaly_a': counts.get('going_dark', 0),
                'anomaly_d': counts.get('teleportation', 0),
                'anomaly_c': counts.get('draft_change', 0),
                'anomaly_b': counts.get('loitering', 0),
                # Sub-type breakdown from the strategy tracking
                'draft_confirmed': (
                    counts.get('draft_gap_and_change', 0) +
                    counts.get('draft_no_gap_change', 0)
                ),
                'draft_concealment': counts.get('draft_concealment', 0),
            })
    print(f"  ok Wrote {len(top_vessels)} vessel scores")


def write_metadata_json(metadata: Dict, filepath: str) -> None:
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'w') as f:
        json.dump(metadata, f, indent=2)
    print(f"  ok Wrote metadata to run_metadata.json")
