"""
Generate Google Maps coordinates/links for the highest DFSI vessel.

Usage:
  python map.py

This script reads top5_suspects.json, selects the top vessel by DFSI,
and prints coordinates and Google Maps URLs for a strong supporting event.
"""

import argparse
import glob
import json
import os
from typing import Any, Dict, List, Optional, Tuple


def _find_latest_run_dir(analysis_dir: str) -> Optional[str]:
    pattern = os.path.join(analysis_dir, "*_v*")
    candidates = [p for p in glob.glob(pattern) if os.path.isdir(p)]
    if not candidates:
        return None
    return max(candidates, key=os.path.getmtime)


def _load_top5(run_dir: str) -> List[Dict[str, Any]]:
    path = os.path.join(run_dir, "top5_suspects.json")
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("top5_suspects.json does not contain a list")
    return data


def _format_coord(coord: Tuple[float, float]) -> str:
    return f"{coord[0]:.6f},{coord[1]:.6f}"


def _maps_url(coord: Tuple[float, float]) -> str:
    return f"https://www.google.com/maps?q={coord[0]:.6f},{coord[1]:.6f}"


def _pick_best_event(anomalies: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not anomalies:
        return None

    going_dark = [a for a in anomalies if a.get("anomaly_type") == "going_dark" and a.get("pos_before") and a.get("pos_after")]
    if going_dark:
        return max(going_dark, key=lambda a: float(a.get("gap_hours", 0.0)))

    teleport = [a for a in anomalies if a.get("anomaly_type") == "teleportation" and a.get("pos_prev") and a.get("pos_curr")]
    if teleport:
        return max(teleport, key=lambda a: float(a.get("distance_nm", 0.0)))

    with_coords = [
        a for a in anomalies
        if (a.get("pos_before") and a.get("pos_after")) or (a.get("pos_prev") and a.get("pos_curr"))
    ]
    if with_coords:
        return with_coords[0]

    return anomalies[0]


def _extract_coord_pair(event: Dict[str, Any]) -> Tuple[Optional[Tuple[float, float]], Optional[Tuple[float, float]]]:
    if event.get("pos_before") and event.get("pos_after"):
        before = tuple(event["pos_before"])
        after = tuple(event["pos_after"])
        return before, after

    if event.get("pos_prev") and event.get("pos_curr"):
        prev = tuple(event["pos_prev"])
        curr = tuple(event["pos_curr"])
        return prev, curr

    return None, None


def main() -> None:
    parser = argparse.ArgumentParser(description="Create map links for top DFSI vessel")
    parser.add_argument("--analysis-dir", default="./analysis", help="Directory containing run folders like 2025-03-03_v2")
    parser.add_argument("--run-dir", default=None, help="Specific run directory (overrides --analysis-dir)")
    args = parser.parse_args()

    run_dir = args.run_dir or _find_latest_run_dir(args.analysis_dir)
    if not run_dir:
        print("No analysis run directories found.")
        print("Run the pipeline first: python task1.py")
        return

    top5_path = os.path.join(run_dir, "top5_suspects.json")
    if not os.path.exists(top5_path):
        print(f"Missing file: {top5_path}")
        print("Run the pipeline first: python task1.py")
        return

    top5 = _load_top5(run_dir)
    if not top5:
        print("top5_suspects.json is empty")
        return

    top = top5[0]
    mmsi = top.get("mmsi", "unknown")
    dfsi = top.get("dfsi", "unknown")
    anomalies = top.get("anomalies", [])

    best = _pick_best_event(anomalies)
    if not best:
        print(f"Top vessel MMSI {mmsi} (DFSI {dfsi}) has no anomaly events")
        return

    coord_a, coord_b = _extract_coord_pair(best)

    print("=" * 72)
    print("TOP DFSI VESSEL MAP CONTEXT")
    print("=" * 72)
    print(f"Run folder : {run_dir}")
    print(f"MMSI       : {mmsi}")
    print(f"DFSI       : {dfsi}")
    print(f"Event type : {best.get('anomaly_type', 'unknown')}")
    print(f"Gap start  : {best.get('gap_start', 'n/a')}")
    print(f"Gap end    : {best.get('gap_end', 'n/a')}")

    if coord_a and coord_b:
        print("\nCoordinate A:", _format_coord(coord_a))
        print("Maps A      :", _maps_url(coord_a))
        print("Coordinate B:", _format_coord(coord_b))
        print("Maps B      :", _maps_url(coord_b))

    else:
        print("\nNo coordinate pair found in the selected event.")


if __name__ == "__main__":
    main()
