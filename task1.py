# task1.py
"""
Shadow Fleet Detection — Parallel File-Chunk Architecture
=========================================================

Architecture overview (matches assignment flowchart):
  data/*.csv - Pass 1 (parallel): N workers each read own byte range - detect.py: Teleportation (D), Draft Change (C) - slow vessel collection for loitering - Pass 1b (streaming, concurrent with Pass 2): - detect.py: Going Dark (A) - needs full MMSI history - runs in background thread while Pass 2 works - Pass 2 (parallel pairs): loiter.py - ProcessPoolExecutor pair verification - Pass 3: scoring.py - vessel_scores.csv, top5_suspects.json

Why Going Dark needs a separate streaming pass:
  Going Dark requires ALL pings for an MMSI sorted chronologically.
  The byte-range split means a vessel's pings can be split across two
  workers — neither sees the full gap. A dedicated streaming pass reads
  the file once more but only accumulates {mmsi: records} and runs
  detection as each MMSI completes. Runs concurrently with Pass 2
  so adds ~0 net time to the total.
"""

import csv
import os
import time
import gc
import json
import threading
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Dict, Any, List, Tuple
from collections import defaultdict

try:
    import resource
    def _mem_mb():
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1024*1024)
except ImportError:
    import psutil
    def _mem_mb():
        return psutil.Process(os.getpid()).memory_info().rss / (1024*1024)

from analysis import write_anomalies_csv, write_vessel_scores_csv, write_metadata_json
from config import (
    NUM_WORKERS, CHUNK_SIZE, ANALYSIS_DIR, OUTPUT_DIRS,
    TOP_N_GOING_DARK, TOP_N_VESSELS, LOITERING_SOG_KNOTS,
    GOING_DARK_GAP_HOURS, GOING_DARK_MOVEMENT_KM,
)
from detect import (
    detect_going_dark_anomalies,
    detect_teleportation_anomalies,
    detect_draft_change_anomalies,
)
from loiter import _check_pair_batch, _build_candidate_pairs
from scoring import aggregate_anomalies_by_vessel, rank_vessels_by_dfsi
from parsing import stream_valid_rows, is_valid_mmsi
from geo import is_valid_coordinate, ts_to_epoch


# ============================================================================
# FILE SPLITTING
# ============================================================================

def get_file_byte_ranges(filepath: str, n_workers: int) -> List[Tuple[int, int]]:
    """Split CSV into N byte ranges each starting on a line boundary."""
    file_size = os.path.getsize(filepath)
    with open(filepath, 'rb') as f:
        f.readline()          # skip header
        header_end = f.tell()

    data_size  = file_size - header_end
    chunk_bytes = data_size // n_workers

    starts = []
    with open(filepath, 'rb') as f:
        for i in range(n_workers):
            if i == 0:
                pos = header_end
            else:
                pos = header_end + i * chunk_bytes
                f.seek(pos)
                f.readline()  # advance to next clean line boundary
                pos = f.tell()
            starts.append(pos)

    return [(starts[i], starts[i+1] if i+1 < len(starts) else file_size)
            for i in range(len(starts))]


# ============================================================================
# WORKER — reads own byte range, detects D and C only
# ============================================================================

def worker_process_range(
    worker_id:    int,
    filepath:     str,
    byte_start:   int,
    byte_end:     int,
    sog_threshold: float,
) -> Dict[str, Any]:
    """
    Read assigned byte range, detect Teleportation (D) and Draft (C).
    Going Dark (A) is NOT detected here — it needs complete MMSI history
    which may span multiple workers' byte ranges.
    """
    mmsi_records:  Dict[str, List[Tuple]] = defaultdict(list)
    slow_vessels:  Dict[str, List[Tuple]] = defaultdict(list)
    total_records  = 0
    skipped        = 0

    try:
        with open(filepath, 'r', encoding='utf-8', errors='replace',
                  buffering=1024*1024) as f:
            f.seek(byte_start)
            while True:
                line = f.readline()
                if not line or f.tell() > byte_end:
                    break

                total_records += 1
                if total_records % 500_000 == 0:
                    print(f"[Worker {worker_id}] {total_records:,} records, "
                          f"mem: {_mem_mb():.0f} MB", flush=True)
                    gc.collect()

                try:
                    parts = line.rstrip('\n\r').split(',')
                    if len(parts) < 16:
                        skipped += 1; continue
                    mmsi  = parts[2].strip()
                    lat_s = parts[3].strip()
                    lon_s = parts[4].strip()
                    sog_s = parts[7].strip()
                    dr_s  = parts[15].strip()
                    ts    = parts[0].strip()
                    if not is_valid_mmsi(mmsi):
                        skipped += 1; continue
                    if not is_valid_coordinate(lat_s, lon_s):
                        skipped += 1; continue
                    lat     = float(lat_s)
                    lon     = float(lon_s)
                    sog     = float(sog_s) if sog_s else 0.0
                    draught = float(dr_s)  if dr_s  else 0.0
                    epoch   = ts_to_epoch(ts)
                    record  = (ts, epoch, lat, lon, sog, draught)
                    mmsi_records[mmsi].append(record)
                    if sog < sog_threshold:
                        slow_vessels[mmsi].append(record)
                except Exception:
                    skipped += 1; continue

    except Exception as e:
        print(f"[Worker {worker_id}] FATAL: {e}", flush=True)
        import traceback; traceback.print_exc()
        return {
            'worker_id': worker_id, 'total_records': 0, 'skipped': 0,
            'mmsi_counts': {}, 'anomalies': [], 'slow_vessels': {},
            'memory_mb': _mem_mb(), 'error': str(e),
        }

    print(f"[Worker {worker_id}] Read done — {total_records:,} records, "
          f"{len(mmsi_records)} vessels. Detecting D+C...", flush=True)

    # Detect Teleportation (D) and Draft (C) — both work correctly on
    # partial records since they look at consecutive ping pairs
    anomalies = []
    for mmsi, records in mmsi_records.items():
        if len(records) < 2:
            continue
        records.sort(key=lambda r: r[1])
        anomalies.extend(detect_teleportation_anomalies(mmsi, records))
        anomalies.extend(detect_draft_change_anomalies(mmsi, records))

    # Release record memory immediately after detection
    mmsi_records.clear()
    gc.collect()

    print(f"[Worker {worker_id}] Done — {len(anomalies)} anomalies (D+C), "
          f"mem: {_mem_mb():.0f} MB", flush=True)

    return {
        'worker_id':     worker_id,
        'total_records': total_records,
        'skipped':       skipped,
        'mmsi_counts':   {m: len(r) for m, r in mmsi_records.items()},
        'anomalies':     anomalies,
        'slow_vessels':  dict(slow_vessels),
        'memory_mb':     _mem_mb(),
    }


# ============================================================================
# GOING DARK STREAMING PASS — runs in background thread concurrently
# ============================================================================

class GoingDarkDetector:
    """
    Streams the CSV file once, keeps full per-MMSI history, detects Going Dark.
    Designed to run in a background thread concurrently with Pass 2 (loitering)
    so it adds ~0 net time to total pipeline duration.
    """

    def __init__(self, filepath: str):
        self.filepath  = filepath
        self.anomalies: List[Dict] = []
        self._thread   = threading.Thread(target=self._run, daemon=True,
                                          name='GoingDarkDetector')
        self._done     = threading.Event()

    def start(self):
        print("[GoingDark] Background streaming pass started...", flush=True)
        self._thread.start()

    def wait(self) -> List[Dict]:
        self._thread.join()
        print(f"[GoingDark] Done — {len(self.anomalies)} going-dark events",
              flush=True)
        return self.anomalies

    def _run(self):
        mmsi_records: Dict[str, List[Tuple]] = defaultdict(list)
        total = 0

        for mmsi, ts_str, epoch, lat, lon, sog, draught in \
                stream_valid_rows(self.filepath):
            mmsi_records[mmsi].append((ts_str, epoch, lat, lon, sog, draught))
            total += 1
            if total % 3_000_000 == 0:
                gc.collect()

        print(f"[GoingDark] {total:,} records, {len(mmsi_records)} vessels — "
              f"running detection...", flush=True)

        for mmsi, records in mmsi_records.items():
            if len(records) < 2:
                continue
            records.sort(key=lambda r: r[1])
            self.anomalies.extend(
                detect_going_dark_anomalies(mmsi, records)
            )

        self._done.set()


# ============================================================================
# PASS 2 — parallel loitering pair verification
# ============================================================================

def run_loitering_parallel(
    slow_vessels: Dict[str, List[Tuple]],
    num_workers:  int,
) -> List[Dict[str, Any]]:
    from config import LOITERING_PROXIMITY_KM, LOITERING_SOG_KNOTS, LOITERING_DURATION_HOURS

    if len(slow_vessels) < 2:
        return []

    loitering_sec = LOITERING_DURATION_HOURS * 3600
    vessel_positions = {
        mmsi: (
            sum(r[2] for r in recs) / len(recs),
            sum(r[3] for r in recs) / len(recs),
        )
        for mmsi, recs in slow_vessels.items()
    }
    slow_with_pos = {
        mmsi: (recs, *vessel_positions[mmsi])
        for mmsi, recs in slow_vessels.items()
    }

    candidates = _build_candidate_pairs(slow_with_pos, vessel_positions)
    print(f"[Loitering] {len(candidates):,} candidate pairs")
    if not candidates:
        return []

    batch_size = max(1, len(candidates) // (num_workers * 4))
    batches = [
        [
            (m1, m2, r1, r2,
             LOITERING_PROXIMITY_KM, loitering_sec, LOITERING_SOG_KNOTS)
            for m1, m2, r1, r2 in candidates[i:i + batch_size]
        ]
        for i in range(0, len(candidates), batch_size)
    ]

    print(f"[Loitering] {len(batches)} batches × ~{batch_size} pairs, "
          f"{num_workers} workers")

    anomalies = []
    done = 0
    with ProcessPoolExecutor(max_workers=num_workers) as ex:
        futures = {ex.submit(_check_pair_batch, b): b for b in batches}
        for fut in as_completed(futures):
            try:
                anomalies.extend(fut.result())
            except Exception as e:
                print(f"[Loitering] Batch error: {e}")
            done += 1
            if done % max(1, len(batches) // 5) == 0:
                print(f"[Loitering] {done}/{len(batches)} batches, "
                      f"{len(anomalies)} found")

    print(f"[Loitering] ok {len(anomalies)} loitering events")
    return anomalies


# ============================================================================
# PIPELINE
# ============================================================================

class AISPipeline:

    def __init__(self, num_workers: int = NUM_WORKERS,
                 chunk_size: int = CHUNK_SIZE):
        self.num_workers = num_workers
        self.chunk_size  = chunk_size
        for d in OUTPUT_DIRS:
            os.makedirs(d, exist_ok=True)

    def process_file(self, filepath: str) -> Dict[str, Any]:
        start_time   = time.time()
        file_size_gb = os.path.getsize(filepath) / (1024**3)

        print(f"\n{'='*70}")
        print(f"Processing : {os.path.basename(filepath)}")
        print(f"File size  : {file_size_gb:.2f} GB")
        print(f"Workers    : {self.num_workers}")
        print(f"{'='*70}\n")

        # ── Pass 1: workers read file in parallel - D + C ─────────────────────
        print(f"{'='*70}")
        print("PASS 1: Parallel detection (D, C) — workers read own chunks")
        print(f"{'='*70}\n")

        ranges = get_file_byte_ranges(filepath, self.num_workers)
        for i, (s, e) in enumerate(ranges):
            print(f"  Worker {i}: {(e-s)/(1024**2):.0f} MB  "
                  f"(bytes {s:,}–{e:,})")
        print()

        pass1_start    = time.time()
        worker_results = []

        with ProcessPoolExecutor(max_workers=self.num_workers) as executor:
            futures = {
                executor.submit(
                    worker_process_range,
                    i, filepath, s, e, LOITERING_SOG_KNOTS
                ): i
                for i, (s, e) in enumerate(ranges)
            }
            for future in as_completed(futures):
                wid = futures[future]
                try:
                    result = future.result()
                    worker_results.append(result)
                    if 'error' in result:
                        print(f"[Main] Worker {wid} failed: {result['error']}")
                    else:
                        print(f"[Main] Worker {wid} done — "
                              f"{result['total_records']:,} records, "
                              f"{len(result['anomalies'])} D+C anomalies")
                except Exception as e:
                    print(f"[Main] Worker {wid} exception: {e}")
                    import traceback; traceback.print_exc()

        pass1_time = time.time() - pass1_start
        print(f"\n[Main] PASS 1 done in {pass1_time:.2f}s")

        if not worker_results:
            print("[Main] ERROR: all workers failed")
            return {}

        # Merge worker results
        seen_keys: set = set()
        dc_anomalies = []
        for r in worker_results:
            for a in r.get('anomalies', []):
                key = (
                    a.get('mmsi', ''),
                    a.get('anomaly_type', ''),
                    a.get('gap_start', ''),
                )
                if key not in seen_keys:
                    seen_keys.add(key)
                    dc_anomalies.append(a)

        combined_slow: Dict[str, List] = defaultdict(list)
        for r in worker_results:
            for mmsi, recs in r.get('slow_vessels', {}).items():
                combined_slow[mmsi].extend(recs)
        for mmsi in combined_slow:
            combined_slow[mmsi].sort(key=lambda r: r[1])

        combined_mmsi: Dict[str, int] = defaultdict(int)
        for r in worker_results:
            for mmsi, cnt in r.get('mmsi_counts', {}).items():
                combined_mmsi[mmsi] += cnt

        total_records = sum(r.get('total_records', 0) for r in worker_results)
        mem_values    = [r.get('memory_mb', 0) for r in worker_results
                         if r.get('memory_mb', 0) > 0]
        max_memory    = max(mem_values) if mem_values else 0

        print(f"[Main] {total_records:,} records, "
              f"{len(combined_mmsi)} vessels, "
              f"{len(dc_anomalies)} D+C anomalies, "
              f"{len(combined_slow)} slow vessels")

        # ── Pass 1b + Pass 2: run Going Dark and Loitering CONCURRENTLY ───────
        print(f"\n{'='*70}")
        print("PASS 1b + PASS 2: Going Dark (streaming) + Loitering (parallel)")
        print("  Running simultaneously — net extra time ≈ 0s")
        print(f"{'='*70}\n")

        # Start Going Dark in background thread
        gd_detector = GoingDarkDetector(filepath)
        gd_detector.start()

        # Run loitering in foreground (parallel pairs)
        pass2_start = time.time()
        loitering_anomalies = run_loitering_parallel(
            dict(combined_slow),
            num_workers=self.num_workers,
        )
        pass2_time = time.time() - pass2_start
        print(f"[Main] Loitering done in {pass2_time:.2f}s")

        # Wait for Going Dark to finish
        going_dark_anomalies = gd_detector.wait()
        print(f"[Main] Going Dark: {len(going_dark_anomalies)} events")

        # ── Pass 3: score ──────────────────────────────────────────────────────
        print(f"\n{'='*70}")
        print("PASS 3: Scoring & ranking (DFSI)")
        print(f"{'='*70}\n")

        all_anomalies = going_dark_anomalies + dc_anomalies + loitering_anomalies
        vessels_dict  = aggregate_anomalies_by_vessel(all_anomalies)
        top_vessels   = rank_vessels_by_dfsi(vessels_dict, top_n=TOP_N_VESSELS)

        elapsed     = time.time() - start_time
        dfsi_scores = [v['dfsi'] for v in top_vessels]

        final_results = {
            'file':              filepath,
            'file_size_gb':      file_size_gb,
            'elapsed_seconds':   elapsed,
            'pass1_seconds':     pass1_time,
            'pass2_seconds':     pass2_time,
            'chunks_processed':  self.num_workers,
            'throughput_mb_per_sec': (file_size_gb * 1024) / elapsed
                                      if elapsed > 0 else 0,
            'total_records':     total_records,
            'unique_vessels':    len(combined_mmsi),
            'mmsi_counts':       dict(combined_mmsi),
            'anomalies':         all_anomalies,
            'vessels_by_dfsi':   top_vessels,
            'total_flagged_vessels': len(vessels_dict),
            'max_memory_mb':     max_memory,
            'worker_results':    worker_results,
            'dfsi_stats': {
                'mean': round(sum(dfsi_scores)/len(dfsi_scores), 2)
                         if dfsi_scores else 0,
                'max':  round(max(dfsi_scores), 2) if dfsi_scores else 0,
                'min':  round(min(dfsi_scores), 2) if dfsi_scores else 0,
            },
        }

        self._print_summary(final_results)
        self._save_results(final_results)
        return final_results

    def _save_results(self, results: Dict[str, Any]) -> None:
        print(f"\n{'='*70}\nWRITING OUTPUT FILES\n{'='*70}\n")
        all_anomalies = results.get('anomalies', [])

        # Versioned output dir: analysis/2025-03-02_v1/, _v2/, etc.
        date_str = os.path.basename(results['file']) \
                     .replace('aisdk-', '').replace('.csv', '')
        version = 1
        while os.path.exists(
                os.path.join(ANALYSIS_DIR, f'{date_str}_v{version}')):
            version += 1
        out_dir = os.path.join(ANALYSIS_DIR, f'{date_str}_v{version}')
        os.makedirs(out_dir, exist_ok=True)
        print(f"  Output dir: {out_dir}")

        write_anomalies_csv(
            all_anomalies,
            os.path.join(out_dir, 'anomaly_events.csv'),
        )

        with open(os.path.join(out_dir, 'all_anomalies.csv'),
                  'w', newline='') as f:
            writer = csv.DictWriter(
                f, fieldnames=['mmsi', 'anomaly_type', 'timestamp'])
            writer.writeheader()
            for a in all_anomalies:
                writer.writerow({
                    'mmsi':         a.get('mmsi', a.get('mmsi_vessel1', '')),
                    'anomaly_type': a.get('anomaly_type'),
                    'timestamp':    a.get('gap_start',
                                         a.get('loitering_start', '')),
                })
        print(f"  ok {len(all_anomalies)} anomalies - all_anomalies.csv")

        top_vessels = results.get('vessels_by_dfsi', [])
        write_vessel_scores_csv(
            top_vessels, os.path.join(out_dir, 'vessel_scores.csv'))
        with open(os.path.join(out_dir, 'top5_suspects.json'), 'w') as f:
            json.dump(top_vessels[:5], f, indent=2, default=str)
        print(f"  ok Top 5 suspects - top5_suspects.json")

        metadata = {
            'file':           os.path.basename(results['file']),
            'date':           date_str,
            'run_output_dir': out_dir,
            'file_size_gb':   round(results['file_size_gb'], 2),
            'total_records':  results['total_records'],
            'unique_vessels': results['unique_vessels'],
            'anomalies': {
                'total':         len(all_anomalies),
                'going_dark':    sum(1 for a in all_anomalies
                                     if a.get('anomaly_type') == 'going_dark'),
                'teleportation': sum(1 for a in all_anomalies
                                     if a.get('anomaly_type') == 'teleportation'),
                'draft_change':  sum(1 for a in all_anomalies
                                     if a.get('anomaly_type') == 'draft_change'),
                'loitering':     sum(1 for a in all_anomalies
                                     if a.get('anomaly_type') == 'loitering'),
            },
            'timing': {
                'pass1_sec':         round(results['pass1_seconds'], 2),
                'pass2_sec':         round(results['pass2_seconds'], 2),
                'total_sec':         round(results['elapsed_seconds'], 2),
                'throughput_mb_sec': round(results['throughput_mb_per_sec'], 2),
            },
            'resources': {
                'peak_memory_mb': round(results['max_memory_mb'], 1),
                'workers':        self.num_workers,
            },
            'dfsi': {
                'mean':            results['dfsi_stats']['mean'],
                'max':             results['dfsi_stats']['max'],
                'min':             results['dfsi_stats']['min'],
                'flagged_vessels': results.get('total_flagged_vessels', 0),
            },
            'filter_notes': {
                'teleportation_min_distance_km': 10.0,
                'note': 'Sub-10km teleportation events filtered as GPS noise',
            },
        }
        write_metadata_json(metadata,
                            os.path.join(out_dir, 'run_metadata.json'))

    def _print_summary(self, results: Dict[str, Any]) -> None:
        print(f"\n{'='*70}\nFINAL SUMMARY\n{'='*70}")
        print(f"File        : {os.path.basename(results['file'])}")
        print(f"Records     : {results['total_records']:,}")
        print(f"Vessels     : {results['unique_vessels']:,}")
        print(f"Pass 1      : {results['pass1_seconds']:.2f}s  (parallel D+C)")
        print(f"Pass 2      : {results['pass2_seconds']:.2f}s  "
              f"(loitering parallel + going dark concurrent)")
        print(f"Total       : {results['elapsed_seconds']:.2f}s")
        print(f"Throughput  : {results['throughput_mb_per_sec']:.2f} MB/s")
        print(f"Peak memory : {results['max_memory_mb']:.0f} MB")

        anomalies = results.get('anomalies', [])
        print(f"\nAnomalies:")
        for atype, label in [
            ('going_dark',    'A Going Dark   '),
            ('teleportation', 'D Teleportation'),
            ('draft_change',  'C Draft Change '),
            ('loitering',     'B Loitering    '),
        ]:
            n = sum(1 for a in anomalies if a.get('anomaly_type') == atype)
            print(f"  {label}: {n:>6}")

        if results.get('vessels_by_dfsi'):
            print(f"\nTop 5 suspects:")
            for i, v in enumerate(results['vessels_by_dfsi'][:5], 1):
                c = v['anomaly_counts']
                print(f"  {i}. MMSI {v['mmsi']}  DFSI {v['dfsi']:.1f}  "
                      f"A:{c.get('going_dark',0)} "
                      f"D:{c.get('teleportation',0)} "
                      f"C:{c.get('draft_change',0)} "
                      f"B:{c.get('loitering',0)}")


# ============================================================================
# Entry point
# ============================================================================

def main():
    DATA_DIR  = './data'
    csv_files = sorted([
        f for f in os.listdir(DATA_DIR)
        if f.startswith('aisdk-') and f.endswith('.csv')
    ])
    if not csv_files:
        print("No aisdk-*.csv files found in ./data/")
        return

    print(f"\n{'='*70}")
    print("SHADOW FLEET DETECTION — PARALLEL FILE-CHUNK ARCHITECTURE")
    print(f"Workers: {NUM_WORKERS}  |  Going Dark: concurrent streaming pass")
    print(f"{'='*70}")

    for csv_file in csv_files:
        filepath = os.path.join(DATA_DIR, csv_file)
        AISPipeline(num_workers=NUM_WORKERS,
                    chunk_size=CHUNK_SIZE).process_file(filepath)
        print(f"\nOK Completed {csv_file}")


if __name__ == '__main__':
    mp.set_start_method('spawn', force=True)
    main()
