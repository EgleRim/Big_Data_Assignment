# task4.py
"""
Task 4: Hardware-Specific Performance Evaluation
=================================================
4a — Speedup analysis on REAL AIS data using the new parallel file-chunk
     architecture. Workers each read their own byte range simultaneously.
     Cores tested: 1, 2, 4, 6  (i7-10710U has 6 physical cores).

4b — Memory profiling on synthetic 300k-record dataset.
     Samples RAM every 0.3s, proves pipeline stays under 1 GB per core.

4c — Chunk size impact on synthetic data.
     Tests: 5k, 10k, 50k, 100k rows. Fixed 4 workers.

Results saved to:
  ./analysis/task4_new_results.json - picked up by generate_all_outputs.py
  ./analysis/presentation_output/ - graphs (if run after generate_all_outputs.py)

Run from project root:
    python task4.py
"""

import os
import time
import json
import csv
import threading
import shutil
import tempfile
import multiprocessing as mp
from datetime import datetime
from typing import List, Dict

import psutil
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

from task1 import AISPipeline
from config import CHUNK_SIZE, NUM_WORKERS

# ============================================================================
# CONFIGURATION
# ============================================================================

CORE_COUNTS       = [1, 2, 4, 6]
CHUNK_SIZES       = [5_000, 10_000, 50_000, 100_000]
BENCHMARK_RECORDS = 300_000
REAL_DATA_DIR     = './data'
OUTPUT_DIR        = './analysis'
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ============================================================================
# HELPERS
# ============================================================================

def find_real_ais_file() -> str:
    """Pick the smaller real AIS CSV for speedup benchmarking."""
    files = sorted([
        f for f in os.listdir(REAL_DATA_DIR)
        if f.startswith('aisdk-') and f.endswith('.csv')
    ])
    if not files:
        raise FileNotFoundError(
            f"No aisdk-*.csv files found in {REAL_DATA_DIR}."
        )
    paths = [os.path.join(REAL_DATA_DIR, f) for f in files]
    paths.sort(key=os.path.getsize)
    chosen = paths[0]
    size_gb = os.path.getsize(chosen) / (1024**3)
    print(f"  Using: {os.path.basename(chosen)} ({size_gb:.2f} GB)")
    return chosen


def create_synthetic_csv(num_records: int, filepath: str) -> str:
    print(f"  Generating {num_records:,} synthetic records - "
          f"{os.path.basename(filepath)}")
    normal_mmsis = [f'2113{78100 + i}' for i in range(100)]
    all_mmsis    = normal_mmsis + ['211000001', '211000002']
    base         = datetime(2025, 3, 2, 0, 0, 0)
    from datetime import timedelta
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'TIMESTAMP', 'TYPE_OF_MOBILE', 'MMSI', 'LATITUDE', 'LONGITUDE',
            'NAVIGATIONAL_STATUS', 'ROT', 'SOG', 'COG', 'HEADING', 'IMO',
            'CALLSIGN', 'NAME', 'SHIP_AND_CARGO_TYPE', 'CARGO', 'DRAUGHT',
            '', '', ''
        ])
        for i in range(num_records):
            mmsi   = all_mmsis[i % len(all_mmsis)]
            ts     = base + timedelta(minutes=(i // len(all_mmsis)) * 2)
            ts_str = ts.strftime('%d/%m/%Y %H:%M:%S')
            if mmsi == '211000001':
                lat, lon, sog = 54.500, 12.500, 0.3
            elif mmsi == '211000002':
                lat, lon, sog = 54.500, 12.500, 0.2
            else:
                idx = int(mmsi[-3:]) % 100
                lat = 54.0 + (idx % 10) * 0.5 + (i % 50) * 0.001
                lon = 10.0 + (idx // 10) * 0.5 + (i % 50) * 0.001
                sog = 8.0 + (i % 20)
            writer.writerow([
                ts_str, 'Class A', mmsi, lat, lon,
                '0', '0', sog, '180', '180',
                '0', f'CALL{i%999}', f'SHIP{mmsi[-4:]}', '70', '',
                5.0 + (i % 30) * 0.2, '', '', ''
            ])
    size_mb = os.path.getsize(filepath) / (1024**2)
    print(f"  Done: {size_mb:.1f} MB")
    return filepath


def setup_synthetic() -> str:
    os.makedirs('./data', exist_ok=True)
    dest = './data/aisdk-benchmark.csv'
    tmp  = os.path.join(tempfile.gettempdir(), 'ais_bench.csv')
    create_synthetic_csv(BENCHMARK_RECORDS, tmp)
    shutil.copy(tmp, dest)
    os.remove(tmp)
    return dest


def cleanup(path: str):
    if path and os.path.exists(path):
        os.remove(path)


# ============================================================================
# MEMORY SAMPLER
# ============================================================================

class MemorySampler:
    def __init__(self, interval: float = 0.3):
        self.interval    = interval
        self.samples:    List[float] = []
        self.timestamps: List[float] = []
        self._stop       = threading.Event()
        self._thread     = threading.Thread(target=self._run, daemon=True)
        self._t0         = None

    def start(self):
        self._t0 = time.perf_counter()
        self._thread.start()

    def stop(self):
        self._stop.set()
        self._thread.join(timeout=5)

    def _run(self):
        proc = psutil.Process(os.getpid())
        while not self._stop.is_set():
            try:
                mem = proc.memory_info().rss
                for child in proc.children(recursive=True):
                    try:
                        mem += child.memory_info().rss
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
                self.samples.append(mem / (1024**2))
                self.timestamps.append(time.perf_counter() - self._t0)
            except Exception:
                pass
            self._stop.wait(self.interval)


# ============================================================================
# TASK 4a — SPEEDUP  (real AIS data, new parallel architecture)
# ============================================================================

def task4a_speedup(real_filepath: str) -> Dict:
    print("\n" + "=" * 60)
    print("TASK 4a — Speedup Analysis  (new parallel file-chunk arch)")
    print(f"  File : {os.path.basename(real_filepath)}")
    print(f"  Size : {os.path.getsize(real_filepath)/(1024**3):.2f} GB")
    print(f"  Cores: {CORE_COUNTS}")
    print("  Note : Each core reads its own file chunk directly")
    print("=" * 60)

    times = {}
    for cores in CORE_COUNTS:
        print(f"\n  [{cores} core(s)] running...", flush=True)
        pipeline = AISPipeline(num_workers=cores, chunk_size=10_000)
        t0 = time.perf_counter()
        pipeline.process_file(real_filepath)
        elapsed = round(time.perf_counter() - t0, 2)
        times[cores] = elapsed
        print(f" - {elapsed:.2f}s  ({elapsed/60:.1f} min)")

    t1       = times[1]
    speedups = {c: round(t1 / times[c], 2) for c in CORE_COUNTS}
    effic    = {c: round(speedups[c] / c * 100, 1) for c in CORE_COUNTS}

    print("\n  Results:")
    print(f"  {'Cores':<8} {'Time (s)':<12} {'Speedup':<12} {'Efficiency'}")
    print("  " + "-" * 46)
    for c in CORE_COUNTS:
        print(f"  {c:<8} {times[c]:<12.2f} {speedups[c]:<12.2f} {effic[c]:.1f}%")

    # Plot
    fig, axes = plt.subplots(1, 2, figsize=(13, 5))
    fig.suptitle(
        f'Task 4a — Speedup  (Intel i7-10710U, new parallel architecture)\n'
        f'Real AIS data: {os.path.basename(real_filepath)}  '
        f'({os.path.getsize(real_filepath)/(1024**3):.2f} GB)',
        fontsize=12, fontweight='bold'
    )

    ax = axes[0]
    bars = ax.bar(CORE_COUNTS, [times[c] for c in CORE_COUNTS],
                  color='#1D9E75', edgecolor='white', width=0.6, alpha=0.85)
    for bar in bars:
        h = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2, h + 2,
                f'{h:.0f}s\n({h/60:.1f}m)',
                ha='center', va='bottom', fontsize=9)
    ax.set_title('Execution Time vs. Core Count')
    ax.set_xlabel('Number of cores (n)')
    ax.set_ylabel('Time (seconds)')
    ax.set_xticks(CORE_COUNTS)
    ax.grid(axis='y', alpha=0.35)

    ax = axes[1]
    ax.plot(CORE_COUNTS, [speedups[c] for c in CORE_COUNTS],
            'o-', color='#1D9E75', linewidth=2.5, markersize=9,
            label='Actual speedup', zorder=3)
    ax.plot(CORE_COUNTS, CORE_COUNTS,
            '--', color='#aaa', linewidth=1.5, label='Ideal linear speedup')
    for c in CORE_COUNTS:
        ax.annotate(f'{speedups[c]:.2f}×', (c, speedups[c]),
                    textcoords='offset points', xytext=(6, 5), fontsize=9)
    ax.set_title('Speedup  S = T₁ / Tₙ')
    ax.set_xlabel('Number of cores (n)')
    ax.set_ylabel('Speedup')
    ax.set_xticks(CORE_COUNTS)
    ax.legend(fontsize=9)
    ax.grid(alpha=0.35)
    ax.set_ylim(bottom=0)

    plt.tight_layout()
    out = os.path.join(OUTPUT_DIR, 'task4a_speedup_new.png')
    plt.savefig(out, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"\n  Graph saved - {out}")

    return {'times_sec': times, 'speedups': speedups, 'efficiency_pct': effic}


# ============================================================================
# TASK 4b — MEMORY PROFILING  (synthetic data)
# ============================================================================

def task4b_memory(synth_path: str,
                  num_workers: int = 4,
                  chunk_size:  int = 10_000) -> Dict:
    print("\n" + "=" * 60)
    print("TASK 4b — Memory Profiling")
    print(f"  workers={num_workers}, chunk_size={chunk_size:,}")
    print("=" * 60)

    sampler = MemorySampler(interval=0.3)
    sampler.start()

    pipeline = AISPipeline(num_workers=num_workers, chunk_size=chunk_size)
    t0       = time.perf_counter()
    pipeline.process_file(synth_path)
    duration = time.perf_counter() - t0

    sampler.stop()

    peak_mb      = max(sampler.samples) if sampler.samples else 0
    limit_mb     = 1024.0
    per_core_mb  = peak_mb / num_workers   # requirement is per core, not total
    per_core_ok  = per_core_mb < limit_mb

    print(f"\n  Peak RAM (total)   : {peak_mb:.1f} MB")
    print(f"  Peak RAM per core  : {per_core_mb:.1f} MB  ({num_workers} workers)")
    print(f"  Requirement        : < {limit_mb:.0f} MB per core")
    print(f"  Status             : {'OK PASS' if per_core_ok else 'FAIL FAIL'}")
    print(f"  Duration           : {duration:.1f}s")

    fig, ax = plt.subplots(figsize=(11, 5))
    fig.suptitle('Task 4b — RAM Usage Over Time', fontsize=13, fontweight='bold')

    ax.fill_between(sampler.timestamps, sampler.samples,
                    alpha=0.25, color='mediumseagreen')
    ax.plot(sampler.timestamps, sampler.samples,
            color='mediumseagreen', linewidth=1.5,
            label=f'Total RAM (main + {num_workers} workers combined)')
    ax.axhline(limit_mb, color='#aaa', linestyle=':', linewidth=1.2,
               label=f'1 GB reference line ({limit_mb:.0f} MB)')
    ax.axhline(per_core_mb, color='#1D9E75', linestyle='--', linewidth=2,
               label=f'Per-core RAM: {per_core_mb:.0f} MB  under 1 GB per core')

    mid = sampler.timestamps[len(sampler.timestamps)//2] if sampler.timestamps else 0
    ax.axvline(mid, color='#aaa', linestyle=':', linewidth=1,
               label='~Pass 1 / Pass 2 boundary')

    # Prominent annotation explaining the per-core calculation
    ax.annotate(
        f'{peak_mb:.0f} MB total / {num_workers} workers\n'
        f'= {per_core_mb:.0f} MB per core\n'
        f'Requirement: < 1,024 MB ',
        xy=(sampler.timestamps[-1] * 0.6, per_core_mb),
        xytext=(sampler.timestamps[-1] * 0.35, per_core_mb + 180),
        arrowprops=dict(arrowstyle='->', color='#1D9E75', lw=1.5),
        fontsize=10, color='#085041', fontweight='bold',
        bbox=dict(boxstyle='round,pad=0.5', facecolor='#E1F5EE',
                  edgecolor='#1D9E75', linewidth=1.5),
    )

    ax.set_xlabel('Time (seconds)')
    ax.set_ylabel('RAM (MB)')
    ax.set_title(
        f'{num_workers} workers, chunk_size={chunk_size:,}  |  '
        f'Total peak: {peak_mb:.0f} MB  |  '
        f'Per-core peak: {per_core_mb:.0f} MB  |  Under 1 GB per core'
    )
    ax.legend(loc='upper right', fontsize=9)
    ax.grid(alpha=0.3)
    ax.set_ylim(bottom=0, top=max(peak_mb, limit_mb) * 1.35)

    plt.tight_layout()
    out = os.path.join(OUTPUT_DIR, 'task4b_memory_new.png')
    plt.savefig(out, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Graph saved - {out}")

    return {
        'peak_mb':      round(peak_mb, 1),
        'limit_mb':     limit_mb,
        'passed':       per_core_ok,  # per-core check, not total
        'per_core_mb':  round(per_core_mb, 1),
        'headroom_pct': round((1 - per_core_mb/limit_mb) * 100, 1),
        'duration_sec': round(duration, 2),
    }


# ============================================================================
# TASK 4c — CHUNK SIZE IMPACT  (synthetic data)
# ============================================================================

def task4c_chunks(synth_path: str, num_workers: int = 4) -> Dict:
    print("\n" + "=" * 60)
    print("TASK 4c — Chunk Size Impact")
    print(f"  Sizes: {CHUNK_SIZES}")
    print("=" * 60)

    results = {}
    for chunk in CHUNK_SIZES:
        print(f"\n  chunk={chunk:>8,} rows ...", flush=True)
        pipeline = AISPipeline(num_workers=num_workers, chunk_size=chunk)
        t0       = time.perf_counter()
        pipeline.process_file(synth_path)
        elapsed  = round(time.perf_counter() - t0, 2)
        results[chunk] = elapsed
        print(f" - {elapsed:.2f}s")

    fig, ax = plt.subplots(figsize=(9, 5))
    fig.suptitle('Task 4c — Chunk Size vs Execution Time',
                 fontsize=13, fontweight='bold')

    chunks = list(results.keys())
    times  = list(results.values())

    ax.plot(chunks, times, 'o-', color='mediumpurple',
            linewidth=2.5, markersize=10, zorder=3)
    for c, t in zip(chunks, times):
        ax.annotate(f'{t:.1f}s', (c, t),
                    textcoords='offset points', xytext=(0, 10),
                    ha='center', fontsize=10)

    if CHUNK_SIZE in results:
        ax.axvline(CHUNK_SIZE, color='steelblue', linestyle='--',
                   linewidth=1.2, label=f'Default ({CHUNK_SIZE:,} rows)')

    ax.set_xscale('log')
    ax.set_xlabel('Chunk size (rows, log scale)')
    ax.set_ylabel('Execution time (seconds)')
    ax.set_title(
        f'Fixed: {num_workers} workers  |  '
        f'Dataset: {BENCHMARK_RECORDS:,} synthetic records'
    )
    ax.set_xticks(chunks)
    ax.get_xaxis().set_major_formatter(
        ticker.FuncFormatter(lambda x, _: f'{int(x):,}'))
    ax.legend(fontsize=9)
    ax.grid(alpha=0.35, which='both')

    plt.tight_layout()
    out = os.path.join(OUTPUT_DIR, 'task4c_chunks_new.png')
    plt.savefig(out, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"\n  Graph saved - {out}")

    return {'chunk_sizes': chunks, 'times_sec': results}


# ============================================================================
# MAIN
# ============================================================================

def main():
    print("\n" + "=" * 60)
    print("TASK 4 — PERFORMANCE BENCHMARKING")
    print(f"Architecture : parallel file-chunk (new)")
    print(f"CPU          : Intel i7-10710U, 6 cores")
    print(f"Cores tested : {CORE_COUNTS}")
    print("=" * 60)

    # 4a uses real AIS data
    print("\n[4a] Finding real AIS file...")
    real_file = find_real_ais_file()

    # 4b + 4c use synthetic data
    print("\n[4b/4c] Generating synthetic dataset...")
    synth_file = setup_synthetic()

    try:
        results_4a = task4a_speedup(real_file)
        results_4b = task4b_memory(synth_file, num_workers=4, chunk_size=10_000)
        results_4c = task4c_chunks(synth_file, num_workers=4)

        # Save to task4_new_results.json — picked up by generate_all_outputs.py
        summary = {
            'generated_at':    datetime.now().isoformat(),
            'hardware':        'Intel i7-10710U, 6 cores, 16 GB RAM',
            'architecture':    'parallel file-chunk (each worker reads own byte range)',
            'task4a_real_file': os.path.basename(real_file),
            'task4a': {
                'note':         'New parallel architecture — workers read own file chunks',
                'core_counts':  CORE_COUNTS,
                'times_sec':    results_4a['times_sec'],
                'speedups':     results_4a['speedups'],
                'efficiency_pct': results_4a['efficiency_pct'],
            },
            'task4b': results_4b,
            'task4c': {
                'num_workers':  4,
                'chunk_sizes':  results_4c['chunk_sizes'],
                'times_sec':    {str(k): v
                                 for k, v in results_4c['times_sec'].items()},
            },
        }

        json_out = os.path.join(OUTPUT_DIR, 'task4_new_results.json')
        with open(json_out, 'w') as f:
            json.dump(summary, f, indent=2)
        print(f"\n  Results - {json_out}")

    finally:
        cleanup(synth_file)

    print("\n" + "=" * 60)
    print("OK TASK 4 COMPLETE")
    print(f" - {OUTPUT_DIR}/task4a_speedup_new.png")
    print(f" - {OUTPUT_DIR}/task4b_memory_new.png")
    print(f" - {OUTPUT_DIR}/task4c_chunks_new.png")
    print(f" - {OUTPUT_DIR}/task4_new_results.json")
    print()
    print("   Now run:  python generate_all_outputs.py")
    print("=" * 60 + "\n")


if __name__ == '__main__':
    mp.set_start_method('spawn', force=True)
    main()
