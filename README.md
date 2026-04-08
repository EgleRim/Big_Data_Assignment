# Maritime Shadow Fleet Detection — Big Data Assignment 1

Detection of illicit vessel behaviour in the Baltic Sea using parallel
computing and AIS (Automatic Identification System) data from the
Danish Maritime Authority.

**Dataset:** 2 days of AIS data (March 02-03 2025) -- ~5.6 GB total  
**Hardware:** Intel i7-10710U, 6 cores, 16 GB RAM

---

## Project Structure

```
├── config.py               # All thresholds, constants, output paths
├── parsing.py              # Streaming CSV parser + dirty data filtering
├── partition.py            # MMSI-partitioned chunk generator
├── geo.py                  # Haversine distance, coordinate validation
├── detect.py               # Anomaly detection: A (Going Dark), C (Draft), D (Teleportation)
├── detect_v1_original.py   # V1 baseline -- strict assignment spec (no improvements)
├── loiter.py               # Anomaly B (Loitering) -- parallel pair verification
├── scoring.py              # DFSI formula + vessel ranking
├── analysis.py             # CSV/JSON output writers
├── task1.py                # Main pipeline -- parallel file-chunk architecture
├── task4.py                # Performance benchmarking (speedup, memory, chunks)
├── compare_versions.py     # V1 vs V2 comparison charts
├── generate_all_outputs.py # All presentation charts -> analysis/presentation_output/
├── generate_top10.py       # Top 10 suspects chart
├── benchmark.py            # Legacy benchmark helper
├── tests.py                # Integration test suite
└── test_validations.py     # Unit tests for anomaly detection
```

---

## Architecture Overview

### Parallel File-Chunk Architecture (Task 1 + 2)

```
aisdk-YYYY-MM-DD.csv  (2.7 GB)
        |
        v
 get_file_byte_ranges()  -- splits file into N equal byte ranges
        |
   +----+----+---------+---------+
   v         v         v         v
Worker 0   Worker 1  Worker 2  Worker 3     (MAP stage)
(0-680MB) (680-1360) (1360-2040) (2040-2710)
   |         |         |         |
   +----+----+---------+---------+
        v
   Main process merges results              (REDUCE stage)
        |
   +----+------------------+
   v                       v
GoingDarkDetector     run_loitering_parallel()
(background thread)   (ProcessPoolExecutor)
        |                   |
        +--------+----------+
                 v
          aggregate_anomalies_by_vessel()   (Final REDUCE)
                 v
          rank_vessels_by_dfsi()
```

Each worker reads its own byte range independently -- no dispatcher
bottleneck. True parallel I/O on all cores simultaneously.

### Dirty Data Handling

Five categories of invalid records are filtered before processing:

1. Invalid MMSI patterns (000000000, 111111111, 123456789 etc.)
2. Invalid coordinates (out of range or null island 0N 0E)
3. Malformed rows (fewer than 16 columns, unparseable timestamps)
4. AIS base stations (992xxxxxx prefix)
5. Missing sensor values (handled with safe defaults)

---

## Quick Start

```bash
pip install -r requirements.txt
```

Place AIS CSV files in `./data/` (do not commit to GitHub -- files are 2-3 GB each):

```
data/
  aisdk-2025-03-02.csv
  aisdk-2025-03-03.csv
```

Run the detection pipeline:

```bash
python task1.py
```

Run performance benchmarks:

```bash
python task4.py
```

Generate all presentation charts:

```bash
python generate_top10.py
python generate_all_outputs.py
```

Get map coordinates:

```bash
python map.py
```
---

## Anomaly Detection (Task 3)

| Anomaly | Description | Threshold |
|---------|-------------|-----------|
| A -- Going Dark | AIS gap + vessel moved | gap > 4h + distance > 5km |
| B -- Loitering | Two vessels close together, slow | < 500m, SOG < 1 knot, > 2h |
| C -- Draft Change | Draught change during blackout | change > 5% during > 2h gap |
| D -- Teleportation | Impossible speed between pings | > 60 knots + > 10km distance |

### DFSI Formula (exact assignment specification)

```
DFSI = (max_gap_hours / 2)
     + (total_impossible_nm / 10)
     + (n_C x 15)
     + (n_B x 10)
```

---

## Performance Results (Task 4)

### Speedup -- New parallel architecture vs old dispatcher

| Cores | Old arch (s) | New arch (s) | Speedup |
|-------|-------------|-------------|---------|
| 1     | 1,691       | 703         | 1.00x   |
| 2     | 1,615       | 562         | 1.25x   |
| 4     | 1,885       | 496         | 1.42x   |
| 6     | 1,949       | 490         | 1.43x   |

### Memory -- per core

- Peak total: ~1,482 MB across 4 workers
- Per core: ~370 MB -- under 1 GB per core requirement

### Amdahl's Law

- Old arch serial fraction: 17.7% -- theoretical max speedup 5.6x
- New arch serial fraction: 6.7% -- theoretical max speedup 15.0x

---

## Top Suspect

**MMSI 236112746** (Gibraltar-registered) -- DFSI 6,138.20

45 teleportation events on 03/03/2025, average jump 1,364 nm per event.
Classic identity cloning: two physical ships broadcasting the same stolen
MMSI simultaneously -- one in Baltic/North Sea, one approximately 6,300 km away.

---

## Key Design Decisions

- No pandas.read_csv() used anywhere -- all file reading uses Python's native
  csv module and readline() generators to comply with the pandas ban
- V1 vs V2: detect_v1_original.py is the strict assignment-spec baseline;
  detect.py is the improved version with GPS noise filter and draft
  concealment detection
- Draft Change (C): Assignment spec (gap > 2h + change > 5%) returns 0 events
  because AIS draught is manually entered and almost never updated by crews.
  V2 adds concealment detection as an extension.
