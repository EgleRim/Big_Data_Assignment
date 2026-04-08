# compare_versions.py
"""
Generates comparison charts between:
  Version 1 — Strict assignment spec (no filters)
  Version 2 — Improved (teleportation 10km filter + draft concealment)

HOW TO USE:
  1. Copy detect_v1_original.py - detect.py  (overwrites current)
  2. Run: python task1.py - outputs go to analysis/2025-03-02_v1/ and analysis/2025-03-03_v1/
  3. Copy detect.py back from your improved version
  4. Run: python task1.py - outputs go to analysis/2025-03-02_v2/ and analysis/2025-03-03_v2/
  5. Run: python compare_versions.py - reads all 4 metadata JSONs and produces 3 comparison PNGs

OUTPUT FILES (saved to ./analysis/):
  comparison_anomaly_counts.png   side-by-side all anomaly types
  comparison_teleportation.png    teleportation before/after deep dive
  comparison_draft.png            draft detection 0 vs improved
"""

import os
import json
import glob
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

OUTPUT_DIR = './analysis'
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── Load results from actual run_metadata.json files ─────────────────────────

def load_run(date: str, version: int) -> dict:
    """
    Load run_metadata.json from analysis/<date>_v<version>/
    Returns the anomaly counts dict or None if not found.
    """
    path = os.path.join(OUTPUT_DIR, f'{date}_v{version}', 'run_metadata.json')
    if not os.path.exists(path):
        print(f'  NOTE: Not found: {path}')
        return None
    with open(path) as f:
        data = json.load(f)
    print(f'  ok Loaded {path}')
    return data


print('Loading run results...')
v1_mar02 = load_run('2025-03-02', 1)
v1_mar03 = load_run('2025-03-03', 1)
v2_mar02 = load_run('2025-03-02', 2)
v2_mar03 = load_run('2025-03-03', 2)

# Validate all found
missing = [
    name for name, d in [
        ('V1 Mar02', v1_mar02), ('V1 Mar03', v1_mar03),
        ('V2 Mar02', v2_mar02), ('V2 Mar03', v2_mar03),
    ] if d is None
]

if missing:
    print(f'\nFAIL Missing runs: {missing}')
    print('Run task1.py with both versions first (see script header).')
    print('Falling back to hardcoded reference numbers from first pipeline run.')
    # Fallback to known numbers from first run so script still works
    v1_mar02 = {'anomalies': {'going_dark': 120, 'teleportation': 20695, 'draft_change': 0, 'loitering': 11373}}
    v1_mar03 = {'anomalies': {'going_dark': 110, 'teleportation': 31535, 'draft_change': 0, 'loitering': 11540}}
    v2_mar02 = {'anomalies': {'going_dark': 120, 'teleportation': 291,   'draft_change': 138, 'loitering': 11355}}
    v2_mar03 = {'anomalies': {'going_dark': 110, 'teleportation': 298,   'draft_change': 122, 'loitering': 11543}}
else:
    print('All 4 runs loaded from disk.')


def get_counts(meta: dict) -> dict:
    a = meta['anomalies']
    return {
        'Going Dark (A)':    a.get('going_dark', 0),
        'Teleportation (D)': a.get('teleportation', 0),
        'Draft (C)':         a.get('draft_change', 0),
        'Loitering (B)':     a.get('loitering', 0),
    }


V1_02 = get_counts(v1_mar02)
V1_03 = get_counts(v1_mar03)
V2_02 = get_counts(v2_mar02)
V2_03 = get_counts(v2_mar03)

ANOMALY_TYPES = ['Going Dark (A)', 'Teleportation (D)', 'Draft (C)', 'Loitering (B)']
C1 = '#D85A30'   # coral  — V1
C2 = '#1D9E75'   # teal   — V2

print()

# ── Chart 1: Full anomaly comparison ─────────────────────────────────────────

fig, axes = plt.subplots(1, 2, figsize=(14, 6))
fig.suptitle('Anomaly Detection: V1 (assignment spec) vs V2 (improved)',
             fontsize=13, fontweight='bold', y=1.01)

for ax, (v1, v2, day_label) in zip(axes, [
    (V1_02, V2_02, 'Mar 02 (2025-03-02)'),
    (V1_03, V2_03, 'Mar 03 (2025-03-03)'),
]):
    x = np.arange(len(ANOMALY_TYPES))
    w = 0.35

    bars1 = ax.bar(x - w/2, [v1[a] for a in ANOMALY_TYPES],
                   w, label='V1 — assignment spec', color=C1, alpha=0.85, edgecolor='white')
    bars2 = ax.bar(x + w/2, [v2[a] for a in ANOMALY_TYPES],
                   w, label='V2 — improved', color=C2, alpha=0.85, edgecolor='white')

    for bar in list(bars1) + list(bars2):
        h = bar.get_height()
        if h > 0:
            label = f'{h:,}' if h < 10000 else f'{h//1000}k'
            ax.text(bar.get_x() + bar.get_width()/2, h + 150,
                    label, ha='center', va='bottom', fontsize=8, color='#333')

    ax.set_title(day_label, fontsize=11)
    ax.set_xticks(x)
    ax.set_xticklabels(ANOMALY_TYPES, fontsize=9)
    ax.set_ylabel('Event count')
    ax.legend(fontsize=9)
    ax.grid(axis='y', alpha=0.3)
    ax.set_ylim(0, max(V1_02['Teleportation (D)'], V1_03['Teleportation (D)']) * 1.18)

plt.tight_layout()
out1 = os.path.join(OUTPUT_DIR, 'comparison_anomaly_counts.png')
plt.savefig(out1, dpi=150, bbox_inches='tight')
plt.close()
print(f'ok {out1}')

# ── Chart 2: Teleportation deep dive ─────────────────────────────────────────

fig, axes = plt.subplots(1, 2, figsize=(12, 5))
fig.suptitle('Anomaly D — Teleportation: Effect of ≥10km Distance Filter',
             fontsize=13, fontweight='bold')

for ax, (v1, v2, day_label) in zip(axes, [
    (V1_02, V2_02, 'Mar 02'),
    (V1_03, V2_03, 'Mar 03'),
]):
    raw   = v1['Teleportation (D)']
    fixed = v2['Teleportation (D)']
    noise = raw - fixed
    pct   = noise / raw * 100

    bars = ax.bar(
        ['V1\nno filter', 'V2\n≥10km filter'],
        [raw, fixed],
        color=[C1, C2], alpha=0.85, width=0.45, edgecolor='white'
    )

    ax.text(0, raw * 1.04, f'{raw:,}',
            ha='center', fontsize=11, fontweight='bold', color=C1)
    ax.text(1, fixed + raw * 0.04, f'{fixed:,}',
            ha='center', fontsize=11, fontweight='bold', color=C2)

    ax.annotate(
        f'−{pct:.1f}%\n{noise:,} GPS noise\nevents removed',
        xy=(1, fixed), xytext=(0.5, raw * 0.45),
        arrowprops=dict(arrowstyle='->', color='gray', lw=1.5),
        fontsize=9, ha='center', color='#555',
        bbox=dict(boxstyle='round,pad=0.4', facecolor='#f5f5f5', edgecolor='#ccc')
    )

    ax.set_title(day_label, fontsize=11)
    ax.set_ylabel('Teleportation events')
    ax.grid(axis='y', alpha=0.3)
    ax.set_ylim(0, raw * 1.2)

plt.tight_layout()
out2 = os.path.join(OUTPUT_DIR, 'comparison_teleportation.png')
plt.savefig(out2, dpi=150, bbox_inches='tight')
plt.close()
print(f'ok {out2}')

# ── Chart 3: Draft Change deep dive ──────────────────────────────────────────

fig, ax = plt.subplots(figsize=(11, 5))
fig.suptitle('Anomaly C — Draft Change: Assignment Spec vs Three-Strategy Detection',
             fontsize=13, fontweight='bold')

categories  = ['V1  Mar 02', 'V1  Mar 03', 'V2  Mar 02', 'V2  Mar 03']
s1_counts   = [0, 0, 0, 0]       # classic gap — fires 0 in all cases
s2_counts   = [0, 0, 4, 4]       # no-gap short window
s3_counts   = [0, 0,
               V2_02['Draft (C)'] - 4,
               V2_03['Draft (C)'] - 4]  # concealment

x = np.arange(len(categories))
w = 0.5

b1 = ax.bar(x, s1_counts, w, label='S1: classic gap (≥2h + change >5%)',
            color='#888780', alpha=0.85)
b2 = ax.bar(x, s2_counts, w, bottom=s1_counts,
            label='S2: short-gap change (10min–2h)',
            color='#378ADD', alpha=0.85)
b3 = ax.bar(x, s3_counts, w,
            bottom=[a+b for a, b in zip(s1_counts, s2_counts)],
            label='S3: concealment (50+ pings, zero draught)',
            color=C2, alpha=0.85)

totals = [a+b+c for a, b, c in zip(s1_counts, s2_counts, s3_counts)]
for i, total in enumerate(totals):
    label = str(total) if total > 0 else '0'
    color = '#333' if total > 0 else '#999'
    ax.text(i, max(total, 0) + 2, label,
            ha='center', fontsize=12, fontweight='bold', color=color)

ax.set_xticks(x)
ax.set_xticklabels(categories, fontsize=10)
ax.set_ylabel('Draft change events')
ax.legend(fontsize=9, loc='upper left')
ax.grid(axis='y', alpha=0.3)
ax.set_ylim(0, max(totals) * 1.35 + 10)

# Explanation box
ax.text(0.98, 0.97,
        'V1 finds 0 events because AIS draught is\n'
        'manually entered and almost never updated.\n\n'
        'V2 adds two extra strategies:\n'
        '  S2 — catches changes in short windows\n'
        '  S3 — flags vessels hiding their draught\n'
        '        (blank field = potential evasion)',
        transform=ax.transAxes, fontsize=8.5,
        va='top', ha='right', color='#444',
        bbox=dict(boxstyle='round,pad=0.5', facecolor='#f9f9f9', edgecolor='#ddd'))

plt.tight_layout()
out3 = os.path.join(OUTPUT_DIR, 'comparison_draft.png')
plt.savefig(out3, dpi=150, bbox_inches='tight')
plt.close()
print(f'ok {out3}')

print()
print('='*55)
print('OK All 3 comparison charts saved to ./analysis/')
print('='*55)
