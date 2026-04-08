# generate_top10.py
"""
Generate top 10 most suspicious vessels chart.
Reads from analysis/2025-03-03_v*/vessel_scores.csv (latest run).
Saves to analysis/presentation_output/top10_suspects.png
"""

import os
import csv
import glob
import json
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

OUT_DIR = './analysis/presentation_output'
os.makedirs(OUT_DIR, exist_ok=True)

# ── Load vessel scores from latest run ───────────────────────────────────────

def find_latest_scores():
    """Find the most recent vessel_scores.csv."""
    patterns = [
        './analysis/2025-03-03_v*/vessel_scores.csv',
        './analysis/2025-03-02_v*/vessel_scores.csv',
    ]
    for pat in patterns:
        files = sorted(glob.glob(pat))
        if files:
            print(f"  Using: {files[-1]}")
            return files[-1]
    return None

scores_path = find_latest_scores()
if not scores_path:
    print("FAIL No vessel_scores.csv found. Run task1.py first.")
    exit(1)

with open(scores_path) as f:
    rows = list(csv.DictReader(f))

top10 = rows[:10]

print(f"Top 10 vessels loaded from {scores_path}")
for r in top10:
    print(f"  #{r['rank']} MMSI {r['mmsi']}  DFSI {float(r['dfsi']):.1f}  "
          f"A:{r['anomaly_a']} D:{r['anomaly_d']} C:{r['anomaly_c']} B:{r['anomaly_b']}")

# ── MMSI country prefix lookup ────────────────────────────────────────────────

def mmsi_flag(mmsi: str) -> str:
    prefix = mmsi[:3]
    flags = {
        '219': ' Denmark',
        '236': ' Gibraltar',
        '265': ' Sweden',
        '266': ' Sweden',
        '538': ' Marshall Islands',
        '211': ' Germany',
        '244': ' Netherlands',
        '245': ' Netherlands',
        '258': ' Norway',
        '230': ' Finland',
    }
    return flags.get(prefix, f'({prefix}xxxxxx)')


# ── Build chart ───────────────────────────────────────────────────────────────

fig, ax = plt.subplots(figsize=(13, 7))
fig.suptitle('Top 10 Shadow Fleet Suspects — DFSI Ranking\n'
             'AIS Data: Mar 02–03 2025  |  Danish Maritime Authority',
             fontsize=13, fontweight='bold')

ranks     = [int(r['rank'])        for r in top10]
mmsis     = [r['mmsi']             for r in top10]
dfsis     = [float(r['dfsi'])      for r in top10]
a_vals    = [int(r['anomaly_a'])   for r in top10]
d_vals    = [int(r['anomaly_d'])   for r in top10]
c_vals    = [int(r['anomaly_c'])   for r in top10]
b_vals    = [int(r['anomaly_b'])   for r in top10]
flags     = [mmsi_flag(r['mmsi'])  for r in top10]

y = np.arange(len(top10))
bar_h = 0.65

# Stacked horizontal bars — one colour per anomaly type
CA = '#E24B4A'  # red    — Going Dark
CD = '#D85A30'  # coral  — Teleportation
CC = '#1D9E75'  # teal   — Draft Change
CB = '#378ADD'  # blue   — Loitering

b1 = ax.barh(y, a_vals, bar_h, color=CA, alpha=0.9, label='A Going Dark')
b2 = ax.barh(y, d_vals, bar_h, left=a_vals, color=CD, alpha=0.9,
             label='D Teleportation')
b3 = ax.barh(y, c_vals, bar_h,
             left=[a+d for a,d in zip(a_vals, d_vals)],
             color=CC, alpha=0.9, label='C Draft Change')
b4 = ax.barh(y, b_vals, bar_h,
             left=[a+d+c for a,d,c in zip(a_vals, d_vals, c_vals)],
             color=CB, alpha=0.9, label='B Loitering')

# DFSI score label at the end of each stacked bar
totals = [a + d + c + b for a, d, c, b in zip(a_vals, d_vals, c_vals, b_vals)]
max_total = max(totals)
x_pad = max_total * 0.02

for i, (dfsi, total) in enumerate(zip(dfsis, totals)):
    ax.text(total + x_pad, i,
            f'DFSI {dfsi:,.0f}', va='center', ha='left', fontsize=9,
            fontweight='bold', color='#333')

# Y-axis labels: rank + MMSI + flag
labels = [f'#{r}  {m}\n{f}' for r, m, f in zip(ranks, mmsis, flags)]
ax.set_yticks(y)
ax.set_yticklabels(labels, fontsize=9)
ax.invert_yaxis()   # rank 1 at top

ax.set_xlabel('Number of anomaly events (stacked by type)')
ax.set_title('Bar length = total anomaly events  |  DFSI score shown next to bar end',
             fontsize=10, color='#555')

ax.legend(loc='lower right', fontsize=9, framealpha=0.9)
ax.grid(axis='x', alpha=0.3)
ax.set_xlim(0, max_total * 1.35)

# Highlight #1 suspect
ax.get_yticklabels()[0].set_fontweight('bold')
ax.get_yticklabels()[0].set_color('#D85A30')

# Annotation for top suspect
ax.annotate(
    'NOTE: Identity cloning\n45 impossible jumps\navg 1,364 nm each',
    xy=(d_vals[0]/2, 0),
    xytext=(d_vals[0]*0.6, 1.5),
    arrowprops=dict(arrowstyle='->', color='#D85A30', lw=1.5),
    fontsize=8, color='#993C1D',
    bbox=dict(boxstyle='round,pad=0.3', facecolor='#FAECE7',
              edgecolor='#D85A30'),
)

plt.tight_layout()
out = f'{OUT_DIR}/top10_suspects.png'
plt.savefig(out, dpi=150, bbox_inches='tight')
plt.close()
print(f'\nOK Saved: {out}')
