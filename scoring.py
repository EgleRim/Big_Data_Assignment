# scoring.py
"""
DFSI (Shadow Fleet Suspicion Index) calculation and vessel ranking.

Assignment specification formula (Task 3):
    DFSI = (max_gap_hours / 2)
         + (total_impossible_nm / 10)
         + (n_C * 15)
         + (n_B * 10)

Where:
    max_gap_hours        = longest single Going Dark gap in hours (Anomaly A)
    total_impossible_nm  = sum of all impossible-speed distances (Anomaly D)
    n_C                  = number of confirmed draft change events (Anomaly C)
    n_B                  = number of loitering pair events (Anomaly B)

Note: draft_concealment events (Strategy 3) are tracked separately for
analysis purposes but are NOT included in the DFSI formula per spec.
The spec formula uses only confirmed cargo-operation evidence (n_C).
"""

from typing import List, Dict, Any
from collections import defaultdict
from config import (
    DFSI_GOING_DARK_WEIGHT,   # = 2.0 - divides max_gap_hours
    DFSI_TELEPORT_WEIGHT,     # = 10.0 - divides total_nm
    DFSI_DRAFT_WEIGHT,        # = 15.0 - multiplies n_C (confirmed only)
    DFSI_LOITERING_WEIGHT,    # = 10.0 - multiplies n_B
    TOP_N_VESSELS,
)


def calculate_dfsi(mmsi: str, anomalies_for_vessel: List[Dict[str, Any]]) -> float:
    """
    Calculate DFSI for one vessel using the exact assignment formula.

    Takes the full list of anomalies for one vessel,
    returns a single DFSI score.

    DFSI = max_gap_hours/2 + total_impossible_nm/10 + n_C*15 + n_B*10
    """
    going_dark    = [a for a in anomalies_for_vessel
                     if a.get('anomaly_type') == 'going_dark']
    teleportation = [a for a in anomalies_for_vessel
                     if a.get('anomaly_type') == 'teleportation']
    loitering     = [a for a in anomalies_for_vessel
                     if a.get('anomaly_type') == 'loitering']

    # Draft: only CONFIRMED cargo operations count toward DFSI (spec formula)
    # Concealment events are informational only
    draft_confirmed = [
        a for a in anomalies_for_vessel
        if a.get('anomaly_type') == 'draft_change'
        and a.get('detection_strategy') in ('gap_and_change', 'no_gap_change')
    ]

    # A: use the SINGLE longest gap (max, not sum)
    max_gap_hours = max((a['gap_hours'] for a in going_dark), default=0.0)

    # D: sum ALL impossible-speed distances
    total_distance_nm = sum(
        a.get('distance_nm', a['distance_km'] / 1.852)
        for a in teleportation
    )

    # DFSI formula — exact match to assignment specification
    dfsi = (
        (max_gap_hours      / DFSI_GOING_DARK_WEIGHT) +   # A component
        (total_distance_nm  / DFSI_TELEPORT_WEIGHT)   +   # D component
        (len(draft_confirmed) * DFSI_DRAFT_WEIGHT)    +   # C component
        (len(loitering)       * DFSI_LOITERING_WEIGHT)    # B component
    )

    return round(dfsi, 2)


def aggregate_anomalies_by_vessel(
    all_anomalies: List[Dict[str, Any]]
) -> Dict[str, Dict]:
    """
    Group anomalies by MMSI and compute DFSI for each vessel.

    Groups anomalies by MMSI and computes DFSI for each vessel.
    Called after all parallel workers have finished and results merged.
    """
    vessels: Dict[str, Dict] = defaultdict(lambda: {
        'anomalies':      [],
        'anomaly_counts': defaultdict(int),
        'dfsi':           0.0,
    })

    for anomaly in all_anomalies:
        # Handle both regular anomalies (mmsi) and loitering (mmsi_vessel1)
        mmsi = anomaly.get('mmsi') or anomaly.get('mmsi_vessel1')
        if not mmsi:
            continue

        vessels[mmsi]['anomalies'].append(anomaly)
        atype = anomaly.get('anomaly_type', 'unknown')
        vessels[mmsi]['anomaly_counts'][atype] += 1

        # Track draft sub-types separately for reporting
        if atype == 'draft_change':
            strategy = anomaly.get('detection_strategy', 'unknown')
            vessels[mmsi]['anomaly_counts'][f'draft_{strategy}'] += 1

    # Calculate DFSI for every vessel
    for mmsi, data in vessels.items():
        data['dfsi']           = calculate_dfsi(mmsi, data['anomalies'])
        data['anomaly_counts'] = dict(data['anomaly_counts'])

    return dict(vessels)


def rank_vessels_by_dfsi(
    vessels_dict: Dict[str, Dict],
    top_n: int = TOP_N_VESSELS,
) -> List[Dict]:
    """Rank vessels by DFSI score descending and return top N."""
    ranked = [
        {
            'mmsi':            mmsi,
            'dfsi':            data['dfsi'],
            'anomaly_counts':  data['anomaly_counts'],
            'anomalies':       data['anomalies'],
            'total_anomalies': len(data['anomalies']),
        }
        for mmsi, data in vessels_dict.items()
    ]
    ranked.sort(key=lambda x: x['dfsi'], reverse=True)
    return ranked[:top_n]
