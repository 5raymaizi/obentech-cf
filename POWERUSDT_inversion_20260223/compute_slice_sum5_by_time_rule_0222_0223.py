#!/usr/bin/env python3
"""
Classify POWERUSDT slices by a fixed Beijing-time rule and compute top5 notional-sum distributions.

Rule:
- Non-inversion: before 2026-02-23 10:30:00 Asia/Shanghai
- Inversion: at/after 2026-02-23 10:30:00 Asia/Shanghai

Data days used:
- 2026-02-22
- 2026-02-23
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List

import pandas as pd

ROOT = Path('/Users/rayxu/Documents/Obentech_code/CF/POWERUSDT_inversion_20260223')

BN_FILES = [
    Path('/Volumes/T7/Obentech/data/binance/perp/books/POWER/powerusdt_2026-02-22_depth5.parquet'),
    Path('/Volumes/T7/Obentech/data/binance/perp/books/POWER/powerusdt_2026-02-23_depth5.parquet'),
]
BY_FILES = [
    Path('/Volumes/T7/Obentech/data/bybit5/perp/books/POWER/powerusdt_2026-02-22_depth5.parquet'),
    Path('/Volumes/T7/Obentech/data/bybit5/perp/books/POWER/powerusdt_2026-02-23_depth5.parquet'),
]

THRESHOLD_BJ = pd.Timestamp('2026-02-23 10:30:00', tz='Asia/Shanghai')


def _must_exists(paths: List[Path]) -> None:
    for p in paths:
        if not p.exists():
            raise FileNotFoundError(f'Missing file: {p}')


def sum5(df: pd.DataFrame, prefix: str = '') -> pd.Series:
    bid = 0.0
    ask = 0.0
    for i in range(5):
        bid += df[f'{prefix}bid_price{i}'] * df[f'{prefix}bid_size{i}']
        ask += df[f'{prefix}ask_price{i}'] * df[f'{prefix}ask_size{i}']
    return bid + ask


def stats_series(s: pd.Series) -> Dict[str, float]:
    q = s.quantile([0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99])
    return {
        'n': int(len(s)),
        'mean': float(s.mean()),
        'std': float(s.std()),
        'min': float(s.min()),
        'p01': float(q.loc[0.01]),
        'p05': float(q.loc[0.05]),
        'p10': float(q.loc[0.10]),
        'p25': float(q.loc[0.25]),
        'p50': float(q.loc[0.50]),
        'p75': float(q.loc[0.75]),
        'p90': float(q.loc[0.90]),
        'p95': float(q.loc[0.95]),
        'p99': float(q.loc[0.99]),
        'max': float(s.max()),
    }


def add_bucket(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out['ts_utc'] = pd.to_datetime(out['E'], unit='ms', utc=True)
    out['ts_bj'] = out['ts_utc'].dt.tz_convert('Asia/Shanghai')
    out['bucket'] = out['ts_bj'].ge(THRESHOLD_BJ).map({True: '倒挂', False: '非倒挂'})
    return out


def collect_raw_stats(df: pd.DataFrame, venue: str) -> List[Dict[str, float]]:
    rows: List[Dict[str, float]] = []
    for bucket in ['非倒挂', '倒挂']:
        d = df[df['bucket'] == bucket]
        st = stats_series(d['sum5'])
        rows.append({'scope': 'raw', 'venue': venue, 'metric': 'slice_sum5', 'bucket': bucket, **st})
    return rows


def main() -> None:
    _must_exists(BN_FILES)
    _must_exists(BY_FILES)
    ROOT.mkdir(parents=True, exist_ok=True)

    bn = pd.concat([pd.read_parquet(p) for p in BN_FILES], ignore_index=True)
    by = pd.concat([pd.read_parquet(p) for p in BY_FILES], ignore_index=True)

    bn = add_bucket(bn)
    by = add_bucket(by)

    bn['sum5'] = sum5(bn)
    by['sum5'] = sum5(by)

    raw_rows = []
    raw_rows.extend(collect_raw_stats(bn, 'binance'))
    raw_rows.extend(collect_raw_stats(by, 'bybit5'))

    keep = ['ts_utc'] + [f'bid_price{i}' for i in range(5)] + [f'bid_size{i}' for i in range(5)] + [f'ask_price{i}' for i in range(5)] + [f'ask_size{i}' for i in range(5)]
    bnm = bn[keep].sort_values('ts_utc').rename(columns={c: f'bn_{c}' if c != 'ts_utc' else 'ts_utc' for c in keep})
    bym = by[keep].sort_values('ts_utc').rename(columns={c: f'by_{c}' if c != 'ts_utc' else 'ts_utc' for c in keep})

    merged = pd.merge_asof(
        bym,
        bnm,
        on='ts_utc',
        direction='backward',
        tolerance=pd.Timedelta('100ms'),
    ).dropna().copy()

    merged['ts_bj'] = merged['ts_utc'].dt.tz_convert('Asia/Shanghai')
    merged['bucket'] = merged['ts_bj'].ge(THRESHOLD_BJ).map({True: '倒挂', False: '非倒挂'})
    merged['bn_sum5'] = sum5(merged, 'bn_')
    merged['by_sum5'] = sum5(merged, 'by_')
    merged['all_sum5'] = merged['bn_sum5'] + merged['by_sum5']

    merged_rows: List[Dict[str, float]] = []
    for bucket in ['非倒挂', '倒挂']:
        d = merged[merged['bucket'] == bucket]
        for metric in ['bn_sum5', 'by_sum5', 'all_sum5']:
            st = stats_series(d[metric])
            merged_rows.append(
                {
                    'scope': 'merged',
                    'venue': 'bn+by_align100ms' if metric == 'all_sum5' else metric.replace('_sum5', ''),
                    'metric': metric,
                    'bucket': bucket,
                    **st,
                }
            )

    raw_df = pd.DataFrame(raw_rows)
    merged_df = pd.DataFrame(merged_rows)
    all_df = pd.concat([raw_df, merged_df], ignore_index=True)

    out_raw = ROOT / 'slice_sum5_stats_raw_time_rule_0222_0223.csv'
    out_merged = ROOT / 'slice_sum5_stats_merged_time_rule_0222_0223.csv'
    out_all = ROOT / 'slice_sum5_stats_all_time_rule_0222_0223.csv'

    raw_df.to_csv(out_raw, index=False)
    merged_df.to_csv(out_merged, index=False)
    all_df.to_csv(out_all, index=False)

    print('[OK] threshold (BJ):', THRESHOLD_BJ)
    print('[OK] data days: 2026-02-22, 2026-02-23')
    print('[OK] wrote:', out_raw)
    print('[OK] wrote:', out_merged)
    print('[OK] wrote:', out_all)
    print('')

    with pd.option_context('display.max_columns', None, 'display.width', 200):
        print(all_df[['scope', 'venue', 'metric', 'bucket', 'n', 'mean', 'p10', 'p50', 'p90', 'p99']].to_string(index=False))


if __name__ == '__main__':
    main()
