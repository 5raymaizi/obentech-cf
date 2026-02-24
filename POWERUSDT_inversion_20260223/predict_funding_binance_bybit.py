#!/usr/bin/env python3
"""
Generate per-timestamp predicted funding rate for next settlement time on Binance and Bybit.

Data assumptions:
- Binance csv has: Time, FundingRate, MarkPrice, IndexPrice, NextFundingTime
- Bybit  csv has: Time, FundingRate, FundingTime, NextFundingTime

Formula references (official):
- Binance: F = P + clamp(I - P, +/-0.05%), with optional interval scaling when interval != 8h.
- Bybit:   F = P + clamp(I - P, +/-0.05%), P is weighted premium index over current interval.

Important limitation:
- Bybit local funding csv usually does NOT include Mark/Index/Premium columns.
  In that case this script outputs exchange-reported FundingRate as predicted value
  and marks method='exchange_reported_funding_rate'.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, Optional

import numpy as np
import pandas as pd


def _to_ts_utc(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True, errors='coerce')


def _ms_to_ts_utc(series: pd.Series) -> pd.Series:
    return pd.to_datetime(pd.to_numeric(series, errors='coerce'), unit='ms', utc=True, errors='coerce')


def _infer_interval_hours(next_funding_ms: pd.Series) -> tuple[float, Dict[int, float]]:
    vals = pd.to_numeric(next_funding_ms, errors='coerce').dropna().astype('int64')
    vals = vals[vals > 0]
    uniq = np.sort(vals.unique())
    if len(uniq) < 2:
        return 8.0, {}

    diffs_ms = pd.Series(uniq).diff().dropna()
    diffs_h = (diffs_ms / 3_600_000).astype(float)
    # Use rounded mode as baseline interval.
    baseline = float(diffs_h.round(6).mode().iloc[0])

    interval_map: Dict[int, float] = {}
    for i in range(1, len(uniq)):
        interval_map[int(uniq[i])] = float((uniq[i] - uniq[i - 1]) / 3_600_000)

    # Fill invalid values with baseline later.
    return baseline, interval_map


def _attach_interval_hours(df: pd.DataFrame, next_col: str) -> pd.DataFrame:
    out = df.copy()
    baseline, imap = _infer_interval_hours(out[next_col])
    ms = pd.to_numeric(out[next_col], errors='coerce').astype('Int64')
    out['funding_interval_h'] = ms.map(imap)
    out['funding_interval_h'] = out['funding_interval_h'].astype(float).fillna(baseline)
    # Guard against pathological values.
    out.loc[(out['funding_interval_h'] <= 0) | (out['funding_interval_h'] > 24), 'funding_interval_h'] = baseline
    return out


def _progressive_weighted_avg(df: pd.DataFrame, group_col: str, value_col: str) -> pd.Series:
    tmp = df[[group_col, value_col]].copy()
    tmp['sample_no'] = tmp.groupby(group_col).cumcount() + 1
    tmp['w'] = tmp['sample_no'].astype(float)
    tmp['wv'] = tmp['w'] * tmp[value_col].astype(float)
    tmp['cum_wv'] = tmp.groupby(group_col)['wv'].cumsum()
    tmp['cum_w'] = tmp.groupby(group_col)['w'].cumsum()
    return tmp['cum_wv'] / tmp['cum_w']


def _interest_rate_per_interval(interval_h: pd.Series, daily_interest_diff: float = 0.0003) -> pd.Series:
    # daily_interest_diff default: 0.03% = 0.0003
    # per interval interest = daily_interest_diff * interval_h / 24
    return daily_interest_diff * interval_h / 24.0


def _clamp(series: pd.Series, lo: float, hi: float) -> pd.Series:
    return series.clip(lower=lo, upper=hi)


def build_binance_prediction(
    csv_path: Path,
    apply_binance_interval_scaling: bool = True,
    daily_interest_diff: float = 0.0003,
    clamp_abs: float = 0.0005,
) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    required = ['Time', 'FundingRate', 'MarkPrice', 'IndexPrice', 'NextFundingTime']
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f'Binance file missing required columns: {missing}')

    out = df.copy()
    out['ts_utc'] = _to_ts_utc(out['Time'])
    out['next_funding_ms'] = pd.to_numeric(out['NextFundingTime'], errors='coerce').astype('Int64')
    out = out[out['ts_utc'].notna() & out['next_funding_ms'].notna()].copy()
    out = out[out['next_funding_ms'] > 0].copy()

    out = _attach_interval_hours(out, 'next_funding_ms')
    out['next_funding_utc'] = _ms_to_ts_utc(out['next_funding_ms'])

    out = out.sort_values(['next_funding_ms', 'ts_utc']).reset_index(drop=True)

    out['premium_proxy'] = (pd.to_numeric(out['MarkPrice'], errors='coerce') / pd.to_numeric(out['IndexPrice'], errors='coerce')) - 1.0
    out['premium_weighted'] = _progressive_weighted_avg(out, 'next_funding_ms', 'premium_proxy')

    out['interest_rate_interval'] = _interest_rate_per_interval(out['funding_interval_h'], daily_interest_diff=daily_interest_diff)

    base_formula = out['premium_weighted'] + _clamp(out['interest_rate_interval'] - out['premium_weighted'], -clamp_abs, clamp_abs)

    if apply_binance_interval_scaling:
        # Binance FAQ mentions interval-adjustment when interval != 8h:
        # F = [P + clamp(I - P, ...)] / (8 / N) = base * N / 8
        out['predicted_funding_formula'] = base_formula * (out['funding_interval_h'] / 8.0)
        out['formula_note'] = 'binance_interval_scaled'
    else:
        out['predicted_funding_formula'] = base_formula
        out['formula_note'] = 'binance_raw'

    out['reported_funding_rate'] = pd.to_numeric(out['FundingRate'], errors='coerce')
    out['prediction_method'] = 'formula_from_mark_index'
    out['abs_error_vs_reported'] = (out['predicted_funding_formula'] - out['reported_funding_rate']).abs()

    out['ts_bj'] = out['ts_utc'].dt.tz_convert('Asia/Shanghai')
    out['next_funding_bj'] = out['next_funding_utc'].dt.tz_convert('Asia/Shanghai')

    cols = [
        'Time', 'ts_utc', 'ts_bj',
        'next_funding_ms', 'next_funding_utc', 'next_funding_bj',
        'funding_interval_h',
        'premium_proxy', 'premium_weighted', 'interest_rate_interval',
        'reported_funding_rate', 'predicted_funding_formula', 'abs_error_vs_reported',
        'prediction_method', 'formula_note',
    ]
    return out[cols].copy()


def _find_bybit_premium_columns(df: pd.DataFrame) -> Optional[str]:
    candidates = ['PremiumIndex', 'premium_index', 'Premium', 'premium']
    for c in candidates:
        if c in df.columns:
            return c
    if 'MarkPrice' in df.columns and 'IndexPrice' in df.columns:
        return '__mark_index_proxy__'
    return None


def build_bybit_prediction(
    csv_path: Path,
    daily_interest_diff: float = 0.0003,
    clamp_abs: float = 0.0005,
    cap_floor_limit: Optional[float] = None,
) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    required = ['Time', 'FundingRate', 'NextFundingTime']
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f'Bybit file missing required columns: {missing}')

    out = df.copy()
    out['ts_utc'] = _to_ts_utc(out['Time'])
    out['next_funding_ms'] = pd.to_numeric(out['NextFundingTime'], errors='coerce').fillna(0).astype('int64')

    # Fallback: some rows might have NextFundingTime == 0; use FundingTime if present.
    if 'FundingTime' in out.columns:
        ft = pd.to_numeric(out['FundingTime'], errors='coerce').fillna(0).astype('int64')
        out.loc[out['next_funding_ms'] <= 0, 'next_funding_ms'] = ft[out['next_funding_ms'] <= 0]

    out = out[out['ts_utc'].notna() & (out['next_funding_ms'] > 0)].copy()
    out = _attach_interval_hours(out, 'next_funding_ms')
    out['next_funding_utc'] = _ms_to_ts_utc(out['next_funding_ms'])
    out = out.sort_values(['next_funding_ms', 'ts_utc']).reset_index(drop=True)

    premium_col = _find_bybit_premium_columns(out)
    out['reported_funding_rate'] = pd.to_numeric(out['FundingRate'], errors='coerce')
    out['interest_rate_interval'] = _interest_rate_per_interval(out['funding_interval_h'], daily_interest_diff=daily_interest_diff)

    if premium_col is None:
        # Missing premium inputs, cannot reconstruct formula path from local file only.
        out['premium_proxy'] = np.nan
        out['premium_weighted'] = np.nan
        out['predicted_funding_formula'] = out['reported_funding_rate']
        out['prediction_method'] = 'exchange_reported_funding_rate'
        out['formula_note'] = 'bybit_formula_inputs_missing'
    else:
        if premium_col == '__mark_index_proxy__':
            out['premium_proxy'] = (pd.to_numeric(out['MarkPrice'], errors='coerce') / pd.to_numeric(out['IndexPrice'], errors='coerce')) - 1.0
            premium_note = 'mark_index_proxy'
        else:
            out['premium_proxy'] = pd.to_numeric(out[premium_col], errors='coerce')
            premium_note = premium_col

        out['premium_weighted'] = _progressive_weighted_avg(out, 'next_funding_ms', 'premium_proxy')
        pred = out['premium_weighted'] + _clamp(out['interest_rate_interval'] - out['premium_weighted'], -clamp_abs, clamp_abs)

        # Optional cap/floor approximation: [I-L, I+L]
        if cap_floor_limit is not None:
            lo = out['interest_rate_interval'] - float(cap_floor_limit)
            hi = out['interest_rate_interval'] + float(cap_floor_limit)
            pred = pred.clip(lower=lo, upper=hi)
            out['formula_note'] = f'bybit_formula_with_capfloor_L={cap_floor_limit}_{premium_note}'
        else:
            out['formula_note'] = f'bybit_formula_no_capfloor_{premium_note}'

        out['predicted_funding_formula'] = pred
        out['prediction_method'] = 'formula_from_premium'

    out['abs_error_vs_reported'] = (out['predicted_funding_formula'] - out['reported_funding_rate']).abs()
    out['ts_bj'] = out['ts_utc'].dt.tz_convert('Asia/Shanghai')
    out['next_funding_bj'] = out['next_funding_utc'].dt.tz_convert('Asia/Shanghai')

    cols = [
        'Time', 'ts_utc', 'ts_bj',
        'next_funding_ms', 'next_funding_utc', 'next_funding_bj',
        'funding_interval_h',
        'premium_proxy', 'premium_weighted', 'interest_rate_interval',
        'reported_funding_rate', 'predicted_funding_formula', 'abs_error_vs_reported',
        'prediction_method', 'formula_note',
    ]
    return out[cols].copy()


def _summary(df: pd.DataFrame, name: str) -> str:
    mae = df['abs_error_vs_reported'].mean()
    med = df['abs_error_vs_reported'].median()
    maxe = df['abs_error_vs_reported'].max()
    interval_mode = df['funding_interval_h'].round(6).mode().iloc[0] if len(df) else np.nan
    return (
        f"[{name}] rows={len(df)} interval_mode_h={interval_mode} "
        f"MAE={mae:.8f} MED={med:.8f} MAX={maxe:.8f} "
        f"method={df['prediction_method'].mode().iloc[0] if len(df) else 'NA'}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Predict next-settlement funding rate for Binance and Bybit.')
    parser.add_argument('--symbol', default='POWERUSDT', help='Symbol, e.g., POWERUSDT')
    parser.add_argument('--binance-file', default=None, help='Path to Binance funding csv')
    parser.add_argument('--bybit-file', default=None, help='Path to Bybit funding csv')
    parser.add_argument('--output-dir', default=str(Path(__file__).resolve().parent / 'funding_prediction_output'))
    parser.add_argument('--daily-interest-diff', type=float, default=0.0003, help='Default daily interest diff (0.03%% => 0.0003)')
    parser.add_argument('--clamp-abs', type=float, default=0.0005, help='Clamp absolute value (0.05%% => 0.0005)')
    parser.add_argument('--no-binance-interval-scaling', action='store_true', help='Disable Binance interval scaling factor (N/8)')
    parser.add_argument('--bybit-capfloor-limit', type=float, default=None, help='Optional L for Bybit cap/floor approximation [I-L, I+L]')
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    symbol = args.symbol.upper()
    default_bn = Path(f'/Volumes/T7/Obentech/fundingRateData/binance/{symbol}.csv')
    default_by = Path(f'/Volumes/T7/Obentech/fundingRateData/bybit/{symbol}.csv')

    bn_file = Path(args.binance_file) if args.binance_file else default_bn
    by_file = Path(args.bybit_file) if args.bybit_file else default_by

    if not bn_file.exists():
        raise FileNotFoundError(f'Binance file not found: {bn_file}')
    if not by_file.exists():
        raise FileNotFoundError(f'Bybit file not found: {by_file}')

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    bn_pred = build_binance_prediction(
        bn_file,
        apply_binance_interval_scaling=not args.no_binance_interval_scaling,
        daily_interest_diff=args.daily_interest_diff,
        clamp_abs=args.clamp_abs,
    )
    by_pred = build_bybit_prediction(
        by_file,
        daily_interest_diff=args.daily_interest_diff,
        clamp_abs=args.clamp_abs,
        cap_floor_limit=args.bybit_capfloor_limit,
    )

    bn_out = out_dir / f'binance_{symbol}_predicted_funding_timeseries.csv'
    by_out = out_dir / f'bybit_{symbol}_predicted_funding_timeseries.csv'

    bn_pred.to_csv(bn_out, index=False)
    by_pred.to_csv(by_out, index=False)

    print('[OK] wrote', bn_out)
    print('[OK] wrote', by_out)
    print(_summary(bn_pred, 'BINANCE'))
    print(_summary(by_pred, 'BYBIT'))

    # Quick sample around latest rows
    print('\n[BINANCE tail]')
    print(
        bn_pred[
            ['ts_bj', 'next_funding_bj', 'reported_funding_rate', 'predicted_funding_formula', 'prediction_method', 'formula_note']
        ]
        .tail(5)
        .to_string(index=False)
    )
    print('\n[BYBIT tail]')
    print(
        by_pred[
            ['ts_bj', 'next_funding_bj', 'reported_funding_rate', 'predicted_funding_formula', 'prediction_method', 'formula_note']
        ]
        .tail(5)
        .to_string(index=False)
    )


if __name__ == '__main__':
    main()
