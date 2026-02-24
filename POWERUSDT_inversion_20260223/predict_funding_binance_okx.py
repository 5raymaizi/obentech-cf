#!/usr/bin/env python3
"""
Predict next-settlement funding rate time series for Binance and OKX.

Default input example (ETH):
- Binance: /Volumes/T7/Obentech/fundingRateData/binance/ETHUSDT.csv
- OKX:     /Volumes/T7/Obentech/fundingRateData/okx/ETH-USDT-SWAP.csv

Formulas used (approximation with available local columns):
- Binance: F = P + clamp(I - P, +/- clamp_abs), optional interval scaling by N/8
           where P is progressive weighted premium proxy from Mark/Index - 1.
- OKX:     F = P + clamp(I - P, +/- clamp_abs), optional cap/floor around I.
           where P uses OKX csv Premium column.

Notes:
- Local files do not include all venue dynamic parameters (e.g., per-symbol dynamic cap/floor updates),
  so this reconstructs a practical formula proxy.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, Optional

import numpy as np
import pandas as pd


def _to_ts_utc(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True, errors='coerce')


def _to_ms(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors='coerce').astype('Int64')


def _ms_to_ts_utc(series: pd.Series) -> pd.Series:
    return pd.to_datetime(pd.to_numeric(series, errors='coerce'), unit='ms', utc=True, errors='coerce')


def _infer_interval_hours(next_funding_ms: pd.Series) -> tuple[float, Dict[int, float]]:
    vals = pd.to_numeric(next_funding_ms, errors='coerce').dropna().astype('int64')
    vals = vals[vals > 0]
    uniq = np.sort(vals.unique())
    if len(uniq) < 2:
        return 8.0, {}

    diffs_h = (pd.Series(uniq).diff().dropna() / 3_600_000).astype(float)
    baseline_h = float(diffs_h.round(6).mode().iloc[0])

    by_next: Dict[int, float] = {}
    for i in range(1, len(uniq)):
        by_next[int(uniq[i])] = float((uniq[i] - uniq[i - 1]) / 3_600_000)

    return baseline_h, by_next


def _attach_interval_hours(df: pd.DataFrame, next_col: str) -> pd.DataFrame:
    out = df.copy()
    base_h, by_next = _infer_interval_hours(out[next_col])
    ms = _to_ms(out[next_col])
    out['funding_interval_h'] = ms.map(by_next).astype(float).fillna(base_h)
    out.loc[(out['funding_interval_h'] <= 0) | (out['funding_interval_h'] > 24), 'funding_interval_h'] = base_h
    return out


def _progressive_weighted_avg(df: pd.DataFrame, group_col: str, value_col: str) -> pd.Series:
    tmp = df[[group_col, value_col]].copy()
    tmp['v'] = pd.to_numeric(tmp[value_col], errors='coerce')
    tmp['k'] = tmp.groupby(group_col).cumcount() + 1
    tmp['w'] = tmp['k'].astype(float)
    tmp['wv'] = tmp['w'] * tmp['v']
    return tmp.groupby(group_col)['wv'].cumsum() / tmp.groupby(group_col)['w'].cumsum()


def _interest_per_interval(interval_h: pd.Series, daily_interest_diff: float) -> pd.Series:
    return daily_interest_diff * interval_h / 24.0


def _clamp(s: pd.Series, lo: float, hi: float) -> pd.Series:
    return s.clip(lower=lo, upper=hi)


def predict_binance(
    csv_path: Path,
    daily_interest_diff: float,
    clamp_abs: float,
    use_interval_scaling: bool,
) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    required = ['Time', 'FundingRate', 'MarkPrice', 'IndexPrice', 'NextFundingTime']
    miss = [c for c in required if c not in df.columns]
    if miss:
        raise ValueError(f'Binance missing columns: {miss}')

    out = df.copy()
    out['ts_utc'] = _to_ts_utc(out['Time'])
    out['next_funding_ms'] = _to_ms(out['NextFundingTime'])
    out = out[out['ts_utc'].notna() & out['next_funding_ms'].notna()]
    out = out[out['next_funding_ms'] > 0].copy()
    out = _attach_interval_hours(out, 'next_funding_ms')
    out['next_funding_utc'] = _ms_to_ts_utc(out['next_funding_ms'])

    out = out.sort_values(['next_funding_ms', 'ts_utc']).reset_index(drop=True)

    out['premium_proxy'] = pd.to_numeric(out['MarkPrice'], errors='coerce') / pd.to_numeric(out['IndexPrice'], errors='coerce') - 1.0
    out['premium_weighted'] = _progressive_weighted_avg(out, 'next_funding_ms', 'premium_proxy')
    out['interest_interval'] = _interest_per_interval(out['funding_interval_h'], daily_interest_diff)

    base = out['premium_weighted'] + _clamp(out['interest_interval'] - out['premium_weighted'], -clamp_abs, clamp_abs)
    if use_interval_scaling:
        out['predicted_funding_rate'] = base * (out['funding_interval_h'] / 8.0)
        out['formula_note'] = 'binance_with_interval_scaling_N_over_8'
    else:
        out['predicted_funding_rate'] = base
        out['formula_note'] = 'binance_no_interval_scaling'

    out['reported_funding_rate'] = pd.to_numeric(out['FundingRate'], errors='coerce')
    out['abs_error'] = (out['predicted_funding_rate'] - out['reported_funding_rate']).abs()
    out['venue'] = 'binance'
    out['ts_bj'] = out['ts_utc'].dt.tz_convert('Asia/Shanghai')
    out['next_funding_bj'] = out['next_funding_utc'].dt.tz_convert('Asia/Shanghai')

    return out[
        [
            'venue', 'Time', 'ts_utc', 'ts_bj',
            'next_funding_ms', 'next_funding_utc', 'next_funding_bj',
            'funding_interval_h',
            'premium_proxy', 'premium_weighted', 'interest_interval',
            'reported_funding_rate', 'predicted_funding_rate', 'abs_error',
            'formula_note',
        ]
    ].copy()


def predict_okx(
    csv_path: Path,
    daily_interest_diff: float,
    clamp_abs: float,
    capfloor_limit: Optional[float],
) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    required = ['Time', 'FundingRate', 'NextFundingTime', 'Premium']
    miss = [c for c in required if c not in df.columns]
    if miss:
        raise ValueError(f'OKX missing columns: {miss}')

    out = df.copy()
    out['ts_utc'] = _to_ts_utc(out['Time'])
    out['next_funding_ms'] = _to_ms(out['NextFundingTime'])
    # fallback to FundingTime when needed
    if 'FundingTime' in out.columns:
        ft = _to_ms(out['FundingTime'])
        use_mask = out['next_funding_ms'].isna() | (out['next_funding_ms'] <= 0)
        out.loc[use_mask, 'next_funding_ms'] = ft[use_mask]

    out = out[out['ts_utc'].notna() & out['next_funding_ms'].notna()]
    out = out[out['next_funding_ms'] > 0].copy()
    out = _attach_interval_hours(out, 'next_funding_ms')
    out['next_funding_utc'] = _ms_to_ts_utc(out['next_funding_ms'])

    out = out.sort_values(['next_funding_ms', 'ts_utc']).reset_index(drop=True)

    out['premium_proxy'] = pd.to_numeric(out['Premium'], errors='coerce')
    out['premium_weighted'] = _progressive_weighted_avg(out, 'next_funding_ms', 'premium_proxy')
    out['interest_interval'] = _interest_per_interval(out['funding_interval_h'], daily_interest_diff)

    pred = out['premium_weighted'] + _clamp(out['interest_interval'] - out['premium_weighted'], -clamp_abs, clamp_abs)
    if capfloor_limit is not None:
        lo = out['interest_interval'] - float(capfloor_limit)
        hi = out['interest_interval'] + float(capfloor_limit)
        pred = pred.clip(lower=lo, upper=hi)
        note = f'okx_with_capfloor_L={capfloor_limit}'
    else:
        note = 'okx_no_capfloor'

    out['predicted_funding_rate'] = pred
    out['reported_funding_rate'] = pd.to_numeric(out['FundingRate'], errors='coerce')
    out['abs_error'] = (out['predicted_funding_rate'] - out['reported_funding_rate']).abs()
    out['formula_note'] = note
    out['venue'] = 'okx'
    out['ts_bj'] = out['ts_utc'].dt.tz_convert('Asia/Shanghai')
    out['next_funding_bj'] = out['next_funding_utc'].dt.tz_convert('Asia/Shanghai')

    return out[
        [
            'venue', 'Time', 'ts_utc', 'ts_bj',
            'next_funding_ms', 'next_funding_utc', 'next_funding_bj',
            'funding_interval_h',
            'premium_proxy', 'premium_weighted', 'interest_interval',
            'reported_funding_rate', 'predicted_funding_rate', 'abs_error',
            'formula_note',
        ]
    ].copy()


def merge_pair(binance_df: pd.DataFrame, okx_df: pd.DataFrame) -> pd.DataFrame:
    b = binance_df[['ts_utc', 'next_funding_utc', 'predicted_funding_rate', 'reported_funding_rate']].rename(
        columns={
            'next_funding_utc': 'next_funding_utc_bn',
            'predicted_funding_rate': 'predicted_funding_bn',
            'reported_funding_rate': 'reported_funding_bn',
        }
    ).sort_values('ts_utc')

    o = okx_df[['ts_utc', 'next_funding_utc', 'predicted_funding_rate', 'reported_funding_rate']].rename(
        columns={
            'next_funding_utc': 'next_funding_utc_okx',
            'predicted_funding_rate': 'predicted_funding_okx',
            'reported_funding_rate': 'reported_funding_okx',
        }
    ).sort_values('ts_utc')

    m = pd.merge_asof(b, o, on='ts_utc', direction='nearest', tolerance=pd.Timedelta('90s')).dropna().copy()
    m['predicted_diff_okx_minus_bn'] = m['predicted_funding_okx'] - m['predicted_funding_bn']
    m['reported_diff_okx_minus_bn'] = m['reported_funding_okx'] - m['reported_funding_bn']
    m['ts_bj'] = m['ts_utc'].dt.tz_convert('Asia/Shanghai')
    return m


def _summary(df: pd.DataFrame, label: str) -> str:
    mae = df['abs_error'].mean()
    med = df['abs_error'].median()
    p95 = df['abs_error'].quantile(0.95)
    mode_h = df['funding_interval_h'].round(6).mode().iloc[0] if len(df) else np.nan
    return f'[{label}] rows={len(df)} interval_mode_h={mode_h} mae={mae:.8f} med={med:.8f} p95={p95:.8f}'


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description='Predict next-settlement funding for Binance and OKX')
    p.add_argument('--symbol', default='ETH', help='Base symbol, e.g., ETH')
    p.add_argument('--binance-file', default=None)
    p.add_argument('--okx-file', default=None)
    p.add_argument('--output-dir', default=str(Path(__file__).resolve().parent / 'funding_prediction_output_bn_okx'))
    p.add_argument('--daily-interest-diff', type=float, default=0.0003, help='0.03%% = 0.0003')
    p.add_argument('--clamp-abs', type=float, default=0.0005, help='0.05%% = 0.0005')
    p.add_argument('--disable-binance-interval-scaling', action='store_true')
    p.add_argument('--okx-capfloor-limit', type=float, default=None, help='optional cap/floor half-band around interest')
    return p.parse_args()


def main() -> None:
    args = parse_args()
    sym = args.symbol.upper()

    bn_file = Path(args.binance_file) if args.binance_file else Path(f'/Volumes/T7/Obentech/fundingRateData/binance/{sym}USDT.csv')
    okx_file = Path(args.okx_file) if args.okx_file else Path(f'/Volumes/T7/Obentech/fundingRateData/okx/{sym}-USDT-SWAP.csv')

    if not bn_file.exists():
        raise FileNotFoundError(f'Binance file missing: {bn_file}')
    if not okx_file.exists():
        raise FileNotFoundError(f'OKX file missing: {okx_file}')

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    bn_pred = predict_binance(
        bn_file,
        daily_interest_diff=args.daily_interest_diff,
        clamp_abs=args.clamp_abs,
        use_interval_scaling=not args.disable_binance_interval_scaling,
    )
    okx_pred = predict_okx(
        okx_file,
        daily_interest_diff=args.daily_interest_diff,
        clamp_abs=args.clamp_abs,
        capfloor_limit=args.okx_capfloor_limit,
    )

    merged = merge_pair(bn_pred, okx_pred)

    bn_out = out_dir / f'binance_{sym}USDT_predicted_funding_timeseries.csv'
    ok_out = out_dir / f'okx_{sym}-USDT-SWAP_predicted_funding_timeseries.csv'
    m_out = out_dir / f'bn_okx_{sym}_predicted_funding_merged.csv'

    bn_pred.to_csv(bn_out, index=False)
    okx_pred.to_csv(ok_out, index=False)
    merged.to_csv(m_out, index=False)

    print('[OK] wrote', bn_out)
    print('[OK] wrote', ok_out)
    print('[OK] wrote', m_out)
    print(_summary(bn_pred, 'BINANCE'))
    print(_summary(okx_pred, 'OKX'))
    print(f'[MERGED] rows={len(merged)} predicted_diff_med={merged["predicted_diff_okx_minus_bn"].median():.8f}')
    print('')
    print('[BINANCE tail]')
    print(bn_pred[['ts_bj','next_funding_bj','reported_funding_rate','predicted_funding_rate','formula_note']].tail(5).to_string(index=False))
    print('')
    print('[OKX tail]')
    print(okx_pred[['ts_bj','next_funding_bj','reported_funding_rate','predicted_funding_rate','formula_note']].tail(5).to_string(index=False))


if __name__ == '__main__':
    main()
