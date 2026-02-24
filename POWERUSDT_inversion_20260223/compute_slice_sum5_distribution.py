#!/usr/bin/env python3
"""
Compute per-slice top5 notional sum distribution for POWERUSDT on 2026-02-23 (Asia/Shanghai).

Per-slice sum5 definition (USDT):
  sum_{i=0..4}(bid_price_i * bid_size_i + ask_price_i * ask_size_i)

Outputs:
- raw exchange-level distribution (Binance, Bybit5)
- merged distribution (Bybit slice + nearest Binance within 100ms)
"""

from __future__ import annotations

from pathlib import Path
import pandas as pd

BN_FILE = Path("/Volumes/T7/Obentech/data/binance/perp/books/POWER/powerusdt_2026-02-23_depth5.parquet")
BY_FILE = Path("/Volumes/T7/Obentech/data/bybit5/perp/books/POWER/powerusdt_2026-02-23_depth5.parquet")
DAY_BJ = pd.Timestamp("2026-02-23").date()


def calc_sum5(df: pd.DataFrame, prefix: str = "") -> pd.Series:
    bid = 0.0
    ask = 0.0
    for i in range(5):
        bid += df[f"{prefix}bid_price{i}"] * df[f"{prefix}bid_size{i}"]
        ask += df[f"{prefix}ask_price{i}"] * df[f"{prefix}ask_size{i}"]
    return bid + ask


def dist_stats(s: pd.Series) -> dict:
    q = s.quantile([0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99])
    return {
        "n": int(len(s)),
        "mean": float(s.mean()),
        "std": float(s.std()),
        "min": float(s.min()),
        "p01": float(q.loc[0.01]),
        "p05": float(q.loc[0.05]),
        "p10": float(q.loc[0.10]),
        "p25": float(q.loc[0.25]),
        "p50": float(q.loc[0.50]),
        "p75": float(q.loc[0.75]),
        "p90": float(q.loc[0.90]),
        "p95": float(q.loc[0.95]),
        "p99": float(q.loc[0.99]),
        "max": float(s.max()),
    }


def print_stats(title: str, s: pd.Series) -> None:
    r = dist_stats(s)
    print(f"\n{title}")
    keys = ["n", "mean", "std", "min", "p01", "p05", "p10", "p25", "p50", "p75", "p90", "p95", "p99", "max"]
    for k in keys:
        if k == "n":
            print(f"{k}: {r[k]}")
        else:
            print(f"{k}: {r[k]:.6f}")


def main() -> None:
    bn = pd.read_parquet(BN_FILE)
    by = pd.read_parquet(BY_FILE)

    for d in (bn, by):
        d["ts"] = pd.to_datetime(d["E"], unit="ms", utc=True)
        d["bj"] = d["ts"].dt.tz_convert("Asia/Shanghai")

    bn = bn[bn["bj"].dt.date == DAY_BJ].copy()
    by = by[by["bj"].dt.date == DAY_BJ].copy()

    bn["sum5"] = calc_sum5(bn)
    by["sum5"] = calc_sum5(by)

    print_stats("RAW_BINANCE_slice_sum5 (bid5+ask5)", bn["sum5"])
    print_stats("RAW_BYBIT5_slice_sum5 (bid5+ask5)", by["sum5"])

    keep = ["ts"] + [f"bid_price{i}" for i in range(5)] + [f"bid_size{i}" for i in range(5)] + [f"ask_price{i}" for i in range(5)] + [f"ask_size{i}" for i in range(5)]
    bnm = bn[keep].sort_values("ts").rename(columns={c: f"bn_{c}" if c != "ts" else c for c in keep})
    bym = by[keep].sort_values("ts").rename(columns={c: f"by_{c}" if c != "ts" else c for c in keep})

    merged = pd.merge_asof(
        bym,
        bnm,
        on="ts",
        direction="backward",
        tolerance=pd.Timedelta("100ms"),
    ).dropna()

    merged["bn_sum5"] = calc_sum5(merged, prefix="bn_")
    merged["by_sum5"] = calc_sum5(merged, prefix="by_")
    merged["all_sum5"] = merged["bn_sum5"] + merged["by_sum5"]

    print_stats("MERGED_BINANCE_slice_sum5", merged["bn_sum5"])
    print_stats("MERGED_BYBIT5_slice_sum5", merged["by_sum5"])
    print_stats("MERGED_ALL_slice_sum5 (bn+by)", merged["all_sum5"])


if __name__ == "__main__":
    main()
