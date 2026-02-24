#!/usr/bin/env python3
"""
POWERUSDT inversion analysis (Binance vs Bybit5) around
2026-02-23 23:00 Asia/Shanghai.

Outputs:
  - output/powerusdt_inversion_20260223_1min.csv
  - output/powerusdt_inversion_20260223_impact.csv
  - output/powerusdt_inversion_20260223_summary.md
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class Config:
    symbol: str = "POWERUSDT"
    tz: str = "Asia/Shanghai"
    event_bj: str = "2026-02-23 23:00:00"
    window_start_bj: str = "2026-02-23 22:30:00"
    window_end_bj: str = "2026-02-24 00:30:00"
    depth_merge_tolerance_ms: int = 100
    funding_merge_tolerance_s: int = 90
    out_dir: Path = Path("output")

    bn_depth_file: Path = Path(
        "/Volumes/T7/Obentech/data/binance/perp/books/POWER/powerusdt_2026-02-23_depth5.parquet"
    )
    by_depth_file: Path = Path(
        "/Volumes/T7/Obentech/data/bybit5/perp/books/POWER/powerusdt_2026-02-23_depth5.parquet"
    )
    bn_funding_file: Path = Path(
        "/Volumes/T7/Obentech/fundingRateData/binance/POWERUSDT.csv"
    )
    by_funding_file: Path = Path(
        "/Volumes/T7/Obentech/fundingRateData/bybit/POWERUSDT.csv"
    )


def _require_exists(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")


def _to_utc(ts_bj: str, tz: str) -> pd.Timestamp:
    return pd.Timestamp(ts_bj, tz=tz).tz_convert("UTC")


def _book_notional5(df: pd.DataFrame, side: str) -> pd.Series:
    # side: "bid" or "ask"
    out = pd.Series(0.0, index=df.index, dtype=float)
    for i in range(5):
        out = out + (df[f"{side}_price{i}"] * df[f"{side}_size{i}"])
    return out


def _fill_avg_px_for_qty(row: pd.Series, side: str, prefix: str, qty: float) -> float:
    """
    Compute avg fill price on top5 levels for target base quantity.
    side:
      - "buy"  -> consume asks
      - "sell" -> consume bids
    prefix: "bn" or "by"
    """
    rem = qty
    accum_quote = 0.0
    if side == "buy":
        px_key = "ask_price"
        sz_key = "ask_size"
    else:
        px_key = "bid_price"
        sz_key = "bid_size"

    for i in range(5):
        px = float(row[f"{prefix}_{px_key}{i}"])
        sz = float(row[f"{prefix}_{sz_key}{i}"])
        take = min(rem, sz)
        accum_quote += take * px
        rem -= take
        if rem <= 1e-12:
            return accum_quote / qty
    return math.nan


def load_depth(cfg: Config) -> pd.DataFrame:
    _require_exists(cfg.bn_depth_file)
    _require_exists(cfg.by_depth_file)

    bn = pd.read_parquet(cfg.bn_depth_file)
    by = pd.read_parquet(cfg.by_depth_file)

    # Use exchange event time in ms.
    for d in (bn, by):
        d["ts"] = pd.to_datetime(d["E"], unit="ms", utc=True)
        d.drop_duplicates("E", keep="last", inplace=True)

    st = _to_utc(cfg.window_start_bj, cfg.tz)
    et = _to_utc(cfg.window_end_bj, cfg.tz)

    bn = bn[(bn["ts"] >= st) & (bn["ts"] <= et)].sort_values("ts").copy()
    by = by[(by["ts"] >= st) & (by["ts"] <= et)].sort_values("ts").copy()

    keep_cols = (
        ["ts"]
        + [f"bid_price{i}" for i in range(5)]
        + [f"bid_size{i}" for i in range(5)]
        + [f"ask_price{i}" for i in range(5)]
        + [f"ask_size{i}" for i in range(5)]
    )
    bn = bn[keep_cols].rename(
        columns={c: f"bn_{c}" if c != "ts" else c for c in keep_cols}
    )
    by = by[keep_cols].rename(
        columns={c: f"by_{c}" if c != "ts" else c for c in keep_cols}
    )

    merged = pd.merge_asof(
        by.sort_values("ts"),
        bn.sort_values("ts"),
        on="ts",
        direction="backward",
        tolerance=pd.Timedelta(milliseconds=cfg.depth_merge_tolerance_ms),
    ).dropna()

    merged["by_mid"] = (merged["by_bid_price0"] + merged["by_ask_price0"]) / 2
    merged["bn_mid"] = (merged["bn_bid_price0"] + merged["bn_ask_price0"]) / 2
    merged["basis_mid_bps"] = (merged["by_mid"] / merged["bn_mid"] - 1.0) * 1e4

    merged["basis_lbs_l1_bps"] = (
        merged["by_ask_price0"] / merged["bn_bid_price0"] - 1.0
    ) * 1e4
    merged["basis_lbs_mid_bps"] = (merged["by_mid"] / merged["bn_mid"] - 1.0) * 1e4
    merged["basis_lbs_close_l1_bps"] = (
        merged["by_bid_price0"] / merged["bn_ask_price0"] - 1.0
    ) * 1e4

    merged["by_bid_notional5"] = _book_notional5(
        merged.rename(columns={f"by_{c}": c for c in [f"bid_price{i}" for i in range(5)] + [f"bid_size{i}" for i in range(5)]}),
        "bid",
    )
    merged["by_ask_notional5"] = _book_notional5(
        merged.rename(columns={f"by_{c}": c for c in [f"ask_price{i}" for i in range(5)] + [f"ask_size{i}" for i in range(5)]}),
        "ask",
    )
    merged["bn_bid_notional5"] = _book_notional5(
        merged.rename(columns={f"bn_{c}": c for c in [f"bid_price{i}" for i in range(5)] + [f"bid_size{i}" for i in range(5)]}),
        "bid",
    )
    merged["bn_ask_notional5"] = _book_notional5(
        merged.rename(columns={f"bn_{c}": c for c in [f"ask_price{i}" for i in range(5)] + [f"ask_size{i}" for i in range(5)]}),
        "ask",
    )

    return merged


def load_funding(cfg: Config) -> pd.DataFrame:
    _require_exists(cfg.bn_funding_file)
    _require_exists(cfg.by_funding_file)

    bn = pd.read_csv(cfg.bn_funding_file)
    by = pd.read_csv(cfg.by_funding_file)

    bn["ts"] = pd.to_datetime(bn["Time"], utc=True, errors="coerce")
    by["ts"] = pd.to_datetime(by["Time"], utc=True, errors="coerce")

    st = _to_utc(cfg.window_start_bj, cfg.tz) - pd.Timedelta(hours=2)
    et = _to_utc(cfg.window_end_bj, cfg.tz) + pd.Timedelta(hours=2)
    bn = bn[(bn["ts"] >= st) & (bn["ts"] <= et)].sort_values("ts").copy()
    by = by[(by["ts"] >= st) & (by["ts"] <= et)].sort_values("ts").copy()

    keep_bn = ["ts", "FundingRate", "MarkPrice", "IndexPrice", "NextFundingTime"]
    keep_by = ["ts", "FundingRate", "FundingTime", "NextFundingTime"]
    for c in keep_bn:
        if c not in bn.columns:
            bn[c] = np.nan
    for c in keep_by:
        if c not in by.columns:
            by[c] = np.nan

    f = pd.merge_asof(
        bn[keep_bn].rename(
            columns={
                "FundingRate": "fr_bn",
                "MarkPrice": "mark_bn",
                "IndexPrice": "index_bn",
                "NextFundingTime": "next_bn",
            }
        ).sort_values("ts"),
        by[keep_by].rename(
            columns={
                "FundingRate": "fr_by",
                "FundingTime": "funding_time_by",
                "NextFundingTime": "next_by",
            }
        ).sort_values("ts"),
        on="ts",
        direction="nearest",
        tolerance=pd.Timedelta(seconds=cfg.funding_merge_tolerance_s),
    )

    f = f.dropna(subset=["fr_bn", "fr_by"]).copy()
    f["funding_edge_bps"] = (f["fr_bn"] - f["fr_by"]) * 1e4
    f["bn_mark_index_bps"] = (f["mark_bn"] / f["index_bn"] - 1.0) * 1e4
    return f


def enrich_metrics(depth: pd.DataFrame, funding: pd.DataFrame) -> pd.DataFrame:
    m = pd.merge_asof(
        depth.sort_values("ts"),
        funding[
            ["ts", "fr_bn", "fr_by", "funding_edge_bps", "bn_mark_index_bps", "next_bn", "next_by"]
        ].sort_values("ts"),
        on="ts",
        direction="backward",
        tolerance=pd.Timedelta(seconds=90),
    )
    m = m.dropna(subset=["funding_edge_bps"]).copy()

    m["preferred_dir"] = np.where(
        m["funding_edge_bps"] > 0, "LONG_BY_SHORT_BN", "LONG_BN_SHORT_BY"
    )
    m["entry_dir_l1_bps"] = np.where(
        m["funding_edge_bps"] > 0,
        (m["by_ask_price0"] / m["bn_bid_price0"] - 1.0) * 1e4,
        (m["bn_ask_price0"] / m["by_bid_price0"] - 1.0) * 1e4,
    )
    m["net_edge_l1_bps"] = m["funding_edge_bps"] - m["entry_dir_l1_bps"]
    m["inversion_flag"] = m["entry_dir_l1_bps"] > 0

    # Directional executable notional (top5, quote USDT)
    m["dir_notional5"] = np.where(
        m["funding_edge_bps"] > 0,
        np.minimum(m["by_ask_notional5"], m["bn_bid_notional5"]),
        np.minimum(m["bn_ask_notional5"], m["by_bid_notional5"]),
    )
    return m


def impact_table(metrics: pd.DataFrame, notionals: Iterable[float]) -> pd.DataFrame:
    out: List[Dict[str, float]] = []
    mid_ref = float(((metrics["bn_ask_price0"] + metrics["bn_bid_price0"]) / 2).median())

    for n in notionals:
        qty = n / mid_ref
        buy_by = metrics.apply(lambda r: _fill_avg_px_for_qty(r, "buy", "by", qty), axis=1)
        sell_bn = metrics.apply(lambda r: _fill_avg_px_for_qty(r, "sell", "bn", qty), axis=1)
        buy_bn = metrics.apply(lambda r: _fill_avg_px_for_qty(r, "buy", "bn", qty), axis=1)
        sell_by = metrics.apply(lambda r: _fill_avg_px_for_qty(r, "sell", "by", qty), axis=1)

        basis_lbs_exec = (buy_by / sell_bn - 1.0) * 1e4
        basis_lbs_exec = basis_lbs_exec.replace([np.inf, -np.inf], np.nan)
        basis_lsb_exec = (buy_bn / sell_by - 1.0) * 1e4
        basis_lsb_exec = basis_lsb_exec.replace([np.inf, -np.inf], np.nan)

        # Use funding-derived direction for each row.
        dir_basis_exec = np.where(
            metrics["funding_edge_bps"] > 0, basis_lbs_exec, basis_lsb_exec
        )
        dir_basis_exec = pd.Series(dir_basis_exec, index=metrics.index).replace(
            [np.inf, -np.inf], np.nan
        )

        valid = dir_basis_exec.notna()
        out.append(
            {
                "notional_usdt": float(n),
                "qty_ref": float(qty),
                "coverage_ratio": float(valid.mean()),
                "entry_exec_basis_median_bps": float(dir_basis_exec.median()),
                "entry_exec_basis_p90_bps": float(dir_basis_exec.quantile(0.9)),
                "entry_exec_basis_p99_bps": float(dir_basis_exec.quantile(0.99)),
            }
        )

    return pd.DataFrame(out)


def _first_consecutive_negative(
    s: pd.Series, threshold: float, consecutive: int
) -> pd.Timestamp | pd.NaT:
    cond = s < threshold
    grp = (cond != cond.shift()).cumsum()
    run = cond.groupby(grp).cumcount() + 1
    hit = cond & (run >= consecutive)
    if hit.any():
        return hit.index[hit.argmax()]
    return pd.NaT


def build_summary(cfg: Config, m: pd.DataFrame, impact: pd.DataFrame) -> str:
    event_ts = _to_utc(cfg.event_bj, cfg.tz)
    pre = m[(m["ts"] >= event_ts - pd.Timedelta(minutes=30)) & (m["ts"] < event_ts)]
    post = m[(m["ts"] >= event_ts) & (m["ts"] < event_ts + pd.Timedelta(minutes=60))]

    one_sec = (
        m.set_index("ts")[
            ["funding_edge_bps", "entry_dir_l1_bps", "net_edge_l1_bps", "dir_notional5", "basis_mid_bps"]
        ]
        .resample("1s")
        .median()
        .dropna()
    )
    one_sec["entry_z"] = (
        one_sec["entry_dir_l1_bps"] - one_sec["entry_dir_l1_bps"].rolling(300, min_periods=60).mean()
    ) / one_sec["entry_dir_l1_bps"].rolling(300, min_periods=60).std()

    post_1s = one_sec[(one_sec.index >= event_ts) & (one_sec.index < event_ts + pd.Timedelta(minutes=30))]

    first_bad = _first_consecutive_negative(post_1s["net_edge_l1_bps"], threshold=0.0, consecutive=10)
    first_z3 = post_1s.index[post_1s["entry_z"] > 3.0]
    first_z3_ts = first_z3[0] if len(first_z3) > 0 else pd.NaT

    pre_p10_liq = pre["dir_notional5"].quantile(0.1) if len(pre) else np.nan
    post_liq_drop = post_1s.index[post_1s["dir_notional5"] < pre_p10_liq] if np.isfinite(pre_p10_liq) else []
    first_liq_drop_ts = post_liq_drop[0] if len(post_liq_drop) > 0 else pd.NaT

    lines = [
        f"# POWERUSDT inversion summary ({cfg.window_start_bj} ~ {cfg.window_end_bj}, {cfg.tz})",
        "",
        "## Core findings",
        f"- Funding direction in this window is stable: `fr_bn - fr_by > 0` (recommended leg is `LONG_BY_SHORT_BN`) in {m['funding_edge_bps'].gt(0).mean():.2%} of samples.",
        f"- Median funding edge: `{m['funding_edge_bps'].median():.2f} bps`.",
        f"- Median directional entry basis (L1): `{m['entry_dir_l1_bps'].median():.2f} bps`.",
        f"- Median net edge (funding - entry basis): `{m['net_edge_l1_bps'].median():.2f} bps`.",
        f"- Inversion ratio (`entry basis > 0` in funding direction): `{m['inversion_flag'].mean():.4%}`.",
        f"- Binance mark-index deviation (proxy premium) median: `{m['bn_mark_index_bps'].median():.2f} bps`.",
        "",
        "## Before/after 23:00 (Beijing)",
        f"- Pre 30m net edge median: `{pre['net_edge_l1_bps'].median():.2f} bps`.",
        f"- Post 60m net edge median: `{post['net_edge_l1_bps'].median():.2f} bps`.",
        f"- Pre 30m directional notional5 median: `{pre['dir_notional5'].median():.2f}` USDT.",
        f"- Post 60m directional notional5 median: `{post['dir_notional5'].median():.2f}` USDT.",
        "",
        "## Fast-escape triggers (30m after event)",
        f"- Trigger A (`net_edge_l1_bps < 0` for 10 consecutive seconds): `{first_bad}`.",
        f"- Trigger B (`entry_dir_l1_bps` rolling z-score > 3): `{first_z3_ts}`.",
        f"- Trigger C (`dir_notional5` below pre-event p10): `{first_liq_drop_ts}`.",
        "",
        "## Impact / executability",
    ]
    for _, r in impact.iterrows():
        lines.append(
            "- "
            f"Notional `{r['notional_usdt']:.0f}` USDT: coverage `{r['coverage_ratio']:.2%}`, "
            f"median exec entry basis `{r['entry_exec_basis_median_bps']:.2f}` bps, "
            f"p90 `{r['entry_exec_basis_p90_bps']:.2f}` bps."
        )

    lines.extend(
        [
            "",
            "## Interpretation",
            "- This event is dominated by microstructure and liquidity: funding edge is large, but entry basis is much larger and keeps net edge deeply negative.",
            "- Given the tiny top5 executable notional, larger-size live trading would likely face worse fills than L1 estimates.",
            "- With current data, this looks more like a liquidity/marking dislocation than a clean, exploitable funding-vs-basis arbitrage.",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> None:
    cfg = Config()
    cfg.out_dir.mkdir(parents=True, exist_ok=True)

    depth = load_depth(cfg)
    funding = load_funding(cfg)
    m = enrich_metrics(depth, funding)

    # 1-min panel for inspection/reporting
    panel_1m = (
        m.set_index("ts")[
            [
                "funding_edge_bps",
                "entry_dir_l1_bps",
                "net_edge_l1_bps",
                "basis_mid_bps",
                "bn_mark_index_bps",
                "dir_notional5",
            ]
        ]
        .resample("1min")
        .median()
        .dropna()
        .reset_index()
    )

    impact = impact_table(m, notionals=[50, 100, 200, 500])
    summary_md = build_summary(cfg, m, impact)

    out_1m = cfg.out_dir / "powerusdt_inversion_20260223_1min.csv"
    out_imp = cfg.out_dir / "powerusdt_inversion_20260223_impact.csv"
    out_md = cfg.out_dir / "powerusdt_inversion_20260223_summary.md"

    panel_1m.to_csv(out_1m, index=False)
    impact.to_csv(out_imp, index=False)
    out_md.write_text(summary_md, encoding="utf-8")

    print(f"[OK] wrote: {out_1m}")
    print(f"[OK] wrote: {out_imp}")
    print(f"[OK] wrote: {out_md}")
    print("")
    print(summary_md)


if __name__ == "__main__":
    main()

