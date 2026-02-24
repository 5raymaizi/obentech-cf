# POWERUSDT inversion summary (2026-02-23 22:30:00 ~ 2026-02-24 00:30:00, Asia/Shanghai)

## Core findings
- Funding direction in this window is stable: `fr_bn - fr_by > 0` (recommended leg is `LONG_BY_SHORT_BN`) in 100.00% of samples.
- Median funding edge: `68.33 bps`.
- Median directional entry basis (L1): `198.43 bps`.
- Median net edge (funding - entry basis): `-137.90 bps`.
- Inversion ratio (`entry basis > 0` in funding direction): `99.9978%`.
- Binance mark-index deviation (proxy premium) median: `-529.17 bps`.

## Before/after 23:00 (Beijing)
- Pre 30m net edge median: `-149.54 bps`.
- Post 60m net edge median: `-129.88 bps`.
- Pre 30m directional notional5 median: `160.75` USDT.
- Post 60m directional notional5 median: `155.92` USDT.

## Fast-escape triggers (30m after event)
- Trigger A (`net_edge_l1_bps < 0` for 10 consecutive seconds): `2026-02-23 15:00:09+00:00`.
- Trigger B (`entry_dir_l1_bps` rolling z-score > 3): `2026-02-23 15:17:13+00:00`.
- Trigger C (`dir_notional5` below pre-event p10): `2026-02-23 15:01:06+00:00`.

## Impact / executability
- Notional `50` USDT: coverage `89.98%`, median exec entry basis `199.41` bps, p90 `270.00` bps.
- Notional `100` USDT: coverage `65.48%`, median exec entry basis `202.20` bps, p90 `271.16` bps.
- Notional `200` USDT: coverage `38.91%`, median exec entry basis `204.58` bps, p90 `270.94` bps.
- Notional `500` USDT: coverage `4.76%`, median exec entry basis `207.03` bps, p90 `273.74` bps.

## Interpretation
- This event is dominated by microstructure and liquidity: funding edge is large, but entry basis is much larger and keeps net edge deeply negative.
- Given the tiny top5 executable notional, larger-size live trading would likely face worse fills than L1 estimates.
- With current data, this looks more like a liquidity/marking dislocation than a clean, exploitable funding-vs-basis arbitrage.
