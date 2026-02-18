#!/usr/bin/env python
# -*- coding: utf-8 -*-
# test02061823pm
import os
import io
import math
import sys
import time
import traceback
from datetime import datetime, timedelta
import json
import numpy as np
import pandas as pd
import pytz
import requests
import platform
import pickle

import matplotlib
matplotlib.use("Agg")  # 服务器上无图形界面，用 Agg 后端
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec
from matplotlib.backends.backend_pdf import PdfPages

import ccxt
from utils_Sep import *
import CONFIG as C

from typing import Dict, Any, List, Set, Optional, Tuple



# ------------------------ small helpers ------------------------

def _to_float(x) -> Optional[float]:
    try:
        return None if x is None else float(x)
    except Exception:
        return None

def _as_dict(x) -> Dict[str, Any]:
    return x if isinstance(x, dict) else {}

# ------------------------ OI helpers (non-gate) ------------------------

def _extract_oi_value_u(oi_resp: Dict[str, Any]) -> Optional[float]:
    """Prefer open interest value in USDT/USD."""
    info = _as_dict(oi_resp.get("info"))
    candidates = [
        oi_resp.get("openInterestValue"),
        oi_resp.get("openInterestUsd"),
        oi_resp.get("value"),
        info.get("openInterestValue"),
        info.get("openInterestUsd"),
        info.get("oiValue"),
        info.get("open_interest_value"),
    ]
    for v in candidates:
        fv = _to_float(v)
        if fv is not None:
            return fv
    return None

def _extract_oi_amount(oi_resp: Dict[str, Any]) -> Optional[float]:
    """Extract open interest amount (contracts or base amount; exchange dependent)."""
    info = _as_dict(oi_resp.get("info"))
    candidates = [
        oi_resp.get("openInterestAmount"),
        oi_resp.get("openInterest"),
        oi_resp.get("amount"),
        info.get("openInterestAmount"),
        info.get("openInterest"),
        info.get("open_interest"),
    ]
    for v in candidates:
        fv = _to_float(v)
        if fv is not None:
            return fv
    return None

def _get_price_usdt(ex, symbol: str, price_cache: Dict[str, float]) -> Optional[float]:
    """Prefer mark price if available; fallback to last/close."""
    if symbol in price_cache:
        return price_cache[symbol]

    try:
        t = ex.fetch_ticker(symbol)
    except Exception:
        return None

    for k in ("mark", "last", "close"):
        pv = _to_float(t.get(k))
        if pv is not None and pv > 0:
            price_cache[symbol] = pv
            return pv

    info = _as_dict(t.get("info"))
    for k in ("markPrice", "mark_price", "lastPrice", "last_price"):
        pv = _to_float(info.get(k))
        if pv is not None and pv > 0:
            price_cache[symbol] = pv
            return pv

    return None


# ------------------------ Insurance Fund helpers (non-gate) ------------------------

def _build_insurance_map_binance_usdm(ex) -> Dict[str, float]:
    """
    Binance USDT-M insurance fund:
      GET /fapi/v1/insuranceBalance
    Map each symbol in group -> USDT marginBalance.
    """
    m: Dict[str, float] = {}
    if not hasattr(ex, "fapiPublicGetInsuranceBalance"):
        return m

    resp = ex.fapiPublicGetInsuranceBalance({})
    groups = resp if isinstance(resp, list) else [resp]

    for g in groups:
        if not isinstance(g, dict):
            continue
        symbols = g.get("symbols") or []
        assets = g.get("assets") or []
        usdt_bal = None
        for a in assets:
            if not isinstance(a, dict):
                continue
            if a.get("asset") == "USDT":
                usdt_bal = _to_float(a.get("marginBalance"))
                break
        if usdt_bal is None:
            continue
        for sym in symbols:
            s = str(sym).strip()
            if s:
                m[s] = usdt_bal
    return m

def _build_insurance_map_bybit(ex, coin: str = "USDT") -> Dict[str, float]:
    """
    Bybit insurance:
      GET /v5/market/insurance?coin=USDT
    Map each symbol -> value (USD).
    """
    m: Dict[str, float] = {}
    if not hasattr(ex, "publicGetV5MarketInsurance"):
        return m

    resp = ex.publicGetV5MarketInsurance({"coin": coin})
    lst = (((resp or {}).get("result") or {}).get("list")) or []
    for item in lst:
        if not isinstance(item, dict):
            continue
        val = _to_float(item.get("value"))
        syms = (item.get("symbols") or "").strip()
        if val is None or not syms:
            continue
        for sym in syms.split(","):
            s = sym.strip()
            if s:
                m[s] = val
    return m

def _okx_inst_family_from_market(market: Dict[str, Any]) -> Optional[str]:
    info = _as_dict(market.get("info"))
    inst_family = info.get("instFamily") or market.get("instFamily")
    if inst_family:
        return str(inst_family)

    mid = market.get("id")  # e.g. BTC-USDT-SWAP
    if isinstance(mid, str):
        parts = mid.split("-")
        if len(parts) >= 3:
            return "-".join(parts[:2])
    return None

def _okx_call_insurance_fund(ex, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    OKX:
      GET /api/v5/public/insurance-fund
    """
    for name in (
        "publicGetPublicInsuranceFund",
        "publicGetApiV5PublicInsuranceFund",
        "publicGetInsuranceFund",
    ):
        if hasattr(ex, name):
            return getattr(ex, name)(params)

    return ex.request("public/insurance-fund", "public", "GET", params)

def _get_okx_insurance_total_usd(ex, inst_family: str, cache: Dict[str, float]) -> Optional[float]:
    """
    OKX insurance fund:
      GET /api/v5/public/insurance-fund?instType=SWAP&instFamily=xxx
    returns "total" (USD). Cache per instFamily.
    """
    if inst_family in cache:
        return cache[inst_family]

    resp = _okx_call_insurance_fund(ex, {"instType": "SWAP", "instFamily": inst_family})
    data = (resp or {}).get("data") or []
    if not data:
        return None

    best = None
    for item in data:
        if not isinstance(item, dict):
            continue
        v = _to_float(item.get("total"))
        if v is None:
            continue
        best = v if best is None else max(best, v)

    if best is not None:
        cache[inst_family] = best
    return best

def _build_insurance_maps(exchange_name: str, ex, settle: str = "USDT") -> Tuple[Dict[str, float], Dict[str, float]]:
    """
    Returns:
      - symbol_map: {market_id -> insurance_value}
      - okx_cache: for OKX only {instFamily -> insurance_total}
    """
    symbol_map: Dict[str, float] = {}
    okx_cache: Dict[str, float] = {}

    if exchange_name in ("binance", "binanceusdm"):
        symbol_map = _build_insurance_map_binance_usdm(ex)
    elif exchange_name == "bybit":
        symbol_map = _build_insurance_map_bybit(ex, coin=settle.upper())
    elif exchange_name == "okx":
        pass  # computed per instFamily via okx_cache
    return symbol_map, okx_cache


# ============================================================
#  Gate (REST v4) — correct OI_USDT calculation
# ============================================================

GATE_BASE_URL = "https://api.gateio.ws/api/v4"

def _gate_get_json(path: str, params: dict = None, timeout: int = 25) -> Any:
    url = f"{GATE_BASE_URL}{path}"
    r = requests.get(url, params=params or {}, timeout=timeout)
    r.raise_for_status()
    return r.json()

def gate_fetch_insurance_latest_usdt(settle: str = "usdt", limit: int = 200) -> Optional[float]:
    """
    Gate global insurance fund (NOT per symbol):
      GET /futures/{settle}/insurance -> [{t,b},...]
    Return latest b.
    """
    data = _gate_get_json(f"/futures/{settle.lower()}/insurance", params={"limit": limit})
    if not isinstance(data, list) or not data:
        return None

    best_t, best_b = None, None
    for it in data:
        if not isinstance(it, dict):
            continue
        b = _to_float(it.get("b"))
        try:
            t = int(it.get("t"))
        except Exception:
            continue
        if b is None:
            continue
        if best_t is None or t > best_t:
            best_t, best_b = t, b
    return best_b

def gate_fetch_futures_contracts_usdt(settle: str = "usdt") -> List[Dict[str, Any]]:
    data = _gate_get_json(f"/futures/{settle.lower()}/contracts")
    return data if isinstance(data, list) else []

def gate_fetch_futures_tickers_usdt(settle: str = "usdt") -> List[Dict[str, Any]]:
    data = _gate_get_json(f"/futures/{settle.lower()}/tickers")
    return data if isinstance(data, list) else []

def gate_fetch_spot_currency_pairs() -> List[Dict[str, Any]]:
    data = _gate_get_json("/spot/currency_pairs")
    return data if isinstance(data, list) else []

def _gate_extract_multiplier(contract: Dict[str, Any]) -> Optional[float]:
    """
    Multiplier field name can vary; try a few.
    Accept positive multiplier only.
    """
    for k in ("contract_multiplier", "multiplier", "quanto_multiplier"):
        v = _to_float(contract.get(k))
        if v is not None and v > 0:
            return v
    return None

def gate_build_multiplier_map_usdt(settle: str = "usdt") -> Dict[str, float]:
    """
    Map: 'BTC_USDT' -> multiplier
    """
    contracts = gate_fetch_futures_contracts_usdt(settle)
    m: Dict[str, float] = {}
    for c in contracts:
        if not isinstance(c, dict):
            continue
        name = c.get("name")  # e.g. "BTC_USDT"
        if not isinstance(name, str) or not name:
            continue
        mult = _gate_extract_multiplier(c)
        if mult is None:
            continue
        m[name] = mult
    return m

def gate_build_oi_usdt_map(settle: str = "usdt") -> Dict[str, float]:
    """
    Correct OI value in USDT:
      OI_USDT = total_size * multiplier * mark_price
      (mark_price not available -> use last)
    """
    mult_map = gate_build_multiplier_map_usdt(settle)
    tickers = gate_fetch_futures_tickers_usdt(settle)

    oi_map: Dict[str, float] = {}
    for t in tickers:
        if not isinstance(t, dict):
            continue
        cid = t.get("contract")  # e.g. "RATS_USDT"
        if not isinstance(cid, str) or not cid:
            continue

        total_size = _to_float(t.get("total_size"))
        if total_size is None or total_size <= 0:
            continue

        mult = mult_map.get(cid)
        if mult is None:
            continue

        px = _to_float(t.get("mark_price")) or _to_float(t.get("last"))
        if px is None or px <= 0:
            continue

        oi_map[cid] = total_size * mult * px

    return oi_map

def list_gate_spot_and_swap_bases_filtered_correct(
    oi_threshold_u: float,
    insurance_threshold_u: float,
    spot_quote: str = "USDT",
    settle: str = "usdt",
    require_spot_tradable: bool = True,
    debug: bool = False,
) -> List[str]:
    """
    Gate 专用（正确口径）：
    - spot: BASE_USDT & tradable（可开关）
    - swap: futures USDT contracts: BASE_USDT
    - insurance: global insurance >= threshold (Gate没有per-coin insurance)
    - OI: OI_USDT = total_size * multiplier * mark_price >= oi_threshold_u

    Return: list of bases like ['BTC','ETH',...]
    """
    settle = settle.lower()

    ins = gate_fetch_insurance_latest_usdt(settle=settle)
    if ins is None:
        raise RuntimeError("[GATE] Failed to fetch insurance fund.")
    if debug:
        print(f"[GATE][INS] latest={ins:,.2f} threshold={insurance_threshold_u:,.2f}")
    if insurance_threshold_u is not None and ins < insurance_threshold_u:
        return []

    # futures bases
    contracts = gate_fetch_futures_contracts_usdt(settle)
    fut_bases: Set[str] = set()
    for c in contracts:
        name = c.get("name")
        if not isinstance(name, str):
            continue
        parts = name.split("_")
        if len(parts) == 2 and parts[1].upper() == "USDT":
            fut_bases.add(parts[0].upper())

    # spot bases
    pairs = gate_fetch_spot_currency_pairs()
    spot_bases: Set[str] = set()
    for p in pairs:
        if not isinstance(p, dict):
            continue
        if p.get("quote") != spot_quote:
            continue
        if require_spot_tradable and p.get("trade_status") not in (None, "tradable"):
            continue
        base = p.get("base")
        if base:
            spot_bases.add(str(base).upper())

    candidates = sorted(spot_bases.intersection(fut_bases))
    if debug:
        print(f"[GATE] spot_bases={len(spot_bases)}, fut_bases={len(fut_bases)}, candidates={len(candidates)}")

    oi_map = gate_build_oi_usdt_map(settle)
    if not oi_map:
        raise RuntimeError("[GATE] OI_USDT map empty (multiplier parse failed?).")
    if debug:
        some = list(oi_map.items())[:3]
        print(f"[GATE][OI] contracts_with_oi={len(oi_map)} sample={some}")

    passed: List[str] = []
    for base in candidates:
        cid = f"{base}_USDT"
        v = oi_map.get(cid)
        if v is None:
            continue
        if v >= oi_threshold_u:
            passed.append(base)

    return passed


# ============================================================
#  MAIN FILTER FUNCTION (supports gate correctly)
# ============================================================

def list_spot_and_swap_bases_filtered(
    exchange_name: str,
    oi_threshold_u: float,
    insurance_threshold_u: float = 5_000_000,
    spot_quote: str = "USDT",
    settle: str = "USDT",
    bybit_alt_endpoint: bool = False,
    debug: bool = False,
) -> List[str]:
    """
    Output: list of bases like ['ETH','SOL','BTC'].
    """

    # alias: gate -> gateio
    if exchange_name == "gate":
        exchange_name = "gateio"

    # ✅ Gate: use REST v4 correct OI_USDT
    if exchange_name == "gateio":
        return list_gate_spot_and_swap_bases_filtered_correct(
            oi_threshold_u=oi_threshold_u,
            insurance_threshold_u=insurance_threshold_u,
            spot_quote=spot_quote,
            settle=settle.lower(),
            require_spot_tradable=True,
            debug=debug,
        )

    if not hasattr(ccxt, exchange_name):
        raise ValueError(f"Unknown exchange '{exchange_name}' (ccxt id).")

    ex = getattr(ccxt, exchange_name)({
        "enableRateLimit": True,
        "options": {"defaultType": "swap"},
    })

    # Optional Bybit alternate endpoint
    if exchange_name == "bybit" and bybit_alt_endpoint:
        def _replace(obj):
            if isinstance(obj, str):
                return obj.replace("https://api.bybit.com", "https://api.bytick.com") \
                          .replace("http://api.bybit.com", "http://api.bytick.com")
            if isinstance(obj, dict):
                return {k: _replace(v) for k, v in obj.items()}
            if isinstance(obj, list):
                return [_replace(v) for v in obj]
            return obj
        ex.urls = _replace(ex.urls)

    markets = ex.load_markets()

    # insurance maps/caches
    insurance_map, okx_ins_cache = _build_insurance_maps(exchange_name, ex, settle=settle)

    if insurance_threshold_u is not None:
        if exchange_name not in ("binance", "binanceusdm", "bybit", "okx"):
            raise RuntimeError(f"Insurance fund filter not implemented for exchange={exchange_name}")
        if exchange_name in ("binance", "binanceusdm", "bybit") and not insurance_map:
            raise RuntimeError(f"Failed to fetch insurance fund data for exchange={exchange_name}")

    # 1) spot bases (quote=USDT)
    spot_bases: Set[str] = set()
    for m in markets.values():
        if not m.get("active", True):
            continue
        if not m.get("spot", False):
            continue
        if spot_quote and m.get("quote") != spot_quote:
            continue
        base = m.get("base")
        if base:
            spot_bases.add(base)

    # 2) swap markets by base (USDT settle)
    swap_markets_by_base: Dict[str, List[Dict[str, Any]]] = {}
    for m in markets.values():
        if not m.get("active", True):
            continue
        if not m.get("contract", False):
            continue
        if not m.get("swap", False):
            continue
        if settle and m.get("settle") != settle:
            continue
        base = m.get("base")
        if base:
            swap_markets_by_base.setdefault(base, []).append(m)

    candidate_bases = sorted(spot_bases.intersection(swap_markets_by_base.keys()))

    # OI support
    has_fetch_oi = ex.has.get("fetchOpenInterest", hasattr(ex, "fetch_open_interest"))
    if not has_fetch_oi:
        raise RuntimeError(f"{exchange_name} does not support fetch_open_interest (fetchOpenInterest). Upgrade ccxt?")

    price_cache: Dict[str, float] = {}
    passed: List[str] = []

    for base in candidate_bases:
        best_oi_value_u: Optional[float] = None

        for m in swap_markets_by_base[base]:
            symbol = m.get("symbol")
            market_id = m.get("id")  # e.g. BTCUSDT / BTC-USDT-SWAP / BTC_USDT ...
            if not symbol or not market_id:
                continue

            # ---- Insurance filter ----
            if insurance_threshold_u is not None:
                if exchange_name == "okx":
                    inst_family = _okx_inst_family_from_market(m)
                    if not inst_family:
                        if debug:
                            print(f"[INS MISS] {symbol}: cannot infer instFamily")
                        continue
                    ins_total = _get_okx_insurance_total_usd(ex, inst_family, okx_ins_cache)
                    if ins_total is None or ins_total < insurance_threshold_u:
                        continue
                else:
                    ins_val = insurance_map.get(str(market_id))
                    if ins_val is None or ins_val < insurance_threshold_u:
                        continue

            # ---- OI ----
            try:
                oi_resp = ex.fetch_open_interest(symbol)
            except Exception as e:
                if debug:
                    print(f"[OI ERR] {symbol}: {type(e).__name__}: {e}")
                continue

            oi_value = _extract_oi_value_u(oi_resp)
            if oi_value is not None:
                best_oi_value_u = oi_value if best_oi_value_u is None else max(best_oi_value_u, oi_value)
                continue

            oi_amount = _extract_oi_amount(oi_resp)
            if oi_amount is None:
                if debug:
                    print(f"[OI MISS] {symbol}: no value/amount")
                continue

            price = _get_price_usdt(ex, symbol, price_cache)
            if price is None:
                if debug:
                    print(f"[PX MISS] {symbol}: cannot fetch price for OI conversion")
                continue

            contract_size = _to_float(m.get("contractSize"))
            base_amount = oi_amount * contract_size if (contract_size is not None and contract_size > 0) else oi_amount
            oi_value_calc = base_amount * price

            best_oi_value_u = oi_value_calc if best_oi_value_u is None else max(best_oi_value_u, oi_value_calc)

        if best_oi_value_u is not None and best_oi_value_u >= oi_threshold_u:
            passed.append(base)

    return passed













def get_tenant_access_token(app_id, app_secret):
    """
    使用 app_id + app_secret 获取 tenant_access_token
    """
    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal/"
    headers = {"Content-Type": "application/json; charset=utf-8"}
    data = {
        "app_id": app_id,
        "app_secret": app_secret
    }

    resp = requests.post(url, headers=headers, json=data, timeout=10)
    result = resp.json()
    print("get_tenant_access_token resp:", result)

    if result.get("code") == 0:
        token = result.get("tenant_access_token")
        expire = result.get("expire", 0)
        print(f"✅ 获取 tenant_access_token 成功，有效期 {expire} 秒")
        return token
    else:
        print("❌ 获取 tenant_access_token 失败")
        return None


def upload_file_to_lark(file_path, access_token):
    """
    上传文件到 Lark，返回 file_key
    """
    if not os.path.exists(file_path):
        print(f"❌ 文件不存在: {file_path}")
        return None

    url = "https://open.larksuite.com/open-apis/im/v1/files"

    ext = os.path.splitext(file_path)[1].lower()
    if ext == ".pdf":
        upload_file_type = "pdf"
    elif ext in (".doc", ".docx"):
        upload_file_type = "doc"
    elif ext in (".xls", ".xlsx"):
        upload_file_type = "xls"
    elif ext in (".ppt", ".pptx"):
        upload_file_type = "ppt"
    else:
        upload_file_type = "stream"

    data = {
        "file_type": upload_file_type,
        "file_name": os.path.basename(file_path)
    }

    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    try:
        with open(file_path, "rb") as f:
            files = {"file": f}
            resp = requests.post(url, headers=headers, data=data, files=files, timeout=60)
        result = resp.json()
        print("upload_file resp:", result)

        if result.get("code") == 0:
            file_key = result.get("data", {}).get("file_key")
            print(f"✅ 文件上传成功，file_key = {file_key}")
            return file_key
        else:
            print("❌ 文件上传失败")
            return None

    except Exception as e:
        print(f"❌ 上传文件时出错: {e}")
        return None


def send_file_message_to_chat(access_token, chat_id, file_key):
    """
    使用应用机器人向指定 chat_id 发送 file 消息（附件）
    """
    url = "https://open.larksuite.com/open-apis/im/v1/messages?receive_id_type=chat_id"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json; charset=utf-8"
    }

    body = {
        "receive_id": chat_id,
        "msg_type": "file",
        "content": json.dumps({
            "file_key": file_key
        })
    }

    resp = requests.post(url, headers=headers, json=body, timeout=10)
    result = resp.json()
    print("send_file_message resp:", result)

    if result.get("code") == 0:
        print("✅ 文件消息发送成功")
        return True
    else:
        print("❌ 文件消息发送失败")
        return False


def send_text_message_to_chat(access_token, chat_id, text):
    """
    使用应用机器人向指定 chat_id 发送 text 消息
    """
    url = "https://open.larksuite.com/open-apis/im/v1/messages?receive_id_type=chat_id"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json; charset=utf-8"
    }

    body = {
        "receive_id": chat_id,
        "msg_type": "text",
        "content": json.dumps({
            "text": text
        })
    }

    resp = requests.post(url, headers=headers, json=body, timeout=10)
    result = resp.json()
    print("send_text_message resp:", result)

    if result.get("code") == 0:
        print("✅ 文本消息发送成功")
        return True
    else:
        print("❌ 文本消息发送失败")
        return False


def build_single_funding_report_pdf(
    symbols, last_time, lookback_window,
    out_pdf_path,
    rows_per_page=3, cols_per_page=3,
    render_dpi=120,
    pad_inches=0.02,
    figsize=(36, 30),
    features_csv_path=None,
    mode='BN'
    ):
    """
    生成单交易所资金费率报告 PDF
    
    参数:
        symbols: list[str] Top20 资金费率的 symbols
        last_time: 结束时间
        lookback_window: 回看天数
        out_pdf_path: 输出 PDF 路径
        rows_per_page, cols_per_page: 每页放几个小图（默认 3×3）
        render_dpi: 单个小图渲染为 PNG 的分辨率
        features_csv_path: 特征数据CSV文件路径，可选
        mode: 交易所模式，如 'BN', 'OKX', 'BYBIT', 'GATE'
    """
    os.makedirs(os.path.dirname(out_pdf_path) or ".", exist_ok=True)
    thumbs = []
    success, fails = [], []

    # 1) 逐个 symbol 生成资金费率图
    for sym in symbols:
        sym_u = str(sym).upper().strip()
        print(f"[Funding] {sym_u}")
        try:
            # 调用 analyze_funding_rate_single_v1，使用 matplotlib 画图并返回 fig
            res = analyze_funding_rate_single_v1(
                sym_u, last_time, lookback_window, 
                isPlotMatplotlib=True, isPlotPlotly=False,
                features_csv_path=features_csv_path,
                mode=mode
            )
            
            # 直接使用返回的 fig
            fig = res['fig_matplotlib']
            if fig is None:
                raise ValueError("fig_matplotlib is None")
            
            # 保存为 PNG
            buf = io.BytesIO()
            fig.savefig(buf, format="png", dpi=render_dpi, bbox_inches="tight", pad_inches=pad_inches)
            plt.close(fig)
            buf.seek(0)
            thumbs.append((sym_u, buf.read()))
            success.append(sym_u)
            print(f"[OK] {sym_u}")
            
        except Exception as e:
            print(f"[FAIL] {sym_u}: {e}")
            traceback.print_exc()
            # 创建错误占位图
            err_fig = plt.figure(figsize=(10, 8))
            ax = err_fig.add_subplot(111)
            ax.axis("off")
            ax.set_title(f"{sym_u} — Funding generation failed", fontweight='bold')
            ax.text(0.5, 0.5, f"Error: {type(e).__name__}: {str(e)[:100]}", 
                   ha='center', va='center', color="crimson", fontsize=10)
            buf = io.BytesIO()
            err_fig.savefig(buf, format="png", dpi=render_dpi, bbox_inches="tight", pad_inches=pad_inches)
            plt.close(err_fig)
            buf.seek(0)
            thumbs.append((sym_u, buf.read()))
            fails.append((sym_u, repr(e)))

    # 2) 将缩略图按 rows×cols 排版写入 PDF
    per_page = rows_per_page * cols_per_page
    num_pages = math.ceil(len(thumbs) / per_page)

    with PdfPages(out_pdf_path) as pdf:
        for p in range(num_pages):
            fig_page, axes = plt.subplots(rows_per_page, cols_per_page, figsize=figsize)
            
            # axes 统一为二维可迭代
            if rows_per_page == 1 and cols_per_page == 1:
                axes = [[axes]]
            elif rows_per_page == 1:
                axes = [axes]
            elif cols_per_page == 1:
                axes = [[ax] for ax in axes]

            # 当前页需要放的区间
            start = p * per_page
            end = min((p + 1) * per_page, len(thumbs))

            # 填充每个格子
            for i in range(rows_per_page):
                for j in range(cols_per_page):
                    ax = axes[i][j]
                    idx = start + i * cols_per_page + j
                    ax.axis("off")
                    if idx < end:
                        sym_u, png_bytes = thumbs[idx]
                        img = plt.imread(io.BytesIO(png_bytes), format='png')
                        ax.imshow(img)
                        ax.set_title(sym_u, fontsize=10, pad=4)
                    else:
                        ax.set_visible(False)

            pdf.savefig(fig_page, dpi=render_dpi)
            plt.close(fig_page)

    return out_pdf_path, success, fails


def run_single_exchange_job(exchange_name, OI_THRESHOLD=2_000_000, INSURANCE_THRESHOLD = 5000000):
    """
    运行单交易所资金费率分析任务
    
    参数:
        exchange_name: 交易所名称，如 'binance', 'okx', 'bybit', 'gate'
        OI_THRESHOLD: 持仓量阈值，默认为 2000000
    """
    # ============ 开始计时 ============
    job_start_time = time.time()
    timing_stats = {}
    
    # 标准化交易所名称
    exchange_name = exchange_name.lower()
    
    # 映射到模式名称
    exchange_to_mode = {
        'binance': 'BN',
        'okx': 'OKX',
        'bybit': 'BYBIT',
        'gate': 'GATE'
    }
    
    if exchange_name not in exchange_to_mode:
        raise ValueError(f"不支持的交易所: {exchange_name}。支持的交易所: {list(exchange_to_mode.keys())}")
    
    mode = exchange_to_mode[exchange_name]
    display_name = exchange_name.upper()
    
    # 设置报告目录
    report_base_dir = C.REPORT_BASE_DIR
    
    print(f"[INFO] ========== 开始运行 {display_name} 单交易所资金费率分析 ==========")
    print(f"[INFO] 交易所: {exchange_name}")
    print(f"[INFO] 模式: {mode}")
    print(f"[INFO] OI阈值: {OI_THRESHOLD:,}")
    print(f"[INFO] Beijing now: {C.now_bj}, dcdl_date={C.dcdl_date_str}")

    # ---------- 1. 获取交易所支持的币种列表 ----------
    step1_start = time.time()
    print(f"[INFO] 正在获取 {display_name} 支持的永续合约币种...")
    try:
        #symbol_list = list_perp_bases(exchange_name)
        # Gate.io 在 ccxt 中的标识符是 'gateio'
        api_exchange_name = 'gateio' if exchange_name == 'gate' else exchange_name
        symbol_list = list_spot_and_swap_bases_filtered(api_exchange_name, oi_threshold_u=OI_THRESHOLD, insurance_threshold_u = INSURANCE_THRESHOLD)
        print(f"[INFO] 获取到 {len(symbol_list)} 个币种")
        print(f"[INFO] 前20个币种: {symbol_list[:20]}")
    except Exception as e:
        print(f"[ERROR] 获取币种列表失败: {e}")
        traceback.print_exc()
        return
    
    timing_stats['1. 获取币种列表'] = time.time() - step1_start

    # ---------- 2. 计算资金费率 ----------
    step2_start = time.time()
    last_time_fr_bj = C.et_bj
    last_time_fr = pd.to_datetime(
        last_time_fr_bj.replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
    )
    lookback_window_fr = 7  # 回看7天

    funding_results = []
    funding_symbol_times = []
    
    print(f"\n[INFO] ========== 开始计算资金费率 (共{len(symbol_list)}个币对) ==========")
    for idx, symbol in enumerate(symbol_list, 1):
        symbol_start = time.time()
        try:
            res = analyze_funding_rate_single_v1(
                symbol, last_time_fr, lookback_window_fr, 
                isPlotMatplotlib=False, isPlotPlotly=False,
                mode=mode
            )
            symbol_time = time.time() - symbol_start
            funding_symbol_times.append(symbol_time)
            
            funding_results.append({
                'symbol': res['symbol'],
                'exchange': res['exchange'],
                'latest_cumulative': res['latest_cumulative'],
                'earn': res['earn'],
                'earn_1day': res['earn_1day'],
                'earn_mean': res['earn_mean'],
                'mode_binary_prop': res['mode_binary_prop'],
                'mode_sign_binary': res['mode_sign_binary'],
                'do_indicator': res['do_indicator'],
                'funding_interval': res['funding_interval'],
                'error': res.get('error', None)
            })
            
            if idx % 50 == 0:  # 每50个币种打印一次进度
                print(f"[FUNDING][{idx}/{len(symbol_list)}] 已完成 {idx} 个币种，平均耗时: {sum(funding_symbol_times)/len(funding_symbol_times):.2f}秒/个")
                
        except Exception as e:
            symbol_time = time.time() - symbol_start
            funding_symbol_times.append(symbol_time)
            print(f"[FUNDING][FAIL][{idx}/{len(symbol_list)}] {symbol}: {e}")
            continue

    step2_calc_time = time.time() - step2_start
    
    # 统计资金费率计算的时间
    if funding_symbol_times:
        avg_time_per_symbol = sum(funding_symbol_times) / len(funding_symbol_times)
        max_time_symbol = max(funding_symbol_times)
        min_time_symbol = min(funding_symbol_times)
        print(f"\n[TIMING] ========== 资金费率计算时间统计 ==========")
        print(f"[TIMING] 总耗时: {step2_calc_time:.2f}秒")
        print(f"[TIMING] 处理币对数量: {len(symbol_list)}个")
        print(f"[TIMING] 成功: {len(funding_results)}个, 失败: {len(symbol_list) - len(funding_results)}个")
        print(f"[TIMING] 平均每个币对耗时: {avg_time_per_symbol:.2f}秒")
        print(f"[TIMING] 最快: {min_time_symbol:.2f}秒, 最慢: {max_time_symbol:.2f}秒")
        print(f"[TIMING] ===============================================\n")
        
        timing_stats['2. 资金费率计算-总耗时'] = step2_calc_time
        timing_stats['2.1 资金费率-币对数量'] = len(symbol_list)
        timing_stats['2.2 资金费率-平均每个币对耗时'] = avg_time_per_symbol
        timing_stats['2.3 资金费率-最快耗时'] = min_time_symbol
        timing_stats['2.4 资金费率-最慢耗时'] = max_time_symbol

    # ---------- 3. 生成报告 ----------
    step3_start = time.time()
    results_df = pd.DataFrame(funding_results)
    funding_pdf_path = None
    funding_success_count = 0
    funding_fail_count = 0
    top20_symbols = []
    
    if not results_df.empty:
        # 过滤掉有错误的记录
        results_df_valid = results_df[results_df['error'].isna()].copy()
        
        # 按 earn_mean 绝对值排序
        # results_df_valid["abs_earn_mean"] = results_df_valid["earn_mean"].abs()
        
        # 获取 Top40
        top40_df = results_df_valid.sort_values(
            by="earn_mean", ascending=False
        ).head(40)

        # 保存 CSV
        csv_name = f"funding_rate_top40_{display_name}_{C.dcdl_date_str}.csv"
        csv_path = os.path.join(report_base_dir, csv_name)
        top40_df.to_csv(csv_path, index=False)
        print(f"[INFO] 资金费率 Top40 CSV 写入: {csv_path}")
        
        # ---------- 3.1 生成 Top20 资金费率图表 PDF ----------
        step3_1_start = time.time()
        print("\n[INFO] ========== 开始生成 Top30 资金费率图表 ==========")
        top20_symbols = top40_df.head(30)['symbol'].tolist()
        
        if top20_symbols:
            funding_pdf_name = f"Daily_Funding_Rate_Top20_{display_name}_{C.dcdl_date_str}_{C.et_bj.strftime('%Y%m%d_%H%M')}.pdf"
            funding_pdf_path = os.path.join(report_base_dir, funding_pdf_name)
            
            print(f"[INFO] 生成资金费率 PDF: {funding_pdf_path}")
            print(f"[INFO] Top30 symbols: {top20_symbols}")
            
            try:
                # 尝试读取特征文件
                data_dir = os.path.join(C.DATA_BASE_DIR, C.dcdl_date_str, f"{C.dcdl_date_str}04") if C.IS_SERVER else C.DATA_BASE_DIR
                features_csv_path = os.path.join(data_dir, f"{C.dcdl_date_str}04_scored_features_swap.csv")
                
                if not os.path.exists(features_csv_path):
                    print(f"[WARN] ⚠️  特征文件不存在: {features_csv_path}")
                    features_csv_path = None
                
                out_pdf_funding, ok_funding, fail_funding = build_single_funding_report_pdf(
                    top20_symbols,
                    last_time_fr,
                    lookback_window_fr,
                    funding_pdf_path,
                    rows_per_page=3,
                    cols_per_page=3,
                    render_dpi=120,
                    figsize=(36, 30),
                    features_csv_path=features_csv_path,
                    mode=mode
                )
                funding_success_count = len(ok_funding)
                funding_fail_count = len(fail_funding)
                step3_1_time = time.time() - step3_1_start
                timing_stats['3. Top20图表生成'] = step3_1_time
                print(f"[INFO] 资金费率报告完成: 成功 {funding_success_count}, 失败 {funding_fail_count}")
                print(f"[TIMING] Top20图表生成耗时: {step3_1_time:.2f}秒")
            except Exception as e:
                print(f"[ERROR] 生成资金费率报告时出错: {e}")
                traceback.print_exc()
                step3_1_time = time.time() - step3_1_start
                timing_stats['3. Top20图表生成'] = step3_1_time

        # 转成文本（给 Lark 用）
        cols_show = [
            "symbol",
            "earn_mean",
            "earn_1day",
            "earn",
            "mode_binary_prop",
            "mode_sign_binary",
            "do_indicator",
            "funding_interval"
        ]
        table_txt = top40_df[cols_show].to_string(
            index=False,
            float_format=lambda x: f"{x:.6f}"
        )
    else:
        table_txt = ""
        csv_path = ""
        print("[WARN] 资金费率结果为空，可能数据有问题。")
    
    step3_total_time = time.time() - step3_start
    timing_stats['3. 报告生成-总耗时'] = step3_total_time

    # ---------- 4. 发送 Lark 消息 ----------
    step4_start = time.time()
    
    token = get_tenant_access_token(C.APP_ID, C.APP_SECRET)
    if not token:
        print("[LARK][FAIL] 获取 tenant_access_token 失败，跳过推送。")
        step4_time = time.time() - step4_start
        timing_stats['4. Lark消息推送'] = step4_time
        
        # 输出总体时间统计
        print_timing_summary(timing_stats, time.time() - job_start_time)
        return
    
    # 4.1 发送资金费率 Top20 报告 PDF
    if funding_pdf_path and os.path.exists(funding_pdf_path):
        print(f"[INFO] 上传资金费率 Top20 报告 PDF: {funding_pdf_path}")
        file_key_funding = upload_file_to_lark(funding_pdf_path, token)
        if file_key_funding:
            send_file_message_to_chat(token, C.LARK_CHAT_ID, file_key_funding)
        else:
            print("[LARK][WARN] 资金费率报告 PDF 上传失败，无法发送附件。")
    
    # 4.2 发送文本消息
    msg_lines = []
    msg_lines.append(f"[{display_name} 单交易所资金费率日报] 日期: {C.dcdl_date_str} (北京时间)")
    msg_lines.append(f"- 交易所: {exchange_name.upper()}")
    msg_lines.append(f"- 分析币种数量: {len(symbol_list)}")
    msg_lines.append(f"- OI阈值: {OI_THRESHOLD:,} USDT")
    msg_lines.append(f"- Insurance阈值: {INSURANCE_THRESHOLD:,} USDT")
    
    if funding_pdf_path:
        if top20_symbols:
            top20_str = ", ".join(top20_symbols[:10])
            if len(top20_symbols) > 10:
                top20_str += f" ... (共{len(top20_symbols)}个)"
            msg_lines.append(f"- Top20 symbols: {top20_str}")

    if table_txt:
        msg_lines.append("")
        msg_lines.append(
            f"{display_name} 资金费率 Top40 表格 截止 {last_time_fr_bj.strftime('%Y-%m-%d %H:%M')} (lookback={lookback_window_fr}d)"
        )
        msg_lines.append(table_txt)


    full_msg = "\n".join(msg_lines)
    send_text_message_to_chat(token, C.LARK_CHAT_ID, full_msg)
    
    step4_time = time.time() - step4_start
    timing_stats['4. Lark消息推送'] = step4_time
    print(f"[TIMING] Lark消息推送耗时: {step4_time:.2f}秒")
    
    # ============ 输出总体时间统计 ============
    total_time = time.time() - job_start_time
    print_timing_summary(timing_stats, total_time)


def print_timing_summary(timing_stats, total_time):
    """
    打印时间统计摘要
    """
    print("\n" + "=" * 80)
    print("⏱️  运行时间统计摘要")
    print("=" * 80)
    
    for key in sorted(timing_stats.keys()):
        value = timing_stats[key]
        if isinstance(value, (int, float)):
            if '平均' in key or '最快' in key or '最慢' in key or '数量' in key:
                if '数量' in key:
                    print(f"  {key}: {value}")
                else:
                    print(f"  {key}: {value:.2f}秒")
            else:
                print(f"  {key}: {value:.2f}秒 ({value/total_time*100:.1f}%)")
        else:
            print(f"  {key}: {value}")
    
    print("-" * 80)
    print(f"  📊 总运行时间: {total_time:.2f}秒 ({total_time/60:.1f}分钟)")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    import sys
    OI_THRESHOLD = 2000000
    INSURANCE_THRESHOLD = 5000000
    # 不传参数：默认全跑
    if len(sys.argv) == 1:
        for ex_name in ["binance", "okx", "bybit", "gate"]:
            print(f"\n[INFO] ===== RUN {ex_name.upper()} =====")
            run_single_exchange_job(ex_name,OI_THRESHOLD,INSURANCE_THRESHOLD)
    else:
        # 传参数：只跑指定交易所
        ex_name = sys.argv[1].lower()
        if ex_name == "gateio":
            ex_name = "gate"
        print(f"\n[INFO] ===== RUN {ex_name.upper()} =====")
        run_single_exchange_job(ex_name,OI_THRESHOLD,INSURANCE_THRESHOLD)
if __name__ == "__main__":
    symbol_list = list_spot_and_swap_bases_filtered('binance', oi_threshold_u=OI_THRESHOLD, insurance_threshold_u = INSURANCE_THRESHOLD)
    print(symbol_list)