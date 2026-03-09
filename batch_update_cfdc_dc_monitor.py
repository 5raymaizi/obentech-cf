#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
批量更新 CFDC 风控 (dc_monitor) 参数。

默认 dry-run，仅打印将要修改的内容；
加 --apply 后才真正调用 save_db_config 写入。
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Any, Dict, List, Tuple

import requests


DEFAULT_ENVS: List[str] = [
    "cfdc_mt_dc_test",
    "cfdc_mt_dc_pmpro_test",
    "cfdc_mt_dc_pmtest2",
    "cfdc_dcpmtest3",
    "cfdc_dctest4",
    "cfdc_dctest5",
    "cfdc_dctest6",
    "cfdc_dcpro1",
    "cfdc_dcpro2",
    "cfdc_dcpro3",
    "cfdc_dcpro4",
    "cfdc_dcpro5",
    "cfdc_dcpro6",
    "cfdc_dcpro7",
    "cfdc_dcpro8",
    "cfdc_dcpro9",
    "cfdc_dcpro10",
    "cfdc_dcpro11",
    "cfdc_dcpro12",
    "cfdc_dcpro13",
    "cfdc_dcpro14",
    "cfdc_dcpro15",
    "cfdc_dcpro16",
    "cfdc_dcpro17",
    "cfdc_dcpro18",
    "cfdc_dcpro19",
    "cfdc_dcpro20",
    "cfdc_dcpro21",
    "cfdc_dcpro22",
    "cfdc_dcpro23",
    "cfdc_dcpro24",
    "cfdc_dcpro25",
    "cfdc_dcpro26",
    "cfdc_dcpro27",
    "cfdc_dcpro28",
    "cfdc_dcpro29",
    "cfdc_dcpro30",
    "cfdc_dcpro31",
    "cfdc_dcpro32",
    "cfdc_dcpro33",
    "cfdc_dcpro34",
    "cfdc_dcpro35",
    "cfdc_dcpro36",
    "cfdc_dcpro37",
    "cfdc_dcpro38",
    "cfdc_dcob1",
    "cfdc_dcbbbqmonitor",
    "cfdc_dcbobqmonitor",
    "cfdc_dcbb1",
    "cfdc_dcbb2",
]


TARGET_CONFIG: Dict[str, str] = {
    # DC监控全局开关
    "dc_monitor__enabled": "1",
    # 跨所主动减仓机制
    "dc_monitor__uni_mmr_up_limit": "1200%",
    "dc_monitor__uni_mmr_lower_limit": "800%",
    "dc_monitor__auto_position_reduce_mode": "TT",
    "dc_monitor__auto_position_reduce_enabled": "1",
    # 单所单边敞口告警
    "dc_monitor__exposure_threshold": "400%",
    "dc_monitor__exposure_threshold_p1": "500%",
    # 资金费率倒挂告警
    "dc_monitor__funding_rate_threshold": "0.05%",
    "dc_monitor__funding_rate_auto_reduce_threshold": "0.4%",
    "dc_monitor__loss_ratio_threshold": "0.0001",
    "dc_monitor__pos_value_ratio_threshold": "0.005",
    # 价格指数风控
    "dc_monitor__index_diff_threshold": "1%",
}


BOOL_KEYS = {
    "dc_monitor__enabled",
    "dc_monitor__auto_position_reduce_enabled",
}


def _to_json(resp: requests.Response) -> Dict[str, Any]:
    try:
        return resp.json()
    except Exception:
        return {"raw_text": resp.text, "status_code": resp.status_code}


def _normalize_value(key: str, value: Any) -> str:
    if value is None:
        return ""
    if key in BOOL_KEYS:
        if value is True:
            return "1"
        if value is False:
            return "0"
        s = str(value).strip().lower()
        if s in {"1", "true", "on", "yes", "enabled", "enable"}:
            return "1"
        if s in {"0", "false", "off", "no", "disabled", "disable"}:
            return "0"
        return s
    return str(value).strip()


def login(prefix: str) -> Tuple[requests.Session, Dict[str, str]]:
    """
    优先复用你 notebook 的登录方式：common_utils.CONFIG.login(prefix=...)
    """
    try:
        from common_utils import CONFIG as C  # type: ignore

        session, cookies = C.login(prefix=prefix)
        return session, cookies
    except Exception as e:
        raise RuntimeError(
            f"登录失败。请确认 common_utils 可用并已配置账号。detail={e}"
        ) from e


def fetch_config(
    session: requests.Session,
    cookies: Dict[str, str],
    prefix: str,
    env: str,
    timeout: float,
) -> Dict[str, Any]:
    url = f"https://{prefix}.digifinex.org/api/cf_strategy/db_config"
    params = {"symbol": "common", "env": env}
    resp = session.get(url, params=params, cookies=cookies, timeout=timeout)
    data = _to_json(resp)
    if resp.status_code != 200:
        raise RuntimeError(f"GET失败 status={resp.status_code}, body={data}")
    if not isinstance(data, dict):
        raise RuntimeError(f"GET返回异常: {data}")
    return data.get("data") or {}


def save_config(
    session: requests.Session,
    cookies: Dict[str, str],
    prefix: str,
    env: str,
    config_patch: Dict[str, str],
    timeout: float,
) -> Dict[str, Any]:
    url = f"https://{prefix}.digifinex.org/api/cf_strategy/save_db_config"
    payload = {
        "symbol": "common",
        "env": env,
        "suffix": "dc_monitor",
        "config": config_patch,
    }
    resp = session.post(
        url,
        data=json.dumps(payload),
        cookies=cookies,
        headers={"Content-Type": "application/json"},
        timeout=timeout,
    )
    data = _to_json(resp)
    if resp.status_code != 200:
        raise RuntimeError(f"POST失败 status={resp.status_code}, body={data}")
    return data


def is_save_success(resp_json: Dict[str, Any]) -> bool:
    errcode = resp_json.get("errcode")
    if str(errcode) == "0":
        return True
    # 某些接口会返回 {"data":"ok"} 这种风格
    if str(resp_json.get("data")).lower() == "ok":
        return True
    return False


def diff_patch(current: Dict[str, Any], target: Dict[str, str]) -> Dict[str, str]:
    patch: Dict[str, str] = {}
    for key, target_v in target.items():
        now_v = _normalize_value(key, current.get(key))
        if now_v != _normalize_value(key, target_v):
            patch[key] = target_v
    return patch


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="批量更新 CFDC dc_monitor 参数")
    parser.add_argument(
        "--prefix",
        default="mmadmin",
        help="后台域名前缀，例如 mmadmin / mmadminjp3",
    )
    parser.add_argument(
        "--envs",
        nargs="*",
        default=DEFAULT_ENVS,
        help="要更新的环境列表；不传则使用页面里的全部环境",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="真正执行写入（默认仅 dry-run）",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.2,
        help="每个环境之间的间隔秒数，默认 0.2",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=15.0,
        help="接口超时秒数，默认 15",
    )
    parser.add_argument(
        "--stop-on-error",
        action="store_true",
        help="遇到失败时立即停止（默认继续处理后续环境）",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    print(f"[INFO] prefix={args.prefix}")
    print(f"[INFO] env_count={len(args.envs)}")
    print(f"[INFO] mode={'APPLY' if args.apply else 'DRY-RUN'}")

    session, cookies = login(args.prefix)

    ok_envs: List[str] = []
    skip_envs: List[str] = []
    fail_envs: List[Tuple[str, str]] = []

    for idx, env in enumerate(args.envs, start=1):
        try:
            current = fetch_config(session, cookies, args.prefix, env, args.timeout)
            patch = diff_patch(current, TARGET_CONFIG)

            if not patch:
                print(f"[{idx}/{len(args.envs)}] {env}: SKIP (already target)")
                skip_envs.append(env)
                continue

            print(f"[{idx}/{len(args.envs)}] {env}: need_update={len(patch)} keys")
            for k, v in patch.items():
                print(f"  - {k}: {current.get(k)!r} -> {v!r}")

            if not args.apply:
                ok_envs.append(env)
                continue

            save_resp = save_config(
                session=session,
                cookies=cookies,
                prefix=args.prefix,
                env=env,
                config_patch=patch,
                timeout=args.timeout,
            )
            if not is_save_success(save_resp):
                raise RuntimeError(f"保存失败: {save_resp}")

            # 二次校验
            after = fetch_config(session, cookies, args.prefix, env, args.timeout)
            mismatch = []
            for k, v in patch.items():
                if _normalize_value(k, after.get(k)) != _normalize_value(k, v):
                    mismatch.append((k, after.get(k), v))
            if mismatch:
                raise RuntimeError(f"保存后校验不一致: {mismatch}")

            print(f"[{idx}/{len(args.envs)}] {env}: OK")
            ok_envs.append(env)
            time.sleep(args.sleep)
        except Exception as e:
            msg = str(e)
            print(f"[{idx}/{len(args.envs)}] {env}: FAIL -> {msg}")
            fail_envs.append((env, msg))
            if args.stop_on_error:
                break

    print("\n========== SUMMARY ==========")
    print(f"ok={len(ok_envs)}, skip={len(skip_envs)}, fail={len(fail_envs)}")
    if fail_envs:
        print("failed_envs:")
        for env, reason in fail_envs:
            print(f"  - {env}: {reason}")

    return 0 if not fail_envs else 1


if __name__ == "__main__":
    sys.exit(main())
