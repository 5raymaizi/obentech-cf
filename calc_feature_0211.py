import concurrent.futures
import gc
import json
import math
import threading
import zipfile
from typing import Literal

from curlify import to_curl
from sklearn.preprocessing import MinMaxScaler

import config
from calc_api_feature import *
from calc_funding_feature import *
from calc_hist_1min_feature import *
from calc_spread_feature import *

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('calc_future')

at_users = {
    "ou_df8c3b80f9d4a2d1484c3e261f5c4f0f": "无极",
    "ou_3dc45ea6ad47050ee94a8f7f08687be1": "洛基",
    "ou_32837360da1aa3b747f551b5af8e6705": "洋芋",
}  # 报警@用户列表

def send_lark_bot(bot_id_req, title=None, message_content=None, at_users=None):
    url = f"https://open.larksuite.com/open-apis/bot/v2/hook/{bot_id_req}"
    if not title:
        title = "双合约自动化"

    if at_users:
        message_content += "\n"
        message_content += " ".join([f"<at user_id=\"{user_id}\">{user_name}</at>" for user_id, user_name in at_users.items()])

    # 定义请求数据
    data = {
        "msg_type": "text",
        "content": {
            "text": f"{title}\n {message_content}"
        }
    }

    logging.info(f"{title} data: {data}")
    try:
        headers = {"Content-Type": "application/json; charset=utf-8"}
        response = requests.post(url, json=data, headers=headers)

        if response.status_code != 200:
            logging.error(f"Bad response: [{response.status_code}] {response.text}")
            return "", response.text

        result = response.json()
        logging.debug(f"Response JSON: {result}")
        logging.info(f"{title} SendLarkBot result: {result}")
        return result.get("StatusMessage", ""), None

    except requests.RequestException as e:
        logging.error(f"{title} SendLarkBot error: {e}")
        return "", str(e)

class Lark:
    def __init__(self, app_id, app_secret):
        self.app_id = app_id
        self.app_secret = app_secret

    def get_access_token(self):
        url = "https://open.larksuite.com/open-apis/auth/v3/app_access_token/internal"
        headers = {
            "Content-Type": "application/json; charset=utf-8"
        }
        data = {
            "app_id": self.app_id,
            "app_secret": self.app_secret
        }

        try:
            resp = requests.post(url, headers=headers, json=data)
            if resp.status_code != 200:
                raise Exception(f"[{resp.status_code}] bad response: {resp.text}")
            result = resp.json()
            return result.get("tenant_access_token", "")
        except Exception as e:
            logger.error(f"Failed to get access token: {e}")
            return ""

    def read_sheet(self, access_token, sheet_token, sheet_id, sheet_range=""):
        if sheet_range:
            s_range = f"{sheet_id}!{sheet_range}"
        else:
            s_range = sheet_id

        url = f"https://open.larksuite.com/open-apis/sheets/v2/spreadsheets/{sheet_token}/values/{s_range}"
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {access_token}"
        }
        params = {
            "valueRenderOption": "FormattedValue",
            "dateTimeRenderOption": "FormattedString"
        }

        try:
            resp = requests.get(url, headers=headers, params=params)
            resp.raise_for_status()

            result = resp.json()
            return result.get("data", {}).get("valueRange", {}).get("values", [])
        except Exception as e:
            logger.error(f"Failed to read sheet: {e}")
            return []
        
def read_cross_single_env_config():
    app_id = "cli_a4563421d8f8d00a"
    app_secret = "wruodZh8fpBBDisKKqPQpf5pkk4yz1j0"
    sheet_token = "FLNcwfvjFirPXfklM3GlrHytgWg"
    sheet_id = "0fl3tq"

    lark = Lark(app_id, app_secret)
    token = lark.get_access_token()
    logger.info(f"Access Token: {token}")
    if token:
        data = lark.read_sheet(token, sheet_token, sheet_id, "A2:A2")
        # logger.info(f"Sheet Data: {data}")
        # 返回是个二维数组
        if len(data) > 0 and len(data[0]) > 0:
            config_str = data[0][0]
            config_str = config_str.replace("'", '')
            config = json.loads(config_str)
            return config
            
        else:
            logger.error("No data found in the specified range.")
            return []

    else:
        logger.error("Failed to obtain access token.")
        return []

# 做市控制台
class Console:
    def __init__(self, url=console_base_url, auto_url=console_base_url_auto):
        self.url = url
        self.url_auto = auto_url

    def request_update_config(self, change=None, add=None):
        """
        请求更新配置
        :param change: 修改的配置。
        :return:
        """

        req = {}
        if change:
            req["change"] = [change]
        if add:
            req["add"] = [add]

        try:
            # 先调接口获取cookie, 从Set-Cookie Header解析
            logger.info(f'{self.url}/api/cf_strategy/update_config')
            resp = requests.get(f'{self.url}/strategy_cf_v5', timeout=5)
            # if resp.status_code != 200:
            #     logger.error(f"Failed to get cookie: {resp.status_code} {resp.text}")
            #     return None
            cookie = resp.headers.get('Set-Cookie')
            if not cookie:
                logger.error("Failed to get cookie from response headers")
                return None
            headers = {
                'Cookie': cookie,
            }

            response = requests.post(f"{self.url}/api/cf_strategy/update_config", json=req, headers=headers, timeout=5)
            # response = requests.post(f"{self.url}/api/cf_strategy/update_config", json=req, timeout=5)
            logger.info(f"request: {to_curl(response.request)}, response: {response.text}")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to update config: {str(e)}", exc_info=True)
            # send_lark_bot(lark_bot, title="请求更新配置失败", message_content=str(e), at_users=at_users)
            return {}

    def get_api_config(self):
        """
        获取api.py中的配置
        :return:
        """
        try:
            response = requests.get(f"{self.url_auto}/api/cf_strategy/df_config", timeout=5)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to get config: {str(e)}", exc_info=True)
            return {}

    def get_trade_stats(self, env=None, symbol=None, start=None, end=None):
        """
        获取跨所小时数据库数据
        :param end:
        :param start:
        :param env:
        :param symbol:
        :return:
        """
        params = {}
        if env:
            params['env'] = env
        if symbol:
            params['symbol'] = symbol
        if start:
            params['start'] = start
        if end:
            params['end'] = end

        try:
            response = requests.get(f"{self.url}/api/cf_strategy/trade_stats", params=params, timeout=5)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to get trade stats: {str(e)}", exc_info=True)
            return None

    def upload_df_stat(self, type, data):
        """
        上传统计数据
        :param type: 数据类型
        :param data: 数据内容
        :return:
        """
        try:
            param = {
                "type": type,
                "data": data
            }
            response = requests.post(f"{self.url_auto}/api/cf_strategy/upload_df_stats", json=param, timeout=5)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to upload df stat: {str(e)}", exc_info=True)
            return None

    def get_config(self, env, symbol):
        """
        获取配置
        :param env:
        :param symbol:
        :return:
        """
        try:
            response = requests.get(f"{self.url}/api/cf_arbitrage/{symbol}?env={env}", timeout=5)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to get config: {str(e)}", exc_info=True)
            return None

    def get_keys(self, env):
        """
        获取配置的keys
        :param env:
        :return: {"data":[],"error":""}
        """
        try:
            response = requests.get(f"{self.url}/api/cf_arbitrage/keys?env={env}", timeout=5)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to get keys: {str(e)}", exc_info=True)
            return {}
        
    def raw_request(self, env_name, method,  url, exchange=None, console_url=None, params=None):
        try:
            c_url = self.url if not console_url else console_url
            # 先调接口获取cookie, 从Set-Cookie Header解析
            logger.info(f'{c_url}/api/cf_strategy/update_config')
            resp = requests.get(f'{c_url}/strategy_cf_v5', timeout=5)
            # if resp.status_code != 200:
            #     logger.error(f"Failed to get cookie: {resp.status_code} {resp.text}")
            #     return None
            cookie = resp.headers.get('Set-Cookie')
            if not cookie:
                logger.error("Failed to get cookie from response headers")
                return None
            headers = {
                'Cookie': cookie,
            }
            data = {
                'env': env_name,
                'account': 'cf_auto',    # 调用接口的程序名
                'method': method,    # GET/POST
                'private': True,    # True/False
                'url': url
            }
            if exchange:
                data['exchange'] = exchange
            if params:
                data['params'] = params

            response = requests.post(f"{c_url}/api/cf_strategy/raw_request", json=data, headers=headers, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to raw request: {str(e)}", exc_info=True)
            return None
    
    def raw_request_binance_leverage(self, env_name, symbol, leverage):
        """
        修改binance杠杆
        symbol: BTCUSDT
        """
        # c_url = "https://mmadminjp3.digifinex.org:8443" # jp控制台
        c_url = "https://mmadmin.digifinex.org:8443"  # hk控制台

        params = {
            "symbol": symbol,
            "leverage": leverage
        }

        resp = self.raw_request(env_name, "POST", "https://fapi.binance.com/fapi/v1/leverage",exchange="binance", console_url=c_url, params=params)
        return resp
    
    def raw_request_okx_leverage(self, env_name, symbol, leverage):
        """
        修改okx杠杆
        symbol: BTC-USDT-SWAP
        """
        c_url = "https://mmadmin.digifinex.org:8443"  # hk控制台
        # c_url = "https://mmadminjp3.digifinex.org:8443" # jp控制台

        params = {
            "instId": symbol,
            "lever": leverage,
            "mgnMode": "cross"
        }

        resp = self.raw_request(env_name, "POST", "/account/set-leverage", exchange="okex5", console_url=c_url, params=params)
        return resp


def get_single_exchange(env_name, ex='okx'):
    """
    查询单所交易对(仅限okx_binance双合约)
    :param ex: 如果是bnDc,则表示是bn单所双合约，如果是spec则判断特殊环境
    :param env_name: okx-binance跨所env
    :return: 返回对应跨所running或者有仓位的交易对
    """
    console_url_hk = "https://mmadmin.digifinex.org:8443"
    console_url_jp3 = "https://mmadminjp3.digifinex.org:8443"
    # 特殊需要相互判断排除的环境,每个列表中任意一个环境，都要排除其余环境的交易对
    env_spec = [
        {
            "cfdc_mt_dc_pmpro_test": console_url_hk,
            "cfdc_mt_dc_pmtest2": console_url_hk,
            "cfdc_dcpmtest4": console_url_jp3,
            "cfdc_dctest5": console_url_jp3,
        }
    ]
    # 单所，跨所对应的环境映射
    env_map = read_cross_single_env_config()
    logger.info(f"跨所环境映射表: {env_map}")

    if ex == 'bnDc':
        # 排除bn单所双合约
        # 单所对应的bn单所双合约环境 bn_v5_pm_pro16 -> cfdc_pro16Dc, bn_v5_pm_test17 -> cfdc_test17Dc
        env_bn_dc_map = {item["dc"]: f'cfdc_{item["binance"].split("_")[-1]}Dc' for item in env_map if "dc" in item and "binance" in item}
        cross_console_url_dc = console_url_jp3  # bn单所双合约控制台
        cross_symbols = []

        # 查询跨所所有交易对
        def get_symbols(en, m, u):
            console = Console(url=u)
            symbols = []
            cross_env = m.get(en, "")
            if not cross_env:
                logger.info(f"未找到对应的跨所环境，env_name: {en}")
                return symbols
            keys = console.get_keys(env=cross_env)
            # 查询跨所环境交易对仓位
            for key in keys.get("data", []):
                detail_resp = console.get_config(cross_env, key)
                if detail_resp:
                    local_pos1 = float(
                        detail_resp.get("data", {}).get('detail', {}).get('pos_list', {}).get("local_pos1_amount", 0))
                    local_pos2 = float(
                        detail_resp.get("data", {}).get('detail', {}).get('pos_list', {}).get('local_pos2_amount', 0))
                    running = detail_resp.get("data", {}).get('running', False)
                    if running or local_pos1 != 0 or local_pos2 != 0:
                        # running为true或者有仓位的都认为是开启状态
                        symbols.append(key)
            return symbols

        # 合并两个env_map的symbols
        cross_symbols += get_symbols(env_name, env_bn_dc_map, u=cross_console_url_dc)
        return cross_symbols
    elif ex == 'spec':
        # 排除特殊环境
        cross_symbols = []

        for spec_item in env_spec:
            if env_name in spec_item:
                for en in spec_item:
                    # 排除其余环境的交易对
                    if env_name == en:
                        # 本环境跳过
                        continue
                    cross_console_url_spec = spec_item[en]
                    cons = Console(url=cross_console_url_spec)
                    # 查询跨所所有交易对
                    cross_env = en
                    keys = cons.get_keys(env=cross_env)
                    # 查询跨所环境交易对仓位
                    for key in keys.get("data", []):
                        detail_resp = console.get_config(cross_env, key)
                        if detail_resp:
                            local_pos1 = float(
                                detail_resp.get("data", {}).get('detail', {}).get('pos_list', {}).get("local_pos1_amount", 0))
                            local_pos2 = float(
                                detail_resp.get("data", {}).get('detail', {}).get('pos_list', {}).get('local_pos2_amount', 0))
                            running = detail_resp.get("data", {}).get('running', False)
                            if running or local_pos1 != 0 or local_pos2 != 0:
                                # running为true或者有仓位的都认为是开启状态
                                cross_symbols.append(key)
        return cross_symbols
    else:
        # 排除单所
        # okx单所对应的跨所环境,bybit在hk
        okx_env_map = {item["dc"]: item[ex] for item in env_map if ex in item and "dc" in item}
        cross_console_url = console_url_hk if ex in ['okx','bybit'] else console_url_jp3
        cons = Console(url=cross_console_url)
        cross_symbols = []

        # 查询跨所环境开关
        # keys_status = get_key_status(env_name,cross_console_url)
        # # keysStatus有两种返回格式，都要兼容下。1.{"data":{"0GUSDT":"Off"}} 2.{"data":{"0GUSDT":{"status":"Off"}}}
        # for symbol, status in keys_status.items():
        #     if isinstance(status, dict):
        #         status_value = status.get("status", "Off")
        #     else:
        #         status_value = status
        #     if status_value == "On":
        #         cross_symbols.append(symbol)

        # 查询跨所所有交易对
        cross_env = okx_env_map.get(env_name, "")
        if not cross_env:
            logger.info(f"未找到对应的跨所环境，env_name: {env_name}")
            return cross_symbols
        keys = cons.get_keys(env=cross_env)
        # 查询跨所环境交易对仓位
        for key in keys.get("data", []):
            detail_resp = cons.get_config(cross_env, key)
            if detail_resp:
                local_size_tt = float(
                    detail_resp.get("data", {}).get('detail', {}).get('pos_list', {}).get("local_pos_amount", 0))
                local_size_mt = float(
                    detail_resp.get("data", {}).get('detail', {}).get('pos_list', {}).get('local_pos_amount_mt', 0))
                local_size_tm = float(
                    detail_resp.get("data", {}).get('detail', {}).get('pos_list', {}).get('local_pos_amount_tm', 0))
                running = detail_resp.get("data", {}).get('running', False)
                if running or local_size_tt != 0 or local_size_mt != 0 or local_size_tm != 0:
                    # running为true或者有仓位的都认为是开启状态
                    cross_symbols.append(key)
        return cross_symbols


def deflation(
        q_profit_high, q_profit_low, q_be_high, q_be_low,
        q_lower, q_upper,
        current_ex0_funding, current_ex1_funding,
        do_indicator, mode_sign_binary, mode_binary_prop,
        earn_mean_abs, earn_mean_abs_1day,
        IsTick1Tick2Equal, tick_size_factor,
        ex0_fr_interval, ex1_fr_interval,
        same_deflation_coeff=0.5,
        cover_deflation_coeff=1.0
):
    """
    根据资金费率差值或方向信号动态调整利润阈值。
    """
    interval_ratio = ex0_fr_interval / ex1_fr_interval
    curr_fr_diff = (current_ex0_funding - current_ex1_funding * interval_ratio) * 10000
    # IsFrReversed = (curr_fr_diff*mode_sign_binary <= -3)
    IsFrLarge = (curr_fr_diff * mode_sign_binary >= 3)

    # ---- 模式 1：根据资金费率差值调整 ----
    if not do_indicator:
        if curr_fr_diff >= 0:
            # 倾向做空：降低做空门槛，小幅降低做多门槛
            q_profit_high -= same_deflation_coeff * curr_fr_diff
            q_profit_low -= cover_deflation_coeff * curr_fr_diff
        else:
            # 倾向做多：降低做多门槛，小幅降低做空门槛
            q_profit_high -= cover_deflation_coeff * curr_fr_diff
            q_profit_low -= same_deflation_coeff * curr_fr_diff

    # ---- 模式 2：根据方向信号强制设置阈值 ----
    else:
        if not IsTick1Tick2Equal:
            tick_factor = 0.5 * tick_size_factor
        if mode_sign_binary == 1:

            q_profit_high = q_upper
            q_profit_low = q_be_low
            if (earn_mean_abs_1day > earn_mean_abs_1day_limit) or (earn_mean_abs > earn_mean_abs_limit):
                q_profit_low = q_profit_low - max(tick_size_factor, 2)
            if IsFrLarge:
                q_profit_low -= curr_fr_diff
            # if IsFrReversed:
            #     q_profit_low += curr_fr_diff

        elif mode_sign_binary == -1:
            q_profit_low = q_lower
            q_profit_high = q_be_high

            if (earn_mean_abs_1day > earn_mean_abs_1day_limit) or (earn_mean_abs > earn_mean_abs_limit):
                q_profit_high = q_profit_high + max(tick_size_factor, 2)
            if IsFrLarge:
                q_profit_high -= curr_fr_diff
            # if IsFrReversed:
            #     q_profit_low += curr_fr_diff
    return q_profit_high, q_profit_low


def adjust_threshold(raw: float,
                     tick_size_factor: float,
                     IsTick1Tick2Equal: bool,
                     do_indicator: bool,
                     mode_sign_binary: int,
                     side: Literal["open", "close"],
                     margin_ratio: float = 0.05,
                     large_tick_threshold: float = 4) -> float:
    """
    raw              : 理论 open / close threshold，可正可负
    tick_size_factor : sr 的最小跳动 (≈ 1 个 tick 对应的价差)
    side             : 'open'  要求   sr ≥ threshold   （做多开仓 / 做空开仓）
                       'close' 要求   sr ≤ threshold   （平仓）
    margin_ratio     : 安全垫占 1 tick 的比例，缺省 0.05
    """
    if tick_size_factor is None or tick_size_factor <= 0:
        raise ValueError("tick_size_factor 必须 > 0")

    if not IsTick1Tick2Equal:
        tick_size_factor /= 2
    sign = 1 if raw >= 0 else -1
    n_float = abs(raw) / tick_size_factor  # 理论是几 tick
    lower = max(0, math.floor(n_float))  # 至少 1 tick
    upper = math.ceil(n_float)

    # 如果tick_size_factor过大（比如万7），并且不吃资金费率，对于open选大的，对于close选小的
    if tick_size_factor > large_tick_threshold and not do_indicator:
        n = upper if ((raw > 0) == (side == "open")) else lower
    else:
        # 谁近选谁
        n = lower if (n_float - lower) <= (upper - n_float) else upper

    margin = margin_ratio * tick_size_factor
    base = n * tick_size_factor

    if side == "open":
        adj = base - sign * margin  # 贴下沿
        if do_indicator:  # 对于吃资金费率的，不能和资金费率的方向相反
            if mode_sign_binary == 1:
                return min(raw, sign * adj)
            elif mode_sign_binary == -1:
                return max(raw, sign * adj)
            else:
                return sign * adj
        else:
            return sign * adj
    elif side == "close":
        adj = base + sign * margin  # 贴上沿
        if do_indicator:
            if mode_sign_binary == 1:
                return min(raw, sign * adj)
            elif mode_sign_binary == -1:
                return max(raw, sign * adj)
            else:
                return sign * adj
        else:
            return sign * adj


def process_symbol_data(symbol, envs, fr_threshold_1day, fr_threshold_nday, fr_threshold_latest, fr_slippage_threshold,
                        fee_rate, slippage_duration_days=14, sr_lookback_days=3):
    """并发处理单个交易对的函数"""
    logger.info(f'Start processing symbol: {symbol}')
    logger.info(f"[runtime_monitor][{now_hour}] Processing symbol: {symbol} started.")
    date_str = datetime.now().strftime("%Y%m%d")
    # date_str = '20250606'

    dfs_depth = [None, None]
    dfs_funding = [None, None]
    current_ex0_funding = None
    current_ex1_funding = None

    def process_exchange(exchange):
        ex = ''
        if exchanges[0] == exchange:
            ex = 'ex0'
        elif exchanges[1] == exchange:
            ex = 'ex1'
        # for exchange in exchanges:
        base_dir = os.path.join(output_dir, f'{date_str}/{exchange}')
        depth_file_path = os.path.join(depth_dir,
                                       f"depth_{exchange}_swap_{symbol.lower()}_5_100.csv")
        logger.info(f"Processing {exchange} depth file: {depth_file_path}")

        # 解压昨日深度数据
        # 昨日日期
        logger.info(f"[runtime_monitor][{now_hour}] Processing {exchange}.{symbol} yesterday depth file started.")
        date_str_yday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        # date_str_yday = '2025-06-05'
        yday_depth_file_path = os.path.join(depth_dir, f'{date_str_yday}/'
                                                       f'{date_str_yday}_depth_{exchange}_swap_{symbol.lower()}_5_100.zip')
        df_depth_yday = None
        if os.path.exists(yday_depth_file_path):
            # 解压zip
            with zipfile.ZipFile(yday_depth_file_path, 'r') as zip_ref:
                with zip_ref.open(f'depth_{exchange}_swap_{symbol.lower()}_5_100.copy.csv') as file:
                    df_depth_yday = pd.read_csv(file,on_bad_lines='skip',)
        if df_depth_yday is None:
            logger.warning(f'No yesterday depth data found for {exchange} {symbol}')
        else:
            # local time,exchange time,bid price 1,bid amount 1,bid price 2,bid amount 2,bid price 3,bid amount 3,bid price 4,bid amount 4,bid price 5,bid amount 5,ask price 1,ask amount 1,ask price 2,ask amount 2,ask price 3,ask amount 3,ask price 4,ask amount 4,ask price 5,ask amount 5,event time
            # 修改为
            # event_time,local_time,exchange,bid_price_1,bid_amount_1,bid_price_2,bid_amount_2,bid_price_3,bid_amount_3,bid_price_4,bid_amount_4,bid_price_5,bid_amount_5,ask_price_1,ask_amount_1,ask_price_2,ask_amount_2,ask_price_3,ask_amount_3,ask_price_4,ask_amount_4,ask_price_5,ask_amount_5
            df_depth_yday = df_depth_yday.rename(columns={
                'exchange time': 'event_time',
                'local time': 'local_time',
                'bid price 1': 'bid_price_1',
                'bid amount 1': 'bid_amount_1',
                'bid price 2': 'bid_price_2',
                'bid amount 2': 'bid_amount_2',
                'bid price 3': 'bid_price_3',
                'bid amount 3': 'bid_amount_3',
                'bid price 4': 'bid_price_4',
                'bid amount 4': 'bid_amount_4',
                'bid price 5': 'bid_price_5',
                'bid amount 5': 'bid_amount_5',
                'ask price 1': 'ask_price_1',
                'ask amount 1': 'ask_amount_1',
                'ask price 2': 'ask_price_2',
                'ask amount 2': 'ask_amount_2',
                'ask price 3': 'ask_price_3',
                'ask amount 3': 'ask_amount_3',
                'ask price 4': 'ask_price_4',
                'ask amount 4': 'ask_amount_4',
                'ask price 5': 'ask_price_5',
                'ask amount 5': 'ask_amount_5'
            })
            df_depth_yday['exchange'] = exchange
            df_depth_yday = df_depth_yday.add_prefix(f'{ex}_')
            df_depth_yday = df_depth_yday.rename(columns={f"{ex}_event_time": "event_time"})
            # 线上偶发 event_time 缺失/非数值/inf，直接 astype('int64') 会抛 IntCastingNaNError
            df_depth_yday['event_time'] = pd.to_numeric(df_depth_yday['event_time'], errors='coerce')
            df_depth_yday = df_depth_yday.replace([np.inf, -np.inf], np.nan)
            df_depth_yday = df_depth_yday.dropna(subset=['event_time'])
            df_depth_yday['event_time'] = df_depth_yday['event_time'].astype('int64')
            df_depth_yday = df_depth_yday.sort_values('event_time')
        logger.info(f"[runtime_monitor][{now_hour}] Processing {exchange}.{symbol} today depth file started.")

        # 读取今日深度数据
        if os.path.exists(depth_file_path):
            df_depth = pd.read_csv(depth_file_path,on_bad_lines='skip')
            # local time,exchange time,bid price 1,bid amount 1,bid price 2,bid amount 2,bid price 3,bid amount 3,bid price 4,bid amount 4,bid price 5,bid amount 5,ask price 1,ask amount 1,ask price 2,ask amount 2,ask price 3,ask amount 3,ask price 4,ask amount 4,ask price 5,ask amount 5,event time
            # 修改为
            # event_time,local_time,exchange,bid_price_1,bid_amount_1,bid_price_2,bid_amount_2,bid_price_3,bid_amount_3,bid_price_4,bid_amount_4,bid_price_5,bid_amount_5,ask_price_1,ask_amount_1,ask_price_2,ask_amount_2,ask_price_3,ask_amount_3,ask_price_4,ask_amount_4,ask_price_5,ask_amount_5
            df_depth = df_depth.rename(columns={
                'exchange time': 'event_time',
                'local time': 'local_time',
                'bid price 1': 'bid_price_1',
                'bid amount 1': 'bid_amount_1',
                'bid price 2': 'bid_price_2',
                'bid amount 2': 'bid_amount_2',
                'bid price 3': 'bid_price_3',
                'bid amount 3': 'bid_amount_3',
                'bid price 4': 'bid_price_4',
                'bid amount 4': 'bid_amount_4',
                'bid price 5': 'bid_price_5',
                'bid amount 5': 'bid_amount_5',
                'ask price 1': 'ask_price_1',
                'ask amount 1': 'ask_amount_1',
                'ask price 2': 'ask_price_2',
                'ask amount 2': 'ask_amount_2',
                'ask price 3': 'ask_price_3',
                'ask amount 3': 'ask_amount_3',
                'ask price 4': 'ask_price_4',
                'ask amount 4': 'ask_amount_4',
                'ask price 5': 'ask_price_5',
                'ask amount 5': 'ask_amount_5'
            })
            df_depth['exchange'] = exchange
            df_depth = df_depth.add_prefix(f'{ex}_')
            df_depth = df_depth.rename(columns={f"{ex}_event_time": "event_time"})
            # 线上偶发 event_time 缺失/非数值/inf，直接 astype('int64') 会抛 IntCastingNaNError
            df_depth['event_time'] = pd.to_numeric(df_depth['event_time'], errors='coerce')
            df_depth = df_depth.replace([np.inf, -np.inf], np.nan)
            df_depth = df_depth.dropna(subset=['event_time'])
            df_depth['event_time'] = df_depth['event_time'].astype('int64')
            df_depth = df_depth.sort_values('event_time')
            # 合并24h内的数据
            last_event_time = df_depth['event_time'].max() if not df_depth.empty else 0
            if df_depth_yday is not None:
                df_depth_yday = df_depth_yday[df_depth_yday['event_time'] >= last_event_time - 24 * 60 * 60 * 1000]
                df_depth = pd.concat([df_depth_yday, df_depth], ignore_index=True)

            # dfs_depth.append(df_depth)

        logger.info(f"[runtime_monitor][{now_hour}] Processing {exchange}.{symbol} funding file started.")
        # 读取资金费率数据 转换成： event_time,local_time,exchange,funding_rate,funding_time
        # 币安    Time,Symbol,MarkPrice,IndexPrice,FundingRate,NextFundingTime
        if exchange == 'binance':
            bn_funding_file_path = os.path.join(binance_funding_dir, f"{symbol.upper().replace('-', '')}.csv")
            if os.path.exists(bn_funding_file_path):
                df_funding = pd.read_csv(bn_funding_file_path,on_bad_lines='skip')
                df_funding = df_funding.rename(columns={
                    'Time': 'event_time',
                    'FundingRate': 'funding_rate',
                    'NextFundingTime': 'funding_time'
                })
                df_funding['exchange'] = exchange
                # 只保留event_time最接近funding_time的数据
                df_funding = df_funding.drop_duplicates(subset=['funding_time'], keep='last')
                # 2025-03-06T15:34:02.584+08:00 -> timestamp
                df_funding['event_time'] = pd.to_datetime(df_funding['event_time']).astype('int64') // 10 ** 6
                df_funding['local_time'] = df_funding['event_time']
                df_funding = df_funding.add_prefix(f'{ex}_')
                df_funding = df_funding.rename(columns={f"{ex}_event_time": "event_time"})
                df_funding = df_funding.sort_values('event_time')
                # dfs_funding.append(df_funding)
                current_funding = df_funding[f'{ex}_funding_rate'].iloc[-1] if not df_funding.empty else None
                # logger.info(f'Binance funding data: {dfs_funding}')
            else:
                logger.warning(f'Binance funding file not found: {bn_funding_file_path}')
        # bybit Time,FundingTime,FundingRate,NextFundingTime
        if exchange == 'bybit5':
            funding_file_path = os.path.join(bybit_funding_dir, f"{symbol.upper().replace('-', '')}.csv")
            if os.path.exists(funding_file_path):
                df_funding = pd.read_csv(funding_file_path,on_bad_lines='skip')
                df_funding = df_funding.rename(columns={
                    'Time': 'event_time',
                    'FundingRate': 'funding_rate',
                    'FundingTime': 'funding_time'
                })
                df_funding['exchange'] = exchange
                # 只保留event_time最接近funding_time的数据
                df_funding = df_funding.drop_duplicates(subset=['funding_time'], keep='last')
                # 2025-03-06T15:34:02.584+08:00 -> timestamp
                df_funding['event_time'] = pd.to_datetime(df_funding['event_time']).astype('int64') // 10 ** 6
                df_funding['local_time'] = df_funding['event_time']
                df_funding = df_funding.add_prefix(f'{ex}_')
                df_funding = df_funding.rename(columns={f"{ex}_event_time": "event_time"})
                df_funding = df_funding.sort_values('event_time')
                # dfs_funding.append(df_funding)
                current_funding = df_funding[f'{ex}_funding_rate'].iloc[-1] if not df_funding.empty else None
                # logger.info(f'Bybit funding data: {dfs_funding}')
            else:
                logger.warning(f'Bybit funding file not found: {funding_file_path}')
        # gateio Time,FundingTime,FundingRate,NextFundingTime
        if exchange == 'gateio':
            funding_file_path = os.path.join(gateio_funding_dir, f"{symbol.upper().replace('-', '_')}.csv")
            if os.path.exists(funding_file_path):
                df_funding = pd.read_csv(funding_file_path,on_bad_lines='skip')
                df_funding = df_funding.rename(columns={
                    'Time': 'event_time',
                    'FundingRate': 'funding_rate',
                    'FundingTime': 'funding_time'
                })
                df_funding['exchange'] = exchange
                # 只保留event_time最接近funding_time的数据
                df_funding['funding_time'] = pd.to_datetime(df_funding['funding_time']).astype('int64') // 10 ** 6
                df_funding = df_funding.drop_duplicates(subset=['funding_time'], keep='last')
                # 2025-03-06T15:34:02.584+08:00 -> timestamp
                df_funding['event_time'] = pd.to_datetime(df_funding['event_time']).astype('int64') // 10 ** 6
                df_funding['local_time'] = df_funding['event_time']
                df_funding = df_funding.add_prefix(f'{ex}_')
                df_funding = df_funding.rename(columns={f"{ex}_event_time": "event_time"})
                df_funding = df_funding.sort_values('event_time')
                # dfs_funding.append(df_funding)
                current_funding = df_funding[f'{ex}_funding_rate'].iloc[-1] if not df_funding.empty else None
                # logger.info(f'Bybit funding data: {dfs_funding}')
            else:
                logger.warning(f'Gateio funding file not found: {funding_file_path}')
        # hyperliquid Time,FundingTime,FundingRate,NextFundingTime
        if exchange == 'hyperliquid':
            funding_file_path = os.path.join(hyperliquid_funding_dir, f"{symbol.upper().replace('-', '')}.csv")
            if os.path.exists(funding_file_path):
                df_funding = pd.read_csv(funding_file_path,on_bad_lines='skip')
                df_funding = df_funding.rename(columns={
                    'Time': 'event_time',
                    'FundingRate': 'funding_rate',
                    'FundingTime': 'funding_time'
                })
                df_funding['exchange'] = exchange
                # 只保留event_time最接近funding_time的数据
                df_funding = df_funding.drop_duplicates(subset=['funding_time'], keep='last')
                # 2025-03-06T15:34:02.584+08:00 -> timestamp
                df_funding['event_time'] = pd.to_datetime(df_funding['event_time']).astype('int64') // 10 ** 6
                df_funding['local_time'] = df_funding['event_time']
                df_funding = df_funding.add_prefix(f'{ex}_')
                df_funding = df_funding.rename(columns={f"{ex}_event_time": "event_time"})
                df_funding = df_funding.sort_values('event_time')
                # dfs_funding.append(df_funding)
                current_funding = df_funding[f'{ex}_funding_rate'].iloc[-1] if not df_funding.empty else None
                # logger.info(f'Hyperliquid funding data: {dfs_funding}')
            else:
                logger.warning(f'Hyperliquid funding file not found: {funding_file_path}')
        # OKX   Time,FundingTime,FundingRate,NextFundingTime,Premium
        elif exchange == 'okx':
            okx_funding_file_path = os.path.join(okx_funding_dir, f"{symbol.upper()}-SWAP.csv")
            if os.path.exists(okx_funding_file_path):
                df_funding = pd.read_csv(okx_funding_file_path,on_bad_lines='skip')
                df_funding = df_funding.rename(columns={
                    'Time': 'event_time',
                    'FundingRate': 'funding_rate',
                    'FundingTime': 'funding_time'
                })
                df_funding['exchange'] = exchange
                # 只保留event_time最接近funding_time的数据
                df_funding['funding_time'] = df_funding['funding_time'].astype('int64')
                df_funding = df_funding.drop_duplicates(subset=['funding_time'], keep='last')
                # 2025-03-06T15:34:02.584+08:00 -> timestamp
                df_funding['event_time'] = pd.to_datetime(df_funding['event_time']).astype('int64') // 10 ** 6
                df_funding['local_time'] = df_funding['event_time']
                df_funding = df_funding.add_prefix(f'{ex}_')
                df_funding = df_funding.rename(columns={f"{ex}_event_time": "event_time"})
                df_funding = df_funding.sort_values('event_time')
                # dfs_funding.append(df_funding)
                current_funding = df_funding[f'{ex}_funding_rate'].iloc[-1] if not df_funding.empty else None
                # logger.info(f'OKX funding data: {dfs_funding}')
            else:
                logger.warning(f'OKX funding file not found: {okx_funding_file_path}')

        logger.info(f"[runtime_monitor][{now_hour}] Finished processing {exchange}.{symbol} funding file.")
        return exchange, df_depth, df_funding, current_funding

    # 使用线程池并发处理所有交易所
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(exchanges)) as executor:
        futures = {executor.submit(process_exchange, exchange): exchange for exchange in exchanges}
        for future in concurrent.futures.as_completed(futures):
            exchange = futures[future]
            try:
                exch, df_depth_ex, df_funding_ex, current_funding_ex = future.result()
                # if df_depth_ex is not None:
                #     dfs_depth.append(df_depth_ex)
                # if df_funding_ex is not None:
                #     dfs_funding.append(df_funding_ex)
                if exch == exchanges[0]:
                    dfs_depth[0] = df_depth_ex
                    dfs_funding[0] = df_funding_ex
                    current_ex0_funding = current_funding_ex
                elif exch == exchanges[1]:
                    dfs_depth[1] = df_depth_ex
                    dfs_funding[1] = df_funding_ex
                    current_ex1_funding = current_funding_ex
            except Exception as e:
                logger.error(f"Error processing exchange {exchange}: {e}", exc_info=True)
                send_lark_bot(lark_bot, message_content=f"Error processing exchange {exchange}: {e}", at_users=at_users)
    logger.info(f"[runtime_monitor][{now_hour}] Finished processing all exchanges for {symbol}.")

    # 计算特征
    if dfs_depth[0] is None or dfs_depth[1] is None:
        logger.warning(f'Skipping symbol {symbol} due to missing depth data')
        return None
    if dfs_funding[0] is None or dfs_funding[1] is None:
        logger.warning(f'Skipping symbol {symbol} due to missing funding data')
        return None

    try:
        spread_features = calc_spread(envs, symbol, dfs_depth, fee_rate, slippage_duration_days=slippage_duration_days)
        logger.info(f"[runtime_monitor][{now_hour}] Calculated spread features for {symbol}.")
        if spread_features is None:
            logger.warning(f'Skipping symbol {symbol} due to missing spread features')
            return None
        theo_slip = spread_features['theo_slip'].iloc[0]
        q_range = spread_features['q_range'].iloc[0]
        funding_features = calc_funding(symbol, dfs_funding, theo_slip, q_range, fr_threshold_1day, fr_threshold_nday,
                                        fr_threshold_latest, fr_slippage_threshold)
        logger.info(f"[runtime_monitor][{now_hour}] Calculated funding features for {symbol}.")

        if funding_features is None or spread_features is None:
            logger.warning(f'Skipping symbol {symbol} due to missing funding or spread features')
            return None
        earn_mean_abs = funding_features['earn_mean_abs'].iloc[0]
        q_range_net_slippage = spread_features['q_range_net_slippage'].iloc[0]
        hist_1min_feature = calc_hist_1min_feature(symbol,theo_slip,fr_slippage_threshold,q_range,earn_mean_abs, dfs_funding, q_range_net_slippage, look_back_days=sr_lookback_days)
        logger.info(f"[runtime_monitor][{now_hour}] Calculated historical 1min features for {symbol}.")

        # 资金费率调整
        do_indicator = funding_features['do_indicator'].iloc[0] or funding_features['do_indicator_low'].iloc[0] or \
            hist_1min_feature['do_indicator_MMR'].iloc[0]
        q_profit_high_with_fr, q_profit_low_with_fr = deflation(
            spread_features['q_profit_high'].iloc[0],
            spread_features['q_profit_low'].iloc[0],
            spread_features['q_be_high'].iloc[0],
            spread_features['q_be_low'].iloc[0],
            spread_features['q_lower_tm'].iloc[0],
            spread_features['q_upper_tm'].iloc[0],
            current_ex0_funding,
            current_ex1_funding,
            do_indicator,
            funding_features['mode_sign_binary'].iloc[0],
            funding_features['mode_prop_binary'].iloc[0],
            funding_features['earn_mean_abs'].iloc[0],
            funding_features['earn_mean_abs_1day'].iloc[0],
            spread_features['IsTick1Tick2Equal'].iloc[0],
            spread_features['tick_size_factor'].iloc[0],
            funding_features['ex0_fr_interval'].iloc[0],
            funding_features['ex1_fr_interval'].iloc[0],
            same_deflation_coeff=same_deflation_coeff,
            cover_deflation_coeff=cover_deflation_coeff
        )
        logger.info(f"[runtime_monitor][{now_hour}] Applied deflation for {symbol}.")
        logger.info(f"Deflation applied: q_profit_high_with_fr={q_profit_high_with_fr}, "
                    f"q_profit_low_with_fr={q_profit_low_with_fr}")
        if (q_profit_high_with_fr is None or q_profit_low_with_fr is None
                or np.isnan(q_profit_high_with_fr) or np.isnan(q_profit_low_with_fr)):
            logger.warning(f'Skipping symbol {symbol} due to deflation resulting in None values')
            return None

        # 精度调整
        open_threshold = adjust_threshold(q_profit_high_with_fr,
                                          spread_features['tick_size_factor'].iloc[0],
                                          spread_features['IsTick1Tick2Equal'].iloc[0],
                                          do_indicator=do_indicator,
                                          mode_sign_binary=funding_features['mode_sign_binary'].iloc[0],
                                          side="open",
                                          large_tick_threshold=large_tick_threshold
                                          )
        close_threshold = adjust_threshold(q_profit_low_with_fr,
                                           spread_features['tick_size_factor'].iloc[0],
                                           spread_features['IsTick1Tick2Equal'].iloc[0],
                                           do_indicator=do_indicator,
                                           mode_sign_binary=funding_features['mode_sign_binary'].iloc[0],
                                           side="close",
                                           large_tick_threshold=large_tick_threshold
                                           )
        logger.info(f"[runtime_monitor][{now_hour}] Adjusted thresholds for {symbol}.")
        spread_features['open_threshold'] = open_threshold
        spread_features['close_threshold'] = close_threshold
        logger.info(f"Adjusted thresholds: open={open_threshold}, close={close_threshold}")

        spread_features['q_profit_high_with_fr'] = q_profit_high_with_fr
        spread_features['q_profit_low_with_fr'] = q_profit_low_with_fr
        funding_features['current_ex0_funding'] = current_ex0_funding
        funding_features['current_ex1_funding'] = current_ex1_funding

        if funding_features is not None and spread_features is not None:
            # 合并特征
            combined_df = pd.concat([spread_features, funding_features, hist_1min_feature], axis=1)
            # combined_df['symbol'] = symbol
            # combined_df['calculation_time'] = datetime.now().isoformat()
            logger.info(f'Completed processing for symbol: {symbol}')
            return combined_df

        logger.warning(f'Skipping symbol {symbol} due to missing data')
        return None
    except Exception as e:
        logger.info(f'Error calculating features for symbol {symbol}: {e}', exc_info=True)
        send_lark_bot(lark_bot, message_content=f"Error calculating features for {symbol}: {e}", at_users=at_users)
        return None


def get_scaled_df(
    final_df: pd.DataFrame,
    weights: dict,
    top_n: int,
    type: Literal['spread', 'fr'] = 'spread',
    open_interest_threshold: float = 2_000_000,
    insurance_fund_threshold: float = 500_000,
) -> pd.DataFrame:
    """
    计算排名时对精度进行过滤，对于tick_size_factor大于20的直接排除
    """

    scaled_df = final_df.copy()

    eligible_mask = (
        (scaled_df['open_interest0'] > open_interest_threshold) &
        (scaled_df['open_interest1'] > open_interest_threshold) &
        (scaled_df['InsuranceFund0'] > insurance_fund_threshold) &
        (scaled_df['InsuranceFund1'] > insurance_fund_threshold) &
        (~scaled_df['IsExtremeSpread']) &
        (~scaled_df['IsExtremeFr']) &
        (~scaled_df['IsExtremeFrSpread']) &
        (scaled_df['tick_size_factor'] <= 20)
    )
    # gateio没有保险基金数据，跳过该条件
    if 'gateio' in exchanges:
        eligible_mask = (
            (scaled_df['open_interest0'] > open_interest_threshold) &
            (scaled_df['open_interest1'] > open_interest_threshold) &
            (~scaled_df['IsExtremeSpread']) &
            (~scaled_df['IsExtremeFr']) &
            (~scaled_df['IsExtremeFrSpread']) &
            (scaled_df['tick_size_factor'] <= 20)
        )

    df_elig = scaled_df.loc[eligible_mask].copy()
    logger.info(f'Number of eligible rows for {type} ranking: {len(df_elig)}, symbols: {df_elig["Symbol"].tolist()}')

    scaled_df[f'scored_{type}'] = np.nan
    scaled_df[f'ArbRank_{type}'] = np.nan
    scaled_df[f'is_candidate_{type}'] = False

    # 没有 eligible，直接返回
    if df_elig.empty:
        return scaled_df.sort_values(f'ArbRank_{type}', na_position='last')

    feat_cols = [c for c in weights.keys() if c in df_elig.columns]

    if not feat_cols:
        # 没有特征，直接返回
        return scaled_df.sort_values(f'ArbRank_{type}', na_position='last')

    df_elig[feat_cols] = (
        df_elig[feat_cols]
        .replace([np.inf, -np.inf], 0)
        .fillna(0)
    )

    # 保存归一化前的原始 q_range_net_slippage 用于后续筛选
    df_elig['q_range_net_slippage_original'] = df_elig['q_range_net_slippage'].copy()

    # 归一化
    scaler = MinMaxScaler()
    df_elig[feat_cols] = scaler.fit_transform(df_elig[feat_cols])

    score_col = f'scored_{type}'
    df_elig[score_col] = 0.0

    for col, weight in weights.items():
        if col in df_elig.columns:
            df_elig[score_col] += df_elig[col] * weight

    rank_col = f'ArbRank_{type}'
    df_elig[rank_col] = (
        df_elig[score_col]
        .rank(method='min', ascending=False)
        .astype(int)
    )

    if type == 'fr':
        # do_indicator 系列列在拼表/读写后可能变成 float/NaN，直接用 | 会报 float|float。
        # 这里仅在使用前转成布尔：缺失当 False，非0当 True。
        ind_mask = (
            df_elig['do_indicator'].fillna(0).astype(float).ne(0) |
            df_elig['do_indicator_low'].fillna(0).astype(float).ne(0) |
            df_elig['do_indicator_MMR'].fillna(0).astype(float).ne(0)
        )
        df_elig[f'is_candidate_{type}'] = (
            (df_elig[rank_col] <= top_n) &
            (ind_mask)
        )
        logger.info(f'FR candidates based on do_indicator: {df_elig[df_elig[f"is_candidate_{type}"]]["Symbol"].tolist()}')
    else:
        df_elig[f'is_candidate_{type}'] = (
            (df_elig[rank_col] <= top_n) &
            (df_elig['rank_by_amount'] <= 80) &
            (df_elig['q_range_net_slippage_original'] >= target_ret_spread)
        )
        # 提取变量避免 f-string 语法冲突
        spread_candidates = df_elig[df_elig[f'is_candidate_{type}']]['Symbol'].tolist()
        rank_satisfied = df_elig[df_elig[rank_col] <= top_n]['Symbol'].tolist()
        amount_satisfied = df_elig[df_elig['rank_by_amount'] <= 80]['Symbol'].tolist()
        slippage_satisfied = df_elig[df_elig['q_range_net_slippage_original'] >= target_ret_spread]['Symbol'].tolist()
        
        logger.info(f'Spread candidates: {spread_candidates}')
        logger.info(f'  rank_col: {rank_col}, top_n: {top_n}, target_ret_spread: {target_ret_spread}')
        logger.info(f'  Symbols with {rank_col} <= {top_n}: {rank_satisfied}')
        logger.info(f'  Symbols with rank_by_amount <= 80: {amount_satisfied}')
        logger.info(f'  Symbols with q_range_net_slippage >= {target_ret_spread}: {slippage_satisfied}')

    # merge 回原表
    scaled_df.loc[eligible_mask, score_col] = df_elig[score_col]
    scaled_df.loc[eligible_mask, rank_col] = df_elig[rank_col]
    scaled_df.loc[eligible_mask, f'is_candidate_{type}'] = df_elig[f'is_candidate_{type}']

    return scaled_df.sort_values(rank_col, na_position='last')


def assign_position_limits(
    df, blacklist, capital, q_range_threshold, amount_threshold_24h, earn_mean_abs_threshold,
    AllowSpreadFrNotEqual=False,
    small_coin_allocation_ratio=0.5,  # 新增：小币资金占比，默认 50%
):
    """
    为每个 symbol 分配仓位，返回一个 dict，key 是 symbol，value 是 pos_limit

    口径保持不变：
    - pos_limit 是“单腿仓位上限”（你原来用 capital/2 做池子）
    - return 的第二个值仍然是 2*total_position
    """
    # ====== 大币配置（按你给的）======
    MAJOR_WEIGHT = {
        'ETH-USDT': 6.0,
        'BTC-USDT': 3.0,
        'SOL-USDT': 0.5,
        'DOGE-USDT': 0.45,
        'FARTCOIN-USDT': 0.05,
        'XRP-USDT': 0.05,
    }
    MAJOR_CAP_RATIO = {
        'BTC-USDT': 1.5,
        'ETH-USDT': 1.5,
        'SOL-USDT': 0.5,
        'DOGE-USDT': 0.5,
        'FARTCOIN-USDT': 0.10,
        'XRP-USDT': 0.10,
    }

    def _allocate_with_caps_waterfilling(budget, candidates, weight_map, cap_map, eps=1e-12):
        """在 candidates 内按权重分配 budget，满足 cap 上限，触顶后回流继续分配。"""
        alloc = {s: 0.0 for s in candidates}
        remain = float(budget)

        active = []
        for s in candidates:
            w = float(weight_map.get(s, 0.0))
            c = float(cap_map.get(s, 0.0))
            if w > 0 and c > 0:
                active.append(s)

        if remain <= eps or not active:
            return alloc, max(0.0, remain)

        if len(active) == 1:
            s = active[0]
            x = min(remain, cap_map[s])
            alloc[s] = x
            remain -= x
            return alloc, max(0.0, remain)

        while remain > eps and active:
            total_w = sum(weight_map[s] for s in active)
            if total_w <= eps:
                break

            raw = {s: remain * (weight_map[s] / total_w) for s in active}

            capped = []
            for s in active:
                cap_rem = cap_map[s] - alloc[s]
                if raw[s] > cap_rem + eps:
                    capped.append(s)

            if not capped:
                for s in active:
                    alloc[s] += raw[s]
                remain = 0.0
                break

            used = 0.0
            for s in capped:
                cap_rem = cap_map[s] - alloc[s]
                if cap_rem > eps:
                    alloc[s] += cap_rem
                    used += cap_rem

            remain -= used
            active = [s for s in active if s not in capped]

        return alloc, max(0.0, remain)

    # ====== 原有逻辑开始 ======
    pos_limit_dict = {}
    blacklist = [f'{s}-USDT' for s in blacklist]
    df_filtered = df[~df['Symbol'].isin(blacklist)].copy()

    n = top_n_spread + top_n_fr
    cap_abs = capital * 0.025  # 小币单腿上限（保持你的口径）

    # 小币池 / 大币池（单腿资金池口径）
    alt_target_pool = capital * small_coin_allocation_ratio / 2.0
    major_target_pool = capital * (1.0 - small_coin_allocation_ratio) / 2.0

    sel = df_filtered['is_candidate'].fillna(False).copy()

    needed = max(0, n - int(sel.sum()))
    if needed > 0:
        pool_fr = df_filtered[
            (~sel) & (df_filtered['do_indicator'] | df_filtered['do_indicator_low'] | df_filtered['do_indicator_MMR']) & (
                df_filtered['IsFrIntervalEqual']) & (df_filtered['ArbRank_fr'] <= 50)
        ].sort_values('ArbRank_fr')
        add_idx = pool_fr.head(needed).index
        sel.loc[add_idx] = True

    needed = max(0, n - int(sel.sum()))
    if needed > 0:
        pool_sp = df_filtered[
            (~sel) &
            (df_filtered['q_range_net_slippage'] > target_ret_spread) &
            (df_filtered['tick_size_factor'] < 0.0005) &
            (df_filtered['theo_slip'] < 0.0005)
        ].sort_values('ArbRank_spread')
        add_idx2 = pool_sp.head(needed).index
        sel.loc[add_idx2] = True

    df_filtered['is_candidate'] = sel

    default_interest = [6000000, 4000000, 2000000, 1000000]
    if 'gateio' in exchanges:
        default_interest = [750000, 500000, 250000, 100000]
    if 'bybit5' in exchanges:
        default_interest = [1000000, 500000, 300000, 100000]

    # 收集“符合条件的大币候选”（通过风控后才算）
    major_candidates_ok = []

    for _, row in df_filtered.iterrows():
        symbol = row['Symbol']
        is_major = symbol in MAJOR_WEIGHT

        do_indicator = row['do_indicator'] or row['do_indicator_low'] or row['do_indicator_MMR']

        # ====== 先按小币逻辑算一个 pos_limit（大币先占位为 0，后面统一 water-filling）======
        if row['is_candidate']:
            if is_major:
                # 大币先不在这里定量，先给一个“正值占位”让下面统一风控规则能把它打成 0
                pos_limit = 1.0
            else:
                # 小币 base：用 alt_target_pool（单腿）均分到 n 个
                # 你原来是 capital/(2*n)，现在把“池子”换成 alt_target_pool
                pos_limit = alt_target_pool / n

                if row['OpenInterestCapRatio'] > 0.3 or min(row['open_interest0'], row['open_interest1']) < default_interest[0]:
                    pos_limit = alt_target_pool / (2 * n)
                if row['OpenInterestCapRatio'] > 0.8 or min(row['open_interest0'], row['open_interest1']) < default_interest[1]:
                    pos_limit = alt_target_pool / (4 * n)
                if row['OpenInterestCapRatio'] > 2 or min(row['open_interest0'], row['open_interest1']) < default_interest[2]:
                    pos_limit = alt_target_pool / (10 * n)
                if row['OpenInterestCapRatio'] > 4 or min(row['open_interest0'], row['open_interest1']) < default_interest[3]:
                    pos_limit = alt_target_pool / (100 * n)

                if not do_indicator:
                    if row['q_range_5_95'] > q_range_threshold:
                        pos_limit = alt_target_pool / (10 * n)
                    if pd.isna(row['slippage']):
                        pos_limit = alt_target_pool / (10 * n)
                    if (row['earn_mean_abs_1day'] > earn_mean_abs_threshold):
                        pos_limit = alt_target_pool / (10 * n)
                    if (row['ex0_fr_interval'] != row['ex1_fr_interval']):
                        pos_limit = alt_target_pool / (10 * n)
                    if row['q_range_by_tick'] < 3:
                        pos_limit = alt_target_pool / (20 * n)

        elif row['ArbRank_spread'] <= 20:
            # 非候选但 spread rank 靠前：仍给很小仓位（按你原来逻辑，缩放到 alt_target_pool 口径）
            pos_limit = alt_target_pool / (100 * n) if (not is_major) else 0.0
        else:
            pos_limit = 0.0

        # ====== 统一风控（保持你原来规则）======
        if (do_indicator is False) and (row['ex0_24h_usdt'] < amount_threshold_24h or row['ex1_24h_usdt'] < amount_threshold_24h):
            pos_limit = 0.0

        if exchanges[1] == 'binance' and row['InsuranceFund1'] < 3000000:
            pos_limit = 0.0

        ratio1 = row['ex0_fr_interval'] / row['ex1_fr_interval']
        ratio2 = row['ex1_fr_interval'] / row['ex0_fr_interval']

        if AllowSpreadFrNotEqual:
            if (row['is_candidate_fr'] and (row['ex0_fr_interval'] != row['ex1_fr_interval'])) or (max(ratio1, ratio2) > 2):
                pos_limit = 0.0
        else:
            if row['ex0_fr_interval'] != row['ex1_fr_interval']:
                pos_limit = 0.0

        if row['q_range_5_95'] > 80:
            pos_limit = 0.0

        # ====== 写回 dict：大币先写 0，小币写 pos_limit ======
        if is_major:
            pos_limit_dict[symbol] = 0.0
            if row['is_candidate'] and pos_limit > 0:
                major_candidates_ok.append(symbol)
        else:
            pos_limit_dict[symbol] = float(pos_limit)

    # ====== 小币：补 leftover（你原逻辑，目标池子换成 alt_target_pool）======
    # 只统计小币的 allocated
    alt_allocated = sum(v for s, v in pos_limit_dict.items() if s not in MAJOR_WEIGHT and v > 0)
    leftover = max(0.0, alt_target_pool - alt_allocated)

    if leftover > 0:
        cand = df_filtered[df_filtered['is_candidate']].copy()

        # 注意：这里只补小币，不补大币
        cand = cand[~cand['Symbol'].isin(list(MAJOR_WEIGHT.keys()))]

        fr_group = cand.sort_values('ArbRank_fr').head(top_n_fr)

        fr_group = fr_group[
            (fr_group['do_indicator']) &
            (fr_group['IsFrIntervalEqual']) &
            (fr_group[['open_interest0', 'open_interest1']].min(axis=1) > default_interest[0]) &
            (fr_group['OpenInterestCapRatio'] < 0.3)
        ]

        eligible_syms = [s for s in fr_group['Symbol'].tolist() if pos_limit_dict.get(s, 0.0) > 0.0]
        m = len(eligible_syms)

        if m > 0:
            add_each = leftover / m
            for s in eligible_syms:
                pos_limit_dict[s] += add_each
                if pos_limit_dict[s] > cap_abs:
                    pos_limit_dict[s] = cap_abs

    # 小币最终使用
    alt_used_final = sum(v for s, v in pos_limit_dict.items() if s not in MAJOR_WEIGHT and v > 0)
    alt_leftover = max(0.0, alt_target_pool - alt_used_final)

    # ====== 大币：预算 = major_target_pool + 小币没用完的 leftover，然后 water-filling 分配 ======
    major_budget = major_target_pool + alt_leftover

    major_caps = {s: 0.5*capital * MAJOR_CAP_RATIO.get(s, 0.0) for s in MAJOR_WEIGHT.keys()}
    major_alloc, major_cash_left = _allocate_with_caps_waterfilling(
        budget=major_budget,
        candidates=major_candidates_ok,
        weight_map=MAJOR_WEIGHT,
        cap_map=major_caps,
    )

    # 写回大币分配
    for s in MAJOR_WEIGHT.keys():
        if s in pos_limit_dict:
            pos_limit_dict[s] = float(major_alloc.get(s, 0.0))

    # ====== 返回：保持你原来的输出格式 ======
    total_position = sum(pos_limit_dict.values())
    return pos_limit_dict, 2 * total_position



def round_effective(x: float, n: int = 1) -> float:
    """
    大于1保留整数，小于1保留有效数字
    如果结果大于1，就保留整数位，如果结果小于1大于0，就保留小数点中第一位不为0 的精度
    :param x:
    :return:
    """
    if x >= 1:
        return round(x)
    elif x > 0:
        return round(x, n - int(math.ceil(math.log10(abs(x)))))
    else:
        return 0.0


# 找到最新一个limit_pos文件
def find_latest_limit_pos_file(env_name):
    # 获取一级子目录，并按名称倒序排序
    subdirs = sorted(
        [d for d in os.listdir(output_dir) if os.path.isdir(os.path.join(output_dir, d))],
        reverse=True
    )

    for subdir in subdirs:
        subdir_path = os.path.join(output_dir, subdir)

        # 获取二级子目录，并按名称倒序排序
        inner_dirs = sorted(
            [d for d in os.listdir(subdir_path) if os.path.isdir(os.path.join(subdir_path, d))],
            reverse=True
        )

        for inner_dir in inner_dirs:
            inner_path = os.path.join(subdir_path, inner_dir)

            # 遍历文件，找包含 limit_pos 的
            for fname in sorted(os.listdir(inner_path), reverse=True):
                if f'{env_name}_limit_pos.csv' in fname:
                    return os.path.join(inner_path, fname)

    return None


def get_okx_delisted_instruments():
    """
    获取OKX下架的交易对
    :return: 所有交易对，下架的交易对列表
    """
    try:
        response = requests.get("https://www.okx.com/api/v5/public/instruments?instType=SWAP", timeout=5,
                                # proxies=proxies,
                                )
        response.raise_for_status()
        data = response.json().get('data', [])
        all_list = [item['instFamily'] for item in data]
        delist = [item['instFamily'] for item in data if item["expTime"] != ""]  # 只要有expTime就认为要下架或者即将下架
        return all_list, delist
    except Exception as e:
        logger.error(f"Failed to fetch delisted instruments: {str(e)}", exc_info=True)
        return [], []


def get_binance_perp_delist():
    """
    获取bn永续下架的交易对
    :return:
    """
    check_time = 24 * 60 * 60 * 1000  # 24小时内会下架的交易对
    try:
        url = 'https://fapi.binance.com/fapi/v1/exchangeInfo'
        response = requests.get(url, timeout=5,
                                proxies=proxies
                                )
        response.raise_for_status()
        data = response.json()

        if not data or 'symbols' not in data:
            raise ValueError("Invalid exchange info data")
        symbols = data['symbols']
        all_list = []
        delist_symbols = []
        current_time = int(time.time() * 1000)
        for symbol in symbols:
            baseAsset = symbol.get('baseAsset', "")
            quoteAsset = symbol.get('quoteAsset', "")
            symbol_name = f"{baseAsset}-{quoteAsset}"
            all_list.append(symbol_name)
            status = symbol.get('status', "")
            delivery_date = symbol.get('deliveryDate', 0)
            if status != 'TRADING' or delivery_date - current_time <= check_time:
                delist_symbols.append(symbol_name)
        return all_list, delist_symbols
    except (requests.RequestException, KeyError, ValueError) as e:
        logger.error(f"获取币安永续下架交易对失败: {e}")
        return [], []


# 历史仓位调整
def adjust_limit0(df, env_name, cf_env_dir, capital, mode_prop_binary_low_limit, earn_mean_abs_low_limit, fee_rate,spread_black_symbols):
    """
    对于 (当前分配仓位为 0 ) &  (持仓价值 > 某一阈值) & (过去一天资金费率损失超过某个阈值 |  mode_prop_binary_low < 0.7 | earn_mean_abs_low < 0.00015) 的币：
    考虑加速把这个仓位给清理出去，这种情况下的open_threshold和close_threshold的设置如下：

    获取某个币当前所有持仓的平均的开仓价差 AvgSrOpen (通过读取账户API的信息)

    定义示性变量:  SpreadSign = 1 if 当前持仓为long, else -1
    定义期望平仓价差 = AvgSrOpen + SpreadSign* (abs(手续费fee) + abs(实盘滑点theo_slip))

    根据当前持仓的Side为long或者short两种情况讨论：

    如果当前持仓为long的话, 只调整open_threshold, close_threshold保持不变
    If: 期望平仓价差 < q_80 ：不需要到80分位数就能平着出去, open_threshold = q_80
    elif：期望平仓价差 < 90分位数 ： 不需要到90分位数就能平着出去，open_threshold = 90分位数
    else:  open_threshold = 95分位数

    如果当前持仓为short的话, 只调整close_threshold, open_threshold保持不变
    If: 期望平仓价差 > q_20 ：不需要到20分位数就能平着出去, open_threshold = q_20
    elif：期望平仓价差 > 10分位数 ： 不需要到10分位数就能平着出去，close_threshold = 10分位数
    else:  close_threshold = 5分位数
    :param df:
    :param env_name:
    :param symbol:
    :return:
    """
    pos_limit_csv = find_latest_limit_pos_file(env_name)
    logger.info(f"Found position limit file for {env_name}: {pos_limit_csv}")
    if pos_limit_csv is None:
        logger.error(f"No limit position file found for {env_name}")
        return
    pos_limit_df = pd.read_csv(pos_limit_csv)
    # max_limit_pos = pos_limit_df['PositionLimit'].max()
    max_limit_pos = capital / (top_n_fr + top_n_spread)
    pos_threshold = capital * 0.005
    fr_threshold = -max_limit_pos * 0.00025  # 资金费率损失阈值, 2*最大的limit_pos*0.00025
    # pos_limit0 = {k: v for k, v in pos_limit_dict.items() if v == 0}
    pos_limit0 = {row['Symbol']: row['PositionLimit'] for _, row in pos_limit_df.iterrows() if
                  row['PositionLimit'] == 0}
    logger.info(
        f"Adjusting limits for {env_name}, found {pos_limit0} with zero position limit, max limit post: {max_limit_pos}")
    df_filtered = df[df['Symbol'].isin(pos_limit0.keys())]
    warn_msg = {}
    for _, row in df_filtered.iterrows():
        symbol = row['Symbol'].replace('-', '')
        ccy = symbol.removesuffix('USDT')
        if ccy in spread_black_symbols:
            logger.info(f"{env_name} {symbol} is in spread black list, skipping adjustment")
            continue
        console_data = console.get_config(env_name, symbol)
        if console_data is None:
            logger.warning(f"No console data found for {env_name} {symbol}")
            continue
        current_pos1 = console_data.get('data', {}).get('detail', {}).get('current_pos1', 0)
        current_pos2 = console_data.get('data', {}).get('detail', {}).get('current_pos2', 0)
        current_open_threshold = console_data.get('data', {}).get('config', {}).get('queue', {}).get('open_threshold',
                                                                                                     0)
        current_close_threshold = console_data.get('data', {}).get('config', {}).get('queue', {}).get('close_threshold',
                                                                                                      0)
        if current_pos1 + current_pos2 < pos_threshold:
            logger.info(
                f"Position for {env_name} {symbol} is below threshold, skipping adjustment, current_pos1: {current_pos1}, current_pos2: {current_pos2}, pos_threshold: {pos_threshold}")
            continue
        if row['do_indicator'] or row['do_indicator_low'] or row['do_indicator_MMR']:
            logger.info(f"{env_name} {symbol} is an indicator pair, skipping adjustment")
            continue
        # 资金费率
        fr = 0
        try:
            fr_csv = os.path.join(cf_env_dir, f"{symbol}/{ccy}_USDT.funding.csv")
            if os.path.exists(fr_csv):
                df_fr = pd.read_csv(fr_csv)
                if not df_fr.empty:
                    # 取最近1天资金费率和
                    df_fr['ts'] = pd.to_datetime(df_fr['ts'])
                    df_fr = df_fr[df_fr['ts'] >= (datetime.now() - timedelta(days=1, hours=1))]
                    if not df_fr.empty:
                        fr = df_fr['funding'].sum()
                        logger.info(f"Total funding for {env_name} {symbol} in last 24 hours: {fr}")
            logger.info(
                f"Adjusting limits for {env_name} {symbol}, current_pos1: {current_pos1}, current_pos2: {current_pos2}, funding in last 24h: {fr}, fr_threshold: {fr_threshold}")
        except Exception as e:
            logger.error(f"Error reading funding file for {env_name} {symbol}: {e}", exc_info=True)
            send_lark_bot(lark_bot, message_content=f"Error reading funding file for {env_name} {symbol}: {e}", at_users=at_users)
            continue
        # mode_prop_binary_low_limit = 0.7
        # earn_mean_abs_low_limit = 0.00005
        # if (fr > fr_threshold
        #         and fr >= 0
        #         and row['mode_prop_binary_low'] >= mode_prop_binary_low_limit
        #         and row['earn_mean_abs_low'] >= earn_mean_abs_low_limit):
        #     continue
        if not (fr <= fr_threshold or
                fr < 0 and
                (row['mode_prop_binary_low'] < mode_prop_binary_low_limit) or
                row['earn_mean_abs_low'] < earn_mean_abs_low_limit):
            continue

        condition_msg = (f"\n\tfr: {fr}({fr < fr_threshold})"
                         f"\n\tmode_prop_binary_low: {row['mode_prop_binary_low']}({row['mode_prop_binary_low'] < mode_prop_binary_low_limit})"
                         f"\n\tearn_mean_abs_low: {row['earn_mean_abs_low']}({row['earn_mean_abs_low'] < earn_mean_abs_low_limit})")

        poss = console_data.get('data', {}).get('detail', {}).get("pos_list", {}).get('poss', [])
        if not poss:
            logger.info(f"No positions found for {env_name} {symbol}")
            continue
        df_poss = pd.DataFrame(poss)
        AvgSrOpen = df_poss['SrOpen'].mean() if not df_poss.empty else 0
        direction = df_poss['Direction'].mode()
        if direction.empty:
            logger.info(f"No direction found for {env_name} {symbol}")
            continue
        direction_mode = direction.iloc[0]
        SpreadSign = 1 if direction_mode == 'buy' else -1
        fee = fee_rate
        theo_slip = row['theo_slip']
        expected_close_spread = AvgSrOpen + SpreadSign * (abs(fee) + abs(theo_slip))  # 期望平仓价差
        q_90 = row['q_90_tm']
        q_10 = row['q_10_tm']
        q_95 = row['q_upper_tm']
        q_05 = row['q_lower_tm']
        q_20 = row['q_20_tm']
        q_80 = row['q_80_tm']
        q_01 = row['q_01_tm']
        q_99 = row['q_99_tm']
        oh = row['open_threshold']
        ch = row['close_threshold']

        adj_open, adj_close = False, False
        open_threshold, close_threshold = 0, 0
        change_msg = ""

        # 如果是过去一天资金费率损失超过某个阈值的情况
        if fr < fr_threshold:
            if direction_mode == 'buy':
                adj_open = True
                if expected_close_spread < q_80:
                    open_threshold = q_80
                    change_msg = f"\n\t{symbol}历史仓位为做多价差，按照80分位数设置阈值加速清仓"
                elif expected_close_spread < q_90:
                    open_threshold = q_90
                    change_msg = f"\n\t{symbol}历史仓位为做多价差，按照90分位数设置阈值加速清仓"
                else:
                    open_threshold = q_95
                    change_msg = f"\n\t{symbol}历史仓位为做多价差，按照95分位数设置阈值加速清仓"
                change_msg += (f"\n\t期望平仓价差: {expected_close_spread}"
                               f", 当前open_threshold: {current_open_threshold}, 修改后open_threshold: {open_threshold},"
                               f"q_80: {q_80}, q_90: {q_90}, q_95: {q_95}, q_99: {q_99}, 滑点: {theo_slip}, 手续费: {fee}")
            elif direction_mode == 'sell':
                adj_close = True
                if expected_close_spread > q_20:
                    close_threshold = q_20
                    change_msg = f"\n\t{symbol}历史仓位为做空价差，按照20分位数设置阈值加速清仓"
                elif expected_close_spread > q_10:
                    close_threshold = q_10
                    change_msg = f"\n\t{symbol}历史仓位为做空价差，按照10分位数设置阈值加速清仓"
                else:
                    close_threshold = q_05
                    change_msg = f"\n\t{symbol}历史仓位为做空价差，按照5分位数设置阈值加速清仓"
                change_msg += (f"\n\t期望平仓价差: {expected_close_spread}"
                               f", 当前close_threshold: {current_close_threshold}, 修改后close_threshold: {close_threshold},"
                               f"q_20: {q_20}, q_10: {q_10}, q_05: {q_05}, q_01: {q_01}, 滑点: {theo_slip}, 手续费: {fee}")
        elif fr < 0 and (row['mode_prop_binary_low'] < mode_prop_binary_low_limit or
                         row['earn_mean_abs_low'] < earn_mean_abs_low_limit):
            if direction_mode == 'buy':
                adj_open = True
                if expected_close_spread < q_80:
                    open_threshold = q_80
                    change_msg = f"\n\t{symbol}历史仓位为做多价差，按照80分位数设置阈值加速清仓"
                elif expected_close_spread < q_90:
                    open_threshold = q_90
                    change_msg = f"\n\t{symbol}历史仓位为做多价差，按照90分位数设置阈值加速清仓"
                elif expected_close_spread < q_95:
                    open_threshold = q_95
                    change_msg = f"\n\t{symbol}历史仓位为做多价差，按照95分位数设置阈值加速清仓"
                elif expected_close_spread < q_99:
                    open_threshold = q_99
                    change_msg = f"\n\t{symbol}历史仓位为做多价差，按照99分位数设置阈值加速清仓"
                elif expected_close_spread < oh:
                    open_threshold = expected_close_spread
                    change_msg = f"\n\t{symbol}历史仓位为做多价差，按照理论平仓价差设置阈值加速清仓"
                else:
                    open_threshold = oh
                    change_msg = f"\n\t{symbol}历史仓位为做多价差，但理论平仓价差设置过高，不进行改变"
                change_msg += (f"\n\t期望平仓价差: {expected_close_spread}"
                               f", 当前open_threshold: {current_open_threshold}, 修改后open_threshold: {open_threshold},"
                               f"q_80: {q_80}, q_90: {q_90}, q_95: {q_95}, q_99: {q_99}, 滑点: {theo_slip}, 手续费: {fee}")
            elif direction_mode == 'sell':
                adj_close = True
                if expected_close_spread > q_20:
                    close_threshold = q_20
                    change_msg = f"\n\t{symbol}历史仓位为做空价差，按照20分位数设置阈值加速清仓"
                elif expected_close_spread > q_10:
                    close_threshold = q_10
                    change_msg = f"\n\t{symbol}历史仓位为做空价差，按照10分位数设置阈值加速清仓"
                elif expected_close_spread > q_05:
                    close_threshold = q_05
                    change_msg = f"\n\t{symbol}历史仓位为做空价差，按照5分位数设置阈值加速清仓"
                elif expected_close_spread > q_01:
                    close_threshold = q_01
                    change_msg = f"\n\t{symbol}历史仓位为做空价差，按照1分位数设置阈值加速清仓"
                elif expected_close_spread > ch:
                    close_threshold = expected_close_spread
                    change_msg = f"\n\t{symbol}历史仓位为做空价差，按照理论平仓价差设置阈值加速清仓"
                else:
                    close_threshold = ch
                    change_msg = f"\n\t{symbol}历史仓位为做空价差，但理论平仓价差设置过高，不进行改变"
                change_msg += (f"\n\t期望平仓价差: {expected_close_spread}"
                               f", 当前close_threshold: {current_close_threshold}, 修改后close_threshold: {close_threshold},"
                               f"q_20: {q_20}, q_10: {q_10}, q_05: {q_05}, q_01: {q_01}, 滑点: {theo_slip}, 手续费: {fee}")

        change = {
            "control": env_name,
            "symbol": symbol,
            "config": {
                "queue": {
                },
            },
        }
        if not adj_open and not adj_close:
            # 避免修改空参数报错
            continue
        if adj_open:
            change['config']['queue']['open_threshold'] = round(open_threshold / 10000, 6)
            change['config']['queue']['cancel_open_threshold'] = round(open_threshold / 10000, 6)
            warn_msg[
                env_name] = f"{warn_msg.get(env_name, '')}\n{symbol}\n\topen_threshold={round(open_threshold / 10000, 6)}\n\tfr={fr}\n\tfr_threshold={fr_threshold}\n\tAvgSrOpen={AvgSrOpen}{condition_msg}{change_msg}"
        if adj_close:
            change['config']['queue']['close_threshold'] = round(close_threshold / 10000, 6)
            change['config']['queue']['cancel_close_threshold'] = round(close_threshold / 10000, 6)
            warn_msg[
                env_name] = f"{warn_msg.get(env_name, '')}\n{symbol}\n\tclose_threshold={round(close_threshold / 10000, 6)}\n\tfr={fr}\n\tfr_threshold={fr_threshold}\n\tAvgSrOpen={AvgSrOpen}{condition_msg}{change_msg}"
        response = console.request_update_config(change=change)
        if response:
            logger.info(
                f"Updated {env_name} {symbol} thresholds: open={round(open_threshold / 10000, 6)}, close={round(close_threshold / 10000, 6)}, resp={response}")
        else:
            logger.error(f"Failed to update {env_name} {symbol} thresholds, resp={response}")

    for env_name, msg in warn_msg.items():
        if len(msg) > 0:
            send_lark_bot(lark_bot, message_content=f'双合约自动价差修改（亏损仓位加速清仓）[{env_name}]\n{msg}')


def gc_worker():
    """后台循环，每分钟执行一次 gc.collect()"""
    while True:
        time.sleep(60)  # 等待 60 秒
        unreachable = gc.collect()
        logger.info(f"[GC] 回收了 {unreachable} 个不可达对象")


def start_gc_timer():
    """启动后台 GC 线程"""
    t = threading.Thread(target=gc_worker, daemon=True)
    t.start()


if __name__ == "__main__":
    try:
        # 启动定时 GC
        start_gc_timer()

        console = Console()

        api_config = console.get_api_config()
        # 判断总控开关
        try:
            if not api_config.get('data', {}).get('global', {}).get('switch', False):
                logger.info("Global switch is off, exiting.")
                send_lark_bot(lark_bot, message_content=f'总控开关关闭，暂停自动化')
                exit()
        except Exception as e:
            logger.error(f"Error checking global switch: {e}", exc_info=True)
            logger.info(f"api_config: {api_config}")
            send_lark_bot(lark_bot, message_content=f'总控开关关闭，暂停自动化', at_users=at_users)
            exit()

        # 使用远程配置覆盖本地配置
        ex = f'{exchanges[0]}_{exchanges[1]}'
        fr_threshold_1day = api_config.get('data', {}).get('exs', {}).get(ex, {}).get('fr_threshold_1day',
                                                                                      fr_threshold_1day)
        fr_threshold_nday = api_config.get('data', {}).get('exs', {}).get(ex, {}).get('fr_threshold_nday',
                                                                                      fr_threshold_nday)
        fr_threshold_latest = api_config.get('data', {}).get('exs', {}).get(ex, {}).get('fr_threshold_latest',
                                                                                        0.00002)
        fr_slippage_threshold = api_config.get('data', {}).get('exs', {}).get(ex, {}).get('fr_slippage_threshold',
                                                                                          0.0006)
        fr_abs_threshold = api_config.get('data', {}).get('exs', {}).get(ex, {}).get('fr_abs_threshold',
                                                                                    0.002)
        open_interest_threshold = api_config.get('data', {}).get('exs', {}).get(ex, {}).get('open_interest_threshold',
                                                                                            2000000)
        insurance_fund_threshold = api_config.get('data', {}).get('exs', {}).get(ex, {}).get('insurance_fund_threshold',
                                                                                             500000)
        NegPosSpreadThreshold = api_config.get('data', {}).get('exs', {}).get(ex, {}).get('NegPosSpreadThreshold',
                                                                                  0.001)
        fr_threshold_nday_MMR = api_config.get('data', {}).get('exs', {}).get(ex, {}).get('fr_threshold_nday_MMR',
                                                                                          0.01)
        prop_threshold_MMR = api_config.get('data', {}).get('exs', {}).get(ex, {}).get('prop_threshold_MMR', 0.8)
        fee_rate = api_config.get('data', {}).get('exs', {}).get(ex, {}).get('fee_rate', 3.4)
        mode_prop_binary_low_limit = api_config.get('data', {}).get('exs', {}).get(ex, {}).get(
            'mode_prop_binary_low_limit',
            0.7)
        earn_mean_abs_low_limit = api_config.get('data', {}).get('exs', {}).get(ex, {}).get('earn_mean_abs_low_limit',
                                                                                            0.00005)
        prop_threshold = api_config.get('data', {}).get('exs', {}).get(ex, {}).get('prop_threshold', prop_threshold)
        symbols = api_config.get('data', {}).get('exs', {}).get(ex, {}).get('symbols', [])
        black_symbols = api_config.get('data', {}).get('exs', {}).get(ex, {}).get('black_symbols', [])
        spread_black_symbols = api_config.get('data', {}).get('exs', {}).get(ex, {}).get('spread_black_symbols', [])
        pos_black_symbols = api_config.get('data', {}).get('exs', {}).get(ex, {}).get('pos_black_symbols', [])
        target_ret_fr = api_config.get('data', {}).get('exs', {}).get(ex, {}).get('target_ret_fr', target_ret_fr)
        target_ret_spread = api_config.get('data', {}).get('exs', {}).get(ex, {}).get('target_ret_spread',
                                                                                      target_ret_spread)
        top_n_fr = api_config.get('data', {}).get('exs', {}).get(ex, {}).get('top_n_fr', top_n_fr)
        top_n_spread = api_config.get('data', {}).get('exs', {}).get(ex, {}).get('top_n_spread', top_n_spread)
        update_limit_pos_hour = api_config.get('data', {}).get('exs', {}).get(ex, {}).get('update_limit_pos_hour',
                                                                                          update_limit_pos_hour)
        slippage_duration_days = api_config.get('data', {}).get('exs', {}).get(ex, {}).get('slippage_duration_days',
                                                                                           14)
        envs = api_config.get('data', {}).get('exs', {}).get(ex, {}).get('envs', {})
        fr_long_short_ratio = api_config.get('data', {}).get('exs', {}).get(ex, {}).get('fr_long_short_ratio', 1)

        sr_lookback_days = api_config.get('data', {}).get('exs', {}).get(ex, {}).get('sr_lookback_days', 3)

        # 先写死
        dir_date_str = (datetime.now() - timedelta(hours=4)).strftime("%Y%m%d")
        date_str = datetime.now().strftime("%Y%m%d%H")
        # date_str = '20250606'
        # all_symbols = get_all_symbols()
        now_hour = datetime.now().hour

        logger.info(f"[runtime_monitor][{now_hour}] start calc_feature")

        # 去重
        symbols = list(set(symbols))
        symbols.sort()
        # all_symbols = [symbol + '-USDT' for symbol in symbols if symbol not in black_symbols]
        all_symbols = [symbol + '-USDT' for symbol in symbols]
        combined_features = []

        # 先写死
        # all_symbols = ['BTC-USDT', 'ETH-USDT', 'XRP-USDT', 'SOL-USDT', 'DOGE-USDT', 'TON-USDT', 'ONDO-USDT', 'AAVE-USDT',
        #               'ICP-USDT', 'TRX-USDT', 'XLM-USDT', 'IMX-USDT', 'WCT-USDT', 'TRUMP-USDT', 'BNB-USDT']
        # 创建线程池并发处理
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(1, len(all_symbols) + 1)) as executor:
            future_to_symbol = {executor.submit(process_symbol_data, sym, envs, fr_threshold_1day, fr_threshold_nday,
                                                fr_threshold_latest, fr_slippage_threshold, fee_rate,
                                                slippage_duration_days, sr_lookback_days): sym for sym in
                                all_symbols}

            for future in concurrent.futures.as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    result_df = future.result()
                    if result_df is not None:
                        combined_features.append(result_df)
                        logger.info(f'Processed {symbol} successfully')
                    else:
                        logger.warning(f'No valid data for {symbol}')
                except Exception as e:
                    logger.error(f"Error processing {symbol}: {str(e)}", exc_info=True)
                    send_lark_bot(lark_bot, message_content=f"Error processing {symbol}: {str(e)}", at_users=at_users)

        logger.info(f"[runtime_monitor][{now_hour}] finished all symbols processing")
        # 合并所有结果并保存
        if combined_features:
            final_df = pd.concat(combined_features, ignore_index=True)
            api_feature = calc_api_feature(all_symbols)
            if api_feature is not None:
                final_df = pd.merge(final_df, api_feature, on='Symbol', how='left')
            output_file_path = os.path.join(output_dir, f"{dir_date_str}/{date_str}/{date_str}_all_features_swap.csv")
            Path(output_file_path).parent.mkdir(parents=True, exist_ok=True)
            final_df.to_csv(output_file_path, index=False)
            logger.info(f'Saved features for {len(combined_features)} symbols to {output_file_path}')

            scaled_df = get_scaled_df(final_df, weights=spread_weights, top_n=top_n_spread, type='spread',
                                      open_interest_threshold=open_interest_threshold,
                                      insurance_fund_threshold=insurance_fund_threshold)
            scaled_df = get_scaled_df(scaled_df, weights=fr_weights, top_n=top_n_fr, type='fr',
                                      open_interest_threshold=open_interest_threshold,
                                      insurance_fund_threshold=insurance_fund_threshold)
            scaled_df['is_candidate'] = scaled_df['is_candidate_spread'] | scaled_df['is_candidate_fr']

            show_cols = ['Symbol', 'Date',
                         'tick_size_factor', 'theo_slip', 'slippage', 'slippage_env', 'slippage_env_24h', 'p_symm',
                         'q_profit_low', 'q_profit_high',
                         'p_symm_be', 'q_be_low', 'q_be_high',
                         'q_profit_high_with_fr', 'q_profit_low_with_fr', 'open_threshold',
                         'close_threshold', 
                         'q_lower_tm', 'q_upper_tm', 'q_10_tm', 'q_90_tm', 'q_20_tm', 'q_80_tm', 'q_01_tm', 'q_99_tm',
                         'q_lower_mm', 'q_upper_mm','q_lower_mt', 'q_upper_mt',
                         'current_ex0_funding', 'current_ex1_funding',
                         'q_range_net_slippage', 'q_range', 'q_range_5_95', 'q_range_by_tick', 'do_indicator',
                         'do_indicator_low','do_indicator_MMR', 'earn_mean_abs_low', 'mode_sign_low', 'mode_prop_low',
                         'mode_prop_binary_low','mode_prop_binary_MMR',
                         'q_range_net_tick_size_factor', 'ZCross_ratio', 'Mean_Crossings_ratio',
                         'Kurtosis', 'Half_Life', 'std', 'bpv', 'jump_share',
                         'IsTick1Tick2Equal', 'upper_long_event_count',
                         'lower_long_event_count', 'event_switch_avg_gap_ms',
                         'upper_avg_duration', 'lower_avg_duration', 'earn_mean_abs',
                         'earn_mean_abs_1day', 'profit_quantile', 'total_opps',
                         'mode_sign', 'mode_prop', 'mode_sign_binary',
                         'mode_prop_binary', 'ex0_fr_interval', 'ex1_fr_interval', 'IsFrIntervalEqual',
                         'mode_sign_1day',
                         'Exchange1', 'Exchange2', 'Count', 'p_threshold',
                         'z_threshold',
                         'scored_spread', 'scored_fr', 'ArbRank_spread', 'ArbRank_fr',
                         'ex0_24h_usdt', 'ex1_24h_usdt', 'total_24h_usdt', 'rank_by_amount', 'ex1_last_price',
                         'is_candidate', 'is_candidate_spread', 'is_candidate_fr',
                         'open_interest0', 'open_interest1', 'market_cap', 'OpenInterestCapRatio',
                         'InsuranceFund0', 'InsuranceFund1',
                         f'fr_spd_adj_50_{look_back_days}d_sum', f'fr_spd_adj_20_80_{look_back_days}d_sum',
                         f'fr_spd_adj_50_{look_back_days}d_sum_MMR', f'fr_spd_adj_20_80_{look_back_days}d_sum_MMR',
                         'IsExtremeSpread', 'IsExtremeFr', 'IsExtremeFrSpread',
                         ]
            # 只有okx-binance才有这些列
            if ('okx' in exchanges) and ('binance' in exchanges):
                scaled_df['earn_mean_abs_MMR'] = scaled_df['earn_mean_abs'] / scaled_df['average_real_MMR']
                scaled_df['earn_mean_abs_1day_MMR'] = scaled_df['earn_mean_abs_1day'] / scaled_df['average_real_MMR']

                show_cols += ['real_MMR_bn', 'real_MMR_okx', 'average_real_MMR', 'earn_mean_abs_MMR',
                              'earn_mean_abs_1day_MMR']
            scaled_df = scaled_df[show_cols]

            # 7. 保存带评分的最终结果
            scored_output_path = os.path.join(output_dir,
                                              f"{dir_date_str}/{date_str}/{date_str}_scored_features_swap.csv")
            scaled_df.to_csv(scored_output_path, index=False)
            logger.info(f'Saved scored features to {scored_output_path}')

            # 8. 打印候选交易对
            candidates = scaled_df[scaled_df['is_candidate_spread'] | scaled_df['is_candidate_fr']].copy()
            logger.info(f"Top {len(candidates)} candidate symbols:")
            # for _, row in candidates.iterrows():
            #     logger.info(f"Rank {row['ArbRank']}: {row['Symbol']} - Score: {row['score']:.2f}")

            logger.info(f"[runtime_monitor][{now_hour}] start environment position allocation")

            # 排除下线交易对,需要处理成BTC-USDT格式
            all_list = set()
            delist_symbols = set()
            if 'okx' in exchanges:
                all_list_okx, delist_symbols_okx = get_okx_delisted_instruments()
                all_list.update(all_list_okx)
                delist_symbols.update(delist_symbols_okx)
                logger.info(f"OKX delisted instruments: {delist_symbols_okx}")
            if 'binance' in exchanges:
                all_list_binance, delist_symbols_bn = get_binance_perp_delist()
                all_list.update(all_list_binance)
                delist_symbols.update(delist_symbols_bn)
                logger.info(f"Binance delisted perpetual instruments: {delist_symbols_bn}")
            # 排除不在所有交易对里面的交易对，以及下线交易对
            scaled_df = scaled_df[scaled_df['Symbol'].isin(all_list)].copy()
            if delist_symbols:
                logger.info(f"Total delisted symbols to exclude: {delist_symbols}")
                scaled_df = scaled_df[~scaled_df['Symbol'].isin(delist_symbols)].copy()
            else:
                logger.info("No delisted symbols found.")

            logger.info(
                f"[runtime_monitor][{now_hour}] after delist filtering, {len(scaled_df)} symbols remain for position allocation")

            # 多环境仓位分配
            for env_name, env_config in envs.items():
                # # TODO: 测试版本，只分配cfdc_mt_dc_pmpro_test
                # if env_name not in ['cfdc_dcpro21',]:
                #     continue

                logger.info(f"[runtime_monitor][{now_hour}] start processing environment: {env_name}")
                blacklist = env_config.get('blacklist', [])
                spread_black_symbols_env = env_config.get('spread_black_symbols', [])
                pos_black_symbols_env = env_config.get('pos_black_symbols', [])
                spread_black_symbols = list(set(spread_black_symbols + spread_black_symbols_env))
                pos_black_symbols = list(set(pos_black_symbols + pos_black_symbols_env))
                capital = env_config.get('capital', 100000)
                small_coin_allocation_ratio = env_config.get('small_coin_allocation_ratio', 0)
                if small_coin_allocation_ratio == 0 or small_coin_allocation_ratio == "":
                    small_coin_allocation_ratio = 0.5
                cf_env_dir = env_config.get('cf_env_dir', '')
                switch = env_config.get('switch', False)
                template_env = env_config.get('template_env', '')
                template_symbol = env_config.get('template_symbol', '')
                volume_limit = env_config.get('volume_limit', 300)
                volume_min = env_config.get('volume_min', 50)
                AllowSpreadFrNotEqual = env_config.get('AllowSpreadFrNotEqual', False)

                if not switch:
                    logger.info(f"Environment {env_name} is switched off, skipping.")
                    continue

                blacklist = list(set(blacklist + black_symbols))

                single_symbols = []
                # 排除单所交易对
                if 'okx' in exchanges:
                    single_symbols += get_single_exchange(env_name, 'okx')
                    logger.info(f"Single-exchange symbols for {env_name} from okx: {single_symbols}")
                if 'bybit5' in exchanges:
                    single_symbols += get_single_exchange(env_name, 'bybit')
                    logger.info(f"Single-exchange symbols for {env_name} from bybit5: {single_symbols}")
                if 'binance' in exchanges:
                    single_symbols += get_single_exchange(env_name, 'binance')
                    logger.info(f"Single-exchange symbols for {env_name} from binance: {single_symbols}")
                    # 排除Bn单所双合约
                    single_symbols += get_single_exchange(env_name, 'bnDc')
                    logger.info(f"Single-exchange symbols for {env_name}: {single_symbols}")
                # 排除特殊指定的环境
                single_symbols += get_single_exchange(env_name, 'spec')
                logger.info(f"Special single-exchange symbols for {env_name}: {single_symbols}")
                
                if single_symbols:
                    ccys = [s[:-4] for s in single_symbols if s.endswith('USDT')]
                    logger.info(f"Excluding single-exchange symbols for {env_name}: {single_symbols}")
                    blacklist += ccys
                logger.info(f"[runtime_monitor][{now_hour}] final blacklist for {env_name}: {blacklist}")

                pos_limit_dict, total_position = assign_position_limits(scaled_df, blacklist,
                                                                        capital,
                                                                        q_range_threshold,
                                                                        amount_threshold_24h, earn_mean_abs_threshold,
                                                                        AllowSpreadFrNotEqual, small_coin_allocation_ratio=small_coin_allocation_ratio)
                logger.info(f"Total position limit: {total_position}, per symbol limits: {pos_limit_dict}")
                logger.info(f"[runtime_monitor][{now_hour}] finished position allocation for environment: {env_name}")

                # 更新配置
                # 获取env已有的交易对
                symbols_dict = {}
                keys = console.get_keys(env_name)
                if keys is None:
                    logger.error(f"Failed to get config for env {env_name}")
                    send_lark_bot(lark_bot, message_content=f'获取配置失败，请检查控制台连接 {env_name}')
                    symbols_dict[env_name] = []
                    continue  # 先跳过这个环境
                else:
                    symbols_dict[env_name] = keys.get('data', [])

                # 获取保证金
                console_data = console.get_config(env_name, "BTCUSDT")
                if console_data is None:
                    logger.warning(f"No console data found for {env_name} BTCUSDT")
                account = console_data.get('data', {}).get('detail', {}).get('account', {}) if console_data else {}
                accountEquity = sum([float(v.get('accountEquity', 0)) for k, v in account.items()])  # 账户权益
                actualEquity = sum([float(v.get('actualEquity', 0)) for k, v in account.items()])  # 实际权益
                accountInitialMargin = sum(
                    [float(v.get('accountInitialMargin', 0)) for k, v in account.items()])  # 初始保证金
                # 判断是更新limit_pos1还是limit_pos_long
                is_limit_long_short = False
                if 'limit_pos_long' in console_data.get('data', {}).get('config', {}):
                    is_limit_long_short = True

                warn_msg_update = {}
                warn_msg_add = {}
                blacklist = [f'{s}-USDT' for s in blacklist]
                df_filtered = scaled_df[~scaled_df['Symbol'].isin(blacklist)].copy()

                # 添加的新币
                new_symbols = []
                symbol_is_running = {}
                for _, row in df_filtered.iterrows():
                    logger.info(f"[runtime_monitor][{now_hour}] Processing {env_name} {row['Symbol']}")
                    # Count > 100000才更新
                    if row['Count'] < 100000:
                        logger.info(f"Skipping {row['Symbol']} due to low Count: {row['Count']}")
                        continue
                    time.sleep(0.1)  # 降频
                    symbol = row['Symbol'].replace('-', '')
                    ccy = row['Symbol'].split('-')[0]
                    open_threshold = round(row['open_threshold'] / 10000, 6)
                    close_threshold = round(row['close_threshold'] / 10000, 6)
                    volume_open_min = round_effective(volume_min / row['ex1_last_price'] \
                                                          if row['ex1_last_price'] > 0 else 0)
                    volume_open_limit = round_effective(volume_limit / row['ex1_last_price'] \
                                                            if row['ex1_last_price'] > 0 else 0)
                    tick_size_factor = float(row['tick_size_factor'])

                    # 如果do_indicator 为 True 且
                    # 如果 open_threshold > fr_abs_threshold > close_threshold + 2*slippage 并且 mode_sign_binary == 1 并且current_ex0_funding-current_ex1_funding > fr_threshold_latest: 
                    # - 新版本：open_threshold, mt_open_threshold, mt_cancel_open_threshold, tm_open_threshold, tm_cancel_open_threshold, mm_open_threshold,mm_cancel_open_threshold 设置成 fr_abs_threshold
                    # - 旧版本 : open_threshold, cancel_open_threshold 设置成 fr_abs_threshold
                    
                    # 如果 close_threshold < - fr_abs_threshold < open_threshold - 2*slippage  并且 mode_sign_binary == -1 并且 current_ex0_funding-current_ex1_funding < -fr_threshold_latest : 
                    # - 新版本： close_threshold, mt_close_threshold, mt_cancel_close_threshold, tm_close_threshold, tm_cancel_close_threshold, mm_close_threshold,mm_cancel_close_threshold  设置成 -fr_abs_threshold
                    # - 旧版本： close_threshold, cancel_close_threshold 设置成 -fr_abs_threshold
                    do_indicator = df_filtered[df_filtered['Symbol'] == row['Symbol']]['do_indicator'].values
                    do_indicator_low = df_filtered[df_filtered['Symbol'] == row['Symbol']]['do_indicator_low'].values
                    do_indicator_MMR = df_filtered[df_filtered['Symbol'] == row['Symbol']]['do_indicator_MMR'].values
                    if len(do_indicator) == 0 or len(do_indicator) == 0 or len(do_indicator_MMR) == 0:
                        logger.error(f"Failed to get do_indicator for {symbol}, skipping")
                        warn_msg_update[env_name] = (f"{warn_msg_update.get(env_name, '')}"
                                                f"\n{symbol}: 获取do_indicator失败，跳过更新")
                        continue
                    condition1 = do_indicator[0] or do_indicator_low[0] or do_indicator_MMR[0]
                    condition2 = (open_threshold > fr_abs_threshold > close_threshold + 2 * row['theo_slip']/10000
                                  and row['mode_sign_binary'] == 1
                                  and row['current_ex0_funding'] - row['current_ex1_funding'] >= fr_threshold_latest)
                    condition3 = (close_threshold < -fr_abs_threshold < open_threshold - 2 * row['theo_slip']/10000
                                  and row['mode_sign_binary'] == -1
                                  and row['current_ex0_funding'] - row['current_ex1_funding'] <= -fr_threshold_latest)
                    warn_condition = None
                    if condition1 and condition2:
                        open_threshold = fr_abs_threshold
                        logger.info(f"Adjusting thresholds for {env_name} {symbol} due to indicator conditions: "
                                    f"new open_threshold={open_threshold}, new close_threshold={close_threshold}")
                        # 满足条件要lark at人, 有仓位才at
                        warn_condition = (f'双合约自动价差修改（指标触发开仓阈值调整）[{env_name}]\n'
                                        f'{symbol}\n'
                                        f'原open_threshold={round(row["open_threshold"] / 10000, 6)}, '
                                        f'原close_threshold={round(row["close_threshold"] / 10000, 6)}\n'
                                        f'新open_threshold={open_threshold}, '
                                        f'close_threshold={close_threshold}\n'
                                        f'do_indicator={condition1}, '
                                        f'当前资金费率差={row["current_ex0_funding"] - row["current_ex1_funding"]}, '
                                        f'fr_threshold_latest={fr_threshold_latest}')
                    elif condition1 and condition3:
                        close_threshold = -fr_abs_threshold
                        logger.info(f"Adjusting thresholds for {env_name} {symbol} due to indicator conditions: "
                                    f"new open_threshold={open_threshold}, new close_threshold={close_threshold}")
                        # 满足条件要lark at人, 有仓位才at
                        warn_condition = (f'双合约自动价差修改（指标触发平仓阈值调整）[{env_name}]\n'
                                                       f'{symbol}\n'
                                                       f'原open_threshold={round(row["open_threshold"] / 10000, 6)}, '
                                                       f'原close_threshold={round(row["close_threshold"] / 10000, 6)}\n'
                                                       f'新open_threshold={open_threshold}, '
                                                       f'close_threshold={close_threshold}\n'
                                                       f'do_indicator={condition1}, '
                                                       f'当前资金费率差={row["current_ex0_funding"] - row["current_ex1_funding"]}, '
                                                       f'fr_threshold_latest={fr_threshold_latest}')

                    if symbol in keys.get('data', []):
                        if ccy in spread_black_symbols:
                            logger.info(f"Skipping update for {env_name} {symbol} as it is in spread_black_symbols")
                            continue
                        # 修改
                        change = {
                            "control": env_name,
                            "symbol": symbol,
                            "config": {
                                "queue": {
                                    "open_threshold": open_threshold,
                                    "cancel_open_threshold": open_threshold,
                                    "close_threshold": close_threshold,
                                    "cancel_close_threshold": close_threshold
                                },
                                # "volume_open_min": volume_open_min,
                                # "volume_open_limit": volume_open_limit,
                                # "volume_close_min": volume_open_min,
                                # "volume_close_limit": volume_open_limit,
                            },
                            # "running": True,
                        }

                        console_data = console.get_config(env_name, symbol)
                        if console_data is not None:
                            symbol_is_running[symbol] = console_data.get('data', {}).get('running', False)
                            logger.info(f"Current running status for {env_name} {symbol}: {symbol_is_running[symbol]}")
                            # 查询仓位，有仓位再判断warn_condition
                            limit_pos_long = console_data.get('data', {}).get('config', {}).get('limit_pos_long', 0)
                            if limit_pos_long > 0 and warn_condition is not None:
                                send_lark_bot(lark_bot, message_content=warn_condition, at_users=at_users)
                        
                        # 每天北京时间0点自动化的时候，如果当前没有分配任何仓位且没有任何仓位，即pos_limit_long和pos_limit_short, current_long1, current_short1,current_long2,current_short2都是0，就把running改成OFF
                        if now_hour == 1:
                            limit_pos_long = float(console_data.get('data', {}).get('config', {}).get('limit_pos_long', 0))
                            limit_pos_short = float(console_data.get('data', {}).get('config', {}).get('limit_pos_short', 0))
                            current_long1 = float(console_data.get('data', {}).get('detail', {}).get('pos_list', {}).get('swap1_long_pos', {}).get("pos_amount",0))
                            current_short1 =  float(console_data.get('data', {}).get('detail', {}).get('pos_list', {}).get('swap1_short_pos', {}).get("pos_amount",0))
                            current_short2 = float(console_data.get('data', {}).get('detail', {}).get('pos_list', {}).get('swap2_short_pos', {}).get("pos_amount",0))
                            current_long2 =  float(console_data.get('data', {}).get('detail', {}).get('pos_list', {}).get('swap2_long_pos', {}).get("pos_amount",0))
                            
                            if (limit_pos_long == 0 and limit_pos_short == 0 and
                                    current_long1 == 0 and current_short1 == 0 and
                                    current_long2 == 0 and current_short2 == 0):
                                change['running'] = False
                                logger.info(f"Setting running=False for {env_name} {symbol} due to zero position and limit")


                        # mm参数更新，兼容两个版本。有mode字段则使用旧版，没有mode字段则使用新版
                        if (console_data is not None
                                and ('mode' in console_data.get('data', {}).get('config', {}))):
                            # 旧版：https://digifinex.sg.larksuite.com/wiki/WKLawvOB7iSAePkIM9hlz9CZgSe#share-KdtPd6CRPopueCxHW0tlOILpgQb
                            if console_data.get('data', {}).get('config', {}).get('mode', '') == 'Swap1MSwap2M':
                                # 自动修改的部分， mode = Swap1MSwap2M时才启用
                                change['config']['queue']['mm_open_threshold'] = round(row['q_upper_tm'] / 10000, 6)
                                change['config']['queue']['mm_cancel_open_threshold'] = round(row['q_upper_tm'] / 10000, 6)
                                change['config']['queue']['mm_close_threshold'] = round(row['q_lower_tm'] / 10000, 6)
                                change['config']['queue']['mm_cancel_close_threshold'] = round(row['q_lower_tm'] / 10000,
                                                                                               6)
                                change['config']['queue']['open_stable_threshold'] = round(row['q_upper_tm'] / 10000, 6)
                                change['config']['queue']['close_stable_threshold'] = round(row['q_lower_tm'] / 10000, 6)
                                logger.info(f"Updating MM thresholds for {env_name} {symbol}: "
                                            f"mm_open_threshold={round(row['q_upper_tm'] / 10000, 6)}, "
                                            f"mm_close_threshold={round(row['q_lower_tm'] / 10000, 6)}")

                        else:
                            # 新版：https://digifinex.sg.larksuite.com/wiki/WKLawvOB7iSAePkIM9hlz9CZgSe#share-AujRd3q8LoCdVBxWW8ilWZuJg9j
                            # tt
                            change['config']['queue']['open_threshold'] = open_threshold
                            change['config']['queue']['close_threshold'] = close_threshold
                            # mt
                            change['config']['queue']['mt_open_threshold'] = round(row['q_upper_mt'] / 10000, 6) if not do_indicator[0] and do_indicator_MMR[0] else open_threshold
                            change['config']['queue']['mt_close_threshold'] = round(row['q_lower_mt'] / 10000, 6) if not do_indicator[0] and do_indicator_MMR[0] else close_threshold
                            change['config']['queue']['mt_cancel_open_threshold'] = round(row['q_upper_mt'] / 10000, 6) if not do_indicator[0] and do_indicator_MMR[0] else open_threshold
                            change['config']['queue']['mt_cancel_close_threshold'] = round(row['q_lower_mt'] / 10000, 6) if not do_indicator[0] and do_indicator_MMR[0] else close_threshold
                            # tm
                            change['config']['queue']['tm_open_threshold'] = round(row['q_upper_tm'] / 10000, 6) if not do_indicator[0] and do_indicator_MMR[0] else open_threshold
                            change['config']['queue']['tm_close_threshold'] = round(row['q_lower_tm'] / 10000, 6) if not do_indicator[0] and do_indicator_MMR[0] else close_threshold
                            change['config']['queue']['tm_cancel_open_threshold'] = round(row['q_upper_tm'] / 10000, 6) if not do_indicator[0] and do_indicator_MMR[0] else open_threshold
                            change['config']['queue']['tm_cancel_close_threshold'] = round(row['q_lower_tm'] / 10000, 6) if not do_indicator[0] and do_indicator_MMR[0] else close_threshold
                            # mm
                            change['config']['queue']['mm_open_threshold'] = round(row['q_upper_mm'] / 10000, 6)
                            change['config']['queue']['mm_close_threshold'] = round(row['q_lower_mm'] / 10000, 6)
                            change['config']['queue']['mm_cancel_open_threshold'] = round(row['q_upper_mm'] / 10000, 6)
                            change['config']['queue']['mm_cancel_close_threshold'] = round(row['q_lower_mm'] / 10000, 6)
                            change['config']['queue']['open_stable_threshold'] = round(row['q_upper_mm'] / 10000, 6)
                            change['config']['queue']['close_stable_threshold'] = round(row['q_lower_mm'] / 10000, 6)

                            # 新版mm每天凌晨4点需要新增更新tm_*和mt_*的volume
                            if now_hour == update_limit_pos_hour:
                                # tm
                                change['config']['tm_volume_open_min'] = volume_open_min
                                change['config']['tm_volume_close_min'] = volume_open_min
                                change['config']['tm_volume_open_limit'] = volume_open_limit
                                change['config']['tm_volume_close_limit'] = volume_open_limit

                                # mt
                                change['config']['mt_volume_open_min'] = volume_open_min
                                change['config']['mt_volume_close_min'] = volume_open_min
                                change['config']['mt_volume_open_limit'] = volume_open_limit
                                change['config']['mt_volume_close_limit'] = volume_open_limit

                        # 若现在的小时位是北京时间凌晨4点，更新limit和min和running
                        if now_hour == update_limit_pos_hour:
                            # 旧版，以及新版都要更新
                            change['config']['volume_open_min'] = volume_open_min
                            change['config']['volume_open_limit'] = volume_open_limit
                            change['config']['volume_close_min'] = volume_open_min
                            change['config']['volume_close_limit'] = volume_open_limit
                            # 账户权益和实际权益较小的那个要大于初始保证金才自动打开
                            if min(accountEquity, actualEquity) > accountInitialMargin:
                                change['config']['open_allowed'] = True
                                change['config']['close_allowed'] = True
                            # if row['is_candidate']:
                            #     # not_running -> running才sleep
                            #     if not symbol_is_running.get(symbol, True):
                            #         change['running'] = True
                            #         symbol_is_running[symbol] = True
                            #         time.sleep(3)
                        response = console.request_update_config(change=change)
                        if response:
                            warn_msg_update[env_name] = (f"{warn_msg_update.get(env_name, '')}"
                                                         f"\n{symbol}"
                                                         f"\n\topen_threshold: {open_threshold}"
                                                         f"\n\tclose_threshold: {close_threshold}")
                            logger.info(f"Updated config for{env_name}.{symbol}: {response}")
                        else:
                            logger.error(f"Failed to update config for {env_name}.{symbol}")
                        # 改为单独起running
                        if now_hour == update_limit_pos_hour:
                            if row['is_candidate']:
                                if not symbol_is_running.get(symbol, True):
                                    time.sleep(3)
                                    run_change = {
                                        "control": env_name,
                                        "symbol": symbol,
                                        "running": True,
                                    }
                                    response = console.request_update_config(change=run_change)
                                    if response:
                                        logger.info(f"Set running=True for {env_name}.{symbol}: {response}")
                                        symbol_is_running[symbol] = True
                                    else:
                                        logger.error(f"Failed to set running=True for {env_name}.{symbol}")
                    else:
                        # 添加
                        new_symbols.append(symbol)
                        add = {
                            "control": env_name,
                            "symbol": symbol,
                            "templateEnv": template_env,
                            "templateSymbol": template_symbol,
                            "config": {
                                "queue": {
                                    "open_threshold": open_threshold,
                                    "cancel_open_threshold": open_threshold,
                                    "close_threshold": close_threshold,
                                    "cancel_close_threshold": close_threshold
                                },
                                "factor": {},
                                "volume_open_min": volume_open_min,
                                "volume_open_limit": volume_open_limit,
                                "volume_close_min": volume_open_min,
                                "volume_close_limit": volume_open_limit,
                            },
                            "running": False,
                        }
                        # 新增的币种设为false
                        symbol_is_running[symbol] = False
                        # mm参数初始化
                        # 有mode字段则使用旧版，没有mode字段则使用新版 (这里取得模板的配置)
                        if (console_data is not None
                                and ('mode' in console_data.get('data', {}).get('config', {}))):
                            # 旧版
                            # mm模式部分
                            add['config']['sr_mode'] = 2  # 0:Normal 1:SrMean 2:SrMedian
                            add['config']['queue']['mesecs'] = 10000
                            add['config']['queue']['abnormal_threshold'] = round(
                                max(0.0002, 2 * tick_size_factor / 10000), 6)
                            # 参数部分
                            add['config']['mm_open_volume'] = 25
                            add['config']['mm_close_volume'] = 25

                            add['config']['mm_1_open_tn'] = 1
                            add['config']['mm_2_open_tn'] = 1
                            add['config']['mm_1_close_tn'] = 1
                            add['config']['mm_2_close_tn'] = 1

                            add['config']['mm_1_open_rtn'] = 1
                            add['config']['mm_2_open_rtn'] = 1
                            add['config']['mm_1_close_rtn'] = 1
                            add['config']['mm_2_close_rtn'] = 1

                            add['config']['mm_open_mh_threshold'] = -0.001
                            add['config']['mm_close_mh_threshold'] = 0.001

                            add['config']['mm_1_open_mhtn'] = 1
                            add['config']['mm_2_open_mhtn'] = 1
                            add['config']['mm_1_close_mhtn'] = 1
                            add['config']['mm_2_close_mhtn'] = 1

                            add['config']['mm_1_open_mhrtn'] = 1
                            add['config']['mm_2_open_mhrtn'] = 1
                            add['config']['mm_1_close_mhrtn'] = 1
                            add['config']['mm_2_close_mhrtn'] = 1

                            add['config']['mm_open_th_threshold'] = -0.001
                            add['config']['mm_close_th_threshold'] = 0.001
                        else:
                            # 新版
                            add['config']['queue']['open_threshold'] = open_threshold
                            add['config']['queue']['close_threshold'] = close_threshold

                            add['config']['queue']['mt_open_threshold'] = open_threshold
                            add['config']['queue']['mt_close_threshold'] = close_threshold
                            add['config']['queue']['mt_cancel_open_threshold'] = open_threshold
                            add['config']['queue']['mt_cancel_close_threshold'] = close_threshold

                            add['config']['queue']['tm_open_threshold'] = open_threshold
                            add['config']['queue']['tm_close_threshold'] = close_threshold
                            add['config']['queue']['tm_cancel_open_threshold'] = open_threshold
                            add['config']['queue']['tm_cancel_close_threshold'] = close_threshold

                            add['config']['queue']['mm_open_threshold'] = open_threshold
                            add['config']['queue']['mm_close_threshold'] = close_threshold
                            add['config']['queue']['mm_cancel_open_threshold'] = open_threshold
                            add['config']['queue']['mm_cancel_close_threshold'] = close_threshold

                            add['config']['volume_open_min'] = volume_open_min
                            add['config']['volume_close_min'] = volume_open_min
                            add['config']['volume_open_limit'] = volume_open_limit
                            add['config']['volume_close_limit'] = volume_open_limit
                            add['config']['tt_concurrency_limit'] = 0

                            add['config']['tm_volume_open_min'] = volume_open_min
                            add['config']['tm_volume_close_min'] = volume_open_min
                            add['config']['tm_volume_open_limit'] = volume_open_limit
                            add['config']['tm_volume_close_limit'] = volume_open_limit
                            add['config']['tm_concurrency_limit'] = 0

                            add['config']['mt_volume_open_min'] = volume_open_min
                            add['config']['mt_volume_close_min'] = volume_open_min
                            add['config']['mt_volume_open_limit'] = volume_open_limit
                            add['config']['mt_volume_close_limit'] = volume_open_limit
                            add['config']['mt_concurrency_limit'] = 1

                            add['config']['sr_mode'] = 0  # 0:Normal 1:SrMean 2:SrMedian
                            add['config']['queue']['mesecs'] = 10000
                            add['config']['queue']['abnormal_threshold'] = round(
                                max(0.0002, 2 * tick_size_factor / 10000), 6)

                            add['config']['mm_open_volume'] = 25
                            add['config']['mm_close_volume'] = 25

                            add['config']['mm_1_open_tn'] = 1
                            add['config']['mm_2_open_tn'] = 1
                            add['config']['mm_1_close_tn'] = 1
                            add['config']['mm_2_close_tn'] = 1

                            add['config']['mm_1_open_rtn'] = 1
                            add['config']['mm_2_open_rtn'] = 1
                            add['config']['mm_1_close_rtn'] = 1
                            add['config']['mm_2_close_rtn'] = 1

                            add['config']['mm_open_mh_threshold'] = -0.001
                            add['config']['mm_close_mh_threshold'] = 0.001

                            add['config']['mm_1_open_mhtn'] = 1
                            add['config']['mm_2_open_mhtn'] = 1
                            add['config']['mm_1_close_mhtn'] = 1
                            add['config']['mm_2_close_mhtn'] = 1

                            add['config']['mm_1_open_mhrtn'] = 1
                            add['config']['mm_2_open_mhrtn'] = 1
                            add['config']['mm_1_close_mhrtn'] = 1
                            add['config']['mm_2_close_mhrtn'] = 1

                            add['config']['mm_open_th_threshold'] = -0.001
                            add['config']['mm_close_th_threshold'] = 0.001
                            add['config']['mm_concurrency_limit'] = 0

                            add['config']['total_limit'] = -1
                            add['config']['volume_threshold0'] = 1
                            add['config']['volume_threshold1'] = 0.9
                            add['config']['volume_threshold2'] = 0.9
                            add['config']['switch_swap_threshold'] = 0
                            add['config']['level1'] = 2
                            add['config']['level2'] = 2
                            add['config']['maker_type'] = 0
                            add['config']['spread_limit_num1'] = 20
                            add['config']['spread_limit_num2'] = 20
                            add['config']['open_allowed'] = True
                            add['config']['close_allowed'] = True
                            add['config']['long_funding_limit'] = -0.001
                            add['config']['short_funding_limit'] = 0.001
                            add['config']['tt_enabled'] = False
                            add['config']['mt_enabled'] = True
                            add['config']['tm_enabled'] = False
                            add['config']['mm_enabled'] = False

                            add['config']['factor']['imb1_threshold'] = 0
                            add['config']['factor']['imb2_threshold'] = 0
                            add['config']['factor']['colo_okx'] = True

                        time.sleep(5)
                        response = console.request_update_config(add=add)
                        if response:
                            warn_msg_add[env_name] = (f"{warn_msg_add.get(env_name, '')}"
                                                      f"\n{symbol}\n\topen_threshold: {open_threshold}\n\tclose_threshold: {close_threshold}"
                                                      f"\n\tvolume_open_min: {volume_open_min}")
                            logger.info(f"Updated config for {env_name}.{symbol}: {response}")
                        else:
                            logger.error(f"Failed to update config for {env_name}.{symbol}")

                        # add 要单独设置running: False
                        time.sleep(3)
                        run_change = {
                            "control": env_name,
                            "symbol": symbol,
                            "running": False,
                        }
                        response = console.request_update_config(change=run_change)
                        if response:
                            logger.info(f"Set running=False for {env_name}.{symbol}: {response}")
                        else:
                            logger.error(f"Failed to set running=True for {env_name}.{symbol}")
                    logger.info(f"[runtime_monitor][{now_hour}] Finished processing {env_name} {row['Symbol']}")
                logger.info(f"[runtime_monitor][{now_hour}] Finished all symbols for environment: {env_name}")

                for env_name, msg in warn_msg_update.items():
                    if len(msg) > 0:
                        send_lark_bot(lark_bot, message_content=f'双合约自动价差修改[{env_name}]\n{msg}')
                for env_name, msg in warn_msg_add.items():
                    if len(msg) > 0:
                        send_lark_bot(lark_bot, message_content=f'双合约自动价差新增[{env_name}]\n{msg}')

                # 若现在的小时位是北京时间凌晨4点，更新limit_pos
                warn_msg = {}
                logger.info(
                    f"Checking if position limits need to be updated for {env_name} at hour {now_hour}, "
                    f"config hour: {update_limit_pos_hour}, config.hour: {config.update_limit_pos_hour},"
                    f" is_limit_long_short: {is_limit_long_short}, is_running status: {symbol_is_running}")
                if now_hour == update_limit_pos_hour:
                    logger.info(
                        f"[runtime_monitor][{now_hour}] start updating position limits for environment: {env_name}")
                    for symbol, pos_limit in pos_limit_dict.items():
                        ccy = symbol.split('-')[0]
                        if ccy in pos_black_symbols:
                            logger.info(f"Skipping position limit update for {env_name} {symbol} as it is in pos_black_symbols")
                            continue
                        logger.info(f"[runtime_monitor][{now_hour}] Updating position limit for {env_name} {symbol}")
                        # 价差和资金费率的排名
                        spread_rank = scaled_df[scaled_df['Symbol'] == symbol]['ArbRank_spread'].values
                        fr_rank = scaled_df[scaled_df['Symbol'] == symbol]['ArbRank_fr'].values
                        # symbol = symbol.replace('-', '')
                        change = {}
                        if is_limit_long_short:
                            # 新版
                            # 1. 如果do_indicator 为 False, limit_pos_long, limit_pos_short均设置为pos_limit_dict对应Symbol的映射
                            # 2. 如果do_indicator 为 True, symbol的do_indicator 为True
                            #     1. 如果 mode_sign_low 为 -1, 则limit_pos_short分配为pos_limit_dict的映射*fr_long_short_ratio, limit_pos_long分配为pos_limit_dict的映射
                            #     2. 如果 mode_sign_low 为 1, 则limit_pos_long分配为pos_limit_dict的映射*fr_long_short_ratio,limit_pos_short分配为pos_limit_dict的映射
                            # 2026-02-05：改成判断mode_sign
                            # 新增：
                            # 3. 首先新增加参数：NegPosSpreadThreshold 到控制台的交易所层面，默认值为 0.001，分配仓位时进行判断
                            # If (mm_enabled == True and mm_open_threshold<-NegPosSpreadThreshold  ) 
                            # 或(mt_enabled == True and mt_open_threshold<-NegPosSpreadThreshold  )
                            # 或(tm_enabled == True and tm_open_threshold<-NegPosSpreadThreshold  )
                            # 或(tt_enabled == True and tt_open_threshold<-NegPosSpreadThreshold  ):  limit_pos_short设置为0
                            # If (mm_enabled == True and mm_close_threshold>NegPosSpreadThreshold  ) 
                            #     或(mt_enabled == True and mt_close_threshold>NegPosSpreadThreshold  )
                            #     或(tm_enabled == True and tm_close_threshold>NegPosSpreadThreshold  )
                            #     或(tt_enabled == True and tt_close_threshold>NegPosSpreadThreshold  ): limit_pos_long设置为0
                            do_indicator = df_filtered[df_filtered['Symbol'] == symbol]['do_indicator'].values
                            do_indicator_low = df_filtered[df_filtered['Symbol'] == symbol]['do_indicator_low'].values
                            do_indicator_MMR = df_filtered[df_filtered['Symbol'] == symbol]['do_indicator_MMR'].values
                            mode_sign = df_filtered[df_filtered['Symbol'] == symbol]['mode_sign'].values
                            if len(do_indicator) == 0 or len(do_indicator) == 0 or len(do_indicator_MMR) == 0 or len(mode_sign) == 0:
                                logger.error(f"Failed to get do_indicator or mode_sign for {symbol}, skipping")
                                warn_msg[env_name] = (f"{warn_msg.get(env_name, '')}"
                                                      f"\n{symbol}: 获取do_indicator或mode_sign失败，跳过更新")
                                continue
                            if not (do_indicator[0] or do_indicator_low[0] or do_indicator_MMR[0]):
                                change = {
                                    "control": env_name,
                                    "symbol": symbol.replace('-', ''),
                                    "config": {
                                        "limit_pos_long": pos_limit,
                                        "limit_pos_short": pos_limit,
                                    },
                                    # "running": True,
                                }
                            else:
                                if mode_sign[0] == -1:
                                    change = {
                                        "control": env_name,
                                        "symbol": symbol.replace('-', ''),
                                        "config": {
                                            "limit_pos_long": pos_limit,
                                            "limit_pos_short": int(pos_limit * fr_long_short_ratio),
                                        },
                                        # "running": True,
                                    }
                                elif mode_sign[0] == 1:
                                    change = {
                                        "control": env_name,
                                        "symbol": symbol.replace('-', ''),
                                        "config": {
                                            "limit_pos_long": int(pos_limit * fr_long_short_ratio),
                                            "limit_pos_short": pos_limit,
                                        },
                                        # "running": True,
                                    }
                                else:
                                    logger.error(
                                        f"Invalid mode_sign value for {symbol}: {mode_sign[0]}, skipping")
                                    warn_msg[env_name] = (f"{warn_msg.get(env_name, '')}"
                                                          f"\n{symbol}: mode_sign值无效，跳过更新")
                                    continue
                            if 'config' in change and pos_limit > 0:
                                # 读取当前配置，判断各个策略的enabled状态和threshold
                                console_data = console.get_config(env_name, symbol.replace('-', ''))
                                if console_data is not None:
                                    mm_enabled = console_data.get('data', {}).get('config', {}).get('mm_enabled', False)
                                    mt_enabled = console_data.get('data', {}).get('config', {}).get('mt_enabled', False)
                                    tm_enabled = console_data.get('data', {}).get('config', {}).get('tm_enabled', False)
                                    tt_enabled = console_data.get('data', {}).get('config', {}).get('tt_enabled', False)

                                    mm_open_threshold = console_data.get('data', {}).get('config', {}).get('queue', {}).get('mm_open_threshold', 0)
                                    mm_close_threshold = console_data.get('data', {}).get('config', {}).get('queue', {}).get('mm_close_threshold', 0)
                                    mt_open_threshold = console_data.get('data', {}).get('config', {}).get('queue', {}).get('mt_open_threshold', 0)
                                    mt_close_threshold = console_data.get('data', {}).get('config', {}).get('queue', {}).get('mt_close_threshold', 0)
                                    tm_open_threshold = console_data.get('data', {}).get('config', {}).get('queue', {}).get('tm_open_threshold', 0)
                                    tm_close_threshold = console_data.get('data', {}).get('config', {}).get('queue', {}).get('tm_close_threshold', 0)
                                    tt_open_threshold = console_data.get('data', {}).get('config', {}).get('queue', {}).get('tt_open_threshold', 0)
                                    tt_close_threshold = console_data.get('data', {}).get('config', {}).get('queue', {}).get('tt_close_threshold', 0)

                                    # 判断是否需要将limit_pos_long或limit_pos_short设置为0
                                    if ((mm_enabled and mm_open_threshold < -NegPosSpreadThreshold) or
                                        (mt_enabled and mt_open_threshold < -NegPosSpreadThreshold) or
                                        (tm_enabled and tm_open_threshold < -NegPosSpreadThreshold) or
                                        (tt_enabled and tt_open_threshold < -NegPosSpreadThreshold)):
                                        change['config']['limit_pos_short'] = 0
                                        logger.info(f"Setting limit_pos_short to 0 for {symbol} due to negative spread condition")
                                    if ((mm_enabled and mm_close_threshold > NegPosSpreadThreshold) or
                                        (mt_enabled and mt_close_threshold > NegPosSpreadThreshold) or
                                        (tm_enabled and tm_close_threshold > NegPosSpreadThreshold) or
                                        (tt_enabled and tt_close_threshold > NegPosSpreadThreshold)):
                                        change['config']['limit_pos_long'] = 0
                                        logger.info(f"Setting limit_pos_long to 0 for {symbol} due to positive spread condition")
                            # 调整交易所初始杠杆。limit_pos大于0才修改, 每次保存调整后的杠杆到csv，如果不一样才修改
                            if exchanges[0] == 'okx' and exchanges[1] == 'binance' and pos_limit > 0:
                                logger.info(f"Adjusting initial leverage for {env_name}.{change['symbol']} based on new position limits")
                                # if pos_limit <= 0:
                                #     logger.info(f"Position limit for {change['symbol']} is {pos_limit}, skipping leverage adjustment")
                                #     continue
                                # okx-bn才修改
                                max_pos_limit = max(change['config']['limit_pos_long'], change['config']['limit_pos_short']) * 2
                                # 查询对应仓位的理论杠杆
                                try:
                                    # 读取杠杆映射文件
                                    leverage_map_path = os.path.join(leverage_dir, f"leverage_map_{exchanges[0]}_{exchanges[1]}.csv")
                                    if os.path.exists(leverage_map_path):
                                        df_leverage_map = pd.read_csv(leverage_map_path)
                                    else:
                                        cols = ['env', 'symbol', 'lever_okx', 'lever_binance']
                                        df_leverage_map = pd.DataFrame(columns=cols)
                                    # 获取当前环境，该币种上一次修改的杠杆
                                    df_prev_leverage = df_leverage_map[(df_leverage_map['env'] == env_name) &
                                                                        (df_leverage_map['symbol'] == change['symbol'])]
                                    prev_lever_okx = None
                                    prev_lever_binance = None
                                    if not df_prev_leverage.empty:
                                        prev_lever_okx = float(df_prev_leverage.iloc[0]['lever_okx'])
                                        prev_lever_binance = float(df_prev_leverage.iloc[0]['lever_binance'])
                                        logger.info(f"Previous leverage for {change['symbol']} in {env_name}: OKX={prev_lever_okx}, Binance={prev_lever_binance}")

                                    # 寻找最近日期的文件
                                    file_prefix = f"limit_pos_leverage_map_{exchanges[0]}_{exchanges[1]}_"
                                    leverage_files = [f for f in os.listdir(leverage_dir) if f.startswith(file_prefix) and f.endswith(".csv")]
                                    
                                    if leverage_files:
                                        leverage_files.sort(reverse=True)
                                        latest_file = leverage_files[0]
                                        leverage_file_path = os.path.join(leverage_dir, latest_file)
                                        
                                        if os.path.exists(leverage_file_path):
                                            df_leverage = pd.read_csv(leverage_file_path)
                                            target_symbol = change['symbol']
                                            df_symbol_leverage = df_leverage[df_leverage['symbol'] == target_symbol]
                                            
                                            if not df_symbol_leverage.empty:
                                                df_qualified = df_symbol_leverage[df_symbol_leverage['pos_limit'] >= max_pos_limit]
                                                if not df_qualified.empty:
                                                    row_leverage = df_qualified.sort_values('pos_limit').iloc[0]
                                                    maxLever_okx = float(row_leverage['maxLever_okx'])
                                                    maxLever_binance = float(row_leverage['maxLever_binance'])
                                                    # 调相应接口修改,只有和上次不一样才修改
                                                    logger.info(f"Changing OKX leverage for {target_symbol} from {prev_lever_okx} to {maxLever_okx}")
                                                    if prev_lever_okx is None or prev_lever_okx != maxLever_okx:
                                                        resp = console.raw_request_okx_leverage(
                                                            env_name=env_name,
                                                            symbol=f'{symbol}-SWAP',
                                                            leverage=maxLever_okx
                                                        )
                                                        logger.info(f"Updated OKX leverage for {target_symbol} to {maxLever_okx}: {resp}")
                                                        time.sleep(0.5)
                                                    else:
                                                        logger.info(f"OKX leverage for {target_symbol} is already {maxLever_okx}, no change needed")
                                                    logger.info(f"Changing Binance leverage for {target_symbol} from {prev_lever_binance} to {maxLever_binance}")
                                                    if prev_lever_binance is None or prev_lever_binance != maxLever_binance:
                                                        resp = console.raw_request_binance_leverage(
                                                            env_name=env_name,
                                                            symbol=symbol.replace('-', ''),
                                                            leverage=maxLever_binance
                                                        )
                                                        logger.info(f"Updated Binance leverage for {target_symbol} to {maxLever_binance}: {resp}")
                                                        time.sleep(0.5)
                                                    else:
                                                        logger.info(f"Binance leverage for {target_symbol} is already {maxLever_binance}, no change needed")
                                                    # 保存修改记录到map文件
                                                    new_record = {
                                                        'env': env_name,
                                                        'symbol': target_symbol,
                                                        'lever_okx': maxLever_okx,
                                                        'lever_binance': maxLever_binance
                                                    }
                                                    if df_prev_leverage.empty:
                                                        df_leverage_map = pd.concat([df_leverage_map, pd.DataFrame([new_record])], ignore_index=True)
                                                    else:
                                                        df_leverage_map.loc[
                                                            (df_leverage_map['env'] == env_name) &
                                                            (df_leverage_map['symbol'] == target_symbol),
                                                            ['lever_okx', 'lever_binance']
                                                        ] = [maxLever_okx, maxLever_binance]
                                                    df_leverage_map.to_csv(leverage_map_path, index=False)
                                                else:
                                                    logger.warning(f"No qualified leverage row found for {target_symbol} with pos_limit >= {max_pos_limit}")
                                            else:
                                                logger.warning(f"Symbol {target_symbol} not found in leverage map")
                                        else:
                                            logger.warning(f"Leverage file not found: {leverage_file_path}")
                                    else:
                                        logger.warning(f"No leverage files found in {leverage_dir} with prefix {file_prefix}")
                                except Exception as e:
                                    logger.error(f"Error querying leverage for {change['symbol']}: {e}")

                        else:
                            # 旧版
                            change = {
                                "control": env_name,
                                "symbol": symbol.replace('-', ''),
                                "config": {
                                    "limit_pos1": pos_limit,
                                    "limit_pos2": pos_limit
                                },
                                # "running": True,
                            }

                        # not_running -> running才sleep
                        # if not symbol_is_running.get(symbol, True):
                        #     if pos_limit > 0:
                        #         change['running'] = True
                        #         time.sleep(3)
                        response = console.request_update_config(change=change)
                        if response:
                            # 改为只推送不为0的
                            if pos_limit > 0:
                                warn_msg[env_name] = (f"{warn_msg.get(env_name, '')}"
                                                      f"\n{symbol}: {pos_limit} "
                                                      f"(价差排名：{spread_rank[0] if len(spread_rank) > 0 else 'N/A'}, "
                                                      f"资金费率排名：{fr_rank[0] if len(fr_rank) > 0 else 'N/A'})")
                            logger.info(f"Updated position limit for {symbol}: {response}")
                        else:
                            warn_msg[env_name] = (f"{warn_msg.get(env_name, '')}"
                                                  f"\n{symbol}: {pos_limit} 更新失败")
                            logger.error(f"Failed to update position limit for {symbol}")
                        # 改为单独起running
                        logger.info(f"[runtime_monitor][{now_hour}] Start setting running=True for {env_name} {symbol}, pos_limit: {pos_limit}, is_running: {symbol_is_running.get(symbol, True)}")
                        if not symbol_is_running.get(symbol.replace('-', ''), True):
                            if pos_limit > 0:
                                time.sleep(3)
                                run_change = {
                                    "control": env_name,
                                    "symbol": symbol.replace('-', ''),
                                    "running": True,
                                }
                                response = console.request_update_config(change=run_change)
                                if response:
                                    logger.info(f"Set running=True for {env_name}.{symbol}: {response}")
                                    symbol_is_running[symbol.replace('-', '')] = True
                                else:
                                    logger.error(f"Failed to set running=True for {env_name}.{symbol}")
                        logger.info(
                            f"[runtime_monitor][{now_hour}] Finished updating position limit for {env_name} {symbol}")
                    # 保存到csv
                    limit_pos_df = pd.DataFrame(list(pos_limit_dict.items()), columns=['Symbol', 'PositionLimit'])
                    limit_pos_file_path = os.path.join(output_dir,
                                                       f"{dir_date_str}/{date_str}/{date_str}_{env_name}_limit_pos.csv")
                    limit_pos_df.to_csv(limit_pos_file_path, index=False)
                    if len(new_symbols) > 0:
                        logger.info(f"Added new symbols to {env_name}: {new_symbols}")
                        msg = f"{env_name}新增交易对:" + " ".join(new_symbols)
                        send_lark_bot(lark_bot_pos, message_content=msg)

                    if len(single_symbols) > 0:
                        logger.info(f"Excluded single-exchange symbols from {env_name}: {single_symbols}")
                        msg = f"{env_name}排除单所交易对:" + " ".join(single_symbols)
                        send_lark_bot(lark_bot_pos, message_content=msg)
                for env_name, msg in warn_msg.items():
                    if len(msg) > 0:
                        send_lark_bot(lark_bot_pos, message_content=f'双合约仓位限制修改[{env_name}]\n{msg}')
                logger.info(
                    f"[runtime_monitor][{now_hour}] finished position limit updates for environment: {env_name}")

                # 历史仓位调整
                adjust_limit0(df_filtered, env_name, cf_env_dir, capital, mode_prop_binary_low_limit,
                              earn_mean_abs_low_limit, fee_rate, spread_black_symbols)
                logger.info(
                    f"[runtime_monitor][{now_hour}] finished historical position adjustment for environment: {env_name}")
        else:
            logger.warning('No valid features generated for any symbols')

        logger.info(f"[runtime_monitor][{now_hour}] calc_feature completed successfully")
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}", exc_info=True)
        send_lark_bot(lark_bot, message_content=f'双合约自动化发生未处理异常:\n{str(e)}',at_users=at_users)
