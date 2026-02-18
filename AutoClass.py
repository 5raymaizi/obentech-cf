import pandas as pd
import json
import requests
import csv
import datetime
from dateutil.relativedelta import relativedelta
import requests
from requests.auth import HTTPBasicAuth
import os
import hmac
import math
import base64
import struct
import hashlib
import time
from contextlib import closing
from zipfile import ZipFile
import pandas as pd
import shutil
from urllib.parse import urlparse
import numpy as np

# 改版测试
def cal_google_code(secret_key: str):
    # secret key 的长度必须是 8 的倍数。所以如果 secret key 不符合要求，需要在后面补上相应个数的 "="
    secret_key_len = len(secret_key)
    secret_key_pad_len = math.ceil(secret_key_len / 8) * 8 - secret_key_len
    secret_key = secret_key + "=" * secret_key_pad_len

    duration_input = int(time.time()) // 30
    _key = base64.b32decode(secret_key)
    msg = struct.pack(">Q", duration_input)
    google_code = hmac.new(_key, msg, hashlib.sha1).digest()
    o = google_code[19] & 15
    google_code = str((struct.unpack(">I", google_code[o:o+4])[0] & 0x7fffffff) % 1000000)

    # 生成的验证码未必是 6 位，注意要在前面补 0
    if len(google_code) == 5:
        google_code = '0' + google_code
    return google_code


def get_cookie(_url: str, _username: str, _password: str):
    _response = requests.post(url=_url, auth=HTTPBasicAuth(_username, _password), allow_redirects=False, stream=True)
    print(_response, _response.text, _response.headers)
    _cookies = requests.utils.dict_from_cookiejar(_response.cookies)
    return _cookies


class AutoParamController:

    def __init__(self, exchange, proxies):
        self.proxies = proxies
        if exchange == 'binance':
            self.prefix = "mmadminjp"
            self.tt_env_list = ["bn_v4_pm_test", "bn_v4_pm_pro2", "bn_v4_pm_pro3", "bn_v4_pm_pro4",
                                "bn_v4_pm_pro7", "bn_v4_pm_pro9", "bn_v4_pm_pro10", "bn_v4_pm_pro11",
                                "bn_v4_pm_pro12",
                                "bn_v4_pm_pro13", "bn_v4_pm_pro14", "bn_v4_pm_pro16",
                                "bn_v4_pm_pro17", "bn_v4_pm_pro18",
                                "bn_v4_pm_pro19", "bn_v4_pm_pro20", "bn_v4_pm_pro21", "bn_v4_pm_pro22",
                                "bn_v4_pm_pro23", "bn_v4_pm_pro24",
                                "bn_v4_pm_pro25", "bn_v4_pm_pro26", "bn_v4_pm_pro27", "bn_v4_pm_pro28",
                                "bn_v4_pm_pro29", "bn_v4_pm_pro30", "bn_v4_pm_pro31", "bn_v4_pm_pro32", "bn_v4_pm_pro33"]
            self.mt_env_list = ["bn_v4_pm_maker_pro7", "bn_v4_pm_maker_pro2", "bn_v4_pm_maker_pro3",
                                "bn_v4_pm_maker_pro4", "bn_v4_pm_maker_pro5",
                                "bn_v4_pm_maker_pro6", "bn_v4_pm_maker_pro", "bn_v4_pm_maker_pro8",
                                "bn_v4_pm_maker_pro9", "bn_v4_pm_maker_pro10",
                                "bn_v4_pm_maker_pro11", "bn_v4_pm_maker_pro12", "bn_v4_pm_maker_pro13",
                                "bn_v4_pm_maker_pro14", "bn_v4_pm_maker_pro15",
                                "bn_v4_pm_maker_pro16", "bn_v4_pm_maker_pro17", "bn_v4_pm_maker_pro18",
                                "bn_v4_pm_maker_pro19", "bn_v4_pm_maker_pro20",
                                "bn_v4_pm_maker_pro21", "bn_v4_pm_maker_pro22", "bn_v4_pm_maker_pro23",
                                "bn_v4_pm_maker_pro24",
                                "bn_v4_pm_maker_pro25", "bn_v4_pm_maker_pro26", "bn_v4_pm_maker_pro27",
                                "bn_v4_pm_maker_pro28", "bn_v4_pm_maker_pro29", "bn_v4_pm_maker_pro30", "bn_v4_pm_maker_pro31"]
            self.tt_symbol_dict = dict(zip(self.tt_env_list, [[] for env in range(len(self.tt_env_list))]))
            self.mt_symbol_dict = dict(zip(self.mt_env_list, [[] for env in range(len(self.mt_env_list))]))

            binanceUsdmExchangeInfoUrl = 'https://fapi.binance.com/fapi/v1/exchangeInfo'
            binanceUsdmFundingRateUrl = 'https://fapi.binance.com/fapi/v1/fundingRate'
            binanceUsdmTickerPriceUrl = 'https://fapi.binance.com/fapi/v2/ticker/price'
            binanceCoinmExchangeInfoUrl = 'https://dapi.binance.com/dapi/v1/exchangeInfo'
            binanceCoinmFundingRateUrl = 'https://dapi.binance.com/dapi/v1/fundingRate'
            binanceColRateUrl = 'https://www.binance.com/bapi/margin/v1/public/margin/portfolio/collateral-rate'

        elif (exchange == 'binance2'):
            self.prefix = "mmadminjp2"
            self.tt_env_list = ["bn_v5_pm_pro1", "bn_v5_pm_pro2", "bn_v5_pm_pro3", "bn_v5_pm_pro4", "bn_v5_pm_pro5",
                                "bn_v5_pm_pro6", "bn_v5_pm_pro7", "bn_v5_pm_pro8", "bn_v5_pm_pro9",
                                "bn_v5_pm_pro10", "bn_v5_pm_pro11", "bn_v5_pm_pro12", "bn_v5_pm_pro13", "bn_v5_pm_pro14",
                                "bn_v5_pm_pro15", "bn_v5_pm_pro16", "bn_v5_pm_pro17", "bn_v5_pm_pro18", "bn_v5_pm_pro18",
                                "bn_v5_pm_pro19", "bn_v5_pm_pro20", "bn_v5_pm_pro21", "bn_v5_pm_pro22",
                                "bn_v5_pm_pro23", "bn_v5_pm_pro24", "bn_v5_pm_pro25", "bn_v5_pm_pro26",
                                "bn_v5_pm_pro27", "bn_v5_pm_pro28", "bn_v5_pm_pro29", "bn_v5_pm_pro30",
                                "bn_v5_pm_pro31", "bn_v5_pm_pro32", 
                                "bn_v5_pm_test17","bn_v5_pm_arbSa","bn_v5_pm_arbSa2"]
            self.mt_env_list = []
            self.tt_symbol_dict = dict(zip(self.tt_env_list, [[] for env in range(len(self.tt_env_list))]))
            self.mt_symbol_dict = dict(zip(self.mt_env_list, [[] for env in range(len(self.mt_env_list))]))

        elif (exchange == 'binance3'):
            self.prefix = "mmadminjp3"
            self.tt_env_list = ["bn_v5_pm_pro1", "bn_v5_pm_pro2", "bn_v5_pm_pro3", "bn_v5_pm_pro4", "bn_v5_pm_pro5",
                                "bn_v5_pm_pro6", "bn_v5_pm_pro7", "bn_v5_pm_pro8", "bn_v5_pm_pro9",
                                "bn_v5_pm_pro10", "bn_v5_pm_pro11", "bn_v5_pm_pro12", "bn_v5_pm_pro13", "bn_v5_pm_pro14",
                                "bn_v5_pm_pro15", "bn_v5_pm_pro16", "bn_v5_pm_pro17", "bn_v5_pm_pro18", "bn_v5_pm_pro18",
                                "bn_v5_pm_pro19", "bn_v5_pm_pro20", "bn_v5_pm_pro21", "bn_v5_pm_pro22",
                                "bn_v5_pm_pro23", "bn_v5_pm_pro24", "bn_v5_pm_pro25", "bn_v5_pm_pro26",
                                "bn_v5_pm_pro27", "bn_v5_pm_pro28", "bn_v5_pm_pro29", "bn_v5_pm_pro30",
                                "bn_v5_pm_pro31", "bn_v5_pm_pro32", 
                                "bn_v5_pm_test17","bn_v5_pm_arbSa","bn_v5_pm_arbSa2", "bn_v5_pm_pro33", "bn_v5_pm_pro34",
                                "bn_v5_pm_pro35", "bn_v5_pm_pro36", "bn_v5_pm_pro37", "bn_v5_pm_pro38", "bn_v5_pm_pro39",
                                "bn_v5_pm_pro40", "bn_v5_pm_pro41", "bn_v5_pm_pro42"]
            self.mt_env_list = []
            self.tt_symbol_dict = dict(zip(self.tt_env_list, [[] for env in range(len(self.tt_env_list))]))
            self.mt_symbol_dict = dict(zip(self.mt_env_list, [[] for env in range(len(self.mt_env_list))]))
            
        elif exchange == 'okx':
            self.prefix = "mmadmin"
            self.tt_env_list = ['okx_v4_u_test', 'okx_v4_u_test2', 'okx_v4_u_test4', 'okx_v4_u_test5',
                                'okx_v4_u_test6', 'okx_v4_u_test7', 'okx_v4_u_test8', 'okx_v4_u_test9',
                                'okx_v4_u_test10', 'okx_v4_u_test11', 'okx_v5_test12', 'okx_v4_u_test13',
                                'okx_v4_u_test14', 'okx_v4_u_test15', 'okx_v4_u_test16', 'okx_v4_u_test17',
                                'okx_v4_u_test18', 'okx_v4_u_test19', 'okx_v4_u_test20', 'okx_v4_u_test21',
                                'okx_v4_u_test22', 'okx_v4_u_test23', 'okx_v4_u_test24', 'okx_v4_u_test25',
                                'okx_v4_u_test26', 'okx_v4_u_test27', 'okx_v4_u_test28', 'okx_v4_u_test29',
                                'okx_v4_u_test30', 'okx_v4_u_test31', 'okx_v4_u_test32', 'okx_v4_u_test33',
                                'okx_v4_u_test34', 'okx_v4_u_test35']
            self.mt_env_list = ['okex5_v4_maker_cross_test', 'okex5_v4_maker_cross_test2', 'okex5_v4_maker_cross_test3',
                                'okex5_v4_maker_cross_test4', 'okex5_v4_maker_cross_test5',
                                'okex5_v4_maker_cross_test6',
                                'okex5_v4_maker_cross_test7', 'okex5_v4_maker_cross_test8',
                                'okex5_v4_maker_cross_test9',
                                'okex5_v4_maker_cross_test10', 'okex5_v4_maker_cross_test11',
                                'okex5_v4_maker_cross_test12',
                                'okex5_v4_maker_cross_test13', 'okex5_v4_maker_cross_test14',
                                'okex5_v4_maker_cross_test15',
                                'okex5_v4_maker_cross_test15', 'okex5_v4_maker_cross_test16',
                                'okex5_v4_maker_cross_test17',
                                'okex5_v4_maker_cross_test18', 'okex5_v4_maker_cross_test19',
                                'okex5_v4_maker_cross_test20', 'okex5_v4_maker_cross_test21',
                                'okex5_v4_maker_cross_test22', 'okex5_v4_maker_cross_test23',
                                'okex5_v4_maker_cross_test24', 'okex5_v4_maker_cross_test25',
                                'okex5_v4_maker_cross_test26', 'okex5_v4_maker_cross_test27',
                                'okex5_v4_maker_cross_test28', 'okex5_v4_maker_cross_test29',
                                'okex5_v4_maker_cross_test30', 'okex5_v4_maker_cross_test31',
                                'okex5_v4_maker_cross_test32', 'okex5_v4_maker_cross_test33',
                                'okex5_v4_maker_cross_test34', 'okex5_v4_maker_cross_test35',
                                'okx_v5_test1', 'okx_v5_test2', 'okx_v5_test5', 'okx_v5_test6', 'okx_v5_test7', 
                                'okx_v5_test8', 'okx_v5_test9', 'okx_v5_test10', 'okx_v5_test11', 
                                'okx_v5_test12', 'okx_v5_test16', 'okx_v5_test17', 'okx_v5_test18', 'okx_v5_test19',
                                'okx_v5_test20', 'okx_v5_test21', 'okx_v5_test22', 
                                'okx_v5_test23', 'okx_v5_test27',
                                'okx_v5_test28', 'okx_v5_test29', 'okx_v5_test30', 'okx_v5_test31', 'okx_v5_test32', 'okx_v5_test33',
                                'okx_v5_test36', 'okx_v5_test37', 'okx_v5_test38', 'okx_v5_test39', 'okx_v5_test40',
                                'okx_v5_test41', 'okx_v5_test42', 'okx_v5_test43', 'okx_v5_test44', 'okx_v5_test45', 
                                'okx_v5_test46', 'okx_v5_test47', 'okx_v5_test48', 'okx_v5_test49', 'okx_v5_test50',
                                'okx_v5_test51', 'okx_v5_test52']
            self.tt_symbol_dict = dict(zip(self.tt_env_list, [[] for env in range(len(self.tt_env_list))]))
            self.mt_symbol_dict = dict(zip(self.mt_env_list, [[] for env in range(len(self.mt_env_list))]))
        elif exchange == 'bybit':
            self.prefix = 'mmadmin'
            self.tt_env_list = ['bybit_v5_bybitTest', 'bybit5_v5_by1', 'bybit5_v5_by2']
            self.mt_env_list = ['bybit5_v4_pm_maker_bybit5Test']
            self.tt_symbol_dict = dict(zip(self.tt_env_list, [[] for env in range(len(self.tt_env_list))]))
            self.mt_symbol_dict = dict(zip(self.mt_env_list, [[] for env in range(len(self.mt_env_list))]))
        else:
            return "初始化有问题"

    # 返回一个被两重验证的、可以使用的cookie
    def login(self):
        s = requests.Session()

        username1 = 'ray_xu'  # dcdl用户名
        if self.prefix == 'mmadminjp' or self.prefix == 'mmadminjp2' or self.prefix == 'mmadminjp3':
            key1 = 'NVCX4PJP32Y4JTJJVCKU5XUQ4M'  # dcdl密钥
        elif self.prefix == 'mmadmin':
            key1 = '7H62KCRCUEOYH2FV4SCYXYMPWE'
        password1 = cal_google_code(key1)
        print(username1, password1)
        # 如果用http://，会被重定向为https://，可以通过headers得到
        url = f'https://{self.prefix}.digifinex.org/'
        login_response1 = s.post(url=url, auth=HTTPBasicAuth(username1, password1), allow_redirects=False, stream=True)
        print(login_response1, login_response1.text, login_response1.headers)
        login_cookies1 = requests.utils.dict_from_cookiejar(login_response1.cookies)
        #     print(login_cookies1)

        username2 = 'ray_xu'  # dcdl用户名
        key2 = 'HBRDG6BUPJZGC6K7PB2TI3LCOM4XQNDO'  # dcdl密钥
        password2 = cal_google_code(key2)
        # 这里必须再登陆一次，否则会重定向到login
        url2 = f'https://{self.prefix}.digifinex.org/api/login/valid'
        print(username2, password2, url)
        data = {
            "account": username2,
            "glecode": password2,
        }
        # 不加第一次登陆的cookie会报错
        login_response2 = s.post(url=url2, data=data, headers={
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
            'Content-Type': 'application/x-www-form-urlencoded'},
                                 cookies=login_cookies1, allow_redirects=False)
        #     print(login_response2, login_response2.text, login_response2.headers)
        print(login_response2)
        login_cookies2 = requests.utils.dict_from_cookiejar(login_response2.cookies)

        self.verified_session = s
        self.verified_cookies = login_cookies1

    #         return s, login_cookies1

    def initSymbolsList(self):
        #         url = f"https://{self.prefix}.digifinex.org/api/cf_arbitrage/keys"
        url = f"https://{self.prefix}.digifinex.org/api/cf_arbitrage/keysStatus"
        for env in self.tt_env_list:
            querystring = {"env": env}
            response = self.verified_session.request("GET", url, cookies=self.verified_cookies, params=querystring)
            #             print(env, response)
            if response.status_code != 200:
                print(f"{env}的symbol_list初始化失败")
                continue
            else:
                self.tt_symbol_dict[env] = response.json()['data']
        for env in self.mt_env_list:
            querystring = {"env": env}
            response = self.verified_session.request("GET", url, cookies=self.verified_cookies, params=querystring)
            #             print(env, response)
            if response.status_code != 200:
                print(f"{env}的symbol_list初始化失败")
                continue
            else:
                self.mt_symbol_dict[env] = response.json()['data']

    def updateSymbolsList(self, env):
        url = f"https://{self.prefix}.digifinex.org/api/cf_arbitrage/keys"
        querystring = {"env": env}
        response = self.verified_session.request("GET", url, cookies=self.verified_cookies, params=querystring)
        if response.status_code != 200:
            print(f"{env}的symbol_list更新失败")
        if env in self.tt_env_list:
            symbol_list = response.json()['data']
            self.tt_symbol_dict[env] = response.json()['data']
        elif env in self.mt_env_list:
            symbol_list = response.json()['data']
            self.mt_symbol_dict[env] = response.json()['data']

    def umiMMRwarning(self, uniThreshold=6):
        umrList = []
        for env, symbol_list in self.tt_symbol_dict.items():
            try:
                url = f"https://{self.prefix}.digifinex.org/api/cf_arbitrage/{list(symbol_list.keys())[0]}"
                querystring = {"env": env}
                payload = ""
                response = self.verified_session.request("GET", url, cookies=self.verified_cookies, params=querystring)
                response_dict = response.json()
                try:
                    if response.status_code != 200:
                        print(f"获取{env}PM信息失败")
                    else:
                        pm_info = response_dict['data']['detail']['account']
                        accountMaintMargin = pm_info['accountMaintMargin']
                        accountEquity = pm_info['accountEquity']
                        print(f"{env}全仓保证金率为{pm_info['uniMMR']}")
                        if float(pm_info['uniMMR']) < uniThreshold:
                            print(f"{env}全仓保证金率{pm_info['uniMMR']}过低")
                        umrList.append([env, pm_info['uniMMR'], accountEquity, accountMaintMargin])
                except Exception as e:
                    print(e)
                    print(f"获取{env}PM信息失败")
                    continue
            except Exception as e:
                continue
        return pd.DataFrame(umrList, columns=["env", "uniMMR", "accountEquity", "accountMaintMargin"]).set_index("env")

    def showUsdtAmount(self):
        usdt_list = []
        for env, symbol_list in self.tt_symbol_dict.items():
            try:
                url = f"https://{self.prefix}.digifinex.org/api/cf_arbitrage/{list(symbol_list.keys())[0]}"
                querystring = {"env": env}
                payload = ""
                response = self.verified_session.request("GET", url, cookies=self.verified_cookies, params=querystring)
                response_dict = response.json()
                try:
                    if response.status_code != 200:
                        print(f"获取{env}Usdt余额失败")
                    else:
                        usdtAmount = response_dict['data']['detail']['account']['quoteAmount']
                        usdtBorrowed = response_dict['data']['detail']['account']['quoteBorrowed']
                        print(f"{env}USDT余额为{usdtAmount}_USDT负债为{usdtBorrowed}")
                        usdt_list.append([env, usdtAmount, usdtBorrowed])
                except Exception as e:
                    print(e)
                    print(f"获取{env}PM信息失败")
                    continue
            except Exception as e:
                    print(e)
        return pd.DataFrame(usdt_list, columns=["env", "usdtAmount", "usdtBorrowed"]).set_index("env")

    # 待用新给的含有running的keyStatus优化
    # 用了keyStatus还是慢，干脆这样，reset的时候，直接提交open_allowed=False，然后分配仓位就好
    def showUnallocatedUsdt(self, reset=False, target_env_list=[]):
        symbol_dict = self.tt_symbol_dict
        if len(target_env_list) == 0:
            target_env_list = list(symbol_dict.keys())
        uau_dict = {}
        for env, symbol_list in symbol_dict.items():
            if env in target_env_list:
                uau_detail_list = []
                print(env)
                for symbol, running in symbol_list.items():
                    print(symbol, running)
                    if '-' not in symbol:
                        try:
                            #  & ((limit_pos - current_pos)>1000) & open_allowed
                            if running == 'On':
                                if reset:
                                    update_dict = {"change": [
                                        {"control": env, "symbol": f"{symbol}", "config": {"open_allowed": False}}]}
                                    self.updateConfig(update_dict)
                                else:
                                    print(symbol)
                                    url = f"https://{self.prefix}.digifinex.org/api/cf_arbitrage/{symbol}"
                                    querystring = {"env": env}
                                    response = self.verified_session.request("GET", url, cookies=self.verified_cookies,
                                                                             params=querystring)
                                    response_dict = response.json()
                                    open_allowed = bool(response_dict['data']['config']['open_allowed'])
                                    if open_allowed:
                                        current_pos = float(response_dict['data']['detail']['current_pos'])
                                        limit_pos = float(response_dict['data']['config']['limit_pos'])
                                        uau_detail_list.append([symbol, limit_pos, current_pos])
                                time.sleep(2)
                        except Exception as e:
                            print(e)
                            continue
                uau_dict[env] = pd.DataFrame(uau_detail_list, columns=['symbol', 'limit_pos', 'current_pos'])
        return uau_dict

    def turnOn(self, env_type='tt', sleep_time=3, target_env_list=[]):
            
            if env_type == 'tt':
                if len(target_env_list) == 0:
                    target_env_list = list(self.tt_symbol_dict.keys())
                symbol_dict = self.tt_symbol_dict
            elif env_type == 'mt':
                if len(target_env_list) == 0:
                    target_env_list = list(self.mt_symbol_dict.keys())
                symbol_dict = self.mt_symbol_dict

            for env, symbol_list in symbol_dict.items():
                print(env)
                if env in target_env_list:
                    if self.prefix == "mmadminjp2":
                        symbol_list = list(symbol_list)
                    for symbol in symbol_list:
                        try:
                            url = f"https://{self.prefix}.digifinex.org/api/cf_arbitrage/{symbol}"
                            querystring = {"env":env}
                            response = self.verified_session.request("GET", url, cookies = self.verified_cookies, params=querystring)
                            response_dict = response.json()
                            current_pos = int(response_dict['data']['detail']['current_pos'])
                            running_state = response_dict['data']['running']
                            config = response_dict['data']['config']
                            pos_amount = float(response_dict['data']['detail']['pos_list']['pos_amount'])
                            local_pos_amount = float(response_dict['data']['detail']['pos_list']['local_pos_amount'])
                            if self.prefix == "mmadminjp2":
                                local_pos_amount = float(response_dict['data']['detail']['pos_list']['local_pos_amount'])
                                local_pos_amount_mt = float(response_dict['data']['detail']['pos_list']['local_pos_amount_mt'])
                                local_pos_amount_tm = float(response_dict['data']['detail']['pos_list']['local_pos_amount_tm'])
                                local_pos_amount = local_pos_amount + local_pos_amount_mt + local_pos_amount_tm
                            print(f'{symbol}_{current_pos}_{running_state}_{pos_amount}_{local_pos_amount}')

                            if (current_pos > 0) & (not running_state) & (pos_amount>0):
                            # if (current_pos > 0) & (not running_state) & (pos_amount > 0) & (local_pos_amount>0):
                                new_config = {}
                                update_json = {"change":[{"control":env, "symbol":f"{symbol}", "running":True, "config":new_config}]}
                                print(update_json)
                                # 后面要把提交json封装成一个专门的函数
                                url = f"https://{self.prefix}.digifinex.org/api/cf_strategy/update_config"
                                response = self.verified_session.request("POST", url, 
                                                            data=json.dumps(update_json),
                                                            cookies = self.verified_cookies,
                                                            headers={'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
                                                                                                'Content-Type': 'application/json'},
                                                            )
                                print(response, response.text)
                                time.sleep(sleep_time)
                        except Exception as e:
                            print(e)
                            continue
                    time.sleep(sleep_time)

    def turnOffOverFreq(self, env_type='tt'):
        if env_type == 'tt':
            symbol_dict = self.tt_symbol_dict
        elif env_type == 'mt':
            symbol_dict = self.mt_symbol_dict

        for env, symbol_list in symbol_dict.items():
            print(env)
            url = f"https://mmadminjp.digifinex.org/api/cf_arbitrage/{symbol_list[0]}"
            querystring = {"env": env}
            payload = ""
            response = self.verified_session.request("GET", url, cookies=self.verified_cookies, params=querystring)
            response_dict = response.json()
            if 'account' not in response_dict['data']['detail'].keys():
                print('全仓信息获取失败，可能出现超频问题')
                url = f"https://{self.prefix}.digifinex.org/api/cf_arbitrage/stopall"
                querystring = {"env": env}
                response = self.verified_session.request("PUT", url, cookies=self.verified_cookies, params=querystring)
                response_dict = response.json()
                print(response, response.text)
                time.sleep(6)

    def turnOff(self, env_type='tt', target_env_list=[]):
        if env_type == 'tt':
            symbol_dict = self.tt_symbol_dict
        elif env_type == 'mt':
            symbol_dict = self.mt_symbol_dict
            
        for env, symbol_list in symbol_dict.items():
            if env in target_env_list:
                if self.prefix == "mmadminjp2":
                    symbol_list = list(symbol_list)
                print(env)
                url = f"https://{self.prefix}.digifinex.org/api/cf_arbitrage/{symbol_list[0]}"
                querystring = {"env":env}
                payload = ""
                response = self.verified_session.request("GET", url, cookies = self.verified_cookies, params=querystring)
                response_dict = response.json()

                url = f"https://{self.prefix}.digifinex.org/api/cf_arbitrage/stopall"
                querystring = {"env":env}
                response = self.verified_session.request("PUT", url, cookies = self.verified_cookies, params=querystring)
                response_dict = response.json()
                print(response, response.text)
                time.sleep(6)

    def turnOnReamin(self, env_type='tt'):
        if env_type == 'tt':
            symbol_dict = self.tt_symbol_dict
        elif env_type == 'mt':
            symbol_dict = self.mt_symbol_dict

        for env, symbol_list in symbol_dict.items():
            querystring = {"env": env}
            print(env)
            for symbol in symbol_list:
                if (not 'USDT' in symbol) & (not '-' in symbol):
                    try:
                        url = f"https://{self.prefix}.digifinex.org/api/cf_arbitrage/{symbol}"
                        print(symbol)
                        new_config = {"remain": True}
                        update_json = {"change": [{"control": env, "symbol": f"{symbol}", "config": new_config}]}

                        # 后面要把提交json封装成一个专门的函数
                        url = f"https://{self.prefix}.digifinex.org/api/cf_strategy/update_config"
                        response = self.verified_session.request("POST", url,
                                                                 data=json.dumps(update_json),
                                                                 cookies=self.verified_cookies,
                                                                 headers={
                                                                     'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
                                                                     'Content-Type': 'application/json'},
                                                                 )
                        print(response, response.text)
                        time.sleep(5)
                    except Exception as e:
                        print(e)
                        time.sleep(5)

    def setOpenAllowed(self, env_type='mt', status=False):
        if env_type == 'tt':
            symbol_dict = self.tt_symbol_dict
        elif env_type == 'mt':
            symbol_dict = self.mt_symbol_dict

        for env, symbol_list in symbol_dict.items():
            querystring = {"env": env}
            print(env)
            for symbol in symbol_list:
                try:
                    url = f"https://{self.prefix}.digifinex.org/api/cf_arbitrage/{symbol}"
                    print(symbol)
                    new_config = {"open_allowed": status}
                    update_json = {"change": [{"control": env, "symbol": f"{symbol}", "config": new_config}]}

                    # 后面要把提交json封装成一个专门的函数
                    url = f"https://{self.prefix}.digifinex.org/api/cf_strategy/update_config"
                    response = self.verified_session.request("POST", url,
                                                             data=json.dumps(update_json),
                                                             cookies=self.verified_cookies,
                                                             headers={
                                                                 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
                                                                 'Content-Type': 'application/json'},
                                                             )
                    print(response, response.text)
                    time.sleep(5)
                except Exception as e:
                    print(e)
                    time.sleep(5)

    def updateConfig(self, update_dict):
        # 后面要把提交json封装成一个专门的函数
        url = f"https://{self.prefix}.digifinex.org/api/cf_strategy/update_config"
        response = self.verified_session.request("POST", url,
                                                 data=json.dumps(update_dict),
                                                 cookies=self.verified_cookies,
                                                 headers={
                                                     'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
                                                     'Content-Type': 'application/json'},
                                                 )
        print(response)

    def setCloseThreshold(self, close_threshold):
        symbol_dict = self.tt_symbol_dict
        for env, symbol_list in symbol_dict.items():
            querystring = {"env": env}
            print(env)
            for symbol in symbol_list:
                try:
                    url = f"https://{self.prefix}.digifinex.org/api/cf_arbitrage/{symbol}"
                    response = self.verified_session.request("GET", url, cookies=self.verified_cookies,
                                                             params=querystring)
                    response_dict = response.json()
                    #                     local_pos_amount = float(response_dict['data']['detail']['pos_list']['local_pos_amount'])
                    #                     pos_amount = float(response_dict['data']['detail']['pos_list']['pos_amount'])
                    total_failed_times = float(response_dict['data']['detail']['total_failed_times'])
                    # 如果失败过，且是U本位合约，且不是交割，则关闭后重启
                    if (total_failed_times > 0) & ('USDT' in symbol) & ('-' not in symbol):
                        print(f'{symbol}_U本位重启_{total_failed_times}')
                        update_dict = {
                            "change": [{"control": env, "symbol": f"{symbol}", "running": False, "config": {}}]}
                        print('关闭')
                        self.updateConfig(update_dict)
                        time.sleep(2)
                        update_dict = {
                            "change": [{"control": env, "symbol": f"{symbol}", "running": True, "config": {}}]}
                        print('重启')
                        self.updateConfig(update_dict)


                except Exception as e:
                    print(e)
                    time.sleep(5)

    # json实例 json.dumps({'type': "spot", 'side': "sell", 'amount': 1, 'param': "MARGIN_BUY"})
    # 注意 addOrder的User-Agent要控制台操作一次，复制里面的才行
    def addOrder(self, symbol, env, order_json):
        url = f'https://{self.prefix}.digifinex.org/api/cf_arbitrage/{symbol}/addOrder?env={env}'
        # Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36
        response = self.verified_session.request("POST", url,
                                                 data=order_json,
                                                 cookies=self.verified_cookies,
                                                 headers={
                                                     'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'}, )
        print(response)

    # 目前只支持tt
    def autoUsdmExceptionRestart(self, target_env_list=[]):
        if len(target_env_list) == 0:
            target_env_list = list(self.tt_symbol_dict.keys())
        symbol_dict = self.tt_symbol_dict
        for env, symbol_list in symbol_dict.items():
            if env in target_env_list:
                querystring = {"env": env}
                print(env)
                for symbol in symbol_list:
                    #                 if bool(symbol_list['symbol']):
                    print(symbol)
                    try:
                        url = f"https://{self.prefix}.digifinex.org/api/cf_arbitrage/{symbol}"
                        response = self.verified_session.request("GET", url, cookies=self.verified_cookies,
                                                                 params=querystring)
                        response_dict = response.json()
                        #                     local_pos_amount = float(response_dict['data']['detail']['pos_list']['local_pos_amount'])
                        #                     pos_amount = float(response_dict['data']['detail']['pos_list']['pos_amount'])
                        if 'total_failed_times' in response_dict['data']['detail'].keys():
                            total_failed_times = float(response_dict['data']['detail']['total_failed_times'])
                        # 如果失败过，且是U本位合约，且不是交割，则关闭后重启
                        #  & ('USDT' in symbol) 现在币本位合约也重启
                        if (total_failed_times > 0) & ('-' not in symbol):
                            print(f'{symbol}_U本位重启_{total_failed_times}')
                            # 注意，running就是在config外面的，这里虽然返回为500，但是实际成功了
                            update_dict = {
                                "change": [{"control": env, "symbol": f"{symbol}", "running": False, "config": {}}]}
                            print('关闭')
                            self.updateConfig(update_dict)
                            time.sleep(1)
                            update_dict = {
                                "change": [{"control": env, "symbol": f"{symbol}", "running": True, "config": {}}]}
                            print('重启')
                            self.updateConfig(update_dict)


                    except Exception as e:
                        print(e)
                        time.sleep(1)

    # 目前只支持tt
    def autoUsdmExceptionExposureHandle(self, target_env_list=[]):
        if len(target_env_list) == 0:
            target_env_list = list(self.tt_symbol_dict.keys())
        symbol_dict = self.tt_symbol_dict
        for env, symbol_list in symbol_dict.items():
            if env in target_env_list:
                querystring = {"env": env}
                print(env)
                for symbol in symbol_list:
                    print(symbol)
                    try:
                        url = f"https://{self.prefix}.digifinex.org/api/cf_arbitrage/{symbol}"
                        response = self.verified_session.request("GET", url, cookies=self.verified_cookies,
                                                                 params=querystring)
                        response_dict = response.json()
                        local_pos_amount = float(response_dict['data']['detail']['pos_list']['local_pos_amount'])
                        pos_amount = float(response_dict['data']['detail']['pos_list']['pos_amount'])
                        print(local_pos_amount, pos_amount)
                        # 如果内外敞口大于0，且是U本位合约，且不是交割，说明交易所合约仓位少了，要开空合约
                        # 一定要注意pos_amount=0有可能对应adl的情况，不能去swap_sell处理
                        if ((local_pos_amount - pos_amount) > 0) & (pos_amount != 0) & ('USDT' in symbol) & (
                                '-' not in symbol):
                            print(f'{symbol}_U本位内外敞口处理_{round(local_pos_amount - pos_amount, 5)}')
                            order_json = json.dumps({'type': "swap", 'side': "sell",
                                                     'amount': (round(local_pos_amount - pos_amount, 5)),
                                                     'param': "None"})
                            print(f"本地仓位:{pos_amount}_交易所仓位_{local_pos_amount}")
                            print(order_json)
                            self.addOrder(symbol, env, order_json)
                            time.sleep(0.5)
                        if ((local_pos_amount - pos_amount) < 0) & (pos_amount != 0) & ('USDT' in symbol) & (
                                '-' not in symbol):
                            print(f'{symbol}_U本位内外敞口处理_{round(local_pos_amount - pos_amount, 5)}')
                            order_json = json.dumps({'type': "swap", 'side': "closeShort",
                                                     'amount': (round(local_pos_amount - pos_amount, 5)),
                                                     'param': "None"})
                            print(f"本地仓位:{pos_amount}_交易所仓位_{local_pos_amount}")
                            print(order_json)
                            self.addOrder(symbol, env, order_json)
                            time.sleep(0.5)

                    #                         print(f'{symbol}_U本位重启')
                    #                         update_dict = {"change":[{"control":env, "symbol":f"{symbol}", "running":False, "config":{}}]}
                    #                         print('关闭')
                    #                         self.updateConfig(update_dict)
                    #                         time.sleep(1)
                    #                         update_dict = {"change":[{"control":env, "symbol":f"{symbol}", "running":True, "config":{}}]}
                    #                         print('重启')
                    #                         self.updateConfig(update_dict)

                    except Exception as e:
                        print(e)
                        time.sleep(5)

    # 目前只支持tt
    def autoUsdmExceptionRestart2(self, target_env_list=[]):
        if len(target_env_list) == 0:
            target_env_list = list(self.tt_symbol_dict.keys())
        symbol_dict = self.tt_symbol_dict
        for env, symbol_list in symbol_dict.items():
            if env in target_env_list:
                querystring = {"env": env}
                print(env)
                for symbol in symbol_list:
                    #                 if bool(symbol_list['symbol']):
                    print(symbol)
                    try:
                        url = f"https://{self.prefix}.digifinex.org/api/cf_arbitrage/{symbol}"
                        response = self.verified_session.request("GET", url, cookies=self.verified_cookies,
                                                                 params=querystring)
                        response_dict = response.json()
                        local_pos_amount = float(response_dict['data']['detail']['pos_list']['local_pos_amount'])
                        pos_amount = float(response_dict['data']['detail']['pos_list']['pos_amount'])
                        if ((local_pos_amount - pos_amount) > 0) & ('-' not in symbol):
                            print(f'{symbol}_U本位重启')
                            # 注意，running就是在config外面的，这里虽然返回为500，但是实际成功了
                            update_dict = {
                                "change": [{"control": env, "symbol": f"{symbol}", "running": False, "config": {}}]}
                            print('关闭')
                            self.updateConfig(update_dict)
                            time.sleep(1)
                            update_dict = {
                                "change": [{"control": env, "symbol": f"{symbol}", "running": True, "config": {}}]}
                            print('重启')
                            self.updateConfig(update_dict)


                    except Exception as e:
                        print(e)
                        time.sleep(1)

    def autoCoinmExposureGet(self, env='tt', target_env_list=[], is_auto=False, auto_ratio=0.75):
        if env == 'tt':
            symbol_dict = self.tt_symbol_dict
        elif env == 'mt':
            symbol_dict = self.mt_symbol_dict
        else:
            return
        if len(target_env_list) == 0:
            target_env_list = list(symbol_dict.keys())
        for env, symbol_list in symbol_dict.items():
            if env in target_env_list:
                querystring = {"env": env}
                print(env)
                for symbol in symbol_list:
                    if ('-' not in symbol) & ('USDT' not in symbol):
                        print(symbol)
                        url = f"https://{self.prefix}.digifinex.org/api/cf_arbitrage/{symbol}"

                        response = self.verified_session.request("GET", url, cookies=self.verified_cookies,
                                                                 params=querystring)
                        response_dict = response.json()

                        local_pos_amount = float(response_dict['data']['detail']['pos_list']['local_pos_amount'])
                        pos_amount = float(response_dict['data']['detail']['pos_list']['pos_amount'])
                        # 防止adl
                        if (local_pos_amount > 0) & (pos_amount > 0):

                            total_failed_times = response_dict['data']['detail']['total_failed_times']

                            baseAmount = float(response_dict['data']['detail']['account']['baseAmount'])
                            baseUpl = float(response_dict['data']['detail']['account']['baseUpl'])
                            baseUpdateTime = response_dict['data']['detail']['account']['baseUpdateTime']
                            poss_df = pd.DataFrame(response_dict['data']['detail']['pos_list']['poss'])
                            spot_remain = float(response_dict['data']['detail']['pos_list']['spot_remain'])
                            baseUpdateTimeDt = pd.to_datetime(
                                response_dict['data']['detail']['account']['baseUpdateTime'],
                                unit='ms') + relativedelta(hours=8)

                            if not poss_df.empty:
                                url = f'https://dapi.binance.com/dapi/v1/aggTrades?symbol={symbol}_PERP&endTime={baseUpdateTime}&limit=1000'
                                aggTradesDf = pd.DataFrame(requests.get(url, proxies=self.proxies).json())
                                coimAggPrice = float(aggTradesDf.iloc[-1]['p'])

                                # 还是别用标记价格，因为1min变数太多了
                                #                                 url = f'https://dapi.binance.com/dapi/v1/markPriceKlines?symbol={symbol}_PERP&interval=1m'
                                #                                 markPriceKlinesDf = pd.DataFrame(requests.get(url, proxies=proxies).json())
                                #                                 markPriceKlinesDf.columns = ['open_ts', 'open', 'high', 'low', 'close', 'ignore1', 'close_ts', 'ignore2', 'construct_num',
                                #                                                              'ignore3', 'ignore4', 'ignore5']
                                #                                 markPriceKlinesDf['open_dt'] = pd.to_datetime(markPriceKlinesDf['open_ts'], unit='ms').apply(lambda x:x+ relativedelta(hours=8))
                                #                                 markPriceKlinesDf['close_dt'] = pd.to_datetime(markPriceKlinesDf['close_ts'], unit='ms').apply(lambda x:x+ relativedelta(hours=8))
                                #                                 markPriceKlinesDf.sort_values('close_dt', inplace=True, ascending=False)
                                #                                 markPriceKlinesDf.set_index('close_dt', inplace=True)
                                #                                 coinm_mp = float(markPriceKlinesDf.loc[:pd.to_datetime(response_dict['data']['detail']['account']['baseUpdateTimeDt'], unit='ms') + relativedelta(hours=8)].iloc[-1]['close'])

                                contract_value = 10
                                inStrategyUpl = (poss_df['Unit'].astype('int') * contract_value * (
                                            1 / coimAggPrice - 1 / poss_df['OpenPrice'].astype('float'))).sum()
                                coinmExposure = (baseAmount + baseUpl) - (
                                            inStrategyUpl + poss_df['SpotKeep'].astype('float').sum() + poss_df[
                                        'SpotToSwap'].astype('float').sum() + poss_df['Funding'].astype(
                                        'float').sum() + spot_remain)

                                url = f'https://api.binance.com/api/v3/aggTrades?symbol={symbol + "T"}&limit=1000'
                                aggTradesDf = pd.DataFrame(requests.get(url, proxies=self.proxies).json())
                                spotAggPrice = float(aggTradesDf.iloc[-1]['p'])
                                coinmExposureUsdtValue = spotAggPrice * coinmExposure
                                print(
                                    f"敞口（币）:{coinmExposure}_敞口usdt价值:{coinmExposureUsdtValue}_持仓张数_{pos_amount}_{local_pos_amount}")
                                if (coinmExposureUsdtValue > 50) & (coinmExposureUsdtValue < 500):
                                    print(f'{symbol}_币本位合约现货合约敞口处理_{round(coinmExposure, 5)}')
                                    order_json = json.dumps({'type': "spot", 'side': "sell",
                                                             'amount': (round(abs(coinmExposure * auto_ratio), 5)),
                                                             'param': "None", "param": "AUTO_REPAY"})
                                    print(order_json)
                                    if is_auto:
                                        self.addOrder(symbol, env, order_json)
                                        time.sleep(0.5)
                                elif (coinmExposureUsdtValue < -50) & (coinmExposureUsdtValue > -500):
                                    print(f'{symbol}_币本位合约现货合约敞口处理_{round(coinmExposure, 5)}')
                                    order_json = json.dumps({'type': "spot", 'side': "buy",
                                                             'amount': (round(abs(coinmExposure * auto_ratio), 5)),
                                                             'param': "None", "param": "MARGIN_BUY"})
                                    print(order_json)
                                    if is_auto:
                                        self.addOrder(symbol, env, order_json)
                                        time.sleep(0.5)

    def spotRemain(self, env='tt', target_env_list=[]):
        if len(target_env_list) == 0:
            if env == 'tt':
                target_env_list = list(self.tt_symbol_dict.keys())
            elif env == 'mt':
                target_env_list = list(self.mt_symbol_dict.keys())
        else:
            if env == 'tt':
                symbol_dict = self.tt_symbol_dict
            elif env == 'mt':
                symbol_dict = self.mt_symbol_dict

        for env, symbol_list in symbol_dict.items():
            if env in target_env_list:
                querystring = {"env": env}
                print(env)
                for symbol in symbol_list:
                    if ('-' not in symbol) & ('USDT' not in symbol):
                        print(symbol)
                        url = f"https://{self.prefix}.digifinex.org/api/cf_arbitrage/{symbol}"

                        response = self.verified_session.request("GET", url, cookies=self.verified_cookies,
                                                                 params=querystring)
                        response_dict = response.json()

                        local_pos_amount = float(response_dict['data']['detail']['pos_list']['local_pos_amount'])
                        pos_amount = float(response_dict['data']['detail']['pos_list']['pos_amount'])
                        if (local_pos_amount > 0) & (pos_amount > 0):

                            total_failed_times = response_dict['data']['detail']['total_failed_times']

                            baseAmount = float(response_dict['data']['detail']['account']['baseAmount'])
                            baseUpl = float(response_dict['data']['detail']['account']['baseUpl'])
                            baseUpdateTime = response_dict['data']['detail']['account']['baseUpdateTime']
                            poss_df = pd.DataFrame(response_dict['data']['detail']['pos_list']['poss'])
                            spot_remain = float(response_dict['data']['detail']['pos_list']['spot_remain'])
                            funding = float(response_dict['data']['detail']['pos_list']['funding'])

                            if not poss_df.empty:
                                url = f'https://dapi.binance.com/dapi/v1/aggTrades?symbol={symbol}_PERP&endTime={baseUpdateTime}&limit=1000'
                                aggTradesDf = pd.DataFrame(requests.get(url, proxies=self.proxies).json())
                                coimAggPrice = float(aggTradesDf.iloc[-1]['p'])
                                contract_value = 10

                                url = f'https://api.binance.com/api/v3/aggTrades?symbol={symbol + "T"}&limit=1000'
                                spotAggPrice = float(aggTradesDf.iloc[-1]['p'])
                                spotRemaminUsdtValue = spotAggPrice * spot_remain
                                fungdingUdstValue = spotAggPrice * funding
                                print(
                                    f"敞口（币）:{symbol}_盈利保留价值:{spotRemaminUsdtValue}_资金费率价值:{fungdingUdstValue}")

    # 目前只支持tt
    # 还没有支持okx的价格查询
    # 应该把所有交易所url都放在初始化那里，币安一个字典、okx一个字典，不然还要在函数内部各种判断
    # 感觉这种字典还不好弄，因为每个交易所结构参数都不一致，还是得分开弄
    def uRlCaculate(self, ):
        symbol_dict = self.tt_symbol_dict

        unreal_env_list = []
        for env, symbol_list in symbol_dict.items():
            try:
                unreal_list = []

                if self.prefix == 'mmadminjp':
                    usdm_cont_dict = {"BTC": 0.001, "ETH": 0.001}

                elif self.prefix == 'mmadmin':
                    usdm_cont_dict = {"BTC": 0.01, "ETH": 0.1, "LTC": 1, "XRP": 100, "ETC": 10, 'EOS': 10}

                querystring = {"env": env}
                print(env)

                for symbol in symbol_list:
                    if '-' in symbol:
                        print(symbol)
                        url = f"https://{self.prefix}.digifinex.org/api/cf_arbitrage/{symbol}"
                        response = self.verified_session.request("GET", url, cookies=self.verified_cookies,
                                                                 params=querystring)
                        response_dict = response.json()
                        #                     print(response_dict)
                        local_pos_amount = float(response_dict['data']['detail']['pos_list']['local_pos_amount'])
                        pos_amount = float(response_dict['data']['detail']['pos_list']['pos_amount'])
                        print(local_pos_amount, pos_amount)

                        if (local_pos_amount > 0) and (pos_amount > 0):
                            poss_df = pd.DataFrame(response_dict['data']['detail']['pos_list']['poss'])

                            poss_df[['OpenPrice', 'SpotKeep', 'SpotToSwap', 'SrOpen', 'Unit']] = poss_df[
                                ['OpenPrice', 'SpotKeep', 'SpotToSwap', 'SrOpen', 'Unit']].astype('float')

                            poss_df['start_date'] = poss_df['ID'].apply(
                                lambda x: datetime.datetime.strptime(x.split('_')[0], '%Y%m%d'))
                            poss_df['settlement_date'] = datetime.datetime.now()
                            poss_df['delivery_date'] = datetime.datetime.strptime('20' + symbol.split('-')[1],
                                                                                  '%Y%m%d')
                            poss_df['start2delivery'] = (poss_df['delivery_date'] - poss_df['start_date']).apply(
                                lambda x: x.days)
                            poss_df['settledelivery'] = (
                                        poss_df['delivery_date'] - poss_df['settlement_date']).apply(
                                lambda x: x.days)
                            poss_df['convert_sr'] = poss_df['settledelivery'] * poss_df['SrOpen'] / poss_df[
                                'start2delivery']

                            poss_df['start_spot_pirce'] = poss_df['OpenPrice'] / (1 + poss_df['SrOpen'])

                            if 'USDT' in symbol:

                                if self.prefix == "mmadminjp":
                                    request_symbol_name = symbol.split('-')[0]
                                    print(request_symbol_name)
                                    url = f'https://api.binance.com/api/v3/aggTrades?symbol={request_symbol_name}&limit=1000'
                                    aggTradesDf = pd.DataFrame(requests.get(url, proxies=self.proxies).json())
                                    cur_spot_price = float(aggTradesDf.iloc[-1]['p'])
                                elif self.prefix == "mmadmin":
                                    request_spot_symbol_name = symbol.split('-')[0].replace("USDT", "") + '-USDT'
                                    print(request_spot_symbol_name)
                                    okxHisTradesUrl = "https://www.okx.com/api/v5/market/history-trades"
                                    url = okxHisTradesUrl + f"?instId={request_spot_symbol_name}&limit=500"
                                    spotHisTradeDf = pd.DataFrame(
                                        requests.get(url, proxies=self.proxies).json()['data'])
                                    cur_spot_price = float(spotHisTradeDf.iloc[0]['px'])

                                cont_value = usdm_cont_dict[f'{symbol.split("-")[0].replace("USDT", "")}']
                                if self.prefix == "mmadminjp":
                                    request_symbol_name = symbol.split('-')[0] + '_' + symbol.split('-')[1]
                                    print(request_symbol_name)
                                    url = f'https://fapi.binance.com/fapi/v1/aggTrades?symbol={request_symbol_name}&limit=1000'
                                    aggTradesDf = pd.DataFrame(requests.get(url, proxies=self.proxies).json())
                                    cur_future_price = float(aggTradesDf.iloc[-1]['p'])
                                elif self.prefix == "mmadmin":
                                    request_symbol_name = request_spot_symbol_name + '-' + symbol.split('-')[1]
                                    print(request_symbol_name)
                                    okxHisTradesUrl = "https://www.okx.com/api/v5/market/history-trades"
                                    url = okxHisTradesUrl + f"?instId={request_symbol_name}&limit=500"
                                    futureHisTradeDf = pd.DataFrame(
                                        requests.get(url, proxies=self.proxies).json()['data'])
                                    cur_future_price = float(futureHisTradeDf.iloc[0]['px'])

                                poss_df['cur_fair_future_price'] = cur_spot_price * (1 + poss_df['convert_sr'])
                                poss_df['usdt'] = poss_df['SpotKeep'] + poss_df['SpotToSwap']
                                poss_df['spot_pnl'] = poss_df['usdt'] / cur_spot_price - poss_df['usdt'] / poss_df[
                                    'start_spot_pirce']

                                poss_df['swap_pnl_fair'] = (cont_value * poss_df['Unit'] * (
                                            poss_df['cur_fair_future_price'] - poss_df[
                                        'OpenPrice'])) / cur_spot_price
                                poss_df['swap_pnl_real'] = (cont_value * poss_df['Unit'] * (
                                            cur_future_price - poss_df['OpenPrice'])) / cur_spot_price

                                poss_df['pnl_fair'] = poss_df['spot_pnl'] + poss_df['swap_pnl_fair']
                                poss_df['pnl_real'] = poss_df['spot_pnl'] + poss_df['swap_pnl_real']
                                poss_df['pnl_unreal'] = poss_df['pnl_real'] - poss_df['pnl_fair']
                                poss_df['pnl_unreal_usdt'] = poss_df['pnl_unreal'] * cur_spot_price
                                unreal_list.append({"symbol": symbol.split("-")[0].replace('USDT', ''),
                                                    "urCoin": poss_df['pnl_unreal'].sum(),
                                                    "urUsdt": poss_df['pnl_unreal_usdt'].sum()})
                                print(
                                    f'币本位合约浮盈计算_标的：{symbol}_浮盈（coin）:{poss_df["pnl_unreal"].sum()}_浮盈（u):{poss_df["pnl_unreal_usdt"].sum()}面值:{cont_value}')

                            else:
                                if self.prefix == "mmadminjp":
                                    request_symbol_name = symbol.split('-')[0] + 'T'
                                    print(request_symbol_name)
                                    url = f'https://api.binance.com/api/v3/aggTrades?symbol={request_symbol_name}&limit=1000'
                                    aggTradesDf = pd.DataFrame(requests.get(url, proxies=self.proxies).json())
                                    cur_spot_price = float(aggTradesDf.iloc[-1]['p'])
                                elif self.prefix == "mmadmin":
                                    request_spot_symbol_name = symbol.split('-')[0].replace("USD", "") + '-USDT'
                                    print(request_spot_symbol_name)
                                    okxHisTradesUrl = "https://www.okx.com/api/v5/market/history-trades"
                                    url = okxHisTradesUrl + f"?instId={request_spot_symbol_name}&limit=500"
                                    spotHisTradeDf = pd.DataFrame(
                                        requests.get(url, proxies=self.proxies).json()['data'])
                                    cur_spot_price = float(spotHisTradeDf.iloc[0]['px'])

                                if symbol.split("-")[0].replace('USD', '') == "BTC":
                                    cont_value = 100
                                else:
                                    cont_value = 10

                                if self.prefix == "mmadminjp":
                                    #                                 request_symbol_name = request_symbol_name + symbol.split('-')[1]
                                    request_symbol_name = symbol.split('-')[0] + '_' + symbol.split('-')[1]
                                    print(request_symbol_name)
                                    url = f'https://dapi.binance.com/dapi/v1/aggTrades?symbol={request_symbol_name}&limit=1000'
                                    aggTradesDf = pd.DataFrame(requests.get(url, proxies=self.proxies).json())
                                    cur_future_price = float(aggTradesDf.iloc[-1]['p'])
                                elif self.prefix == "mmadmin":
                                    request_symbol_name = request_spot_symbol_name + '-' + symbol.split('-')[1]
                                    print(request_symbol_name)
                                    okxHisTradesUrl = "https://www.okx.com/api/v5/market/history-trades"
                                    url = okxHisTradesUrl + f"?instId={request_symbol_name}&limit=500"
                                    futureHisTradeDf = pd.DataFrame(
                                        requests.get(url, proxies=self.proxies).json()['data'])
                                    cur_future_price = float(futureHisTradeDf.iloc[0]['px'])

                                poss_df['cur_fair_future_price'] = cur_spot_price * (1 + poss_df['convert_sr'])
                                poss_df['usdt'] = poss_df['SpotKeep'] + poss_df['SpotToSwap']
                                poss_df['spot_pnl'] = poss_df['usdt'] / cur_spot_price - poss_df['usdt'] / poss_df[
                                    'start_spot_pirce']

                                poss_df['swap_pnl_fair'] = cont_value * poss_df['Unit'] * (
                                            1 / poss_df['OpenPrice'] - 1 / poss_df['cur_fair_future_price'])
                                poss_df['swap_pnl_real'] = cont_value * poss_df['Unit'] * (
                                            1 / poss_df['OpenPrice'] - 1 / cur_future_price)

                                poss_df['pnl_fair'] = poss_df['spot_pnl'] + poss_df['swap_pnl_fair']
                                poss_df['pnl_real'] = poss_df['spot_pnl'] + poss_df['swap_pnl_real']
                                poss_df['pnl_unreal'] = poss_df['pnl_real'] - poss_df['pnl_fair']
                                poss_df['pnl_unreal_usdt'] = poss_df['pnl_unreal'] * cur_spot_price
                                unreal_list.append({"symbol": symbol.split("-")[0].replace('USD', ''),
                                                    "urCoin": poss_df['pnl_unreal'].sum(),
                                                    "urUsdt": poss_df['pnl_unreal_usdt'].sum(),
                                                    "cur_spot_price": cur_spot_price})
                                print(
                                    f'币本位合约浮盈计算_标的：{symbol}_浮盈（coin）:{poss_df["pnl_unreal"].sum()}_浮盈（u):{poss_df["pnl_unreal_usdt"].sum()}面值:{cont_value}')
                urDf = pd.DataFrame(unreal_list)
                if not urDf.empty:
                    print(urDf, urDf['urUsdt'].sum())
                unreal_env_list.append({env: pd.DataFrame(unreal_list)})
            except Exception as e:
                print(e)
        return unreal_env_list

    # 持仓均价计算测试
    def posAvgSr(self, env='tt', target_env_list=[]):
        if env == 'tt':
            symbol_dict = self.tt_symbol_dict
        elif env == 'mt':
            symbol_dict = self.mt_symbol_dict

        if env == 'tt':
            if len(target_env_list) == 0:
                target_env_list = list(self.tt_symbol_dict.keys())
            symbol_dict = self.tt_symbol_dict
        elif env == 'mt':
            if len(target_env_list) == 0:
                target_env_list = list(self.mt_symbol_dict.keys())
            symbol_dict = self.mt_symbol_dict

        env_sr_dict = {}
        for env, symbol_list in symbol_dict.items():
            if env in target_env_list:
                print(env)
    
                querystring = {"env": env}
                srOpenMeanList = []
    
                for symbol, status in symbol_list.items():
                    if status == 'On':
                        url = f"https://{self.prefix}.digifinex.org/api/cf_arbitrage/{symbol}"
                        querystring = {"env": env}
                        response = self.verified_session.request("GET", url, cookies=self.verified_cookies,
                                                                 params=querystring)
                        response_dict = response.json()
                        try:
                            local_pos_amount = float(response_dict['data']['detail']['pos_list']['local_pos_amount'])
                        except Exception as e:
                            print(e)
                            continue
                        try:
                            cur_funding = float(response_dict['data']['detail']['funding_rate']['fundingRate'])
                        except Exception as e:
                            print(e)
                            cur_funding = 0
    
                        if self.prefix == "mmadminjp":
                            if '-' not in symbol:
                                if not ('USDT' in symbol):
                                    request_symbol_name = symbol + 'T'
                                    if 'BTC' in symbol:
                                        cur_spot_price = 100
                                    else:
                                        cur_spot_price = 10
                                else:
                                    request_symbol_name = symbol
                                    print(request_symbol_name)
                                    url = f'https://api.binance.com/api/v3/aggTrades?symbol={request_symbol_name}&limit=1000'
                                    aggTradesDf = pd.DataFrame(requests.get(url, proxies=self.proxies).json())
                                    cur_spot_price = float(aggTradesDf.iloc[-1]['p'])
                        elif self.prefix == "mmadmin":
                            if not ('USDT' in symbol):
                                if not ('USDT' in symbol):
                                    request_symbol_name = symbol + 'T'
                                    if 'BTC' in symbol:
                                        cur_spot_price = 100
                                    else:
                                        cur_spot_price = 10
                            else:
                                request_spot_symbol_name = symbol.split('-')[0].replace("USDT", "") + '-USDT'
                                print(request_spot_symbol_name)
                                okxHisTradesUrl = "https://www.okx.com/api/v5/market/history-trades"
                                url = okxHisTradesUrl + f"?instId={request_spot_symbol_name}&limit=500"
                                spotHisTradeDf = pd.DataFrame(requests.get(url, proxies=self.proxies).json()['data'])
                                cur_spot_price = float(spotHisTradeDf.iloc[0]['px'])
    
                        if local_pos_amount > 0 and ('-' not in symbol):
                            poss = response_dict['data']['detail']['pos_list']['poss']
                            poss_df = pd.DataFrame(poss)
                            poss_df['Unit'] = poss_df['Unit'].astype(float)
                            poss_df['SrOpen'] = poss_df['SrOpen'].astype(float)
                            poss_df['OpenPrice'] = poss_df['OpenPrice'].astype(float)
                            poss_df['UnitRatio'] = poss_df['Unit']/poss_df['Unit'].sum()
                            # print(poss_df['SrOpen'], poss_df['UnitRatio'])
                            srOpenWeightedMean = np.multiply(poss_df['SrOpen'], poss_df['UnitRatio']).sum()
                            openPriceWeightedMean = np.multiply(poss_df['OpenPrice'], poss_df['UnitRatio']).sum()
                            srOpenMeanList.append({'symbol': symbol, 'srOpen': srOpenWeightedMean,
                                                   'local_pos_amount': local_pos_amount,
                                                   'cur_spot_price': cur_spot_price, 'cur_funding': cur_funding,
                                                   'open_price':openPriceWeightedMean})
    
                srOpenMeanDf = pd.DataFrame(srOpenMeanList)
                print(srOpenMeanDf)
                if not srOpenMeanDf.empty:
                    srOpenMeanDf.set_index('symbol', inplace=True)
                    srOpenMeanDf['cur_usdt_value'] = srOpenMeanDf['local_pos_amount'] * srOpenMeanDf['cur_spot_price']
                    srOpenMeanDf['cur_usdt_value_ratio'] = srOpenMeanDf['cur_usdt_value'] / srOpenMeanDf[
                        'cur_usdt_value'].sum()
                env_sr_dict[env] = srOpenMeanDf
        return env_sr_dict

    def futureAvgCost(self, env='tt'):
        if env == 'tt':
            symbol_dict = self.tt_symbol_dict
        elif env == 'mt':
            symbol_dict = self.mt_symbol_dict

        env_fac_dict = {}
        for env, symbol_list in symbol_dict.items():
            print(env)

            querystring = {"env": env}
            futureAvgCostList = []

            for symbol, status in symbol_list.items():
                if '-' in symbol:
                    print(symbol)
                    url = f"https://{self.prefix}.digifinex.org/api/cf_arbitrage/{symbol}"
                    response = self.verified_session.request("GET", url, cookies=self.verified_cookies,
                                                             params=querystring)
                    response_dict = response.json()
                    print(response_dict)
                    local_pos_amount = float(response_dict['data']['detail']['pos_list']['local_pos_amount'])
                    pos_amount = float(response_dict['data']['detail']['pos_list']['pos_amount'])
                    print(local_pos_amount, pos_amount)

                    if (local_pos_amount > 0) and (pos_amount > 0):
                        poss_df = pd.DataFrame(response_dict['data']['detail']['pos_list']['poss'])

                        poss_df[['OpenPrice', 'SpotKeep', 'SpotToSwap', 'SrOpen', 'Unit']] = poss_df[
                            ['OpenPrice', 'SpotKeep', 'SpotToSwap', 'SrOpen', 'Unit']].astype('float')

                        poss_df['start_date'] = poss_df['ID'].apply(
                            lambda x: datetime.datetime.strptime(x.split('_')[0], '%Y%m%d'))
                        #                         poss_df['settlement_date'] = datetime.datetime.now()
                        poss_df['delivery_date'] = datetime.datetime.strptime('20' + symbol.split('-')[1], '%Y%m%d')
                        poss_df['start2delivery'] = (poss_df['delivery_date'] - poss_df['start_date']).apply(
                            lambda x: x.days)
                        #                         poss_df['settledelivery'] = (poss_df['delivery_date'] - poss_df['settlement_date']).apply(lambda x:x.days)
                        poss_df['future_avg_cost'] = 365 * poss_df['SrOpen'] / poss_df['start2delivery']
                        poss_df['Unit_ratio'] = poss_df['Unit'] / poss_df['Unit'].sum()
                        future_avg_cost = (poss_df['future_avg_cost'] * poss_df['Unit_ratio']).sum()
                        if self.prefix == "mmadminjp":
                            request_symbol_name = symbol.split('-')[0]
                            # 如果是币本位合约，则乘数为u的面值
                            if 'USDT' not in request_symbol_name:
                                if 'BTC' in request_symbol_name:
                                    mf = 100
                                elif 'ETH' in request_symbol_name:
                                    mf = 10
                            # 如果是U本位合约，pos_amount是币，乘数用价格
                            else:
                                url = f'https://api.binance.com/api/v3/aggTrades?symbol={request_symbol_name}&limit=1000'
                                aggTradesDf = pd.DataFrame(requests.get(url, proxies=self.proxies).json())
                                cur_spot_price = float(aggTradesDf.iloc[-1]['p'])
                                mf = cur_spot_price
                        elif self.prefix == "mmadmin":
                            request_spot_symbol_name = symbol.split('-')[0].replace("USDT", "") + '-USDT'
                            print(request_spot_symbol_name)
                            okxHisTradesUrl = "https://www.okx.com/api/v5/market/history-trades"
                            url = okxHisTradesUrl + f"?instId={request_spot_symbol_name}&limit=500"
                            spotHisTradeDf = pd.DataFrame(requests.get(url, proxies=self.proxies).json()['data'])
                            cur_spot_price = float(spotHisTradeDf.iloc[0]['px'])
                        futureAvgCostList.append({'symbol': symbol, 'local_pos_amount': local_pos_amount, 'mf': mf,
                                                  'future_avg_cost': future_avg_cost})

            futureAvgCostDf = pd.DataFrame(futureAvgCostList)
            print(futureAvgCostDf)
            if not futureAvgCostDf.empty:
                futureAvgCostDf.set_index('symbol', inplace=True)
                futureAvgCostDf['usdt_value'] = futureAvgCostDf['local_pos_amount'] * futureAvgCostDf['mf']
                futureAvgCostDf['usdt_value_ratio'] = futureAvgCostDf['usdt_value'] / futureAvgCostDf[
                    'usdt_value'].sum()
                env_fac_dict[env] = futureAvgCostDf
                print(futureAvgCostDf)
        return env_fac_dict

    def totalPos(self, symbol):
        total_pos = 0
        for env, symbol_list in self.tt_symbol_dict.items():
            if symbol in symbol_list:
                try:
                    url = f"https://{self.prefix}.digifinex.org/api/cf_arbitrage/{symbol}"
                    querystring = {"env": env}
                    response = self.verified_session.request("GET", url, cookies=self.verified_cookies,
                                                             params=querystring)
                    response_dict = response.json()
                    pos_amount = float(response_dict['data']['detail']['pos_list']['pos_amount'])
                    total_pos += pos_amount
                except Exception as e:
                    print(e)
                    continue

        for env, symbol_list in self.mt_symbol_dict.items():
            if symbol in symbol_list:
                url = f"https://{self.prefix}.digifinex.org/api/cf_arbitrage/{symbol}"
                querystring = {"env": env}
                response = self.verified_session.request("GET", url, cookies=self.verified_cookies,
                                                         params=querystring)
                response_dict = response.json()
                pos_amount = float(response_dict['data']['detail']['pos_list']['pos_amount'])
                total_pos += pos_amount

        return total_pos

