import pandas as pd
import numpy as np
import pickle
import os
import re
from datetime import datetime
from pandas.errors import EmptyDataError
import pytz
import argparse
import matplotlib
matplotlib.use('Agg')  # 使用非交互式后端，适合服务器环境
import matplotlib.pyplot as plt
import requests
import logging
import pytz
import traceback

pd.set_option('display.max_columns', None)

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')




PORTFOLIO_CONFIG = {
    'pmpro': {
        'base_path': '/data/vhosts/cf_dc/manager_maker_dc_pmpro_test/app',
        'total_capital': 100000,
        'title_prefix': '10W U Portfolio PnL',
        'file_suffix': '_10WU',
        'denominator_ratio': 0.04,
        'long_term_start': '2025-05-22 06:00:00'
    },
    'dcbb1': {
        'base_path': '/data_file/CF_data/cf_dc/manager_dcbb1/app',
        'total_capital': 1350000,
        'title_prefix': 'Dcbb1(ltp15BTC) PnL',
        'file_suffix': '_dcbb1',
        'denominator_ratio': 0.04,
        'long_term_start': '2025-01-01 04:00:00'
    },   
    'dctest6': {
        'base_path': '/data_file/CF_data/cf_dc/manager_dctest6/app',
        'total_capital': 10000,
        'title_prefix': 'dctest6(1WU) PnL',
        'file_suffix': '_dctest6',
        'denominator_ratio': 0.04,
        'long_term_start': '2025-02-28 04:00:00'
    },  
    'dcpro1': {
        'base_path': '/data_file/CF_data/cf_dc/manager_dcpro1/app',
        'total_capital': 1000000,
        'title_prefix': 'Pro1(100WU) PnL',
        'file_suffix': '_100WU',
        'denominator_ratio': 0.04,
        'long_term_start': '2025-06-25 04:00:00'
    },
    'dcpro2': {
        'base_path': '/data_file/CF_data/cf_dc/manager_dcpro2/app',
        'total_capital': 2100000,
        'title_prefix': 'Pro2 (210WU) PnL',
        'file_suffix': '_dcpro2',
        'denominator_ratio': 0.04,
        'long_term_start': '2025-07-29 22:00:00'
    },
    # 'dcob1': {
    #     'base_path': '/data_file/CF_data/cf_dc/manager_dcob1/app',
    #     'total_capital': 1000000,
    #     'title_prefix': '100W U OK-Bybit Portfolio PnL',
    #     'file_suffix': '_dcob1',
    #     'denominator_ratio': 0.02,
    #     'long_term_start': '2025-07-22 19:00:00'
    # },
    'dcpro3': {
        'base_path': '/data_file/CF_data/cf_dc/manager_dcpro3/app',
        'total_capital': 3800000,
        'title_prefix': 'Pro3(380WU) PnL',
        'file_suffix': '_dcpro3',
        'denominator_ratio': 0.04,
        'long_term_start': '2025-07-17 19:00:00'
    },
    'dcpro4': {
        'base_path': '/data_file/CF_data/cf_dc/manager_dcpro4/app',
        'total_capital': 2700000,
        'title_prefix': 'Pro4(270WU) PnL',
        'file_suffix': '_dcpro4',
        'denominator_ratio': 0.04,
        'long_term_start': '2025-07-29 22:00:00'
    },
    'dcpro5': {
        'base_path': '/data_file/CF_data/cf_dc/manager_dcpro5/app',
        'total_capital': 650000,
        'title_prefix': 'Pro5(65WU) PnL',
        'file_suffix': '_dcpro5',
        'denominator_ratio': 0.04,
        'long_term_start': '2025-08-02 00:00:00'
    },
    'dcpro6': {
        'base_path': '/data_file/CF_data/cf_dc/manager_dcpro6/app',
        'total_capital': 2000000,
        'title_prefix': 'Pro6(23BTC) Portfolio PnL',
        'file_suffix': '_dcpro6',
        'denominator_ratio': 0.04,
        'long_term_start': '2025-08-16 00:00:00'
    },
    'dcpro7': {
        'base_path': '/data_file/CF_data/cf_dc/manager_dcpro7/app',
        'total_capital': 2300000,
        'title_prefix': 'Pro7(230WU) PnL',
        'file_suffix': '_dcpro7',
        'denominator_ratio': 0.04,
        'long_term_start': '2025-08-16 00:00:00'
    },
    'dcpro8': {
        'base_path': '/data_file/CF_data/cf_dc/manager_dcpro8/app',
        'total_capital': 2000000,
        'title_prefix': 'Pro8(200WU) PnL',
        'file_suffix': '_dcpro8',
        'denominator_ratio': 0.04,
        'long_term_start': '2025-09-12 04:00:00'
    },
    'dcpro9': {
        'base_path': '/data_file/CF_data/cf_dc/manager_dcpro9/app',
        'total_capital': 2500000,
        'title_prefix': 'Pro9(250WU) PnL',
        'file_suffix': '_dcpro9',
        'denominator_ratio': 0.04,
        'long_term_start': '2025-09-12 04:00:00'
    },
    'dcpro10': {
        'base_path': '/data_file/CF_data/cf_dc/manager_dcpro10/app',
        'total_capital': 3000000,
        'title_prefix': 'Pro10(300WU) PnL',
        'file_suffix': '_dcpro10',
        'denominator_ratio': 0.04,
        'long_term_start': '2025-09-12 04:00:00'
    },
    'dcpro11': {
        'base_path': '/data_file/CF_data/cf_dc/manager_dcpro11/app',
        'total_capital': 900000,
        'title_prefix': 'Pro11(10BTC) PnL',
        'file_suffix': '_dcpro11',
        'denominator_ratio': 0.04,
        'long_term_start': '2025-09-13 16:00:00'
    },
    'dcpro12': {
        'base_path': '/data_file/CF_data/cf_dc/manager_dcpro12/app',
        'total_capital': 1400000,
        'title_prefix': 'Pro12(16BTC) PnL',
        'file_suffix': '_dcpro12',
        'denominator_ratio': 0.04,
        'long_term_start': '2025-09-17 00:00:00'
    },
    'dcpro13': {
        'base_path': '/data_file/CF_data/cf_dc/manager_dcpro13/app',
        'total_capital': 3000000,
        'title_prefix': 'Pro13(1083ETH) PnL',
        'file_suffix': '_dcpro13',
        'denominator_ratio': 0.04,
        'long_term_start': '2025-10-01 00:00:00'
    },
    'dcpro14': {
        'base_path': '/data_file/CF_data/cf_dc/manager_dcpro14/app',
        'total_capital': 2700000,
        'title_prefix': 'Pro14(30BTC) PnL',
        'file_suffix': '_dcpro14',
        'denominator_ratio': 0.04,
        'long_term_start': '2025-10-01 00:00:00'
    },
    'dcpro15': {
        'base_path': '/data_file/CF_data/cf_dc/manager_dcpro15/app',
        'total_capital': 2500000,
        'title_prefix': 'Pro15(250WU) PnL',
        'file_suffix': '_dcpro15',
        'denominator_ratio': 0.04,
        'long_term_start': '2025-10-01 00:00:00'
    },
    'dcpro16': {
        'base_path': '/data_file/CF_data/cf_dc/manager_dcpro16/app',
        'total_capital': 3250000,
        'title_prefix': 'Pro16(325WU) PnL',
        'file_suffix': '_dcpro16',
        'denominator_ratio': 0.04,
        'long_term_start': '2025-10-01 00:00:00'
    },
    'dcpro17': {
        'base_path': '/data_file/CF_data/cf_dc/manager_dcpro17/app',
        'total_capital': 3400000,
        'title_prefix': 'Pro17(340WU) PnL',
        'file_suffix': '_dcpro17',
        'denominator_ratio': 0.04,
        'long_term_start': '2025-10-01 00:00:00'
    },
    # 'dcpmtest3': {
    #     'base_path': '/data_file/CF_data/cf_dc/manager_dcpmtest3/app',
    #     'total_capital': 100000,
    #     'title_prefix': '10W U Test3 Portfolio PnL',
    #     'file_suffix': '_dcpmtest3',
    #     'denominator_ratio': 0.04,
    #     'long_term_start': '2025-07-17 19:00:00'
    # },
    'pmtest2': {
        'base_path': '/data_file/CF_data/cf_dc/manager_maker_dc_pmtest2/app',
        'total_capital': 75000,
        'title_prefix': 'Pmtest2(7.5WU) Bn-bybit PnL',
        'file_suffix': '_pmtest2',
        'denominator_ratio': 0.04,
        'long_term_start': '2025-09-01 00:00:00'
    },
    'pmtest4': {
        'base_path': '/data_file/CF_data/aws_cf_csv/manager_dcpmtest4/app',
        'total_capital': 5000,
        'title_prefix': 'Pmtest4(5000U) Bn-Gate PnL',
        'file_suffix': '_pmtest4',
        'denominator_ratio': 0.04,
        'long_term_start': '2025-10-15 00:00:00'
    },
    'dcpro18': {
        'base_path': '/data_file/CF_data/cf_dc/manager_dcpro18/app',
        'total_capital': 1900000,
        'title_prefix': 'Pro18(634ETH) PnL',
        'file_suffix': '_dcpro18',
        'denominator_ratio': 0.04,
        'long_term_start': '2025-11-15 10:00:00'
    },    
    'dcpro19': {
        'base_path': '/data_file/CF_data/cf_dc/manager_dcpro19/app',
        'total_capital': 1665000,
        'title_prefix': 'Pro19(555ETH) PnL',
        'file_suffix': '_dcpro19',
        'denominator_ratio': 0.04,
        'long_term_start': '2025-11-25 18:00:00'
    },
    'dcpro20': {
        'base_path': '/data_file/CF_data/cf_dc/manager_dcpro20/app',
        'total_capital': 2357550,
        'title_prefix': 'Pro20(26.195BTC) PnL',
        'file_suffix': '_dcpro20',
        'denominator_ratio': 0.04,
        'long_term_start': '2025-11-25 18:00:00'
    },    
    'dcpro21': {
        'base_path': '/data_file/CF_data/cf_dc/manager_dcpro21/app',
        'total_capital': 2160000,
        'title_prefix': 'Pro21(24BTC) PnL',
        'file_suffix': '_dcpro21',
        'denominator_ratio': 0.04,
        'long_term_start': '2025-11-25 18:00:00'
    },    

    # 'dcpro22': {
    #     'base_path': '/data_file/CF_data/cf_dc/manager_dcpro22/app',
    #     'total_capital': 400000,
    #     'title_prefix': 'Pro22(40WU) PnL',
    #     'file_suffix': '_dcpro22',
    #     'denominator_ratio': 0.04,
    #     'long_term_start': '2025-11-25 18:00:00'
    # },    

    # 'dcpro23': {
    #     'base_path': '/data_file/CF_data/cf_dc/manager_dcpro23/app',
    #     'total_capital': 390000,
    #     'title_prefix': 'Pro23(130ETH) PnL',
    #     'file_suffix': '_dcpro23',
    #     'denominator_ratio': 0.04,
    #     'long_term_start': '2025-11-25 18:00:00'
    # },    

    'dcpro24': {
        'base_path': '/data_file/CF_data/cf_dc/manager_dcpro24/app',
        'total_capital': 3870000,
        'title_prefix': 'Pro24(42.955BTC) PnL',
        'file_suffix': '_dcpro24',
        'denominator_ratio': 0.04,
        'long_term_start': '2025-11-28 20:00:00'
    },    
    'dcpro25': {
        'base_path': '/data_file/CF_data/cf_dc/manager_dcpro25/app',
        'total_capital': 1545000,
        'title_prefix': 'Pro25(515ETH) PnL',
        'file_suffix': '_dcpro25',
        'denominator_ratio': 0.04,
        'long_term_start': '2025-12-05 12:00:00'
    },   
    'dcpro26': {
        'base_path': '/data_file/CF_data/cf_dc/manager_dcpro26/app',
        'total_capital': 2000000,
        'title_prefix': 'Pro26(26BTC) PnL',
        'file_suffix': '_dcpro26',
        'denominator_ratio': 0.04,
        'long_term_start': '2025-12-11 18:00:00'
    },   
    'dcpro27': {
        'base_path': '/data_file/CF_data/cf_dc/manager_dcpro27/app',
        'total_capital': 1050000,
        'title_prefix': 'Pro27(352ETH) PnL',
        'file_suffix': '_dcpro27',
        'denominator_ratio': 0.04,
        'long_term_start': '2025-12-11 18:00:00'
    },   
    'dcpro28': {
        'base_path': '/data_file/CF_data/cf_dc/manager_dcpro28/app',
        'total_capital': 1800000,
        'title_prefix': 'Pro28(600ETH) PnL',
        'file_suffix': '_dcpro28',
        'denominator_ratio': 0.04,
        'long_term_start': '2025-12-17 18:00:00'
    },   
# 不确定
    'dcpro29': {
        'base_path': '/data_file/CF_data/cf_dc/manager_dcpro29/app',
        'total_capital': 1000000,
        'title_prefix': 'Pro29(300ETH) PnL',
        'file_suffix': '_dcpro29',
        'denominator_ratio': 0.04,
        'long_term_start': '2025-12-17 18:00:00'
    },   
    'dcpro30': {
        'base_path': '/data_file/CF_data/cf_dc/manager_dcpro30/app',
        'total_capital': 650000,
        'title_prefix': 'Pro30(65WU) PnL',
        'file_suffix': '_dcpro30',
        'denominator_ratio': 0.04,
        'long_term_start': '2026-01-01 00:00:00'
    },   
    'dcpro31': {
        'base_path': '/data_file/CF_data/cf_dc/manager_dcpro31/app',
        'total_capital': 900000,
        'title_prefix': 'Pro31(10BTC) PnL',
        'file_suffix': '_dcpro31',
        'denominator_ratio': 0.04,
        'long_term_start': '2026-01-01 00:00:00'
    },   
    'dcpro32': {
        'base_path': '/data_file/CF_data/cf_dc/manager_dcpro32/app',
        'total_capital': 1500000,
        'title_prefix': 'Pro32(500ETH) PnL',
        'file_suffix': '_dcpro32',
        'denominator_ratio': 0.04,
        'long_term_start': '2026-01-07 00:00:00'
    },   
    'dcpro33': {
        'base_path': '/data_file/CF_data/cf_dc/manager_dcpro33/app',
        'total_capital': 1200000,
        'title_prefix': 'Pro33(13BTC) PnL',
        'file_suffix': '_dcpro33',
        'denominator_ratio': 0.04,
        'long_term_start': '2026-01-01 00:00:00'
    },   

    'dcpro34': {
        'base_path': '/data_file/CF_data/cf_dc/manager_dcpro34/app',
        'total_capital': 550000,
        'title_prefix': 'Pro34(55WU) PnL',
        'file_suffix': '_dcpro34',
        'denominator_ratio': 0.04,
        'long_term_start': '2026-01-16 00:00:00'
    },   

    'dcpro35': {
        'base_path': '/data_file/CF_data/cf_dc/manager_dcpro35/app',
        'total_capital': 1100000,
        'title_prefix': 'Pro35(12BTC) PnL',
        'file_suffix': '_dcpro35',
        'denominator_ratio': 0.04,
        'long_term_start': '2026-01-08 00:00:00'
    },   

    'dcpro38': {
        'base_path': '/data_file/CF_data/cf_dc/manager_dcpro38/app',
        'total_capital': 2700000,
        'title_prefix': 'Pro31(30BTC) PnL',
        'file_suffix': '_dcpro38',
        'denominator_ratio': 0.04,
        'long_term_start': '2026-01-25 00:00:00'
    },   


    # 'dcbb2': {
    #     'base_path': '/data_file/CF_data/cf_dc/manager_dcbb2/app',
    #     'total_capital': 400000,
    #     'title_prefix': 'Dcbb2(ltp40WU) PnL',
    #     'file_suffix': '_dcbb2',
    #     'denominator_ratio': 0.04,
    #     'long_term_start': '2025-01-01 04:00:00'
    # }  

}

def send_lark_bot(bot_id_req, message_content=None):
    """
    Parameters
    ----------
    bot_id_req : str, Lark机器人的webhook ID
    message_content : str, optional, 消息内容
        
    Returns
    -------
    tuple
        (status_message, error_message)
    """
    url = f"https://open.larksuite.com/open-apis/bot/v2/hook/{bot_id_req}"
    data = {"msg_type": "text","content": {"text": message_content}}
    headers = {"Content-Type": "application/json; charset=utf-8"}

    try:
        response = requests.post(url, json=data, headers=headers)
        if response.status_code != 200:
            return "", response.text

        result = response.json()

        if result.get('code') != 0: return "", result.get('msg', 'Unknown error')
        else: return result.get("StatusMessage", ""), None

    except requests.RequestException as e:
        return "", str(e)

def parse_start_time(start_str):
    match = re.match(r'^([0-9\-:.\s]+)\s\+0800', str(start_str))
    return pd.Timestamp(match.group(1)) if match else pd.NaT

def get_tickers_from_directory(app_dir):
    """从目录获取ticker列表"""
    try:
        if os.path.exists(app_dir):
            tickers = []
            for item in os.listdir(app_dir):
                item_path = os.path.join(app_dir, item)
                if os.path.isdir(item_path) and item.endswith('USDT'):
                    tickers.append(item.replace('USDT', ''))
            return tickers
    except Exception as e:
        logging.error(f"Error reading directory {app_dir}: {e}")
    return None

def load_server_data(ticker, kind, base_path):
    """从服务器加载数据"""
    file_path = f'{base_path}/{ticker}USDT/{ticker}_USDT.{kind}.csv'
    try:
        df = pd.read_csv(file_path, on_bad_lines="skip")
        if kind in ['open', 'close']:
            df = df.dropna(subset=['pos_id' if kind == 'open' else 'pos_ids'])
        return df
    except Exception:
        return None
    
def calculate_turnover(df_open, df_close):
    """计算成交额"""
    turnover = 0
    if df_open is not None and not df_open.empty:
        turnover += (df_open['swap1_avg_price'] * df_open['swap1_deal_amount'] + 
                     df_open['swap2_avg_price'] * df_open['swap2_deal_amount']).sum()
    if df_close is not None and not df_close.empty:
        turnover += (df_close['swap1_avg_price'] * df_close['swap1_deal_amount'] + 
                     df_close['swap2_avg_price'] * df_close['swap2_deal_amount']).sum()
    return turnover

def process_dataframe(df, start_date, end_date, numeric_cols):
    """处理和过滤DataFrame"""
    if df is None or df.empty:
        return None
    
    df = df[df['start'].astype(str).str.contains(r'\d{4}-\d{2}-\d{2}')].copy()
    df['time'] = df['start'].apply(parse_start_time)
    df = df[(df['time'] >= start_date) & (df['time'] <= end_date)]
    
    if not df.empty:
        for c in numeric_cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors='coerce')
    
    return df if not df.empty else None

def calculate_slippage(df_open, df_close):
    """计算滑点统计"""
    short_slippage = pd.Series(dtype=float)
    long_slippage = pd.Series(dtype=float)
    
    if df_open is not None and not df_open.empty:
        df_open['sign_short'] = np.where(df_open['swap1_side'] == 'buy', 1, 0)
        df_open['sign_long'] = np.where(df_open['swap1_side'] == 'sell', 1, 0)
        short_slippage = pd.concat([short_slippage, 
            (df_open['sr_open_real'] - df_open['sr_open'])[df_open['sign_short'] == 1]])
        long_slippage = pd.concat([long_slippage, 
            (df_open['sr_open'] - df_open['sr_open_real'])[df_open['sign_long'] == 1]])
    
    if df_close is not None and not df_close.empty:
        df_close['sign_short'] = np.where(df_close['swap1_side'] == 'closeShort', 1, 0)
        df_close['sign_long'] = np.where(df_close['swap1_side'] == 'closeLong', 1, 0)
        short_slippage = pd.concat([short_slippage, 
            (df_close['sr_close_real'] - df_close['sr_close'])[df_close['sign_short'] == 1]])
        long_slippage = pd.concat([long_slippage, 
            (df_close['sr_close'] - df_close['sr_close_real'])[df_close['sign_long'] == 1]])
    
    return {
        'short_slippage_mean': short_slippage.mean() if not short_slippage.empty else np.nan,
        'short_slippage_std': short_slippage.std() if not short_slippage.empty else np.nan,
        'short_count': len(short_slippage),
        'long_slippage_mean': long_slippage.mean() if not long_slippage.empty else np.nan,
        'long_slippage_std': long_slippage.std() if not long_slippage.empty else np.nan,
        'long_count': len(long_slippage)
    }

def analyze_pnl(start_date, end_date, portfolio_type='pmpro', tickers=None, save_files=True):
    """
    Parameters
    ----------
    start_date : pd.Timestamp
    end_date : pd.Timestamp
    portfolio_type : str, default 'pmpro'
    tickers : list, optional
    save_files : bool, default False
        
    Returns
    -------
    tuple
        (symbol_pnl_dict, portfolio_stats, result_df)
    """

    if portfolio_type not in PORTFOLIO_CONFIG:
        raise ValueError(f"Invalid portfolio_type: {portfolio_type}")
    
    config = PORTFOLIO_CONFIG[portfolio_type]
    base_path = config['base_path']
    total_capital = config['total_capital']
    denominator = total_capital * config['denominator_ratio']
    title_prefix = config['title_prefix']
    file_suffix = config['file_suffix']


    if tickers is None:
        tickers = get_tickers_from_directory(base_path)
        if tickers is None:
            logging.warning(f"Using default tickers for {portfolio_type}")
            tickers = ['BTC', 'ETH', 'XRP', 'SOL']  
            
    output_dir = '/data/vhosts/pnl_analysis' if save_files else None
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    records = []
    symbol_pnl_dict = {}
    portfolio_cum_pnl = pd.Series(dtype=float)
    portfolio_trade_pnl = pd.Series(dtype=float)
    portfolio_funding_pnl = pd.Series(dtype=float)
    total_portfolio_turnover = 0  




    for ticker in tickers:
        # Load data
        df_open = load_server_data(ticker, "open" ,base_path)
        df_close = load_server_data(ticker, "close" ,base_path)
        df_funding = load_server_data(ticker, "funding" ,base_path)

        # 首先检查是否有足够的数据进行分析
        if df_close is None and df_funding is None:
            # print(f'Both close and funding data missing for {ticker}, skipping...')
            continue

        ticker_end_date = end_date
        # Remove WCT special logic for dcpro
        if portfolio_type == 'pmpro' and ticker == 'WCT':  
            ticker_end_date = pd.Timestamp('2025-06-04 04:40:00')


        # 处理 funding 数据
        has_funding_data = False
        funding_pnl = 0
        funding_pnl_series = pd.Series(dtype=float)
        
        if df_funding is not None:
            df_funding['ts'] = pd.to_datetime(df_funding['ts'], errors='coerce')
            df_funding = df_funding.dropna(subset=['ts'])
            df_funding = df_funding[(df_funding['ts'] >= start_date) & (df_funding['ts'] <= ticker_end_date)]
            df_funding['funding'] = pd.to_numeric(df_funding['funding'], errors='coerce')
            if not df_funding.empty:
                has_funding_data = True
                funding_pnl = df_funding['funding'].sum()
                funding_pnl_series = df_funding.groupby(df_funding['ts'].dt.floor('1s'))['funding'].sum().cumsum()
        

        # 处理 trade 数据
        has_trade_data = False
        trade_pnl = pd.Series(dtype=float)
        spread_pnl = 0
        spread_winning_rate = np.nan
        
        df_open = process_dataframe(df_open, start_date, end_date, 
            ['sr_open_real', 'sr_open', 'swap2_avg_price', 'swap2_deal_amount', 'swap1_avg_price', 'swap1_deal_amount'])
        
        df_close = process_dataframe(df_close, start_date, end_date,
            ['sr_close_real', 'sr_close', 'sr_open_real', 'sr_open', 'real_earn', 
             'funding1_real', 'funding2_real', 'swap1_avg_price', 'swap1_deal_amount', 
             'swap2_avg_price', 'swap2_deal_amount'])
        
        if df_close is not None:
            has_trade_data = True
            spread_series = df_close['real_earn'] - df_close['funding1_real'] - df_close['funding2_real']
            spread_pnl = spread_series.sum()
            spread_winning_rate = (spread_series > 0).mean()
            trade_pnl = spread_series.cumsum()
            trade_pnl.index = df_close['time']
        
        # 如果两种数据都没有，跳过这个ticker
        if not has_funding_data and not has_trade_data:
            print(f'No data in date range for {ticker}, skipping...')
            continue
   


        # 确定起始时间
        valid_timestamps = []
        if has_funding_data:
            valid_timestamps.append(df_funding['ts'].iloc[0])
        if has_trade_data:
            valid_timestamps.append(df_close['time'].iloc[0])
        if df_open is not None and not df_open.empty:
            valid_timestamps.append(df_open['time'].iloc[0])
            
        s = min(valid_timestamps) if valid_timestamps else start_date
        
        # 计算总天数
        # if has_funding_data and has_trade_data:
        #     end_ts = max(df_funding['ts'].iloc[-1], df_close['time'].iloc[-1])
        # elif has_funding_data:
        #     end_ts = df_funding['ts'].iloc[-1]
        # elif has_trade_data:
        #     end_ts = df_close['time'].iloc[-1]
        # else:
        #     end_ts = ticker_end_date

        # 结束日期默认都用end_date
        end_ts = ticker_end_date       

        total_days = (end_ts - s).total_seconds() / 86400


        # 计算新的turnover - 单个币对的成交额
        # 计算turnover
        turnover = calculate_turnover(df_open, df_close)
        turnover_rate = turnover / (denominator * total_days) if denominator > 0 and total_days > 0 else 0
        total_portfolio_turnover += turnover

        
        # 合并 PnL 序列
        if has_trade_data and has_funding_data:
            trade_pnl = trade_pnl[~trade_pnl.index.duplicated(keep='last')].sort_index()
            combined_index = trade_pnl.index.union(funding_pnl_series.index)
            cum_pnl_combined = (
                trade_pnl.reindex(combined_index, method='ffill').fillna(0) +
                funding_pnl_series.reindex(combined_index, method='ffill').fillna(0)
            )
        elif has_trade_data:
            cum_pnl_combined = trade_pnl
        elif has_funding_data:
            cum_pnl_combined = funding_pnl_series
        
        # 计算统计数据
        stats = {
            'ticker': ticker,
            'spread_pnl': spread_pnl,
            'funding_pnl': funding_pnl,
            'total_pnl': spread_pnl + funding_pnl,
            'Total Ret Rate': 365 * (spread_pnl + funding_pnl)/(denominator*total_days) if total_days > 0 else 0,
            'Spread Ret Rate': 365*spread_pnl/(denominator*total_days) if total_days > 0 else 0,
            'Funding Ret Rate': 365*funding_pnl/(denominator*total_days) if total_days > 0 else 0,
            'turnover_rate': turnover_rate,  # 新的turnover_rate替换daily_turnover
            'turnover': turnover,  # 添加成交额
            'total_days': total_days
        }
        
        # 如果有交易数据，计算滑点和其他交易相关统计数据
        if has_trade_data or df_open is not None:
            stats.update(calculate_slippage(df_open, df_close))

        # Store PnL series for this symbol
        symbol_pnl_dict[ticker] = {
            'cum_pnl_combined': cum_pnl_combined,
            'funding_pnl_series': funding_pnl_series,
            'trade_pnl': trade_pnl,
            'Total_Ret_Rate': stats['Total Ret Rate'],
            'Spread_Ret_Rate': stats['Spread Ret Rate'],
            'Funding_Ret_Rate': stats['Funding Ret Rate'],
            'turnover_rate': turnover_rate
        }

        # Update portfolio PnL
        cum_pnl_combined = cum_pnl_combined[~cum_pnl_combined.index.duplicated(keep='last')].sort_index()

        combined_index = cum_pnl_combined.index.union(portfolio_cum_pnl.index)

        portfolio_cum_pnl = (
            portfolio_cum_pnl.reindex(combined_index, method='ffill').fillna(0) +
            cum_pnl_combined.reindex(combined_index, method='ffill').fillna(0)
        )

        trade_pnl = trade_pnl[~trade_pnl.index.duplicated(keep='last')].sort_index()
        portfolio_trade_pnl = (
            portfolio_trade_pnl.reindex(combined_index, method='ffill').fillna(0) +
            trade_pnl.reindex(combined_index, method='ffill').fillna(0)
        )

        funding_pnl_series = funding_pnl_series[~funding_pnl_series.index.duplicated(keep='last')].sort_index()
        portfolio_funding_pnl = (
            portfolio_funding_pnl.reindex(combined_index, method='ffill').fillna(0) +
            funding_pnl_series.reindex(combined_index, method='ffill').fillna(0)
        )

        # Add statistics to records
        records.append(stats)

    # 计算portfolio turnover rate
    total_days = (end_date - start_date).total_seconds() / 86400
    portfolio_turnover_rate = total_portfolio_turnover / (total_capital * total_days) if total_days > 0 else 0
    
    result_df = pd.DataFrame(records).sort_values(by='Total Ret Rate', ascending=False)
    
    # Add portfolio data to symbol_pnl_dict
    symbol_pnl_dict['portfolio'] = {
        'cum_pnl_combined': portfolio_cum_pnl,
        'funding_pnl_series': portfolio_funding_pnl,
        'trade_pnl': portfolio_trade_pnl,
        'turnover': total_portfolio_turnover,
        'turnover_rate': portfolio_turnover_rate
    }
    
    # Save results only if save_files is True
    if save_files and output_dir:
        # Save results with portfolio-specific suffix
        result_df.to_csv(os.path.join(output_dir, f'pnl_analysis_results{file_suffix}.csv'), index=False)
        
        # Save symbol_pnl_dict to pickle file with portfolio-specific suffix
        pkl_path = os.path.join(output_dir, f'symbol_pnl_dict{file_suffix}.pkl')
        with open(pkl_path, 'wb') as f:
            pickle.dump(symbol_pnl_dict, f)
        
        print(f"Symbol PnL dictionary saved to: {pkl_path}")
        
        # Plot portfolio PnL curves
        # plot_portfolio_pnl(portfolio_cum_pnl, portfolio_trade_pnl, portfolio_funding_pnl, output_dir, start_date, end_date, portfolio_turnover_rate, title_prefix, file_suffix, total_capital)
    
    return symbol_pnl_dict, portfolio_cum_pnl, portfolio_trade_pnl, portfolio_funding_pnl, result_df

def plot_portfolio_pnl(portfolio_cum_pnl, portfolio_trade_pnl, portfolio_funding_pnl, output_dir, start_date, end_date, portfolio_turnover_rate, title_prefix, file_suffix, total_capital):
    """
    绘制portfolio的三条PnL曲线并保存
    """
    if output_dir is None:
        return None
        
    try:
        # 创建图表
        fig, ax = plt.subplots(figsize=(15, 8))
        
        # 设置颜色
        colors = {
            'cum_pnl_combined': '#1f77b4',  # 蓝色
            'funding_pnl_series': '#ff7f0e',  # 橙色
            'trade_pnl': '#2ca02c'  # 绿色
        }
        
        # 绘制三条曲线
        if not portfolio_cum_pnl.empty:
            ax.plot(portfolio_cum_pnl.index, portfolio_cum_pnl.values, 
                   color=colors['cum_pnl_combined'], linewidth=2, label='Combined PnL')
        
        if not portfolio_funding_pnl.empty:
            ax.plot(portfolio_funding_pnl.index, portfolio_funding_pnl.values, 
                   color=colors['funding_pnl_series'], linewidth=2, label='Funding PnL')
        
        if not portfolio_trade_pnl.empty:
            ax.plot(portfolio_trade_pnl.index, portfolio_trade_pnl.values, 
                   color=colors['trade_pnl'], linewidth=2, label='Trade PnL')
        
        # 计算收益率
        total_days = (end_date - start_date).total_seconds() / 86400
        total_pnl = portfolio_cum_pnl.iloc[-1] if not portfolio_cum_pnl.empty else 0
        trade_pnl_total = portfolio_trade_pnl.iloc[-1] if not portfolio_trade_pnl.empty else 0
        funding_pnl_total = portfolio_funding_pnl.iloc[-1] if not portfolio_funding_pnl.empty else 0
        
        total_ret_rate = 365 * total_pnl / (total_capital * total_days) if total_days > 0 else 0
        spread_ret_rate = 365 * trade_pnl_total / (total_capital * total_days) if total_days > 0 else 0
        funding_ret_rate = 365 * funding_pnl_total / (total_capital * total_days) if total_days > 0 else 0
        
        # 设置图表属性 - 精确到秒的日期和收益率，添加turnover_rate
        title = (
            f"{title_prefix} from {start_date.strftime('%Y-%m-%d %H:%M:%S')} to {end_date.strftime('%Y-%m-%d %H:%M:%S')} ({total_days:.2f} days)\n"
            f"Annualized Return: {total_ret_rate:.2%} | Spread: {spread_ret_rate:.2%} | Funding: {funding_ret_rate:.2%} | Turnover: {portfolio_turnover_rate:.2f}"
        )
        
        ax.set_title(title, fontsize=14, fontweight='bold')
        ax.set_xlabel('Time', fontsize=12)
        ax.set_ylabel('PnL', fontsize=12)
        ax.legend(fontsize=12)
        ax.grid(True, alpha=0.3)
        
        # 旋转x轴标签
        ax.tick_params(axis='x', rotation=45)
        
        # 设置y轴格式
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:,.0f}'))
        
        plt.tight_layout()
        
        # 保存图片 - 只保存一个，不重复保存
        filename = f'portfolio_pnl_curves{file_suffix}.png'
        filepath = os.path.join(output_dir, filename)
        plt.savefig(filepath, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"Portfolio PnL chart saved to: {filepath}")
        return filepath
        
    except Exception as e:
        print(f"Error plotting portfolio PnL: {e}")
        return None

def calculate_portfolio_stats(start_date, end_date, portfolio_type, result_df, symbol_pnl_dict, config):
    """计算portfolio的总体统计指标"""
    total_days = (end_date - start_date).total_seconds() / 86400
    total_capital = config['total_capital']
    
    total_pnl = result_df['total_pnl'].sum()
    total_trade_pnl = result_df['spread_pnl'].sum()
    total_funding_pnl = result_df['funding_pnl'].sum()
    
    total_ret_rate = 365 * total_pnl / (total_days * total_capital) if total_days > 0 else 0
    spread_ret_rate = 365 * total_trade_pnl / (total_days * total_capital) if total_days > 0 else 0
    funding_ret_rate = 365 * total_funding_pnl / (total_days * total_capital) if total_days > 0 else 0
    
    portfolio_turnover_rate = symbol_pnl_dict.get('portfolio', {}).get('turnover_rate', 0)
    
    return {
        'date': start_date.strftime('%Y-%m-%d'),
        'total_pnl': total_pnl,
        'trade_pnl': total_trade_pnl,
        'funding_pnl': total_funding_pnl,
        'total_ret_rate': total_ret_rate,
        'spread_ret_rate': spread_ret_rate,
        'funding_ret_rate': funding_ret_rate,
        'turnover_rate': portfolio_turnover_rate,
        'total_days': total_days
    }

def calculate_weighted_portfolio_stats(all_portfolio_results, end_date, start_date):
    """
    计算资金加权的总体portfolio统计指标
    """
    total_capital = 0
    weighted_pnl = 0
    weighted_trade_pnl = 0
    weighted_funding_pnl = 0
    weighted_turnover = 0
    
    for portfolio_type, (stats, _) in all_portfolio_results.items():
        config = PORTFOLIO_CONFIG[portfolio_type]
        capital = config['total_capital']
        
        total_capital += capital
        weighted_pnl += stats['total_pnl']
        weighted_trade_pnl += stats['trade_pnl']
        weighted_funding_pnl += stats['funding_pnl']
        weighted_turnover += stats['turnover_rate'] * capital
    
    total_days = (end_date - start_date).total_seconds() / 86400
    
    return {
        'total_capital': total_capital,
        'total_pnl': weighted_pnl,
        'trade_pnl': weighted_trade_pnl,
        'funding_pnl': weighted_funding_pnl,
        'total_ret_rate': 365 * weighted_pnl / (total_days * total_capital) if total_days > 0 and total_capital > 0 else 0,
        'spread_ret_rate': 365 * weighted_trade_pnl / (total_days * total_capital) if total_days > 0 and total_capital > 0 else 0,
        'funding_ret_rate': 365 * weighted_funding_pnl / (total_days * total_capital) if total_days > 0 and total_capital > 0 else 0,
        'weighted_turnover_rate': weighted_turnover / total_capital if total_capital > 0 else 0,
        'total_days': total_days
    }

def aggregate_symbol_performance(all_portfolio_results):
    """
    聚合所有环境的币种表现，计算Top5和Bottom5
    返回: (aggregated_df, top5_stats, bottom5_stats)
    """
    # 合并所有环境的result_df
    all_symbols_data = []
    
    for portfolio_type, (stats, result_df) in all_portfolio_results.items():
        if result_df is not None and not result_df.empty:
            df_copy = result_df.copy()
            df_copy['portfolio_type'] = portfolio_type
            all_symbols_data.append(df_copy)
    
    if not all_symbols_data:
        return None, None, None
    
    # 合并所有数据
    combined_df = pd.concat(all_symbols_data, ignore_index=True)
    
    # 按ticker聚合
    aggregated = combined_df.groupby('ticker').agg({
        'total_pnl': 'sum',
        'spread_pnl': 'sum',
        'funding_pnl': 'sum'
    }).reset_index()
    
    # 排序获取Top5和Bottom5
    aggregated_sorted = aggregated.sort_values('total_pnl', ascending=False)
    top5 = aggregated_sorted.head(5)
    bottom5 = aggregated_sorted.tail(5)
    
    # 计算Top5和Bottom5的汇总统计
    top5_stats = {
        'total_pnl': top5['total_pnl'].sum(),
        'spread_pnl': top5['spread_pnl'].sum(),
        'funding_pnl': top5['funding_pnl'].sum(),
        'symbols': top5
    }
    
    bottom5_stats = {
        'total_pnl': bottom5['total_pnl'].sum(),
        'spread_pnl': bottom5['spread_pnl'].sum(),
        'funding_pnl': bottom5['funding_pnl'].sum(),
        'symbols': bottom5
    }
    
    return aggregated, top5_stats, bottom5_stats

def calculate_big_small_coin_performance(all_portfolio_results, end_date, start_date, total_capital):
    """
    计算大币和小币的分类收益率
    大币: ['BTC','ETH','DOGE','SOL','XRP','FARTCOIN']
    小币: 其他所有币
    """
    # 定义大币
    big_coins = ['BTC', 'ETH', 'DOGE', 'SOL', 'XRP', 'FARTCOIN']
    
    # 合并所有环境的result_df
    all_symbols_data = []
    
    for portfolio_type, (stats, result_df) in all_portfolio_results.items():
        if result_df is not None and not result_df.empty:
            df_copy = result_df.copy()
            all_symbols_data.append(df_copy)
    
    if not all_symbols_data:
        return None
    
    # 合并所有数据
    combined_df = pd.concat(all_symbols_data, ignore_index=True)
    
    # 按ticker聚合
    aggregated = combined_df.groupby('ticker').agg({
        'total_pnl': 'sum',
        'spread_pnl': 'sum',
        'funding_pnl': 'sum'
    }).reset_index()
    
    # 分类为大币和小币
    big_coin_df = aggregated[aggregated['ticker'].isin(big_coins)]
    small_coin_df = aggregated[~aggregated['ticker'].isin(big_coins)]
    
    # 计算总天数
    total_days = (end_date - start_date).total_seconds() / 86400
    
    # 计算年化收益率
    big_coin_pnl = big_coin_df['total_pnl'].sum()
    big_coin_spread_pnl = big_coin_df['spread_pnl'].sum()
    big_coin_funding_pnl = big_coin_df['funding_pnl'].sum()
    
    small_coin_pnl = small_coin_df['total_pnl'].sum()
    small_coin_spread_pnl = small_coin_df['spread_pnl'].sum()
    small_coin_funding_pnl = small_coin_df['funding_pnl'].sum()
    
    big_coin_ret_rate = 365 * big_coin_pnl / (total_days * total_capital) if total_days > 0 and total_capital > 0 else 0
    small_coin_ret_rate = 365 * small_coin_pnl / (total_days * total_capital) if total_days > 0 and total_capital > 0 else 0
    
    return {
        'big_coin': {
            'total_pnl': big_coin_pnl,
            'spread_pnl': big_coin_spread_pnl,
            'funding_pnl': big_coin_funding_pnl,
            'ret_rate': big_coin_ret_rate,
            'count': len(big_coin_df)
        },
        'small_coin': {
            'total_pnl': small_coin_pnl,
            'spread_pnl': small_coin_spread_pnl,
            'funding_pnl': small_coin_funding_pnl,
            'ret_rate': small_coin_ret_rate,
            'count': len(small_coin_df)
        }
    }

def should_run_long_term_analysis():
    """
    判断是否应该运行长期分析
    """
    beijing_tz = pytz.timezone('Asia/Shanghai')
    now = datetime.now(beijing_tz)
    
    
    # 检查是否是早上8点（允许8:00-8:59之间运行）
    is_4am_hour = now.hour == 4
    
    return is_4am_hour

def IsDate8AM(target_date):
    """
    判断是否是北京时间每周target_date早上8点
    target_date: 0=Monday, 1=Tuesday, 2 = Wednesday, 3 = Thursday, 4 = Friday, 5 = Saturday, 6=Sunday
    """
    beijing_tz = pytz.timezone('Asia/Shanghai')
    now = datetime.now(beijing_tz)
    is_target_day = now.weekday() == target_date
    
    # 检查是否是早上8点（允许8:00-8:59之间运行）
    is_8am_hour = now.hour == 8
    
    return is_target_day and is_8am_hour


def main():
    parser = argparse.ArgumentParser(description='Analyze PnL data from server files')
    parser.add_argument('--end_date', type=str, required=None,
                      help='End date and time in "YYYY-MM-DD HH:MM:SS" format')
    parser.add_argument('--start_date', type=str, default='all',
                      help='Start date and time in "YYYY-MM-DD HH:MM:SS" format (optional, for custom date range)')
    parser.add_argument('--portfolio_type', type=str, default='all', choices=list(PORTFOLIO_CONFIG.keys()) + ['all'],
                      help='Entering an existing portfolio type')
    parser.add_argument('--save_files', action='store_true',
                      help='Save files to disk (only for 04:00 daily runs)')
    
    args = parser.parse_args()
    
    # 改动3：默认使用当前时间
    end_date = pd.Timestamp(args.end_date) if args.end_date else pd.Timestamp.now()
    IsMonday8AM_flag = IsDate8AM(0)
    IsFriday8AM_flag = IsDate8AM(4)
    IsThursday8AM_flag = IsDate8AM(3)

    run_long_term = should_run_long_term_analysis()

    config = PORTFOLIO_CONFIG.get(args.portfolio_type, PORTFOLIO_CONFIG['pmpro'])

    
    # 改动5：如果 portfolio_type == 'all'，遍历所有portfolio
    if args.portfolio_type == 'all':
        yesterday_results = {}
        daily_results_by_portfolio = {}
        long_term_results = {}


        for portfolio_type, config in PORTFOLIO_CONFIG.items():
            try:
                print(f"Analyzing {portfolio_type} ")
                
                # 昨天
                yesterday_start = end_date - pd.Timedelta(days=1)
                print(f"\n1. 分析昨天一天的数据: {yesterday_start} 至 {end_date}")
                yesterday_symbol_pnl_dict, yesterday_portfolio_cum_pnl, yesterday_portfolio_trade_pnl, yesterday_portfolio_funding_pnl, yesterday_result_df = analyze_pnl(
                    start_date=yesterday_start,
                    end_date=end_date,
                    portfolio_type=portfolio_type,
                    save_files=False  # 不保存昨天分析的文件
                )
                yesterday_stats = calculate_portfolio_stats(yesterday_start, end_date, portfolio_type,
                                                            yesterday_result_df, yesterday_symbol_pnl_dict, config)
                yesterday_results[portfolio_type] = (yesterday_stats, yesterday_result_df)       

                # 2. 分析过去7天每天的数据
                print(f"\n2. 分析过去7天每天的数据")
                daily_results = []
                for i in range(7):
                    day_start = end_date - pd.Timedelta(days=i+1)
                    day_end = end_date - pd.Timedelta(days=i)
                    
                    try:
                        daily_symbol_pnl_dict, _, _, _, daily_result_df = analyze_pnl(
                            start_date=day_start,
                            end_date=day_end,
                            portfolio_type=portfolio_type,
                            save_files=False  
                        )
                        
                        stats = calculate_portfolio_stats(day_start, day_end, portfolio_type,daily_result_df, daily_symbol_pnl_dict, config)
                        daily_results.append(stats)
                        
                    except Exception as e:
                        print(f"Error analyzing day {day_start.strftime('%Y-%m-%d')}: {e}")

                daily_results_by_portfolio[portfolio_type] = daily_results
                
                # 3. 分析长期数据 (从固定开始日期到end_date)
                if run_long_term:
                    long_term_start_date = pd.Timestamp(config['long_term_start'])
                    print(f"\n3. 分析长期数据: {long_term_start_date} 至 {end_date}")
                    
                    long_term_symbol_pnl_dict, long_term_portfolio_cum_pnl, long_term_portfolio_trade_pnl, long_term_portfolio_funding_pnl, long_term_result_df = analyze_pnl(
                        start_date=long_term_start_date,
                        end_date=end_date,
                        portfolio_type=portfolio_type,
                        save_files=True  # 保存长期分析的文件
                    )
                    long_term_stats = calculate_portfolio_stats(long_term_start_date, end_date, portfolio_type,
                                                                long_term_result_df, long_term_symbol_pnl_dict, config)
                    long_term_results[portfolio_type] = (long_term_stats, long_term_result_df)

                    long_term_top_10 = long_term_result_df.sort_values(by='total_pnl', ascending=False).head(10)[['ticker', 'Total Ret Rate', 'total_pnl', 'spread_pnl', 'funding_pnl']].to_string(index=False)


                # 处理昨天数据

                yesterday_top_15 = yesterday_result_df.sort_values(by='total_pnl', ascending=False).head(15)[['ticker', 'Total Ret Rate', 'total_pnl', 'spread_pnl', 'funding_pnl']].to_string(index=False)
                yesterday_bottom_10 = yesterday_result_df.sort_values(by='total_pnl', ascending=True).head(10)[['ticker', 'Total Ret Rate', 'total_pnl', 'spread_pnl', 'funding_pnl']].to_string(index=False)

                # 处理过去7天的数据
                daily_df = pd.DataFrame(daily_results).sort_values('date', ascending=False)  # 按日期倒序排列
                
                # 计算过去7天总体表现
                past_7_days_total_pnl = daily_df['total_pnl'].sum()
                past_7_days_avg_ret_rate = daily_df['total_ret_rate'].mean()
                past_7_days_avg_ret_rate_spread = daily_df['spread_ret_rate'].mean()
                past_7_days_avg_ret_rate_funding = daily_df['funding_ret_rate'].mean()
                past_7_days_avg_turnover_rate = daily_df['turnover_rate'].mean()
                
                try:
                    # 构建过去7天的表格
                    daily_table = daily_df.to_string(index=False)
                    
                    # 构建消息内容
                    message_content = f"""📊 {config['title_prefix']} 综合分析报告

昨日表现 ({yesterday_start.strftime('%Y-%m-%d %H:%M:%S')} 至 {end_date.strftime('%Y-%m-%d %H:%M:%S')}):
总PnL: {yesterday_stats['total_pnl']:,.2f}
Spread PnL: {yesterday_stats['trade_pnl']:,.2f}
Funding PnL: {yesterday_stats['funding_pnl']:,.2f}
年化收益率: {yesterday_stats['total_ret_rate']:.2%}
价差收益率: {yesterday_stats['spread_ret_rate']:.2%}
资金费率收益率: {yesterday_stats['funding_ret_rate']:.2%}
换手率: {yesterday_stats['turnover_rate']:.2f}

昨天绝对收益Top15币种:
{yesterday_top_15}

昨天绝对收益Bottom10币种:
{yesterday_bottom_10}

过去7天每天表现:
{daily_table}

过去7天总体表现:
总PnL: {past_7_days_total_pnl:,.2f}
年化收益率: {past_7_days_avg_ret_rate:.2%}
价差收益率: {past_7_days_avg_ret_rate_spread:.2%}
资金费率收益率: {past_7_days_avg_ret_rate_funding:.2%}
平均日换手率: {past_7_days_avg_turnover_rate:.2f}

"""     
                    # 发送Lark消息
                    bot_id = "d2ed3efe-7d3b-4746-809e-3fcc40f934d6"
                    
                    status_msg, error_msg = send_lark_bot(bot_id, message_content)
                    
                    if error_msg:
                        print(f"Lark消息发送失败: {error_msg}")
                    else:
                        print(f"Lark消息发送成功: {status_msg}")
                        
                except Exception as e:
                    print(f"发送Lark消息时出错: {e}")
                    
            except Exception as e:
                # 捕获整个环境分析过程中的任何异常
                error_msg = f"⚠️ 环境 {portfolio_type} 分析出错\n\n错误信息: {str(e)}\n\n堆栈信息:\n{traceback.format_exc()}"
                print(error_msg)
                
                # 发送错误通知到飞书
                bot_id = "d2ed3efe-7d3b-4746-809e-3fcc40f934d6"
                status_msg, error_send = send_lark_bot(bot_id, error_msg)
                
                if error_send:
                    print(f"发送错误通知失败: {error_send}")
                else:
                    print(f"错误通知已发送到飞书")
                
                # 继续处理下一个环境
                continue

        # 改动6：计算资金加权的总体指标
        yesterday_weighted = calculate_weighted_portfolio_stats(yesterday_results, end_date, yesterday_start)
        env_rows = []

        for portfolio_type, (stats, _) in yesterday_results.items():
            env_rows.append({
                "环境": portfolio_type,
                "总PnL": f"{stats['total_pnl']:,.2f}",
                "年化收益率": f"{stats['total_ret_rate']:.2%}",
                "价差收益率": f"{stats['spread_ret_rate']:.2%}",
                "资金费率收益率": f"{stats['funding_ret_rate']:.2%}",
                "换手率": f"{stats['turnover_rate']:.2f}",
            })


        if env_rows:
            env_table = pd.DataFrame(env_rows).to_string(index=False)
        else:
            env_table = "（无可用环境数据）"
        
        # 新增：计算Top5和Bottom5币种表现
        aggregated_df, top5_stats, bottom5_stats = aggregate_symbol_performance(yesterday_results)
        
        top_bottom_section = ""
        if top5_stats is not None and bottom5_stats is not None:
            top5_table = top5_stats['symbols'][['ticker', 'total_pnl', 'spread_pnl', 'funding_pnl']].to_string(index=False)
            bottom5_table = bottom5_stats['symbols'][['ticker', 'total_pnl', 'spread_pnl', 'funding_pnl']].to_string(index=False)
            
            top_bottom_section = f"""
盈利Top5币种:
{top5_table}

亏损Bottom5币种:
{bottom5_table}
"""
        
        # 新增：计算大币和小币分类收益率
        coin_category_stats = calculate_big_small_coin_performance(
            yesterday_results, end_date, yesterday_start, yesterday_weighted['total_capital']
        )
        
        coin_category_section = ""
        if coin_category_stats is not None:
            big_coin = coin_category_stats['big_coin']
            small_coin = coin_category_stats['small_coin']
            
            coin_category_section = f"""
大币类别表现 (BTC/ETH/DOGE/SOL/XRP/FARTCOIN):
币种数量: {big_coin['count']}
总收益: {big_coin['total_pnl']:,.2f}
年化收益率: {big_coin['ret_rate']:.2%}

小币类别表现:
币种数量: {small_coin['count']}
总收益: {small_coin['total_pnl']:,.2f}
年化收益率: {small_coin['ret_rate']:.2%}
"""
        
        # 构建消息内容
        message_content = f"""所有环境总体表现

昨日表现总体表现 ({yesterday_start.strftime('%Y-%m-%d %H:%M:%S')} 至 {end_date.strftime('%Y-%m-%d %H:%M:%S')}):
计算使用本金: {yesterday_weighted['total_capital']:,.2f}
总PnL: {yesterday_weighted['total_pnl']:,.2f}
Spread PnL: {yesterday_weighted['trade_pnl']:,.2f}
Funding PnL: {yesterday_weighted['funding_pnl']:,.2f}
年化收益率: {yesterday_weighted['total_ret_rate']:.2%}
价差收益率: {yesterday_weighted['spread_ret_rate']:.2%}
资金费率收益率: {yesterday_weighted['funding_ret_rate']:.2%}
换手率: {yesterday_weighted['weighted_turnover_rate']:.2f}

各环境昨日表现:
{env_table}
{top_bottom_section}
{coin_category_section}
"""     

        if IsThursday8AM_flag or IsMonday8AM_flag or IsFriday8AM_flag:
            if IsThursday8AM_flag:
                # 表头
                header = "日期\t" + "\t".join([pt for pt in PORTFOLIO_CONFIG.keys()])
                table_rows = [header]             

                # 每天的数据
                for i in range(7):
                    day_start = end_date - pd.Timedelta(days=i+1)
                    row_data = [day_start.strftime('%m/%d/%Y')]
                    
                    for portfolio_type in PORTFOLIO_CONFIG.keys():
                        if portfolio_type in daily_results_by_portfolio and i < len(daily_results_by_portfolio[portfolio_type]):
                            ret_rate = daily_results_by_portfolio[portfolio_type][i]['total_ret_rate']
                            row_data.append(f"{ret_rate:.2%}")
                        else:
                            row_data.append("N/A")
                    
                    table_rows.append("\t".join(row_data))

                # 7日平均年化收益率
                avg_row = ["7日年化"]
                for portfolio_type in PORTFOLIO_CONFIG.keys():
                    if portfolio_type in daily_results_by_portfolio:
                        avg_ret = np.mean([d['total_ret_rate'] for d in daily_results_by_portfolio[portfolio_type]])
                        avg_row.append(f"{avg_ret:.2%}")
                    else:
                        avg_row.append("N/A")
                
                table_rows.append("\t".join(avg_row))

                # 输出格式1的表格
                table_output = "\n".join(table_rows)

            # === 计算过去7天资金加权总体指标 ===
            past_7_days_total_capital = 0
            weighted_total_ret = 0
            weighted_spread_ret = 0
            weighted_funding_ret = 0
            weighted_turnover = 0


            for portfolio_type, daily_data in daily_results_by_portfolio.items():
                if not daily_data:
                    continue
                capital = PORTFOLIO_CONFIG[portfolio_type].get("total_capital", 0)
                avg_total_ret = np.mean([d['total_ret_rate'] for d in daily_data])
                avg_spread_ret = np.mean([d['spread_ret_rate'] for d in daily_data])
                avg_funding_ret = np.mean([d['funding_ret_rate'] for d in daily_data])
                avg_turnover = np.mean([d['turnover_rate'] for d in daily_data])
                
                weighted_total_ret += avg_total_ret * capital
                weighted_spread_ret += avg_spread_ret * capital
                weighted_funding_ret += avg_funding_ret * capital
                weighted_turnover += avg_turnover * capital
                past_7_days_total_capital += capital

            if past_7_days_total_capital > 0:
                past_7_days_avg = {
                    'total_ret_rate': weighted_total_ret / past_7_days_total_capital,
                    'spread_ret_rate': weighted_spread_ret / past_7_days_total_capital,
                    'funding_ret_rate': weighted_funding_ret / past_7_days_total_capital,
                    'weighted_turnover_rate': weighted_turnover / past_7_days_total_capital,
                }
            else:
                past_7_days_avg = {
                    'total_ret_rate': 0,
                    'spread_ret_rate': 0,
                    'funding_ret_rate': 0,
                    'weighted_turnover_rate': 0,
                }



            # 构建格式2：各环境过去七天总体指标（周五和周一都要）
            summary_lines = ["各环境过去七天总体指标"]
            
            # 分组：bn-okx 和 Bn-bybit
            bnokx_portfolios = [pt for pt in PORTFOLIO_CONFIG.keys() if pt != 'pmtest2']
            bnbybit_portfolios = ['pmtest2']
            
            summary_lines.append("bn-okx")
            
            for portfolio_type in bnokx_portfolios:
                if portfolio_type not in daily_results_by_portfolio:
                    continue
                    
                portfolio_name = portfolio_type
                daily_data = daily_results_by_portfolio[portfolio_type]
                
                avg_total_ret = np.mean([d['total_ret_rate'] for d in daily_data])
                avg_spread_ret = np.mean([d['spread_ret_rate'] for d in daily_data])
                avg_funding_ret = np.mean([d['funding_ret_rate'] for d in daily_data])
                avg_turnover = np.mean([d['turnover_rate'] for d in daily_data])
                
                summary_lines.append(
                    f"{portfolio_name}过去七天：年化收益率: {avg_total_ret:.2%}, "
                    f"价差收益率: {avg_spread_ret:.2%}, 资金费率收益率: {avg_funding_ret:.2%}, "
                    f"平均日换手率: {avg_turnover:.2f}"
                )
            
            summary_lines.append("Bn-bybit")
            
            for portfolio_type in bnbybit_portfolios:
                if portfolio_type not in daily_results_by_portfolio:
                    continue
                    
                portfolio_name = portfolio_type
                daily_data = daily_results_by_portfolio[portfolio_type]
                
                avg_total_ret = np.mean([d['total_ret_rate'] for d in daily_data])
                avg_spread_ret = np.mean([d['spread_ret_rate'] for d in daily_data])
                avg_funding_ret = np.mean([d['funding_ret_rate'] for d in daily_data])
                avg_turnover = np.mean([d['turnover_rate'] for d in daily_data])
                
                summary_lines.append(
                    f"{portfolio_name}过去七天：年化收益率: {avg_total_ret:.2%}, "
                    f"价差收益率: {avg_spread_ret:.2%}, 资金费率收益率: {avg_funding_ret:.2%}, "
                    f"平均日换手率: {avg_turnover:.2f}"
                )
            
            summary_output = "\n".join(summary_lines)
            print("📊 各环境过去七天总体指标:")
            print(summary_output)
            print()
            
            # 资金加权的过去7天指标
            print("💼 过去7天资金加权总体指标:")
            print(f"年化收益率: {past_7_days_avg['total_ret_rate']:.2%}")
            print(f"价差收益率: {past_7_days_avg['spread_ret_rate']:.2%}")
            print(f"资金费率收益率: {past_7_days_avg['funding_ret_rate']:.2%}")
            print(f"平均日换手率: {past_7_days_avg['weighted_turnover_rate']:.2f}")
            print()


        if IsThursday8AM_flag:
            lark_message_extra = f"""
过去7天每日收益率表格:
{table_output}

{summary_output}

过去7天资金加权总体指标:
年化收益率: {past_7_days_avg['total_ret_rate']:.2%}
价差收益率: {past_7_days_avg['spread_ret_rate']:.2%}
资金费率收益率: {past_7_days_avg['funding_ret_rate']:.2%}
平均日换手率: {past_7_days_avg['weighted_turnover_rate']:.2f}
"""            
        elif IsMonday8AM_flag or IsFriday8AM_flag:
            lark_message_extra = f"""
{summary_output}

过去7天资金加权总体指标:
年化收益率: {past_7_days_avg['total_ret_rate']:.2%}
价差收益率: {past_7_days_avg['spread_ret_rate']:.2%}
资金费率收益率: {past_7_days_avg['funding_ret_rate']:.2%}
平均日换手率: {past_7_days_avg['weighted_turnover_rate']:.2f}
"""
        else:
            lark_message_extra = ""

        message_content += lark_message_extra

        # 发送Lark消息
        bot_id = "d2ed3efe-7d3b-4746-809e-3fcc40f934d6"
        
        status_msg, error_msg = send_lark_bot(bot_id, message_content)
        
        if error_msg:
            print(f"Lark消息发送失败: {error_msg}")
        else:
            print(f"Lark消息发送成功: {status_msg}")



if __name__ == "__main__":
    main()