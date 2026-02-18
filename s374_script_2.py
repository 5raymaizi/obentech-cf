# 改写为支持传参的版本，startdate 从 argparse 传入


#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import time  
import os
import pandas as pd
import numpy as np
import itertools
import io
import zipfile
import matplotlib.pyplot as plt
import argparse
from collections import defaultdict
from sklearn.preprocessing import MinMaxScaler
from statsmodels.tsa.ar_model import AutoReg
from scipy.stats import kurtosis

# ==== 用你自己的工具包 ====
from utils_swap_swap import *
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)

# ==== 解析参数 ====
parser = argparse.ArgumentParser()
parser.add_argument('--startdate', type=str, required=True, help='交易日期，例如 2025-06-05')
args = parser.parse_args()
startdate = args.startdate

# ==== 参数 ====
symbols = ['BTC','ETH','XRP','SOL','DOGE','TON','ONDO','AAVE','ICP','1000SHIB','TRX','XLM','IMX','WCT','TRUMP','BNB','ADA','SUI','LINK','AVAX','HBAR','BCH','DOT','LTC','APT','UNI','ETC', 'RENDER', 'POL', 'ATOM', 'ALGO', 'FIL', 'TIA', 'TAO','SONIC','VIRTUAL','MOVE']+['MOODENG','PNUT','WIF','GOAT','FARTCOIN','PARTI','KAITO','VINE','NEIRO','ETHFI','WLD','JST']+['PEOPLE','JELLYJELLY','PENGU','EOS','GRASS','OP']+['CRV','NEAR','ARB','LDO']+['TRB'] + ['1INCH', 'ACE', 'ACH', 'ACT', 'AEVO', 'AGLD', 'AI16Z', 'AIXBT', 'ALCH', 'ALPHA', 'ANIME', 'APE', 'API3', 'AR', 'ARC', 'ARKM', 'ATH', 'AUCTION', 'AVAAI', 'AXS', 'BABY', 'BAND', 'BAT', 'BERA', 'BICO', 'BIGTIME', 'BIO', 'BLUR', 'BNT', 'BOME', 'BR', 'BRETT', 'BSV', 'CATI', 'CELO', 'CETUS', 'CFX', 'CHZ', 'COMP', 'COOKIE', 'DEGEN', 'DOGS', 'DOOD', 'DYDX', 'EGLD', 'EIGEN', 'ENJ', 'ENS', 'ETHW', 'FLM', 'FLOW', 'FXS', 'GALA', 'GAS', 'GLM', 'GMT', 'GMX', 'GPS', 'GRIFFAIN', 'GRT', 'GUN', 'HMSTR', 'ICX', 'ID', 'INIT', 'INJ', 'IOST', 'IOTA', 'IP', 'JOE', 'JST', 'JTO', 'JUP', 'KNC', 'KSM', 'LAYER', 'LPT', 'LQTY', 'LRC', 'LSK', 'MAGIC', 'MANA', 'MASK', 'ME', 'MEME', 'METIS', 'MEW', 'MINA', 'MKR', 'MORPHO', 'MOVR', 'NEIROETH', 'NEO', 'NIL', 'NMR', 'NOT', 'OM', 'ONE', 'ONT', 'PERP', 'PIPPIN', 'PLUME', 'POPCAT', 'PROMPT', 'PYTH', 'QTUM', 'RDNT', 'RSR', 'RVN', 'S', 'SAND', 'SCR', 'SHELL', 'SIGN', 'SLERF', 'SNX', 'SOLV', 'SSV', 'STORJ', 'STRK', 'STX', 'SUSHI', 'SWARMS', 'SWELL', 'T', 'THETA', 'TNSR', 'TURBO', 'UMA', 'UXLINK', 'VANA', 'W', 'WAL', 'WAXP', 'WOO', 'XTZ', 'YFI', 'YGG', 'ZEREBRO', 'ZETA', 'ZIL', 'ZK', 'ZRO', 'ZRX']
# symbols = ['BTC','ETH','XRP','SOL','DOGE','TON','ONDO','AAVE','ICP','TRX','XLM','IMX','TRUMP','BNB','ADA','SUI','LINK','AVAX','HBAR','BCH','DOT','LTC','APT','UNI','ETC', 'RENDER', 'POL', 'ATOM', 'ALGO', 'FIL', 'TIA', 'TAO','SONIC','VIRTUAL','MOVE', 'ETHFI' ,'KAITO' ,'MOODENG' ,'VINE' ,'GOAT','PARTI','FARTCOIN','GRASS','JELLYJELLY','EOS', 'NEIRO', 'OP', 'PENGU', 'PEOPLE', 'PNUT', 'WIF', 'WLD','ME','ALCH','WAL','ICX','VANA','NEIROETH','T','BABY','AGLD','SSV','PLUME','NMR','JST','ANIME','IOST','UMA','MASK','RVN','COMP','WCT']

exchanges = ['okx', 'binance']
trade_types = ['swap']
data_type = 'depth'
add = '_5_100'
spread_plot_dir = './spread_plots'
os.makedirs(spread_plot_dir, exist_ok=True)

# ==== 读取盘口数据到内存 ====
data_dict = {}
availability_stats = defaultdict(lambda: defaultdict(lambda: {'available': False, 'error': None}))
read_start = time.time()

for symbol in symbols:
    for exchange in exchanges:
        for trade_type in trade_types:
            logging.info(f"正在加载数据 {symbol}-{exchange}-{trade_type}...")
            symbol_fmt = f"{symbol.lower()}-usdt"
            zip_path = f"/data/vhosts/download/subscribe_to_csv/{startdate}/{startdate}_{data_type}_{exchange}_swap_{symbol_fmt}{add}.zip"
            key = f"{exchange}_book_{trade_type}_{symbol}"
            try:
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv')]
                    if not csv_files:
                        raise FileNotFoundError("No CSV file in zip.")
                    with zip_ref.open(csv_files[0]) as f:
                        df = pd.read_csv(io.TextIOWrapper(f, encoding='utf-8'))

                # ==== 字段标准化 ====
                df['E'] = df['event time']
                df['local time'] = df['local time'].apply(lambda x: startdate[:5] + x)
                df = df[['local time','E','exchange time','bid price 1','bid amount 1',
                         'bid price 2','bid amount 2','bid price 3','bid amount 3',
                         'bid price 4','bid amount 4','bid price 5','bid amount 5',
                         'ask price 1','ask amount 1','ask price 2','ask amount 2',
                         'ask price 3','ask amount 3','ask price 4','ask amount 4',
                         'ask price 5','ask amount 5']]
                df.columns = ['received_time','E','T','bid_price0','bid_size0','bid_price1','bid_size1','bid_price2','bid_size2',
                              'bid_price3','bid_size3','bid_price4','bid_size4','ask_price0','ask_size0','ask_price1','ask_size1',
                              'ask_price2','ask_size2','ask_price3','ask_size3','ask_price4','ask_size4']

                df['symbol'] = symbol
                df['exchange'] = exchange
                df['trade_type'] = trade_type
                df['E'] = pd.to_datetime(df['E'], unit='ms')
                df['T'] = pd.to_datetime(df['T'], unit='ms')
                df['received_time'] = pd.to_datetime(df['received_time'])
                df = df[df['T'] > (pd.to_datetime(startdate) - pd.Timedelta(days=1)).replace(hour=16)]
                df.sort_values("T", inplace=True)
                data_dict[key] = df
                availability_stats[symbol][f'{exchange}_{trade_type}']['available'] = True

                # logging.info(f"成功加载并解析, 总行数: {len(df)}")

            except Exception as e:
                availability_stats[symbol][f'{exchange}_{trade_type}']['error'] = str(e)
read_end = time.time()

# ==== Spread 可视化函数 ====
def plot_spread_comparison_from_score(spread_dict_mm, top_df, save_dir):
    for row in top_df.itertuples():
        sym, ex1, ex2 = row.Symbol, row.Exchange1, row.Exchange2
        key = (sym, ex1, ex2)
        if key not in spread_dict_mm:
            continue
        df = spread_dict_mm[key]
        plt.figure(figsize=(12, 4))
        plt.plot(df['time'], df['spread_clipped'], label='MM Spread (clipped)')
        plt.title(f'{sym}: {ex1} vs {ex2}')
        plt.xlabel('Time')
        plt.ylabel('Spread')
        plt.legend()
        plt.tight_layout()
        save_path = os.path.join(save_dir, f'{sym}_{ex1}_{ex2}.png')
        plt.savefig(save_path)
        plt.close()


calc_start = time.time()
# ==== 主流程 ====
df_stats = screen_contract_contract_mm_candidates(data_dict, symbols, exchanges, type='bid', z_threshold=1.64)
sorted_df, top_df, bottom_df = compute_and_rank_contract_contract_mm(df_stats, top_n=8, type='bid')




calc_end = time.time()

print(f"\n==== 执行时间统计 ====")
print(f"数据读取耗时: {(read_end - read_start) / 60:.2f} 分钟")
print(f"计算逻辑耗时: {(calc_end - calc_start) / 60:.2f} 分钟")
print(f"总耗时: {(calc_end - read_start) / 60:.2f} 分钟")



# # ==== 构建 spread_dict 并画图 ====
# spread_dict_mm = {}
# for row in top_df.itertuples():
#     sym, ex1, ex2 = row.Symbol, row.Exchange1, row.Exchange2
#     df_sp = compute_all_spreads_unified(
#         data_dict[f'{ex1}_book_swap_{sym}'],
#         data_dict[f'{ex2}_book_swap_{sym}'],
#         time_col=get_time_col(ex1, ex2),
#         comparison_type='swap_swap'
#     )
#     mm = df_sp['mm_spread'].dropna()
#     low, high = mm.quantile(0.001), mm.quantile(0.999)
#     clipped = mm.clip(low, high)
#     spread_dict_mm[(sym, ex1, ex2)] = pd.DataFrame({
#         'time': df_sp['time'],
#         'spread_clipped': clipped
#     })

# plot_spread_comparison_from_score(spread_dict_mm, top_df, spread_plot_dir)
sorted_df.to_csv(f'swap_swap_stats_{startdate}.csv', index=False)
print("Complete")

