import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import traceback
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import requests
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from collections import deque
from sys import getrefcount
import math
import os
from decimal import Decimal
import platform
from utils_swap_swap import *

if 's374' in platform.node():
    print("Running on S374 server")
    IS_SERVER = True
else:
    IS_SERVER = False


if not IS_SERVER:
    from tqdm import tqdm


# 2025-06-20 修改，增加end_time参数，用于筛选funding_cum

def compute_dynamic_fifo_pnl_correct_fee_with_funding(
    trades_df: pd.DataFrame,
    funding_okx_csv: str,
    funding_binance_csv: str,
    notional: float = 1.0,
    end_time = None
    ) -> dict:
    """
    交易 FIFO PnL + 动态资金费率 PnL（Binance ↔ OKX）

    依赖:
        - process_funding_time(df, exchange)       # 你已有的函数
        - funding_df_to_series(df)                 # 你已有的函数
        - compute_dynamic_fifo_pnl_correct_fee     # 你已有（费率已修正）的 FIFO 函数
    返回:
        dict{
            'trade_pnl_series', 'funding_pnl_series',
            'total_pnl_series', 'position_series'
        }
    """

    df_okx      = process_funding_time(funding_okx_csv,  exchange='okx')
    df_binance  = process_funding_time(funding_binance_csv,  exchange='binance')

    fr_okx      = funding_df_to_series(df_okx)       # OKX
    fr_binance  = funding_df_to_series(df_binance)   # Binance

    price_series = (
        df_binance
        .drop_duplicates(subset="FundingTime", keep="last")
        .set_index("FundingTime")["IndexPrice"]
        .astype(float)
        .sort_index())

    # ------ FIFO 交易 PnL (不含资金费率) ------
    pnl_series, cum_pnl, position_series = compute_dynamic_fifo_pnl_correct_fee(
        trades_df, notional=notional
    )
    # print(f'test 0620: position_series: {position_series}')
    # ------ 构造统一事件时间轴 & 仓位 ------
    all_events = fr_okx.index.union(fr_binance.index).sort_values()
    position_series = position_series[~position_series.index.duplicated(keep='last')]

    pos_series = position_series.reindex(all_events, method="ffill").fillna(0)

    # -1: open  (short ex2, long ex1)
    # +1: close (long  ex2, short ex1)
    sign_spread = np.sign(pos_series)
    qty         = pos_series.abs()*notional

    dir_m1 =  sign_spread      # Binance leg
    dir_m2 = -sign_spread      # OKX    leg

    fr_m1 = fr_binance.reindex(all_events).fillna(0)
    fr_m2 = fr_okx.reindex(all_events).fillna(0)

    price_all = price_series.reindex(all_events).ffill()   # 补齐价格

    funding_pnl_series = ((dir_m1 * fr_m1 * qty) + (dir_m2 * fr_m2 * qty))*price_all
    funding_cum = funding_pnl_series.cumsum()
    # print(f'test 0620:    筛选前的 funding_cum: {funding_cum}')
    # ------ 合并: funding_cum 贴到 trade_ts ------
    first_trade_ts = position_series.index[0]
    last_trade_ts = position_series.index[-1]
    # print(f'test 0620: first_trade_ts, last_trade_ts: {first_trade_ts}, {last_trade_ts}')
    # print(funding_cum.index)
    funding_cum = funding_cum[funding_cum.index >= first_trade_ts]
    if end_time is None:
        funding_cum = funding_cum[funding_cum.index <= last_trade_ts] #之前这里肯定是错的，因为如果是吃资金费率的币的话，没有trade的时候就直接结束了，但实际上是还有后续的资金费率收益的
    else:
        funding_cum = funding_cum[funding_cum.index <= end_time]
    # print(f'test 0620: funding_cum: {funding_cum}')
    # print(funding_cum)

    all_events2 = cum_pnl.index.union(funding_cum.index).sort_values()

    cum_pnl = cum_pnl.reindex(all_events2, method="ffill").fillna(0)
    funding_cum = funding_cum.reindex(all_events2, method="ffill").fillna(0)
    # print(cum_pnl, cum_pnl.dropna().empty)
    # print(funding_pnl_series)
    # print(funding_cum)
    if cum_pnl.dropna().empty:
        print(f'test 0620: cum_pnl is empty')
        # 主表是 funding_cum
        merged = pd.merge_asof(
            funding_cum.rename("funding_pnl").to_frame(),
            cum_pnl.rename("trade_pnl").to_frame(),
            left_index=True,
            right_index=True,
            direction="backward",
            allow_exact_matches=True
        )
        # print(f'test 0620: merged: {merged}')
    else:
        merged = pd.merge_asof(
            cum_pnl.rename("trade_pnl").to_frame(),
            funding_cum.rename("funding_pnl").to_frame(),
            left_index=True,
            right_index=True,
            direction="backward",
            allow_exact_matches=True
        )
    merged["funding_pnl"].fillna(0, inplace=True)   # 建仓前 funding 为 0
    merged["total_pnl"] = merged["trade_pnl"] + merged["funding_pnl"]

    return merged["trade_pnl"], merged['funding_pnl'], merged['total_pnl'], position_series

def compute_dynamic_fifo_pnl_correct_fee(trades_df, notional=1.0):
    """
    支持方向识别、加仓、方向反转自动平仓、逐笔手续费处理的 FIFO PnL 模型。
    """
    position_queue = deque()  # 存放当前持仓（方向, m1, m2, fee_m1, fee_m2, notional）
    realized_pnl = []
    timestamps = []
    position_vals, pos_ts = [], [] 

    for idx, row in trades_df.iterrows():
        side = row['side']  # 'open' = short, 'close' = long
        direction = -1 if side == 'open' else 1
        m1 = row['market1_traded_price']
        m2 = row['market2_traded_price']
        fee_m1 = row['fee_market1']
        fee_m2 = row['fee_market2']
        if direction == -1: #做空价差， 多m1，空m2
            adj_m1 = m1 * (1 + fee_m1)
            adj_m2 = m2 * (1 - fee_m2)
        elif direction == 1: #做多价差，空m1，多m2
            adj_m1 = m1 * (1 - fee_m1)
            adj_m2 = m2 * (1 + fee_m2)

        # 当前交易的方向和持仓方向一致：加仓
        if not position_queue or position_queue[0][0] == direction:
            position_queue.append((direction, adj_m1, adj_m2, 1))
        else:
            # 方向不一致：进行平仓（FIFO）
            remaining = 1
            while position_queue and remaining > 0 and position_queue[0][0] != direction:
                pos_dir, open_adj_m1, open_adj_m2, pos_n = position_queue[0]

                close_n = min(pos_n, remaining)


                # 平仓逻辑：open 是 open side，当前是 close side（或者反之）
                if pos_dir == -1:  # 开的是 short spread #我们在平空 close short
                    pnl = ((m1 * (1 - fee_m1) - open_adj_m1) + (open_adj_m2 - m2 * (1 + fee_m2))) * close_n* notional
                else:  # 开的是 long spread
                    pnl = (open_adj_m1 - m1 * (1 + fee_m1) +  m2 * (1 - fee_m2) - open_adj_m2) * close_n * notional

                realized_pnl.append(pnl)
                timestamps.append(idx)

                if close_n < pos_n:
                    # 部分平仓：修改头部仓位剩余
                    position_queue[0] = (pos_dir, open_adj_m1, open_adj_m2, pos_n - close_n)
                else:
                    # 完全平仓
                    position_queue.popleft()

                remaining -= close_n

            # 如果当前方向还有未平掉的量，记为新开仓
            if remaining > 0:
                position_queue.append((direction, adj_m1, adj_m2, remaining))

        # ─── 记录当前净仓位 ───────────────────────────────────────────────
        net_position = sum(dir_ * qty for dir_, _, _, qty in position_queue)
        position_vals.append(net_position)
        pos_ts.append(idx)
    pnl_series = pd.Series(realized_pnl, index=pd.to_datetime(timestamps), name='pnl')
    cum_pnl = pnl_series.cumsum()
    position_series = pd.Series(position_vals, index=pd.to_datetime(pos_ts), name='position')
    return pnl_series, cum_pnl, position_series




# 2025-05-06 回测增加fundingRate
def compute_real_time_spread_pnl_correct_fee_with_funding(
    trades_df: pd.DataFrame,
    funding_okx_csv: str,
    funding_binance_csv: str,
    notional: float = 1.0,
    end_time = None
    ):
    """
    实时价差 PnL  + 动态 Funding PnL  （Binance ↔ OKX）

    返回
    ----
    trade_pnl_series   : 逐笔实时累计 PnL（仅交易）
    funding_pnl_series : Funding 累计 PnL（merge_asof 贴到 trade_ts）
    total_pnl_series   : trade + funding
    position_series    : 净仓位随成交时间
    """

    # ---------- 1) Funding 序列（沿用 v3 逻辑） ----------
    df_okx     = process_funding_time(funding_okx_csv,  exchange='okx')
    df_binance = process_funding_time(funding_binance_csv, exchange='binance')

    fr_okx      = funding_df_to_series(df_okx)       # OKX   FundingRate (index = FundingTime)
    fr_binance  = funding_df_to_series(df_binance)   # Binance FundingRate
    
    price_series = (
        df_binance
        .drop_duplicates(subset="FundingTime", keep="last")
        .set_index("FundingTime")["IndexPrice"]
        .astype(float)
        .sort_index())



    # ---------- 2) 逐笔实时交易 PnL（含手续费） ----------
    cash, position = 0.0, 0.0
    trade_pnl_vals, timestamps, pos_vals, records = [], [], [], []

    for ts, row in trades_df.iterrows():
        side      = row['side']
        u1, u2    = row['market1_traded_price'], row['market2_traded_price']
        fee_m1, fee_m2 = row['fee_market1'], row['fee_market2']

        if side == 'open':          # short spread: long m1, short m2
            adj_u1 = u1 * (1 + fee_m1)
            adj_u2 = u2 * (1 - fee_m2)
            cash      += (adj_u2 - adj_u1) * notional
            position  -= 1
        elif side == 'close':       # long spread: short m1, long m2
            adj_u1 = u1 * (1 - fee_m1)
            adj_u2 = u2 * (1 + fee_m2)
            cash      -= (adj_u2 - adj_u1) * notional
            position  += 1
        else:
            raise ValueError(f"Unknown side: {side}")

        current_spread = u2 - u1
        trade_pnl = cash + position * current_spread

        timestamps.append(pd.to_datetime(ts))
        trade_pnl_vals.append(trade_pnl)
        pos_vals.append(position)

        records.append({
            "timestamp"      : pd.to_datetime(ts),
            "position"       : position,
            "cash"           : cash,
            "spread_price"   : current_spread,
            "trade_pnl"      : trade_pnl    # 先只有交易 PnL
        })


    trade_pnl_series = pd.Series(trade_pnl_vals, index=timestamps, name="trade_pnl")
    position_series  = pd.Series(pos_vals, index=timestamps, name="position")
    pnl_df = pd.DataFrame(records).set_index("timestamp")

    # ---------- 3) Funding 累计 PnL（与 v3 完全一致） ----------
    # 统一事件轴
    all_events = fr_okx.index.union(fr_binance.index).union(trade_pnl_series.index).sort_values()
    position_series = position_series[~position_series.index.duplicated(keep='last')]  # 2025-05-28 新增，发现有重复的时间戳，导致 reindex 出错
    pos_series = position_series.reindex(all_events, method="ffill").fillna(0)

    sign_spread = np.sign(pos_series)
    qty         = pos_series.abs()*notional

    dir_m1 =  sign_spread     # Binance leg
    dir_m2 = -sign_spread     # OKX    leg

    fr_m1 = fr_binance.reindex(all_events).fillna(0)
    fr_m2 = fr_okx.reindex(all_events).fillna(0)


    price_all = price_series.reindex(all_events).ffill()

    funding_pnl_increment = dir_m1 * fr_m1 * qty * price_all + dir_m2 * fr_m2 * qty * price_all
    funding_cum = funding_pnl_increment.cumsum()

    # 仅保留建仓至最后一笔之间
    first_trade_ts = position_series.index[0]
    last_trade_ts  = position_series.index[-1]
    if end_time is None:
        funding_cum = funding_cum[(funding_cum.index >= first_trade_ts) &
                                (funding_cum.index <= last_trade_ts)]
    else:
        funding_cum = funding_cum[(funding_cum.index >= first_trade_ts) &
                                (funding_cum.index <= end_time)]

    # merge_asof 贴到 trade_ts
    merged = pd.merge_asof(
        trade_pnl_series.to_frame(),
        funding_cum.rename("funding_pnl").to_frame(),
        left_index=True,
        right_index=True,
        direction="backward",
        allow_exact_matches=True
    )
    merged["funding_pnl"].fillna(0, inplace=True)

    # ---------- 4) 总 PnL ----------
    merged["total_pnl"] = merged["trade_pnl"] + merged["funding_pnl"]
    pnl_df["funding_pnl"] = merged["funding_pnl"]          # 贴回逐行表
    pnl_df["total_pnl"]   = merged["total_pnl"]

    return merged["trade_pnl"], merged["funding_pnl"], merged["total_pnl"], position_series, pnl_df

# def compute_dynamic_fifo_pnl_correct_fee_with_funding(
#     trades_df: pd.DataFrame,
#     funding_okx_csv: str,
#     funding_binance_csv: str,
#     notional: float = 1.0,
# ) -> dict:
#     """
#     交易 FIFO PnL + 动态资金费率 PnL（Binance ↔ OKX）

#     依赖:
#         - process_funding_time(df, exchange)       # 你已有的函数
#         - funding_df_to_series(df)                 # 你已有的函数
#         - compute_dynamic_fifo_pnl_correct_fee     # 你已有（费率已修正）的 FIFO 函数
#     返回:
#         dict{
#             'trade_pnl_series', 'funding_pnl_series',
#             'total_pnl_series', 'position_series'
#         }
#     """

#     df_okx      = process_funding_time(funding_okx_csv,  exchange='okx')
#     df_binance  = process_funding_time(funding_binance_csv,  exchange='binance')

#     fr_okx      = funding_df_to_series(df_okx)       # OKX
#     fr_binance  = funding_df_to_series(df_binance)   # Binance

#     price_series = (
#         df_binance
#         .drop_duplicates(subset="FundingTime", keep="last")
#         .set_index("FundingTime")["IndexPrice"]
#         .astype(float)
#         .sort_index())

#     # ------ FIFO 交易 PnL (不含资金费率) ------
#     pnl_series, cum_pnl, position_series = compute_dynamic_fifo_pnl_correct_fee(
#         trades_df, notional=notional
#     )

#     # ------ 构造统一事件时间轴 & 仓位 ------
#     all_events = fr_okx.index.union(fr_binance.index).sort_values()
#     # position_series = position_series[~position_series.index.duplicated(keep='last')]

#     pos_series = position_series.reindex(all_events, method="ffill").fillna(0)

#     # -1: open  (short ex2, long ex1)
#     # +1: close (long  ex2, short ex1)
#     sign_spread = np.sign(pos_series)
#     qty         = pos_series.abs()

#     dir_m1 =  sign_spread      # Binance leg
#     dir_m2 = -sign_spread      # OKX    leg

#     fr_m1 = fr_binance.reindex(all_events).fillna(0)
#     fr_m2 = fr_okx.reindex(all_events).fillna(0)

#     price_all = price_series.reindex(all_events).ffill()   # 补齐价格

#     funding_pnl_series = ((dir_m1 * fr_m1 * qty) + (dir_m2 * fr_m2 * qty))*price_all
#     funding_cum = funding_pnl_series.cumsum()

#     # ------ 合并: funding_cum 贴到 trade_ts ------
#     first_trade_ts = position_series.index[0]
#     last_trade_ts = position_series.index[-1]
#     funding_cum = funding_cum[funding_cum.index >= first_trade_ts]
#     funding_cum = funding_cum[funding_cum.index <= last_trade_ts]

#     merged = pd.merge_asof(
#         cum_pnl.rename("trade_pnl").to_frame(),
#         funding_cum.rename("funding_pnl").to_frame(),
#         left_index=True,
#         right_index=True,
#         direction="backward",
#         allow_exact_matches=True
#     )
#     merged["funding_pnl"].fillna(0, inplace=True)   # 建仓前 funding 为 0
#     merged["total_pnl"] = merged["trade_pnl"] + merged["funding_pnl"]

#     return merged["trade_pnl"], merged['funding_pnl'], merged['total_pnl'], position_series

    
# 2025-05-01 发现fee好像算的有问题，修改一下啊
# def compute_dynamic_fifo_pnl_correct_fee(trades_df, notional=1.0):
#     """
#     支持方向识别、加仓、方向反转自动平仓、逐笔手续费处理的 FIFO PnL 模型。
#     """
#     position_queue = deque()  # 存放当前持仓（方向, m1, m2, fee_m1, fee_m2, notional）
#     realized_pnl = []
#     timestamps = []
#     position_vals, pos_ts = [], [] 

#     for idx, row in trades_df.iterrows():
#         side = row['side']  # 'open' = short, 'close' = long
#         direction = -1 if side == 'open' else 1
#         m1 = row['market1_traded_price']
#         m2 = row['market2_traded_price']
#         fee_m1 = row['fee_market1']
#         fee_m2 = row['fee_market2']
#         if direction == -1: #做空价差， 多m1，空m2
#             adj_m1 = m1 * (1 + fee_m1)
#             adj_m2 = m2 * (1 - fee_m2)
#         elif direction == 1: #做多价差，空m1，多m2
#             adj_m1 = m1 * (1 - fee_m1)
#             adj_m2 = m2 * (1 + fee_m2)

#         # 当前交易的方向和持仓方向一致：加仓
#         if not position_queue or position_queue[0][0] == direction:
#             position_queue.append((direction, adj_m1, adj_m2, notional))
#         else:
#             # 方向不一致：进行平仓（FIFO）
#             remaining = notional
#             while position_queue and remaining > 0 and position_queue[0][0] != direction:
#                 pos_dir, open_adj_m1, open_adj_m2, pos_n = position_queue[0]

#                 close_n = min(pos_n, remaining)


#                 # 平仓逻辑：open 是 open side，当前是 close side（或者反之）
#                 if pos_dir == -1:  # 开的是 short spread #我们在平空 close short
#                     pnl = ((m1 * (1 - fee_m1) - open_adj_m1) + (open_adj_m2 - m2 * (1 + fee_m2))) * close_n
#                 else:  # 开的是 long spread
#                     pnl = (open_adj_m1 - m1 * (1 + fee_m1) +  m2 * (1 - fee_m2) - open_adj_m2) * close_n

#                 realized_pnl.append(pnl)
#                 timestamps.append(idx)

#                 if close_n < pos_n:
#                     # 部分平仓：修改头部仓位剩余
#                     position_queue[0] = (pos_dir, open_adj_m1, open_adj_m2, pos_n - close_n)
#                 else:
#                     # 完全平仓
#                     position_queue.popleft()

#                 remaining -= close_n

#             # 如果当前方向还有未平掉的量，记为新开仓
#             if remaining > 0:
#                 position_queue.append((direction, adj_m1, adj_m2, remaining))

#         # ─── 记录当前净仓位 ───────────────────────────────────────────────
#         net_position = sum(dir_ * qty for dir_, _, _, qty in position_queue)
#         position_vals.append(net_position)
#         pos_ts.append(idx)
#     pnl_series = pd.Series(realized_pnl, index=pd.to_datetime(timestamps), name='pnl')
#     cum_pnl = pnl_series.cumsum()
#     position_series = pd.Series(position_vals, index=pd.to_datetime(pos_ts), name='position')
#     return pnl_series, cum_pnl, position_series


def compute_real_time_spread_pnl_correct_fee(trades_df, notional=1.0):
    cash = 0.0
    position = 0.0
    pnl_list = []
    timestamps = []
    records = []
    for time, row in trades_df.iterrows():
        side = row['side']  # 'open' or 'close'
        u1 = row['market1_traded_price']
        u2 = row['market2_traded_price']
        fee_m1 = row['fee_market1']
        fee_m2 = row['fee_market2']
        # open ： 多market1 空market2
        # close: 空market1 多market2
        # 成交时两边价格考虑手续费
        if side == 'open':
            adj_u1 = u1 * (1 + fee_m1)
            adj_u2 = u2 * (1 - fee_m2)
        elif side == 'close':
            adj_u1 = u1 * (1 - fee_m1)
            adj_u2 = u2 * (1 + fee_m2)


        # 成交时的spread price
        spread_price = adj_u2 - adj_u1

        if side == 'open':
            delta_cash = spread_price * notional
            delta_position = -notional
        elif side == 'close':
            delta_cash = -spread_price * notional
            delta_position = notional
        else:
            raise ValueError(f"Unknown side: {side}")

        # 更新cash和position
        cash += delta_cash
        position += delta_position

        # 当前时刻，用最新的adj_u1, adj_u2计算当前spread
        current_spread_price = u2 - u1

        # 实时PnL
        pnl = cash + position * current_spread_price
        pnl_list.append(pnl)
        timestamps.append(time)

        records.append({
            'timestamp': pd.to_datetime(time),
            'position': position,
            'cash': cash,
            'spread_price': current_spread_price,
            'real_time_pnl': pnl
        })
    pnl_series = pd.Series(pnl_list, index=pd.to_datetime(timestamps), name='real_time_spread_pnl')
    pnl_df = pd.DataFrame(records).set_index('timestamp')
    return pnl_series,pnl_df

def turnover_cycles(position: pd.Series, P_std: float) -> float:
    """
    计算完整开＋平轮次的换手率（cycles）。
    参数：
        position: 按时间排序的仓位序列（可以正负）。
        P_std:    标准仓位大小，定义一次满开的仓位量。
    返回：
        turnover_cycles: 完整开平轮次数量（1 轮次 = 满开 + 满平）。
    """
    V = position.diff().abs().sum()
    return V / (2 * P_std)

def infer_tick_size_from_column(column):
    decimal_places = column.dropna().apply(lambda x: -Decimal(str(x)).as_tuple().exponent)
    most_common = decimal_places.mode().iloc[0]
    
    return 1 / (10 ** most_common)
    
def rename_columns(columns, prefix):
    output_columns = []

    for c in columns:
        output_columns.append(prefix + c)
            
    return output_columns

def read_bookTicker_data(ccy, start_date, end_date, exchange1, market1, exchange2, market2, data_source, tolerance='10ms'):    

    if data_source == 'inner_win':
        market1_depthBBO_path = f'/Users/rayxu/Desktop/Obentech/dcdlData/{exchange1}/books/{ccy}/{market1}'
        market2_depthBBO_path = f'/Users/rayxu/Desktop/Obentech/dcdlData/{exchange2}/books/{ccy}/{market2}'
    elif data_source == 'outer_ssd':
        market1_depthBBO_path = f'/Volumes/T7/Obentech/dcdlData/{exchange1}/books/{ccy}/{market1}'
        market2_depthBBO_path = f'/Volumes/T7/Obentech/dcdlData/{exchange2}/books/{ccy}/{market2}'    
        
    market1_depthBBO = pd.concat([pd.read_csv(f'{market1_depthBBO_path}/{ccy}usdt_{dd}_depthBBO.csv')
                            for dd in pd.date_range(start_date, end_date).strftime('%Y-%m-%d')])
    market2_depthBBO = pd.concat([pd.read_csv(f'{market2_depthBBO_path}/{ccy}usdt_{dd}_depthBBO.csv')
                            for dd in pd.date_range(start_date, end_date).strftime('%Y-%m-%d')])
    
    market1_depthBBO.columns = ['local_time', 'T', 'bid_price0', 'bid_size0', 'ask_price0', 'ask_size0']
    market2_depthBBO.columns = ['local_time', 'T', 'bid_price0', 'bid_size0', 'ask_price0', 'ask_size0']

    market1_depthBBO = market1_depthBBO.drop_duplicates(subset='T', keep='last')
    market2_depthBBO = market2_depthBBO.drop_duplicates(subset='T', keep='last')



    # -------- 2. 转换 T 列为 datetime --------
    market1_depthBBO['T'] = pd.to_datetime(market1_depthBBO['T'], unit='ms')
    market2_depthBBO['T']     = pd.to_datetime(market2_depthBBO['T'], unit='ms')

    # -------- 3. 加上 ws_type --------
    market1_depthBBO['ws_type'] = 'market1_depth'
    market2_depthBBO['ws_type']     = 'market2_depth'

    market1_depthBBO.drop_duplicates(inplace=True)
    market2_depthBBO.drop_duplicates(inplace=True)

    # -------- 4. 给所有列加前缀 --------
    def rename_columns(cols, prefix):
        return [prefix + col if col != 'T' else 'T' for col in cols]

    market1_depthBBO.columns = rename_columns(market1_depthBBO.columns, 'market1_')
    market2_depthBBO.columns     = rename_columns(market2_depthBBO.columns, 'market2_')

    # -------- 5. reset_index 用于 merge_asof --------
    market1_depthBBO = market1_depthBBO.reset_index(drop=True)
    market2_depthBBO     = market2_depthBBO.reset_index(drop=True)

    # -------- 6. merge_asof 合并两边数据 --------
    cf_depth = pd.merge_asof(
        market1_depthBBO.sort_values('T'),
        market2_depthBBO.sort_values('T'),
        on='T',
        direction='backward',
        tolerance=pd.Timedelta(tolerance),
    )

    # -------- 7. 合并 ws_type，构造 spread 和 ratio --------
    cf_depth[['market1_ws_type', 'market2_ws_type']] = cf_depth[['market1_ws_type', 'market2_ws_type']].fillna('')
    cf_depth['ws_type'] = cf_depth['market1_ws_type'] + cf_depth['market2_ws_type']

    cf_depth.dropna(inplace=True)

    cf_depth = cf_depth.fillna(method='ffill').assign(
        sp_open=lambda df: df['market2_bid_price0'] - df['market1_ask_price0'],
        sp_close=lambda df: df['market2_ask_price0'] - df['market1_bid_price0'],
        sr_open=lambda df: df['sp_open'] / df['market1_ask_price0'],
        sr_close=lambda df: df['sp_close'] / df['market1_bid_price0']
    )

    # -------- 8. 添加跳跃时间差，设置索引 --------
    cf_depth.reset_index(drop=True, inplace=True)
    cf_depth['received_time_diff_1jump_later'] = cf_depth['T'].shift(-1) - cf_depth['T']
    cf_depth['received_time_diff_1jump_later'] = cf_depth['received_time_diff_1jump_later'].dt.total_seconds()
    cf_depth.set_index('T', inplace=True)


    return cf_depth


def get_time_col(ex1, ex2):
    """
    根据两个交易所决定要用的时间列（跨所merge用的主时间轴）。
    优先使用OKX的 T 字段，否则默认用 E。
    """
    return 'T' if 'okx' in [ex1,ex2] else 'E'


def read_cf_depth(ccy, start_date, end_date, exchange1, market1, exchange2, market2, data_source):
    #只读取一档数据，其他的数据先不要了。
    cols = ['received_time', 'E', 'T', 'bid_price0', 'bid_size0', 'ask_price0', 'ask_size0']
    if data_source == 'inner_win':
        market1_depth_path = f'/Users/rayxu/Desktop/Obentech/dcdlData/{exchange1}/books/{ccy}/{market1}'
        market2_depth_path = f'/Users/rayxu/Desktop/Obentech/dcdlData/{exchange2}/books/{ccy}/{market2}'
    elif data_source == 'outer_ssd':
        market1_depth_path = f'/Volumes/T7/Obentech/dcdlData/{exchange1}/books/{ccy}/{market1}'
        market2_depth_path = f'/Volumes/T7/Obentech/dcdlData/{exchange2}/books/{ccy}/{market2}'        
    elif data_source == 'nuts_mm':
        market1_depth_path = f'/Volumes/T7/data/{exchange1}/perp/books/{ccy}'
        market2_depth_path = f'/Volumes/T7/data/{exchange2}/perp/books/{ccy}'
    # if data_source == 'nuts_am_on_mac':
    #     market1_depth_path = f'/Users/rayxu/Downloads/nuts_am/data/{exchange1}/perp/books/{ccy}'
    #     market2_depth_path = f'/Users/rayxu/Downloads/nuts_am/data/{exchange2}/perp/books/{ccy}'
        market1_depth = pd.concat([pd.read_parquet(f'{market1_depth_path}/{ccy}usdt_{dd}_depth5.parquet')
                            for dd in pd.date_range(start_date, end_date).strftime('%Y-%m-%d')])        
        market2_depth = pd.concat([pd.read_parquet(f'{market2_depth_path}/{ccy}usdt_{dd}_depth5.parquet')
                            for dd in pd.date_range(start_date, end_date).strftime('%Y-%m-%d')])


    
    # 不知道为啥24年的数据，是23年的年份，所以先加1年处理
    # market1_depth["received_time"] = pd.to_datetime(market1_depth["received_time"]).apply(lambda x:x+relativedelta(years=1))
    time_col = get_time_col(exchange1,exchange2)
    market1_depth = market1_depth.drop_duplicates(subset= time_col, keep='last') # 2025-05-28新增，发现binance的数据有重复的时间戳，导致trades_df里面会有重复index，导致再算pnl reindex的时候会出问题。

    market1_depth[time_col] = pd.to_datetime(market1_depth[time_col], unit = 'ms')
    market1_depth.set_index(time_col, inplace=True)
    # market1_depth['mid_price'] = market1_depth['bid_price1']/2 + market1_depth['ask_price1']/2
    # market1_depth['bid_price1_100ms_later'] = market1_depth['bid_price1'].shift(-1)
    # market1_depth['ask_price1_100ms_later'] = market1_depth['ask_price1'].shift(-1)
    # market1_depth['bid_price1_200ms_later'] = market1_depth['bid_price1'].shift(-2)
    # market1_depth['ask_price1_200ms_later'] = market1_depth['ask_price1'].shift(-2)
    market1_depth['ws_type'] = "market1_depth"
    market1_depth.drop_duplicates(inplace=True)

    # 不知道为啥24年的数据，是23年的年份，所以先加1年处理
    # market2_depth["received_time"] = pd.to_datetime(market2_depth["received_time"]).apply(lambda x:x+relativedelta(years=1))
    market2_depth[time_col] = pd.to_datetime(market2_depth[time_col], unit = 'ms')
    market2_depth.set_index(time_col, inplace=True)
    # market2_depth['bid_price1_100ms_later'] = market2_depth['bid_price1'].shift(-1)
    # market2_depth['ask_price1_100ms_later'] = market2_depth['ask_price1'].shift(-1)
    # market2_depth['bid_price1_200ms_later'] = market2_depth['bid_price1'].shift(-2)
    # market2_depth['ask_price1_200ms_later'] = market2_depth['ask_price1'].shift(-2)
    # market2_depth['mid_price'] = market2_depth['bid_price1']/2 + market2_depth['ask_price1']/2
    market2_depth['ws_type'] = "market2_depth"
    market2_depth.drop_duplicates(inplace=True)
    
#     cf_depth = (market1_depth.filter(regex='_price|_size').rename(columns=lambda c: 'market1_'+c)).join((market2_depth.filter(regex='_price|_size').rename(columns=lambda c: 'market2_'+c)), how='outer')
    market1_depth.columns = rename_columns(list(market1_depth.columns), 'market1_')
    market2_depth.columns = rename_columns(list(market2_depth.columns), 'market2_')
########################################################################################
# 04-21修改： 改用merge_asof
    # 保证时间索引已经在 column 中（因为 merge_asof 不能用 index 作为 on）
    market1_depth = market1_depth.reset_index()
    market2_depth = market2_depth.reset_index()

    # 使用 merge_asof 精准时间对齐，100ms 容差，向后对齐
    cf_depth = pd.merge_asof(
        market1_depth.sort_values(time_col),
        market2_depth.sort_values(time_col),
        on=time_col,
        direction='backward',
        tolerance=pd.Timedelta('100ms'),
        suffixes=('_market1', '_market2')
    )


########################################################################################
# 之前的版本

    #cf_depth = (market1_depth).join(market2_depth, how='outer')

########################################################################################
    cf_depth[['market1_ws_type', 'market2_ws_type']] = cf_depth[['market1_ws_type', 'market2_ws_type']].fillna('')
    cf_depth['ws_type'] = cf_depth['market1_ws_type'] + cf_depth['market2_ws_type']
#     cf_depth = pd.concat([market1_depth, market2_depth], axis=1)
    
#    cf_depth.fillna(method='ffill', inplace=True)
    cf_depth.dropna(inplace=True)
    #改成了 dropna，因为可能会有一边数据缺失的问题，如果单纯用fillna会导致spread爆炸
    # cf_depth = cf_depth.fillna(method='ffill').assign(
    #     sp_open=lambda df: df['market2_bid_price1']-df['market1_ask_price1'],
    #     sp_close=lambda df: df['market2_ask_price1']-df['market1_bid_price1'],
    #     sr_open=lambda df: df['sp_open']/df['market1_ask_price1'],
    #     sr_close=lambda df: df['sp_close']/df['market1_bid_price1'],)
    
    cf_depth.reset_index(inplace=True)
    # cf_depth['received_time_diff_1jump_later'] = cf_depth[time_col].shift(-1) - cf_depth[time_col]
    # cf_depth['received_time_diff_1jump_later'] = cf_depth['received_time_diff_1jump_later'].apply(lambda x:x.total_seconds())
    cf_depth.set_index(time_col, inplace=True)
    cf_depth_st_index = cf_depth.index[0]
    cf_depth_et_index = cf_depth.index[-1]
    
    return cf_depth
def plot_spread_and_funding_combined(
    cf_depth: pd.DataFrame,
    market2_price_col: str,
    market1_price_col: str = "market1_bid_price0",
    fr_okx: pd.Series | None = None,
    fr_binance: pd.Series | None = None,
    ccy: str = "",
    q_10_bt: float | None = None,
    q_90_bt: float | None = None,
    figsize: tuple = (24, 6)
):
    """
    左图 : Bid‑Price Spread (bps)           —— 基于 cf_depth
    右图 : OKX / Binance FundingRate + 差值 —— 需传 fr_okx / fr_binance

    参数
    ----
    cf_depth : 含深度数据，index 为时间戳
    market2_price_col / market1_price_col : 价差两边列名
    fr_okx / fr_binance : FundingRate Series；可不传（则不画右图）
    q_10_bt / q_90_bt : 可选阈值线 (bps)
    """
    # --------------------------------------------------
    # 计算 Bid‑Price Spread
    spread     = ((cf_depth[market2_price_col] / cf_depth[market1_price_col]) - 1) * 1e4
    lower, upper = spread.quantile([0.005, 0.995])
    # spread_clip = spread.copy()
    spread_clip  = spread.clip(lower, upper)

    q10, q90 = spread_clip.quantile([0.05, 0.95])

    # --------------------------------------------------
    fig, (ax_left, ax_right) = plt.subplots(1, 2, figsize=figsize, sharex=False)

    # 左图：价差
    ax_left.plot(spread_clip.index, spread_clip.values,
                 color='steelblue', linestyle='-', label='Bid Spread (bps)')
    ax_left.axhline(q10, color='red',   ls='-', label='5% quantile')
    ax_left.axhline(q90, color='green', ls='-', label='95% quantile')

    if q_10_bt is not None:
        ax_left.axhline(q_10_bt, color='yellow', ls='--', label='5% q (bt)')
    if q_90_bt is not None:
        ax_left.axhline(q_90_bt, color='purple', ls='--', label='95% q (bt)')

    ax_left.set_title(f'{ccy} Bid‑Price Spread', fontweight='bold')
    ax_left.set_xlabel('Time'); ax_left.set_ylabel('Spread (bps)')
    ax_left.grid(ls='--', alpha=0.6); ax_left.legend(fontsize=9)

    # --------------------------------------------------
    # 右图：FundingRates（若提供）
    if fr_okx is not None and fr_binance is not None:
        # 时间窗 : cf_depth 首尾
        start_ts, end_ts = cf_depth.index[0], cf_depth.index[-1]

        union_idx   = fr_okx.index.union(fr_binance.index).sort_values()
        fr_okx_u    = fr_okx.reindex(union_idx).ffill()
        fr_bin_u    = fr_binance.reindex(union_idx).ffill()

        fr_okx_w    = fr_okx_u.loc[start_ts:end_ts]
        fr_bin_w    = fr_bin_u.loc[start_ts:end_ts]
        spread_w    = fr_okx_w - fr_bin_w

        ln1 = ax_right.plot(fr_okx_w.index,  fr_okx_w.values,
                            color='indianred',  label='OKX FundingRate')[0]
        ln2 = ax_right.plot(fr_bin_w.index,  fr_bin_w.values,
                            color='royalblue', label='Binance FundingRate')[0]

        ax2 = ax_right.twinx()
        ln3 = ax2.plot(spread_w.index, spread_w.values,
                       color='purple', ls='--', label='OKX − Binance')[0]
        ax2.set_ylabel('Funding Spread')

        lines = [ln1, ln2, ln3]
        ax_right.legend(lines, [l.get_label() for l in lines],
                        loc='upper left', fontsize=9)

        ax_right.set_title(f'{ccy} Funding Rates', fontweight='bold')
        ax_right.set_xlabel('Time'); ax_right.set_ylabel('FundingRate')
        ax_right.grid(ls='--', alpha=0.6)
    else:
        ax_right.axis('off')
        ax_right.text(0.5, 0.5, 'Funding data not provided',
                      ha='center', va='center', fontsize=12)

    plt.tight_layout()
    plt.show()

def plot_bid_price_spread(cf_depth, market2_price_col, market1_price_col='market1_bid_price0', q_10_bt=None, q_90_bt=None,ccy = 'BTC'):
    """
    绘制两个市场 bid price 的相对差异（bps），去除极端值并标出10%和90%分位数。
    可选参数q_10_bt和q_90_bt允许在图上绘制额外的参考线。

    参数:
        cf_depth (pd.DataFrame): 含有价格数据的 DataFrame。
        market2_price_col (str): market2 的 bid price 列名。
        market1_price_col (str): market1 的 bid price 列名（默认为 'market1_bid_price0'）。
        q_10_bt (float, optional): 额外的10%分位数参考线。默认为None，不显示。
        q_90_bt (float, optional): 额外的90%分位数参考线。默认为None，不显示。
    """
    # 计算差值（以 basis points 表示）
    spread = ((cf_depth[market2_price_col] / cf_depth[market1_price_col]) - 1) * 10000

    # 去除极端值（clip到0.1%和99.9%分位）
    lower_bound = spread.quantile(0.001)
    upper_bound = spread.quantile(0.999)
    spread_clipped = spread.clip(lower=lower_bound, upper=upper_bound)

    # 计算10%和90%的分位数
    q10 = spread_clipped.quantile(0.05)
    q90 = spread_clipped.quantile(0.95)

    # 画图
    plt.figure(figsize=(12, 6))
    spread_clipped.plot()

    plt.axhline(q10, color='red', linestyle='-', label='5% quantile')
    plt.axhline(q90, color='green', linestyle='-', label='95% quantile')
    
    # 如果提供了额外的分位数参考线，也绘制出来
    if q_10_bt is not None:
        plt.axhline(q_10_bt, color='yellow', linestyle='--', label='5% quantile (backtest)')
    if q_90_bt is not None:
        plt.axhline(q_90_bt, color='purple', linestyle='--', label='95% quantile (backtest)')
    
    plt.grid(True)
    plt.legend()
    plt.xlabel('T')
    plt.ylabel('Spread (bps)')
    plt.title(f'Spread between {market2_price_col} and {market1_price_col} for {ccy}')

    plt.show()


### 2025-05-27 preTakerMaker增加深度不平衡因子
def maker_maker_backtest_0528(cf_depth, mt_params, pos_limit = np.inf):
    """
    exchange1='binance', 
    market1='swap',
    exchange2='okx',      
    market2='swap',

    """

    oir_threshold = mt_params["oir_threshold"]

    open_replace_tick_num = mt_params["open_replace_tick_num"]
    close_replace_tick_num = mt_params["close_replace_tick_num"]

    target_open_sr = mt_params["target_open_sr"]
    target_close_sr = mt_params["target_close_sr"]

    market1_stop_loss_sr_delta = mt_params['market1_stop_loss_sr_delta']

    sr_open_threshold = target_open_sr
    sr_close_threshold = target_close_sr
    

    tick_size1 = infer_tick_size_from_column(cf_depth['market1_bid_price0'])
    tick_ratio1 = round(tick_size1/cf_depth.iloc[-1]['market1_bid_price0'], 6)
    round_num1 = int(np.log10(1/tick_size1))
    tick_size2 = infer_tick_size_from_column(cf_depth['market2_bid_price0'])
    tick_ratio2 = round(tick_size2/cf_depth.iloc[-1]['market2_bid_price0'], 6)
    round_num2 = int(np.log10(1/tick_size2))
    print("交易所1精度信息", tick_size1, tick_ratio1, round_num1)
    print("交易所2精度信息", tick_size2, tick_ratio2, round_num2)


    

    is_open_done = False
    is_open_market1_maker = False
    is_open_market2_taker = False
    open_market1_maker_price = None
    open_market1_taker_price = None
    open_market2_taker_price = None
    market1_open_time = None
    market2_open_time = None

    is_close_done = False
    is_close_market1_maker = False
    is_close_market2_taker = False
    close_market1_maker_price = None
    close_market1_taker_price = None
    close_market2_taker_price = None
    market1_close_time = None
    market2_close_time = None
    
    sr_open = None
    sr_open_real = None
    sr_close = None
    sr_close_real = None

    open_trades = []
    close_trades = []
    position_count = 0

    for index, ds in cf_depth.iterrows():
    
    # exchange1 maker; exchange2 taker
        

        market1_ask_price1 = ds['market1_ask_price0']
        market1_bid_price1 = ds['market1_bid_price0']
        market2_ask_price1 = ds['market2_ask_price0']
        market2_bid_price1 = ds['market2_bid_price0']

        market1_bid_vol = ds['market1_bid_size0'] 
        market1_ask_vol = ds['market1_ask_size0']
        market2_bid_vol = ds['market2_bid_size0']
        market2_ask_vol = ds['market2_ask_size0']
        # sr_open = (market2_bid_price1)/(market1_ask_price1 - tick_size1 * 1) - 1
        # sr_close = (market2_ask_price1)/(market1_bid_price1 + tick_size1 * 1) - 1

        sr_open = (market2_bid_price1)/(market1_ask_price1 - tick_size1 * 1) - 1  # sr_open是要买exchange1, 卖exchange2, 所以exchange1用bid price, exchange2用bid price
        sr_close = (market2_ask_price1)/(market1_bid_price1 + tick_size1 * 1) - 1 # sr_close是要卖exchange1, 买exchange2, 所以exchange1用ask price, exchange2用ask price

        # 下单判断
        open_judgement = (sr_open >= sr_open_threshold)
        close_judgement = (sr_close <= sr_close_threshold)

        oir1 = (market1_bid_vol - market1_ask_vol) / (market1_bid_vol + market1_ask_vol + 1e-8)
        oir2 = (market2_bid_vol - market2_ask_vol) / (market2_bid_vol + market2_ask_vol + 1e-8)

        # 默认过滤极端值：开仓方向不利、平仓方向不利的极端情况
        # open_oir_extreme_unfavorable = (oir1 >  oir_threshold) or (oir2 < -oir_threshold)  # 做多开仓：希望market1卖压不重（oir1低），market2买压强（oir2高）
        # close_oir_extreme_unfavorable = (oir1 < -oir_threshold) or (oir2 >  oir_threshold)  # 平多平仓：希望market1买压强（oir1高），market2卖压不重（oir2低）

        open_oir_extreme_unfavorable = (oir1 <  -oir_threshold) 
        close_oir_extreme_unfavorable = (oir1 > oir_threshold) 

                

        #开仓
        if open_judgement and (open_oir_extreme_unfavorable) and (not is_open_market2_taker) and (not is_open_market1_maker) and position_count < pos_limit:
            # 如果开仓条件成立，且market2没有成交，market1也没有成交，并且持仓没有超过最大仓位
            open_market2_taker_price = round(market2_bid_price1 ,round_num2)  
            open_market1_maker_price = round(market1_ask_price1 - tick_size1 * 1, round_num1)  
            is_open_market2_taker = True
            is_open_market1_maker = True
            sr_open_init = sr_open
            market2_open_time = index
            #print(f'{index}, 开仓挂单，market1挂单价:{open_market1_maker_price}，market2成交价：{open_market2_taker_price}')

        # 如果market2已经taker成交，但是market1还没有成交
        if is_open_market2_taker and is_open_market1_maker:
            if market1_ask_price1 <= open_market1_maker_price: # 如果当前ask小于等于开仓时候的bid price就成交
                is_open_done = True
                is_open_market1_maker = False
                sr_open_real = open_market2_taker_price/open_market1_maker_price -1
                market1_open_time = index
                #print(f'{index}, market1Maker成交, market1Maker成交价格{open_market1_maker_price}')
                open_trades.append([index, sr_open_init, sr_open_real, open_market1_maker_price, open_market2_taker_price,
                                    'market1_maker', 'market2_taker',
                                    market1_open_time, market2_open_time])
                
            elif round(market1_bid_price1 - open_market1_maker_price, round_num1) > tick_size1 * open_replace_tick_num:
                #print(f'{index}, market1Maker撤单重挂：{open_market1_maker_price}_撤掉重挂至_{round(market1_bid_price1 - tick_size1 * 1, round_num1)}')
                open_market1_maker_price = round(market1_bid_price1 - tick_size1 * 1, round_num1)

            elif (open_market2_taker_price/market1_ask_price1 - 1) <= (sr_open_threshold - market1_stop_loss_sr_delta):
                open_market1_taker_price = market1_ask_price1
                is_open_done = True
                is_open_market1_maker = False
                sr_open_real = open_market2_taker_price/open_market1_taker_price - 1
                market1_open_time = index
                open_trades.append([index, sr_open_init, sr_open_real, open_market1_taker_price, open_market2_taker_price,
                                    'market1_taker', 'market2_taker',
                                    market1_open_time, market2_open_time])
                #print(f'{index}, market1Taker对冲， market1对冲价格：{open_market1_taker_price}')  



        # 成交完成，重置
        if is_open_done:
            is_open_done = False
            position_count += 1
            is_open_market1_taker = False
            is_open_market1_maker = False
            is_open_market2_taker = False
            open_market1_maker_price = None
            open_market1_taker_price = None
            open_market2_taker_price = None
            market1_open_time = None
            market2_open_time = None


        # 平仓，market2 buy, market1 sell
        if close_judgement and (close_oir_extreme_unfavorable)  and (not is_close_market2_taker) and (not is_close_market1_maker) and (position_count > -pos_limit):
            close_market2_taker_price = round(market2_ask_price1 ,round_num2)  
            close_market1_maker_price = round(market1_bid_price1 + tick_size1 * 1, round_num1)  
            is_close_market2_taker = True
            is_close_market1_maker = True
            sr_close_init = sr_close
            market2_close_time = index
            #print(f'{index}, 平仓挂单，market1挂单价:{close_market1_maker_price}，market2成交价：{close_market2_taker_price}')

        # 如果market2已经taker成交，但是market1还没有成交
        if is_close_market2_taker and is_close_market1_maker:
            if market1_bid_price1 >= close_market1_maker_price: 
                is_close_done = True
                is_close_market1_maker = False
                sr_close_real = close_market2_taker_price/close_market1_maker_price -1
                market1_close_time = index
               # print(f'{index}, market1Maker成交, market1Maker成交价格{close_market1_maker_price}')
                close_trades.append([index, sr_close_init, sr_close_real, close_market1_maker_price, close_market2_taker_price,
                                    'market1_maker', 'market2_taker',
                                    market1_close_time, market2_close_time])
                
            elif round(close_market1_maker_price - market1_ask_price1, round_num1) > tick_size1 * close_replace_tick_num:   
                #print(f'{index}, market1Maker撤单重挂：{close_market1_maker_price}_撤掉重挂至_{round(market1_ask_price1 + tick_size1 * 1, round_num1)}')
                close_market1_maker_price = round(market1_ask_price1 + tick_size1 * 1, round_num1)

            elif (close_market2_taker_price/market1_bid_price1 - 1) >= (sr_close_threshold + market1_stop_loss_sr_delta): # 如果market1 立刻taker的价差超过了market1_stop_loss_sr_delta，taker对冲掉
                close_market1_taker_price = market1_bid_price1
                is_close_done = True
                is_close_market1_maker = False
                sr_close_real = close_market2_taker_price/close_market1_taker_price - 1
                market1_close_time = index
                close_trades.append([index, sr_close_init, sr_close_real, close_market1_taker_price, close_market2_taker_price,
                                    'market1_taker', 'market2_taker',
                                    market1_close_time, market2_close_time])
                #print(f'{index}, market1Taker对冲， market1对冲价格：{close_market1_taker_price}')  





        # 成交完成，重置
        if is_close_done:
            position_count -= 1
            is_close_done = False
            is_close_market1_taker = False
            is_close_market1_maker = False
            is_close_market2_taker = False
            close_market1_taker_price = None
            close_market1_maker_price = None
            close_market2_taker_price = None
            market1_close_time = None
            market2_close_time = None


    
    open_trades_df = pd.DataFrame(open_trades, columns=['index', 'sr_open', 'sr_open_real', 
                                                        'market1_traded_price', 'market2_traded_price',
                                                        'market1_traded_type', 'market2_traded_type',
                                                        'market1_open_time', 'market2_open_time'])
    open_trades_df.set_index('index', inplace=True)
    close_trades_df = pd.DataFrame(close_trades, columns=['index', 'sr_close', 'sr_close_real', 
                                                        'market1_traded_price', 'market2_traded_price',
                                                        'market1_traded_type', 'market2_traded_type',
                                                        'market1_close_time', 'market2_close_time'])
    close_trades_df.set_index('index', inplace=True)

    
    
    return open_trades_df, close_trades_df
    


### 2025-05-25 增加深度不平衡因子来过滤

def maker_maker_backtest_0525(cf_depth, mt_params, pos_limit = np.inf):

    oir_threshold = mt_params["oir_threshold"]

    open_replace_tick_num = mt_params["open_replace_tick_num"]
    close_replace_tick_num = mt_params["close_replace_tick_num"]
    target_open_sr = mt_params["target_open_sr"]
    target_close_sr = mt_params["target_close_sr"]

    market1_stop_loss_sr_delta = mt_params['market1_stop_loss_sr_delta']
    market2_stop_loss_sr_delta = mt_params['market2_stop_loss_sr_delta']

    sr_open_threshold = target_open_sr
    sr_close_threshold = target_close_sr
    

    tick_size1 = infer_tick_size_from_column(cf_depth['market1_bid_price0'])
    tick_ratio1 = round(tick_size1/cf_depth.iloc[-1]['market1_bid_price0'], 6)
    round_num1 = int(np.log10(1/tick_size1))
    tick_size2 = infer_tick_size_from_column(cf_depth['market2_bid_price0'])
    tick_ratio2 = round(tick_size2/cf_depth.iloc[-1]['market2_bid_price0'], 6)
    round_num2 = int(np.log10(1/tick_size2))
    print("交易所1精度信息", tick_size1, tick_ratio1, round_num1)
    print("交易所2精度信息", tick_size2, tick_ratio2, round_num2)
    
    is_open_market1_done = False
    is_open_market2_done = False
    is_open_market1_maker = False
    is_open_market2_maker = False
    open_market1_maker_price = None
    open_market1_taker_price = None
    open_market2_maker_price = None
    open_market2_taker_price = None
    market1_open_time = None
    market2_open_time = None

    is_close_market1_done = False
    is_close_market2_done = False
    is_close_market1_maker = False
    is_close_market2_maker = False
    close_market1_maker_price = None
    close_market1_taker_price = None
    close_market2_maker_price = None
    close_market2_taker_price = None
    market1_close_time = None
    market2_close_time = None
    
    sr_open = None
    sr_open_real = None
    sr_close = None
    sr_close_real = None


    # 回测输出变量
    open_trades = []
    close_trades = []
    position_count = 0

    for index, ds in cf_depth.iterrows():
        #新加仓位限制

        sr_open = ds['sr_open']
        sr_close = ds['sr_close']
        market1_ask_price1 = ds['market1_ask_price0']
        market1_bid_price1 = ds['market1_bid_price0']
        market2_ask_price1 = ds['market2_ask_price0']
        market2_bid_price1 = ds['market2_bid_price0']

        market1_bid_vol = ds['market1_bid_size0'] 
        market1_ask_vol = ds['market1_ask_size0']
        market2_bid_vol = ds['market2_bid_size0']
        market2_ask_vol = ds['market2_ask_size0']

        oir1 = (market1_bid_vol - market1_ask_vol) / (market1_bid_vol + market1_ask_vol + 1e-8)
        oir2 = (market2_bid_vol - market2_ask_vol) / (market2_bid_vol + market2_ask_vol + 1e-8)

        # 默认过滤极端值：开仓方向不利、平仓方向不利的极端情况
        open_oir_extreme_unfavorable =(oir2 < -oir_threshold)  # 做多开仓：希望market1卖压不重（oir1低），market2买压强（oir2高）
        close_oir_extreme_unfavorable = (oir2 >  oir_threshold)  # 平多平仓：希望market1买压强（oir1高），market2卖压不重（oir2低）

                
        # 改成TM的价差会好一点
        sr_open = (market2_bid_price1)/(market1_ask_price1 - tick_size1 * 1) - 1
        sr_close = (market2_ask_price1)/(market1_bid_price1 + tick_size1 * 1) - 1


        # 把开平仓预设撤单价差作为撤单条件(实盘逻辑)
        sr_open_cancel_threshold = target_open_sr
        sr_close_cancel_threshold = target_close_sr
        
        # 下单判断
        open_judgement = (sr_open >= sr_open_threshold)
        close_judgement = (sr_close <= sr_close_threshold)
        # 撤单判断
        open_cancel_judgement = (sr_open < sr_open_cancel_threshold)
        close_cancel_judgement = (sr_close > sr_close_cancel_threshold)
        # print(type(open_judgement), type(open_oir_extreme_unfavorable))

        if open_judgement and (open_oir_extreme_unfavorable) and (not is_open_market1_maker) and (not is_open_market2_maker) and position_count < pos_limit:
            open_market1_maker_price = round(market1_ask_price1 - tick_size1 * 1, round_num1)  #bid
            open_market2_maker_price = round(market2_bid_price1 + tick_size2 * 1, round_num2)  #ask
            is_open_market1_maker = True
            is_open_market2_maker = True
            ### print(f'{index}, 开仓挂单，market1挂单价:{open_market1_maker_price}，market2挂单价：{open_market2_maker_price}')
        # 如果价差消失且没成交，撤单
        if open_cancel_judgement and is_open_market1_maker and is_open_market2_maker:
            is_open_market1_maker = False
            is_open_market2_maker = False

        # 如果双边都没有成交
        if (is_open_market1_maker and (not is_open_market1_done)) and (is_open_market2_maker and (not is_open_market2_done)):
            # 判断market1是否成交
            if (market1_ask_price1 <= open_market1_maker_price):
                is_open_market1_done = True
                is_open_market1_maker = False
                market1_open_time = index
              ###  print(f'{index}, market1Maker成交：{open_market1_maker_price}')
            # 判断market2是否成交
            elif (market2_bid_price1 >= open_market2_maker_price):
                is_open_market2_done = True
                is_open_market2_maker = False
                market2_open_time = index
            ###    print(f'{index}, market2Maker成交：{open_market1_maker_price}')
            # 判断是否重新挂单
            else:
                if (market1_ask_price1 - open_market1_maker_price)> tick_size1 * open_replace_tick_num:         # 这个地方等于的逻辑是不是有点问题？ 如果价格不变的话，好像也在重新挂单。。。
                    open_market1_maker_price = round(market1_ask_price1 - tick_size1 * 1, round_num1)           # 这个地方是不是应该再进行一下撤单判断？ 不然有可能直接成交了； 好像不需要，因为到这一步了就说明market1，2都没有成交，就直接continue了。
                if (open_market2_maker_price - market2_bid_price1)> tick_size2 * open_replace_tick_num:
                    open_market2_maker_price = round(market2_bid_price1 + tick_size2 * 1, round_num2)       # 同样，这个地方是不是应该再进行一下撤单判断？ 不然有可能直接成交了
                
        # 如果market1成交了
        if is_open_market1_done and is_open_market2_maker and (not is_open_market2_done):
            # 判读market2是否要taker对冲掉
            if (market2_bid_price1/open_market1_maker_price - 1) <= (sr_open_threshold - market2_stop_loss_sr_delta):
                open_market2_taker_price = market2_bid_price1
                is_open_market2_done = True
                is_open_market2_maker = False
                sr_open_real = open_market2_taker_price/open_market1_maker_price - 1
                market2_open_time = index
                open_trades.append([index, sr_open, sr_open_real, open_market1_maker_price, open_market2_taker_price,
                                    'market1_maker', 'market2_taker',
                                    market1_open_time, market2_open_time])
            ###    print(f'{index}, market2Taker对冲， market2对冲价格：{open_market2_taker_price}')
            # 判断market2是否成交
            elif market2_bid_price1 >= open_market2_maker_price:
                is_open_market2_done = True
                is_open_market2_maker = False
                sr_open_real = open_market2_maker_price/open_market1_maker_price - 1
                market2_open_time = index
                open_trades.append([index, sr_open, sr_open_real, open_market1_maker_price, open_market2_maker_price,
                                    'market1_maker', 'market2_maker',
                                    market1_open_time, market2_open_time])
            ###    print(f'{index}, market2Maker成交， market2成交价格：{open_market2_maker_price}')
            # 判断market2是否重挂
            elif (open_market2_maker_price - market2_bid_price1)> tick_size2 * open_replace_tick_num:    # 这个地方等于的逻辑是不是有点问题？ 如果价格不变的话，好像也在重新挂单。。。
                    open_market2_maker_price = round(market2_bid_price1 + tick_size2 * 1, round_num2)

        # 如果market2成交了
        if is_open_market2_done and is_open_market1_maker and (not is_open_market1_done):
            # 判读market1是否要taker对冲掉
            if (open_market2_maker_price/market1_ask_price1 - 1) <= (sr_open_threshold - market1_stop_loss_sr_delta):
                open_market1_taker_price = market1_ask_price1
                is_open_market1_done = True
                is_open_market1_maker = False
                sr_open_real = open_market2_maker_price/open_market1_taker_price - 1
                market1_open_time = index
                open_trades.append([index, sr_open, sr_open_real, open_market1_taker_price, open_market2_maker_price,
                                    'market1_taker', 'market2_maker',
                                    market1_open_time, market2_open_time])
            ###    print(f'{index}, market1Taker对冲， maker1对冲价格{open_market1_taker_price}')
            # 判断market1是否成交
            elif market1_ask_price1 <= open_market1_maker_price:
                is_open_market1_done = True
                is_open_market1_maker = False
                sr_open_real = open_market2_maker_price/open_market1_maker_price - 1
                market1_open_time = index
                open_trades.append([index, sr_open, sr_open_real, open_market1_maker_price, open_market2_maker_price,
                                    'market1_maker', 'market2_maker',
                                    market1_open_time, market2_open_time])
            ###    print(f'{index}, market1Maker现货成交， market1Maker成交价格{open_market1_maker_price}')
            # 判断market1是否重挂
            elif (market1_ask_price1 - open_market1_maker_price)> tick_size1 * open_replace_tick_num:    # 这个地方等于的逻辑是不是有点问题？ 如果价格不变的话，好像也在重新挂单。。。
                    open_market1_maker_price = round(market1_ask_price1 - tick_size1 * 1, round_num1)

        # 成交完成，重置
        if is_open_market1_done and is_open_market2_done:
            position_count += 1
            is_open_market1_done = False
            is_open_market2_done = False
            is_open_market1_maker = False
            is_open_market2_maker = False
            open_market1_maker_price = None
            open_market1_taker_price = None
            open_market2_maker_price = None
            open_market2_taker_price = None
            market1_open_time = None
            market2_open_time = None


        # 平仓
        # print(close_judgement,is_close_market1_maker,is_close_market2_maker)
        if close_judgement and (close_oir_extreme_unfavorable) and (not is_close_market1_maker) and (not is_close_market2_maker) and (position_count > -pos_limit):
            close_market1_maker_price = round(market1_bid_price1 + tick_size1 * 1, round_num1)
            close_market2_maker_price = round(market2_ask_price1 - tick_size2 * 1, round_num2)
            is_close_market1_maker = True
            is_close_market2_maker = True
        ###    print(f'{index}, 平仓挂单，market1挂单价:{close_market1_maker_price}，market2挂单价：{close_market2_maker_price}')

        # 如果价差消失且没成交，撤单
        if close_cancel_judgement and is_close_market1_maker and is_close_market2_maker:
            is_close_market1_maker = False
            is_close_market2_maker = False
        ###    print(f'{index}, 平仓撤单，market1挂单价：{market1_bid_price1 + tick_size1 * 1},market2挂单价:{market2_ask_price1-tick_size2 * 1}')

        # 如果边都没有成交
        if (is_close_market1_maker and (not is_close_market1_done)) and (is_close_market2_maker and (not is_close_market2_done)):
            # 判断market1是否成交
            if (market1_bid_price1 >= close_market1_maker_price):
                is_close_market1_done = True
                is_close_market1_maker = False
                market1_close_time = index
        ###        print(f'{index}, market1Maker成交：{close_market1_maker_price}')
            # 判断market2是否成交
            elif (market2_ask_price1 <= close_market2_maker_price):
                is_close_market2_done = True
                is_close_market2_maker = False
                market2_close_time = index
        ###        print(f'{index}, market2Maker成交：{close_market2_maker_price}')
            # 判断是否重新挂单
            else:
                if (market2_ask_price1 - close_market2_maker_price)> tick_size2 * close_replace_tick_num:
                    close_market2_maker_price = round(market2_ask_price1 - tick_size2 * close_replace_tick_num, round_num2)
                if (close_market1_maker_price - market1_bid_price1)> tick_size1 * close_replace_tick_num:
                    close_market1_maker_price = round(market1_bid_price1 + tick_size1 * close_replace_tick_num, round_num1)

        # 如果market1成交了
        if is_close_market1_done and is_close_market2_maker and (not is_close_market2_done):
            # 判断market2是否要taker对冲掉
            if (market2_ask_price1/close_market1_maker_price - 1) >= (sr_close_threshold + market2_stop_loss_sr_delta):
                close_market2_taker_price = market2_ask_price1
                is_close_market2_done = True
                is_close_market2_maker = False
                sr_close_real = close_market2_taker_price/close_market1_maker_price - 1
                swap_close_time = index
                close_trades.append([index, sr_close, sr_close_real, close_market1_maker_price, close_market2_taker_price,
                                     'market1_maker', 'market2_taker',
                                     market1_close_time, market2_close_time])
        ###        print(f'{index}, marke2Taker成交：{close_market2_taker_price}')
            # 判断market2是否成交
            elif market2_ask_price1 <= close_market2_maker_price:
                is_close_market2_done = True
                is_close_market2_maker = False
                sr_close_real = close_market2_maker_price/close_market1_maker_price - 1
                market2_close_time = index
                close_trades.append([index, sr_close, sr_close_real, close_market1_maker_price, close_market2_maker_price,
                                     'market1_maker', 'market2_maker',
                                     market1_close_time, market2_close_time])
        ###        print(f'{index}, marke2Maker成交：{close_market2_maker_price}')
            # 判断market2是否重挂
            elif (market2_ask_price1 - close_market2_maker_price)> tick_size2 * close_replace_tick_num:
                    close_market2_maker_price = round(market2_ask_price1 - tick_size2 * 1, round_num2)
            

        # 如果market2成交了
        if is_close_market2_done and is_close_market1_maker and (not is_close_market1_done):
            # 判断market1是否要taker对冲掉
            if (close_market2_maker_price/market1_bid_price1 - 1) >= (sr_close_threshold + market1_stop_loss_sr_delta):
                close_market1_taker_price = market1_bid_price1
                is_close_market1_done = True
                is_close_market1_maker = False
                sr_close_real = close_market2_maker_price/close_market1_taker_price - 1
                market1_close_time = index
                close_trades.append([index, sr_close, sr_close_real, close_market1_taker_price, close_market2_maker_price,
                                     'market1_taker', 'market2_maker',
                                     market1_close_time, market2_close_time])
        ###        print(f'{index}, marke1Taker成交：{close_market1_taker_price}')
            # 判断market1是否成交
            elif market1_bid_price1 >= close_market1_maker_price:
                is_close_market1_done = True
                is_close_market1_maker = False
                sr_close_real = close_market2_maker_price/close_market1_maker_price - 1
                market1_close_time = index
                close_trades.append([index, sr_close, sr_close_real, close_market1_maker_price, close_market2_maker_price,
                                     'market1_maker', 'market2_maker',
                                     market1_close_time, market2_close_time])
        ###        print(f'{index}, marke1Maker成交：{close_market1_maker_price}')
            # 判断market1是否重挂
            elif (close_market1_maker_price - market1_bid_price1)> tick_size1 * close_replace_tick_num:
                    close_market1_maker_price = round(market1_bid_price1 + tick_size1 * 1, round_num1)

        # 成交完成，重置
        if is_close_market1_done and is_close_market2_done:
            position_count -= 1
            is_close_market1_done = False
            is_close_market2_done = False
            is_close_market1_maker = False
            is_close_market2_maker = False
            close_market1_maker_price = None
            close_market1_taker_price = None
            close_market2_maker_price = None
            close_market2_taker_price = None
            market1_close_time = None
            market2_close_time = None
            

        # except Exception as e:
                # traceback.print_exc()
    
    open_trades_df = pd.DataFrame(open_trades, columns=['index', 'sr_open', 'sr_open_real', 
                                                        'market1_traded_price', 'market2_traded_price',
                                                        'market1_traded_type', 'market2_traded_type',
                                                        'market1_open_time', 'market2_open_time'])
    open_trades_df.set_index('index', inplace=True)
    close_trades_df = pd.DataFrame(close_trades, columns=['index', 'sr_close', 'sr_close_real', 
                                                        'market1_traded_price', 'market2_traded_price',
                                                        'market1_traded_type', 'market2_traded_type',
                                                        'market1_close_time', 'market2_close_time'])
    close_trades_df.set_index('index', inplace=True)

    
    
    return open_trades_df, close_trades_df
    



### 2025-05-19 改成tm的下单方式，增加了is_any_trade_in_progress
def maker_maker_backtest_0519(cf_depth, mt_params, pos_limit = np.inf):
    """
    exchange1='binance', 
    market1='swap',
    exchange2='okx',      
    market2='swap',

    """


    open_replace_tick_num = mt_params["open_replace_tick_num"]
    close_replace_tick_num = mt_params["close_replace_tick_num"]

    target_open_sr = mt_params["target_open_sr"]
    target_close_sr = mt_params["target_close_sr"]

    market1_stop_loss_sr_delta = mt_params['market1_stop_loss_sr_delta']

    sr_open_threshold = target_open_sr
    sr_close_threshold = target_close_sr
    

    tick_size1 = infer_tick_size_from_column(cf_depth['market1_bid_price0'])
    tick_ratio1 = round(tick_size1/cf_depth.iloc[-1]['market1_bid_price0'], 6)
    round_num1 = int(np.log10(1/tick_size1))
    tick_size2 = infer_tick_size_from_column(cf_depth['market2_bid_price0'])
    tick_ratio2 = round(tick_size2/cf_depth.iloc[-1]['market2_bid_price0'], 6)
    round_num2 = int(np.log10(1/tick_size2))
    print("交易所1精度信息", tick_size1, tick_ratio1, round_num1)
    print("交易所2精度信息", tick_size2, tick_ratio2, round_num2)


    

    is_open_done = False
    is_open_market1_maker = False
    is_open_market2_taker = False
    open_market1_maker_price = None
    open_market1_taker_price = None
    open_market2_taker_price = None
    market1_open_time = None
    market2_open_time = None

    is_close_done = False
    is_close_market1_maker = False
    is_close_market2_taker = False
    close_market1_maker_price = None
    close_market1_taker_price = None
    close_market2_taker_price = None
    market1_close_time = None
    market2_close_time = None
    
    sr_open = None
    sr_open_real = None
    sr_close = None
    sr_close_real = None

    open_trades = []
    close_trades = []
    position_count = 0

    for index, ds in cf_depth.iterrows():

    # exchange1 maker; exchange2 taker
        

        market1_ask_price1 = ds['market1_ask_price0']
        market1_bid_price1 = ds['market1_bid_price0']
        market2_ask_price1 = ds['market2_ask_price0']
        market2_bid_price1 = ds['market2_bid_price0']

        # sr_open = (market2_bid_price1)/(market1_ask_price1 - tick_size1 * 1) - 1
        # sr_close = (market2_ask_price1)/(market1_bid_price1 + tick_size1 * 1) - 1

        sr_open = (market2_bid_price1)/(market1_ask_price1 - tick_size1 * 1) - 1  # sr_open是要买exchange1, 卖exchange2, 所以exchange1用bid price, exchange2用bid price
        sr_close = (market2_ask_price1)/(market1_bid_price1 + tick_size1 * 1) - 1 # sr_close是要卖exchange1, 买exchange2, 所以exchange1用ask price, exchange2用ask price

        # 下单判断
        open_judgement = (sr_open >= sr_open_threshold)
        close_judgement = (sr_close <= sr_close_threshold)

        is_any_trade_in_progress = (
            is_open_market1_maker or is_open_market2_taker or 
            is_close_market1_maker or is_close_market2_taker
        )
        # is_any_trade_in_progress = False

        #开仓
        if open_judgement and (not is_any_trade_in_progress) and (not is_open_market2_taker) and (not is_open_market1_maker) and position_count < pos_limit:
            # 如果开仓条件成立，且market2没有成交，market1也没有成交，并且持仓没有超过最大仓位
            open_market2_taker_price = round(market2_bid_price1 ,round_num2)  
            open_market1_maker_price = round(market1_ask_price1 - tick_size1 * 1, round_num1)  
            is_open_market2_taker = True
            is_open_market1_maker = True
            sr_open_init = sr_open
            market2_open_time = index
            #print(f'{index}, 开仓挂单，market1挂单价:{open_market1_maker_price}，market2成交价：{open_market2_taker_price}')

        # 如果market2已经taker成交，但是market1还没有成交
        if is_open_market2_taker and is_open_market1_maker:
            if market1_ask_price1 <= open_market1_maker_price: # 如果当前ask小于等于开仓时候的bid price就成交
                is_open_done = True
                is_open_market1_maker = False
                sr_open_real = open_market2_taker_price/open_market1_maker_price -1
                market1_open_time = index
                #print(f'{index}, market1Maker成交, market1Maker成交价格{open_market1_maker_price}')
                open_trades.append([index, sr_open_init, sr_open_real, open_market1_maker_price, open_market2_taker_price,
                                    'market1_maker', 'market2_taker',
                                    market1_open_time, market2_open_time])
                
            elif round(market1_bid_price1 - open_market1_maker_price, round_num1) > tick_size1 * open_replace_tick_num:
                #print(f'{index}, market1Maker撤单重挂：{open_market1_maker_price}_撤掉重挂至_{round(market1_bid_price1 - tick_size1 * 1, round_num1)}')
                open_market1_maker_price = round(market1_bid_price1 - tick_size1 * 1, round_num1)

            elif (open_market2_taker_price/market1_ask_price1 - 1) <= (sr_open_threshold - market1_stop_loss_sr_delta):
                open_market1_taker_price = market1_ask_price1
                is_open_done = True
                is_open_market1_maker = False
                sr_open_real = open_market2_taker_price/open_market1_taker_price - 1
                market1_open_time = index
                open_trades.append([index, sr_open_init, sr_open_real, open_market1_taker_price, open_market2_taker_price,
                                    'market1_taker', 'market2_taker',
                                    market1_open_time, market2_open_time])
                #print(f'{index}, market1Taker对冲， market1对冲价格：{open_market1_taker_price}')  



        # 成交完成，重置
        if is_open_done:
            is_open_done = False
            position_count += 1
            is_open_market1_taker = False
            is_open_market1_maker = False
            is_open_market2_taker = False
            open_market1_maker_price = None
            open_market1_taker_price = None
            open_market2_taker_price = None
            market1_open_time = None
            market2_open_time = None


        # 平仓，market2 buy, market1 sell
        if close_judgement  and (not is_any_trade_in_progress) and (not is_close_market2_taker) and (not is_close_market1_maker) and (position_count > -pos_limit):
            close_market2_taker_price = round(market2_ask_price1 ,round_num2)  
            close_market1_maker_price = round(market1_bid_price1 + tick_size1 * 1, round_num1)  
            is_close_market2_taker = True
            is_close_market1_maker = True
            sr_close_init = sr_close
            market2_close_time = index
            #print(f'{index}, 平仓挂单，market1挂单价:{close_market1_maker_price}，market2成交价：{close_market2_taker_price}')

        # 如果market2已经taker成交，但是market1还没有成交
        if is_close_market2_taker and is_close_market1_maker:
            if market1_bid_price1 >= close_market1_maker_price: 
                is_close_done = True
                is_close_market1_maker = False
                sr_close_real = close_market2_taker_price/close_market1_maker_price -1
                market1_close_time = index
               # print(f'{index}, market1Maker成交, market1Maker成交价格{close_market1_maker_price}')
                close_trades.append([index, sr_close_init, sr_close_real, close_market1_maker_price, close_market2_taker_price,
                                    'market1_maker', 'market2_taker',
                                    market1_close_time, market2_close_time])
                
            elif round(close_market1_maker_price - market1_ask_price1, round_num1) > tick_size1 * close_replace_tick_num:   
                #print(f'{index}, market1Maker撤单重挂：{close_market1_maker_price}_撤掉重挂至_{round(market1_ask_price1 + tick_size1 * 1, round_num1)}')
                close_market1_maker_price = round(market1_ask_price1 + tick_size1 * 1, round_num1)

            elif (close_market2_taker_price/market1_bid_price1 - 1) >= (sr_close_threshold + market1_stop_loss_sr_delta): # 如果market1 立刻taker的价差超过了market1_stop_loss_sr_delta，taker对冲掉
                close_market1_taker_price = market1_bid_price1
                is_close_done = True
                is_close_market1_maker = False
                sr_close_real = close_market2_taker_price/close_market1_taker_price - 1
                market1_close_time = index
                close_trades.append([index, sr_close_init, sr_close_real, close_market1_taker_price, close_market2_taker_price,
                                    'market1_taker', 'market2_taker',
                                    market1_close_time, market2_close_time])
                #print(f'{index}, market1Taker对冲， market1对冲价格：{close_market1_taker_price}')  





        # 成交完成，重置
        if is_close_done:
            position_count -= 1
            is_close_done = False
            is_close_market1_taker = False
            is_close_market1_maker = False
            is_close_market2_taker = False
            close_market1_taker_price = None
            close_market1_maker_price = None
            close_market2_taker_price = None
            market1_close_time = None
            market2_close_time = None


    
    open_trades_df = pd.DataFrame(open_trades, columns=['index', 'sr_open', 'sr_open_real', 
                                                        'market1_traded_price', 'market2_traded_price',
                                                        'market1_traded_type', 'market2_traded_type',
                                                        'market1_open_time', 'market2_open_time'])
    open_trades_df.set_index('index', inplace=True)
    close_trades_df = pd.DataFrame(close_trades, columns=['index', 'sr_close', 'sr_close_real', 
                                                        'market1_traded_price', 'market2_traded_price',
                                                        'market1_traded_type', 'market2_traded_type',
                                                        'market1_close_time', 'market2_close_time'])
    close_trades_df.set_index('index', inplace=True)

    
    
    return open_trades_df, close_trades_df
    

def compute_oir(bid_vol, ask_vol):
    return (bid_vol - ask_vol) / (bid_vol + ask_vol + 1e-8)



### 2025-05-27 增加MT/TM type
def maker_maker_backtest_0527(cf_depth, mt_params, pos_limit = np.inf):

    order_type = mt_params['order_type']  # 'MT' or 'TM'
    open_replace_tick_num = mt_params["open_replace_tick_num"]
    close_replace_tick_num = mt_params["close_replace_tick_num"]
    target_open_sr = mt_params["target_open_sr"]
    target_close_sr = mt_params["target_close_sr"]

    market1_stop_loss_sr_delta = mt_params['market1_stop_loss_sr_delta']
    market2_stop_loss_sr_delta = mt_params['market2_stop_loss_sr_delta']

    sr_open_threshold = target_open_sr
    sr_close_threshold = target_close_sr
    

    tick_size1 = infer_tick_size_from_column(cf_depth['market1_bid_price0'])
    tick_ratio1 = round(tick_size1/cf_depth.iloc[-1]['market1_bid_price0'], 6)
    round_num1 = int(np.log10(1/tick_size1))
    tick_size2 = infer_tick_size_from_column(cf_depth['market2_bid_price0'])
    tick_ratio2 = round(tick_size2/cf_depth.iloc[-1]['market2_bid_price0'], 6)
    round_num2 = int(np.log10(1/tick_size2))
    print("交易所1精度信息", tick_size1, tick_ratio1, round_num1)
    print("交易所2精度信息", tick_size2, tick_ratio2, round_num2)
    
    is_open_market1_done = False
    is_open_market2_done = False
    is_open_market1_maker = False
    is_open_market2_maker = False
    open_market1_maker_price = None
    open_market1_taker_price = None
    open_market2_maker_price = None
    open_market2_taker_price = None
    market1_open_time = None
    market2_open_time = None

    is_close_market1_done = False
    is_close_market2_done = False
    is_close_market1_maker = False
    is_close_market2_maker = False
    close_market1_maker_price = None
    close_market1_taker_price = None
    close_market2_maker_price = None
    close_market2_taker_price = None
    market1_close_time = None
    market2_close_time = None
    
    sr_open = None
    sr_open_real = None
    sr_close = None
    sr_close_real = None


    # 回测输出变量
    open_trades = []
    close_trades = []
    position_count = 0

    for index, ds in cf_depth.iterrows():
        #新加仓位限制

        sr_open = ds['sr_open']
        sr_close = ds['sr_close']
        market1_ask_price1 = ds['market1_ask_price0']
        market1_bid_price1 = ds['market1_bid_price0']
        market2_ask_price1 = ds['market2_ask_price0']
        market2_bid_price1 = ds['market2_bid_price0']


        # TM: binance maker ,okx taker
        # 默认是MT
        sr_open = (market2_bid_price1)/(market1_ask_price1 - tick_size1 * 1) - 1
        sr_close = (market2_ask_price1)/(market1_bid_price1 + tick_size1 * 1) - 1

        if order_type == 'TM':
            sr_open = (market2_ask_price1)/(market1_bid_price1 + tick_size1 * 1) - 1
            sr_close = (market2_bid_price1)/(market1_ask_price1 - tick_size1 * 1) - 1

        
        # 把开平仓预设撤单价差作为撤单条件(实盘逻辑)
        sr_open_cancel_threshold = target_open_sr
        sr_close_cancel_threshold = target_close_sr
        
        # 下单判断
        open_judgement = (sr_open >= sr_open_threshold)
        close_judgement = (sr_close <= sr_close_threshold)
        # 撤单判断
        open_cancel_judgement = (sr_open < sr_open_cancel_threshold)
        close_cancel_judgement = (sr_close > sr_close_cancel_threshold)

        if open_judgement and (not is_open_market1_maker) and (not is_open_market2_maker) and position_count < pos_limit:
            open_market1_maker_price = round(market1_ask_price1 - tick_size1 * 1, round_num1)  #bid
            open_market2_maker_price = round(market2_bid_price1 + tick_size2 * 1, round_num2)  #ask
            is_open_market1_maker = True
            is_open_market2_maker = True
            ### print(f'{index}, 开仓挂单，market1挂单价:{open_market1_maker_price}，market2挂单价：{open_market2_maker_price}')
        # 如果价差消失且没成交，撤单
        if open_cancel_judgement and is_open_market1_maker and is_open_market2_maker:
            is_open_market1_maker = False
            is_open_market2_maker = False

        # 如果双边都没有成交
        if (is_open_market1_maker and (not is_open_market1_done)) and (is_open_market2_maker and (not is_open_market2_done)):
            # 判断market1是否成交
            if (market1_ask_price1 <= open_market1_maker_price):
                is_open_market1_done = True
                is_open_market1_maker = False
                market1_open_time = index
              ###  print(f'{index}, market1Maker成交：{open_market1_maker_price}')
            # 判断market2是否成交
            elif (market2_bid_price1 >= open_market2_maker_price):
                is_open_market2_done = True
                is_open_market2_maker = False
                market2_open_time = index
            ###    print(f'{index}, market2Maker成交：{open_market1_maker_price}')
            # 判断是否重新挂单
            else:
                if (market1_ask_price1 - open_market1_maker_price)> tick_size1 * open_replace_tick_num:         # 这个地方等于的逻辑是不是有点问题？ 如果价格不变的话，好像也在重新挂单。。。
                    open_market1_maker_price = round(market1_ask_price1 - tick_size1 * 1, round_num1)           # 这个地方是不是应该再进行一下撤单判断？ 不然有可能直接成交了； 好像不需要，因为到这一步了就说明market1，2都没有成交，就直接continue了。
                if (open_market2_maker_price - market2_bid_price1)> tick_size2 * open_replace_tick_num:
                    open_market2_maker_price = round(market2_bid_price1 + tick_size2 * 1, round_num2)       # 同样，这个地方是不是应该再进行一下撤单判断？ 不然有可能直接成交了
                
        # 如果market1成交了
        if is_open_market1_done and is_open_market2_maker and (not is_open_market2_done):
            # 判读market2是否要taker对冲掉
            if (market2_bid_price1/open_market1_maker_price - 1) <= (sr_open_threshold - market2_stop_loss_sr_delta):
                open_market2_taker_price = market2_bid_price1
                is_open_market2_done = True
                is_open_market2_maker = False
                sr_open_real = open_market2_taker_price/open_market1_maker_price - 1
                market2_open_time = index
                open_trades.append([index, sr_open, sr_open_real, open_market1_maker_price, open_market2_taker_price,
                                    'market1_maker', 'market2_taker',
                                    market1_open_time, market2_open_time])
            ###    print(f'{index}, market2Taker对冲， market2对冲价格：{open_market2_taker_price}')
            # 判断market2是否成交
            elif market2_bid_price1 >= open_market2_maker_price:
                is_open_market2_done = True
                is_open_market2_maker = False
                sr_open_real = open_market2_maker_price/open_market1_maker_price - 1
                market2_open_time = index
                open_trades.append([index, sr_open, sr_open_real, open_market1_maker_price, open_market2_maker_price,
                                    'market1_maker', 'market2_maker',
                                    market1_open_time, market2_open_time])
            ###    print(f'{index}, market2Maker成交， market2成交价格：{open_market2_maker_price}')
            # 判断market2是否重挂
            elif (open_market2_maker_price - market2_bid_price1)> tick_size2 * open_replace_tick_num:    # 这个地方等于的逻辑是不是有点问题？ 如果价格不变的话，好像也在重新挂单。。。
                    open_market2_maker_price = round(market2_bid_price1 + tick_size2 * 1, round_num2)

        # 如果market2成交了
        if is_open_market2_done and is_open_market1_maker and (not is_open_market1_done):
            # 判读market1是否要taker对冲掉
            if (open_market2_maker_price/market1_ask_price1 - 1) <= (sr_open_threshold - market1_stop_loss_sr_delta):
                open_market1_taker_price = market1_ask_price1
                is_open_market1_done = True
                is_open_market1_maker = False
                sr_open_real = open_market2_maker_price/open_market1_taker_price - 1
                market1_open_time = index
                open_trades.append([index, sr_open, sr_open_real, open_market1_taker_price, open_market2_maker_price,
                                    'market1_taker', 'market2_maker',
                                    market1_open_time, market2_open_time])
            ###    print(f'{index}, market1Taker对冲， maker1对冲价格{open_market1_taker_price}')
            # 判断market1是否成交
            elif market1_ask_price1 <= open_market1_maker_price:
                is_open_market1_done = True
                is_open_market1_maker = False
                sr_open_real = open_market2_maker_price/open_market1_maker_price - 1
                market1_open_time = index
                open_trades.append([index, sr_open, sr_open_real, open_market1_maker_price, open_market2_maker_price,
                                    'market1_maker', 'market2_maker',
                                    market1_open_time, market2_open_time])
            ###    print(f'{index}, market1Maker现货成交， market1Maker成交价格{open_market1_maker_price}')
            # 判断market1是否重挂
            elif (market1_ask_price1 - open_market1_maker_price)> tick_size1 * open_replace_tick_num:    # 这个地方等于的逻辑是不是有点问题？ 如果价格不变的话，好像也在重新挂单。。。
                    open_market1_maker_price = round(market1_ask_price1 - tick_size1 * 1, round_num1)

        # 成交完成，重置
        if is_open_market1_done and is_open_market2_done:
            position_count += 1
            is_open_market1_done = False
            is_open_market2_done = False
            is_open_market1_maker = False
            is_open_market2_maker = False
            open_market1_maker_price = None
            open_market1_taker_price = None
            open_market2_maker_price = None
            open_market2_taker_price = None
            market1_open_time = None
            market2_open_time = None


        # 平仓
        # print(close_judgement,is_close_market1_maker,is_close_market2_maker)
        if close_judgement and (not is_close_market1_maker) and (not is_close_market2_maker) and (position_count > -pos_limit):
            close_market1_maker_price = round(market1_bid_price1 + tick_size1 * 1, round_num1)
            close_market2_maker_price = round(market2_ask_price1 - tick_size2 * 1, round_num2)
            is_close_market1_maker = True
            is_close_market2_maker = True
        ###    print(f'{index}, 平仓挂单，market1挂单价:{close_market1_maker_price}，market2挂单价：{close_market2_maker_price}')

        # 如果价差消失且没成交，撤单
        if close_cancel_judgement and is_close_market1_maker and is_close_market2_maker:
            is_close_market1_maker = False
            is_close_market2_maker = False
        ###    print(f'{index}, 平仓撤单，market1挂单价：{market1_bid_price1 + tick_size1 * 1},market2挂单价:{market2_ask_price1-tick_size2 * 1}')

        # 如果边都没有成交
        if (is_close_market1_maker and (not is_close_market1_done)) and (is_close_market2_maker and (not is_close_market2_done)):
            # 判断market1是否成交
            if (market1_bid_price1 >= close_market1_maker_price):
                is_close_market1_done = True
                is_close_market1_maker = False
                market1_close_time = index
        ###        print(f'{index}, market1Maker成交：{close_market1_maker_price}')
            # 判断market2是否成交
            elif (market2_ask_price1 <= close_market2_maker_price):
                is_close_market2_done = True
                is_close_market2_maker = False
                market2_close_time = index
        ###        print(f'{index}, market2Maker成交：{close_market2_maker_price}')
            # 判断是否重新挂单
            else:
                if (market2_ask_price1 - close_market2_maker_price)> tick_size2 * close_replace_tick_num:
                    close_market2_maker_price = round(market2_ask_price1 - tick_size2 * close_replace_tick_num, round_num2)
                if (close_market1_maker_price - market1_bid_price1)> tick_size1 * close_replace_tick_num:
                    close_market1_maker_price = round(market1_bid_price1 + tick_size1 * close_replace_tick_num, round_num1)

        # 如果market1成交了
        if is_close_market1_done and is_close_market2_maker and (not is_close_market2_done):
            # 判断market2是否要taker对冲掉
            if (market2_ask_price1/close_market1_maker_price - 1) >= (sr_close_threshold + market2_stop_loss_sr_delta):
                close_market2_taker_price = market2_ask_price1
                is_close_market2_done = True
                is_close_market2_maker = False
                sr_close_real = close_market2_taker_price/close_market1_maker_price - 1
                swap_close_time = index
                close_trades.append([index, sr_close, sr_close_real, close_market1_maker_price, close_market2_taker_price,
                                     'market1_maker', 'market2_taker',
                                     market1_close_time, market2_close_time])
        ###        print(f'{index}, marke2Taker成交：{close_market2_taker_price}')
            # 判断market2是否成交
            elif market2_ask_price1 <= close_market2_maker_price:
                is_close_market2_done = True
                is_close_market2_maker = False
                sr_close_real = close_market2_maker_price/close_market1_maker_price - 1
                market2_close_time = index
                close_trades.append([index, sr_close, sr_close_real, close_market1_maker_price, close_market2_maker_price,
                                     'market1_maker', 'market2_maker',
                                     market1_close_time, market2_close_time])
        ###        print(f'{index}, marke2Maker成交：{close_market2_maker_price}')
            # 判断market2是否重挂
            elif (market2_ask_price1 - close_market2_maker_price)> tick_size2 * close_replace_tick_num:
                    close_market2_maker_price = round(market2_ask_price1 - tick_size2 * 1, round_num2)
            

        # 如果market2成交了
        if is_close_market2_done and is_close_market1_maker and (not is_close_market1_done):
            # 判断market1是否要taker对冲掉
            if (close_market2_maker_price/market1_bid_price1 - 1) >= (sr_close_threshold + market1_stop_loss_sr_delta):
                close_market1_taker_price = market1_bid_price1
                is_close_market1_done = True
                is_close_market1_maker = False
                sr_close_real = close_market2_maker_price/close_market1_taker_price - 1
                market1_close_time = index
                close_trades.append([index, sr_close, sr_close_real, close_market1_taker_price, close_market2_maker_price,
                                     'market1_taker', 'market2_maker',
                                     market1_close_time, market2_close_time])
        ###        print(f'{index}, marke1Taker成交：{close_market1_taker_price}')
            # 判断market1是否成交
            elif market1_bid_price1 >= close_market1_maker_price:
                is_close_market1_done = True
                is_close_market1_maker = False
                sr_close_real = close_market2_maker_price/close_market1_maker_price - 1
                market1_close_time = index
                close_trades.append([index, sr_close, sr_close_real, close_market1_maker_price, close_market2_maker_price,
                                     'market1_maker', 'market2_maker',
                                     market1_close_time, market2_close_time])
        ###        print(f'{index}, marke1Maker成交：{close_market1_maker_price}')
            # 判断market1是否重挂
            elif (close_market1_maker_price - market1_bid_price1)> tick_size1 * close_replace_tick_num:
                    close_market1_maker_price = round(market1_bid_price1 + tick_size1 * 1, round_num1)

        # 成交完成，重置
        if is_close_market1_done and is_close_market2_done:
            position_count -= 1
            is_close_market1_done = False
            is_close_market2_done = False
            is_close_market1_maker = False
            is_close_market2_maker = False
            close_market1_maker_price = None
            close_market1_taker_price = None
            close_market2_maker_price = None
            close_market2_taker_price = None
            market1_close_time = None
            market2_close_time = None
            

        # except Exception as e:
                # traceback.print_exc()
    
    open_trades_df = pd.DataFrame(open_trades, columns=['index', 'sr_open', 'sr_open_real', 
                                                        'market1_traded_price', 'market2_traded_price',
                                                        'market1_traded_type', 'market2_traded_type',
                                                        'market1_open_time', 'market2_open_time'])
    open_trades_df.set_index('index', inplace=True)
    close_trades_df = pd.DataFrame(close_trades, columns=['index', 'sr_close', 'sr_close_real', 
                                                        'market1_traded_price', 'market2_traded_price',
                                                        'market1_traded_type', 'market2_traded_type',
                                                        'market1_close_time', 'market2_close_time'])
    close_trades_df.set_index('index', inplace=True)

    
    
    return open_trades_df, close_trades_df
    


### 2025-05-02 修改一下判断重新挂单的逻辑

def maker_maker_backtest_0502(cf_depth, mt_params, pos_limit = np.inf):


    open_replace_tick_num = mt_params["open_replace_tick_num"]
    close_replace_tick_num = mt_params["close_replace_tick_num"]
    target_open_sr = mt_params["target_open_sr"]
    target_close_sr = mt_params["target_close_sr"]

    market1_stop_loss_sr_delta = mt_params['market1_stop_loss_sr_delta']
    market2_stop_loss_sr_delta = mt_params['market2_stop_loss_sr_delta']

    sr_open_threshold = target_open_sr
    sr_close_threshold = target_close_sr
    

    tick_size1 = infer_tick_size_from_column(cf_depth['market1_bid_price0'])
    tick_ratio1 = round(tick_size1/cf_depth.iloc[-1]['market1_bid_price0'], 6)
    round_num1 = int(np.log10(1/tick_size1))
    tick_size2 = infer_tick_size_from_column(cf_depth['market2_bid_price0'])
    tick_ratio2 = round(tick_size2/cf_depth.iloc[-1]['market2_bid_price0'], 6)
    round_num2 = int(np.log10(1/tick_size2))
    print("交易所1精度信息", tick_size1, tick_ratio1, round_num1)
    print("交易所2精度信息", tick_size2, tick_ratio2, round_num2)
    
    is_open_market1_done = False
    is_open_market2_done = False
    is_open_market1_maker = False
    is_open_market2_maker = False
    open_market1_maker_price = None
    open_market1_taker_price = None
    open_market2_maker_price = None
    open_market2_taker_price = None
    market1_open_time = None
    market2_open_time = None

    is_close_market1_done = False
    is_close_market2_done = False
    is_close_market1_maker = False
    is_close_market2_maker = False
    close_market1_maker_price = None
    close_market1_taker_price = None
    close_market2_maker_price = None
    close_market2_taker_price = None
    market1_close_time = None
    market2_close_time = None
    
    sr_open = None
    sr_open_real = None
    sr_close = None
    sr_close_real = None


    # 回测输出变量
    open_trades = []
    close_trades = []
    position_count = 0

    for index, ds in cf_depth.iterrows():
        #新加仓位限制

        market1_ask_price1 = ds['market1_ask_price0']
        market1_bid_price1 = ds['market1_bid_price0']
        market2_ask_price1 = ds['market2_ask_price0']
        market2_bid_price1 = ds['market2_bid_price0']


        # 改成TM的价差会好一点
        sr_open = (market2_bid_price1)/(market1_ask_price1 - tick_size1 * 1) - 1
        sr_close = (market2_ask_price1)/(market1_bid_price1 + tick_size1 * 1) - 1


        # 把开平仓预设撤单价差作为撤单条件(实盘逻辑)
        sr_open_cancel_threshold = target_open_sr
        sr_close_cancel_threshold = target_close_sr
        
        # 下单判断
        open_judgement = (sr_open >= sr_open_threshold)
        close_judgement = (sr_close <= sr_close_threshold)
        # 撤单判断
        open_cancel_judgement = (sr_open < sr_open_cancel_threshold)
        close_cancel_judgement = (sr_close > sr_close_cancel_threshold)

        if open_judgement and (not is_open_market1_maker) and (not is_open_market2_maker) and position_count < pos_limit:
            open_market1_maker_price = round(market1_ask_price1 - tick_size1 * 1, round_num1)  #bid
            open_market2_maker_price = round(market2_bid_price1 + tick_size2 * 1, round_num2)  #ask
            is_open_market1_maker = True
            is_open_market2_maker = True
            ### print(f'{index}, 开仓挂单，market1挂单价:{open_market1_maker_price}，market2挂单价：{open_market2_maker_price}')
        # 如果价差消失且没成交，撤单
        if open_cancel_judgement and is_open_market1_maker and is_open_market2_maker:
            is_open_market1_maker = False
            is_open_market2_maker = False

        # 如果双边都没有成交
        if (is_open_market1_maker and (not is_open_market1_done)) and (is_open_market2_maker and (not is_open_market2_done)):
            # 判断market1是否成交
            if (market1_ask_price1 <= open_market1_maker_price):
                is_open_market1_done = True
                is_open_market1_maker = False
                market1_open_time = index
              ###  print(f'{index}, market1Maker成交：{open_market1_maker_price}')
            # 判断market2是否成交
            elif (market2_bid_price1 >= open_market2_maker_price):
                is_open_market2_done = True
                is_open_market2_maker = False
                market2_open_time = index
            ###    print(f'{index}, market2Maker成交：{open_market1_maker_price}')
            # 判断是否重新挂单
            else:
                if (market1_ask_price1 - open_market1_maker_price)> tick_size1 * open_replace_tick_num:         # 这个地方等于的逻辑是不是有点问题？ 如果价格不变的话，好像也在重新挂单。。。
                    open_market1_maker_price = round(market1_ask_price1 - tick_size1 * 1, round_num1)           # 这个地方是不是应该再进行一下撤单判断？ 不然有可能直接成交了； 好像不需要，因为到这一步了就说明market1，2都没有成交，就直接continue了。
                if (open_market2_maker_price - market2_bid_price1)> tick_size2 * open_replace_tick_num:
                    open_market2_maker_price = round(market2_bid_price1 + tick_size2 * 1, round_num2)       # 同样，这个地方是不是应该再进行一下撤单判断？ 不然有可能直接成交了
                
        # 如果market1成交了
        if is_open_market1_done and is_open_market2_maker and (not is_open_market2_done):
            # 判读market2是否要taker对冲掉
            if (market2_bid_price1/open_market1_maker_price - 1) <= (sr_open_threshold - market2_stop_loss_sr_delta):
                open_market2_taker_price = market2_bid_price1
                is_open_market2_done = True
                is_open_market2_maker = False
                sr_open_real = open_market2_taker_price/open_market1_maker_price - 1
                market2_open_time = index
                open_trades.append([index, sr_open, sr_open_real, open_market1_maker_price, open_market2_taker_price,
                                    'market1_maker', 'market2_taker',
                                    market1_open_time, market2_open_time])
            ###    print(f'{index}, market2Taker对冲， market2对冲价格：{open_market2_taker_price}')
            # 判断market2是否成交
            elif market2_bid_price1 >= open_market2_maker_price:
                is_open_market2_done = True
                is_open_market2_maker = False
                sr_open_real = open_market2_maker_price/open_market1_maker_price - 1
                market2_open_time = index
                open_trades.append([index, sr_open, sr_open_real, open_market1_maker_price, open_market2_maker_price,
                                    'market1_maker', 'market2_maker',
                                    market1_open_time, market2_open_time])
            ###    print(f'{index}, market2Maker成交， market2成交价格：{open_market2_maker_price}')
            # 判断market2是否重挂
            elif (open_market2_maker_price - market2_bid_price1)> tick_size2 * open_replace_tick_num:    # 这个地方等于的逻辑是不是有点问题？ 如果价格不变的话，好像也在重新挂单。。。
                    open_market2_maker_price = round(market2_bid_price1 + tick_size2 * 1, round_num2)

        # 如果market2成交了
        if is_open_market2_done and is_open_market1_maker and (not is_open_market1_done):
            # 判读market1是否要taker对冲掉
            if (open_market2_maker_price/market1_ask_price1 - 1) <= (sr_open_threshold - market1_stop_loss_sr_delta):
                open_market1_taker_price = market1_ask_price1
                is_open_market1_done = True
                is_open_market1_maker = False
                sr_open_real = open_market2_maker_price/open_market1_taker_price - 1
                market1_open_time = index
                open_trades.append([index, sr_open, sr_open_real, open_market1_taker_price, open_market2_maker_price,
                                    'market1_taker', 'market2_maker',
                                    market1_open_time, market2_open_time])
            ###    print(f'{index}, market1Taker对冲， maker1对冲价格{open_market1_taker_price}')
            # 判断market1是否成交
            elif market1_ask_price1 <= open_market1_maker_price:
                is_open_market1_done = True
                is_open_market1_maker = False
                sr_open_real = open_market2_maker_price/open_market1_maker_price - 1
                market1_open_time = index
                open_trades.append([index, sr_open, sr_open_real, open_market1_maker_price, open_market2_maker_price,
                                    'market1_maker', 'market2_maker',
                                    market1_open_time, market2_open_time])
            ###    print(f'{index}, market1Maker现货成交， market1Maker成交价格{open_market1_maker_price}')
            # 判断market1是否重挂
            elif (market1_ask_price1 - open_market1_maker_price)> tick_size1 * open_replace_tick_num:    # 这个地方等于的逻辑是不是有点问题？ 如果价格不变的话，好像也在重新挂单。。。
                    open_market1_maker_price = round(market1_ask_price1 - tick_size1 * 1, round_num1)

        # 成交完成，重置
        if is_open_market1_done and is_open_market2_done:
            position_count += 1
            is_open_market1_done = False
            is_open_market2_done = False
            is_open_market1_maker = False
            is_open_market2_maker = False
            open_market1_maker_price = None
            open_market1_taker_price = None
            open_market2_maker_price = None
            open_market2_taker_price = None
            market1_open_time = None
            market2_open_time = None


        # 平仓
        # print(close_judgement,is_close_market1_maker,is_close_market2_maker)
        if close_judgement and (not is_close_market1_maker) and (not is_close_market2_maker) and (position_count > -pos_limit):
            close_market1_maker_price = round(market1_bid_price1 + tick_size1 * 1, round_num1)
            close_market2_maker_price = round(market2_ask_price1 - tick_size2 * 1, round_num2)
            is_close_market1_maker = True
            is_close_market2_maker = True
        ###    print(f'{index}, 平仓挂单，market1挂单价:{close_market1_maker_price}，market2挂单价：{close_market2_maker_price}')

        # 如果价差消失且没成交，撤单
        if close_cancel_judgement and is_close_market1_maker and is_close_market2_maker:
            is_close_market1_maker = False
            is_close_market2_maker = False
        ###    print(f'{index}, 平仓撤单，market1挂单价：{market1_bid_price1 + tick_size1 * 1},market2挂单价:{market2_ask_price1-tick_size2 * 1}')

        # 如果边都没有成交
        if (is_close_market1_maker and (not is_close_market1_done)) and (is_close_market2_maker and (not is_close_market2_done)):
            # 判断market1是否成交
            if (market1_bid_price1 >= close_market1_maker_price):
                is_close_market1_done = True
                is_close_market1_maker = False
                market1_close_time = index
        ###        print(f'{index}, market1Maker成交：{close_market1_maker_price}')
            # 判断market2是否成交
            elif (market2_ask_price1 <= close_market2_maker_price):
                is_close_market2_done = True
                is_close_market2_maker = False
                market2_close_time = index
        ###        print(f'{index}, market2Maker成交：{close_market2_maker_price}')
            # 判断是否重新挂单
            else:
                if (market2_ask_price1 - close_market2_maker_price)> tick_size2 * close_replace_tick_num:
                    close_market2_maker_price = round(market2_ask_price1 - tick_size2 * close_replace_tick_num, round_num2)
                if (close_market1_maker_price - market1_bid_price1)> tick_size1 * close_replace_tick_num:
                    close_market1_maker_price = round(market1_bid_price1 + tick_size1 * close_replace_tick_num, round_num1)

        # 如果market1成交了
        if is_close_market1_done and is_close_market2_maker and (not is_close_market2_done):
            # 判断market2是否要taker对冲掉
            if (market2_ask_price1/close_market1_maker_price - 1) >= (sr_close_threshold + market2_stop_loss_sr_delta):
                close_market2_taker_price = market2_ask_price1
                is_close_market2_done = True
                is_close_market2_maker = False
                sr_close_real = close_market2_taker_price/close_market1_maker_price - 1
                swap_close_time = index
                close_trades.append([index, sr_close, sr_close_real, close_market1_maker_price, close_market2_taker_price,
                                     'market1_maker', 'market2_taker',
                                     market1_close_time, market2_close_time])
        ###        print(f'{index}, marke2Taker成交：{close_market2_taker_price}')
            # 判断market2是否成交
            elif market2_ask_price1 <= close_market2_maker_price:
                is_close_market2_done = True
                is_close_market2_maker = False
                sr_close_real = close_market2_maker_price/close_market1_maker_price - 1
                market2_close_time = index
                close_trades.append([index, sr_close, sr_close_real, close_market1_maker_price, close_market2_maker_price,
                                     'market1_maker', 'market2_maker',
                                     market1_close_time, market2_close_time])
        ###        print(f'{index}, marke2Maker成交：{close_market2_maker_price}')
            # 判断market2是否重挂
            elif (market2_ask_price1 - close_market2_maker_price)> tick_size2 * close_replace_tick_num:
                    close_market2_maker_price = round(market2_ask_price1 - tick_size2 * 1, round_num2)
            

        # 如果market2成交了
        if is_close_market2_done and is_close_market1_maker and (not is_close_market1_done):
            # 判断market1是否要taker对冲掉
            if (close_market2_maker_price/market1_bid_price1 - 1) >= (sr_close_threshold + market1_stop_loss_sr_delta):
                close_market1_taker_price = market1_bid_price1
                is_close_market1_done = True
                is_close_market1_maker = False
                sr_close_real = close_market2_maker_price/close_market1_taker_price - 1
                market1_close_time = index
                close_trades.append([index, sr_close, sr_close_real, close_market1_taker_price, close_market2_maker_price,
                                     'market1_taker', 'market2_maker',
                                     market1_close_time, market2_close_time])
        ###        print(f'{index}, marke1Taker成交：{close_market1_taker_price}')
            # 判断market1是否成交
            elif market1_bid_price1 >= close_market1_maker_price:
                is_close_market1_done = True
                is_close_market1_maker = False
                sr_close_real = close_market2_maker_price/close_market1_maker_price - 1
                market1_close_time = index
                close_trades.append([index, sr_close, sr_close_real, close_market1_maker_price, close_market2_maker_price,
                                     'market1_maker', 'market2_maker',
                                     market1_close_time, market2_close_time])
        ###        print(f'{index}, marke1Maker成交：{close_market1_maker_price}')
            # 判断market1是否重挂
            elif (close_market1_maker_price - market1_bid_price1)> tick_size1 * close_replace_tick_num:
                    close_market1_maker_price = round(market1_bid_price1 + tick_size1 * 1, round_num1)

        # 成交完成，重置
        if is_close_market1_done and is_close_market2_done:
            position_count -= 1
            is_close_market1_done = False
            is_close_market2_done = False
            is_close_market1_maker = False
            is_close_market2_maker = False
            close_market1_maker_price = None
            close_market1_taker_price = None
            close_market2_maker_price = None
            close_market2_taker_price = None
            market1_close_time = None
            market2_close_time = None
            

        # except Exception as e:
                # traceback.print_exc()
    
    open_trades_df = pd.DataFrame(open_trades, columns=['index', 'sr_open', 'sr_open_real', 
                                                        'market1_traded_price', 'market2_traded_price',
                                                        'market1_traded_type', 'market2_traded_type',
                                                        'market1_open_time', 'market2_open_time'])
    open_trades_df.set_index('index', inplace=True)
    close_trades_df = pd.DataFrame(close_trades, columns=['index', 'sr_close', 'sr_close_real', 
                                                        'market1_traded_price', 'market2_traded_price',
                                                        'market1_traded_type', 'market2_traded_type',
                                                        'market1_close_time', 'market2_close_time'])
    close_trades_df.set_index('index', inplace=True)

    
    
    return open_trades_df, close_trades_df
    

def maker_maker_backtest_0605(cf_depth, mt_params, pos_limit = np.inf):


    open_replace_tick_num = mt_params["open_replace_tick_num"]
    close_replace_tick_num = mt_params["close_replace_tick_num"]


    market1_stop_loss_sr_delta = mt_params['market1_stop_loss_sr_delta']
    market2_stop_loss_sr_delta = mt_params['market2_stop_loss_sr_delta']

    
    target_open_sr = mt_params["target_open_sr"]
    target_close_sr = mt_params["target_close_sr"]

    tick_size1 = infer_tick_size_from_column(cf_depth['market1_bid_price0'])
    tick_ratio1 = round(tick_size1/cf_depth.iloc[-1]['market1_bid_price0'], 6)
    round_num1 = int(np.log10(1/tick_size1))
    tick_size2 = infer_tick_size_from_column(cf_depth['market2_bid_price0'])
    tick_ratio2 = round(tick_size2/cf_depth.iloc[-1]['market2_bid_price0'], 6)
    round_num2 = int(np.log10(1/tick_size2))
    print("交易所1精度信息", tick_size1, tick_ratio1, round_num1)
    print("交易所2精度信息", tick_size2, tick_ratio2, round_num2)
    
    is_open_market1_done = False
    is_open_market2_done = False
    is_open_market1_maker = False
    is_open_market2_maker = False
    open_market1_maker_price = None
    open_market1_taker_price = None
    open_market2_maker_price = None
    open_market2_taker_price = None
    market1_open_time = None
    market2_open_time = None

    is_close_market1_done = False
    is_close_market2_done = False
    is_close_market1_maker = False
    is_close_market2_maker = False
    close_market1_maker_price = None
    close_market1_taker_price = None
    close_market2_maker_price = None
    close_market2_taker_price = None
    market1_close_time = None
    market2_close_time = None

    sr_open_threshold = target_open_sr
    sr_close_threshold = target_close_sr


    sr_open = None
    sr_open_real = None
    sr_close = None
    sr_close_real = None


    # 回测输出变量
    open_trades = []
    close_trades = []
    position_count = 0

    m1_ask_arr = cf_depth['market1_ask_price0'].values
    m1_bid_arr = cf_depth['market1_bid_price0'].values
    m2_ask_arr = cf_depth['market2_ask_price0'].values
    m2_bid_arr = cf_depth['market2_bid_price0'].values
    idx_arr = cf_depth.index.to_numpy()

    for i in range(len(cf_depth)):
        idx = idx_arr[i]
        market1_ask_price1 = m1_ask_arr[i]
        market1_bid_price1 = m1_bid_arr[i]
        market2_ask_price1 = m2_ask_arr[i]
        market2_bid_price1 = m2_bid_arr[i]

        # 改成TM的价差会好一点
        sr_open = (market2_bid_price1)/(market1_ask_price1 - tick_size1 * 1) - 1
        sr_close = (market2_ask_price1)/(market1_bid_price1 + tick_size1 * 1) - 1


        # 把开平仓预设撤单价差作为撤单条件(实盘逻辑)
        sr_open_cancel_threshold = sr_open_threshold
        sr_close_cancel_threshold = sr_close_threshold
        
        # 下单判断
        open_judgement = (sr_open >= sr_open_threshold)
        close_judgement = (sr_close <= sr_close_threshold)
        # 撤单判断
        open_cancel_judgement = (sr_open < sr_open_cancel_threshold)
        close_cancel_judgement = (sr_close > sr_close_cancel_threshold)

        is_any_trade_in_progress = (
            is_open_market1_maker  or 
            is_close_market1_maker 
        )




        if open_judgement and (not is_any_trade_in_progress) and (not is_open_market1_maker) and position_count < pos_limit:
            open_market1_maker_price = round(market1_ask_price1 - tick_size1 * 1, round_num1)  #bid
            is_open_market1_maker = True
            # print(f'{idx},开仓挂单，当前价差: {sr_open}, {sr_close}, 当前threshold: {sr_open_threshold},{sr_close_threshold}, 开仓挂单，market1挂单价:{open_market1_maker_price}')
        # 如果价差消失且没成交，撤单
        if open_cancel_judgement and is_open_market1_maker:
            is_open_market1_maker = False
            # print(f'{idx},价差消失，开仓撤单，当前价差: {sr_open}, 当前threshold: {sr_open_threshold}')

        # 如果双边都没有成交
        if (is_open_market1_maker and (not is_open_market1_done)):
            # 判断market1是否成交
            if (market1_ask_price1 <= open_market1_maker_price):
                is_open_market1_done = True
                is_open_market1_maker = False
                market1_open_time = idx
                # print(f'{idx}, market1Maker成交：{open_market1_maker_price}')
            if (market1_ask_price1 - open_market1_maker_price)> tick_size1 * open_replace_tick_num:         # 这个地方等于的逻辑是不是有点问题？ 如果价格不变的话，好像也在重新挂单。。。
                # print(f'market1_ask_price1: {market1_ask_price1}, open_market1_maker_price: {open_market1_maker_price}, tick_size1: {tick_size1}, open_replace_tick_num: {open_replace_tick_num}')
                open_market1_maker_price = round(market1_ask_price1 - tick_size1 * 1, round_num1)           # 这个地方是不是应该再进行一下撤单判断？ 不然有可能直接成交了； 好像不需要，因为到这一步了就说明market1，2都没有成交，就直接continue了。
                # print(f'{idx}, market1Maker重新挂单，当前行为: buy, 当前ask_price: {market1_ask_price1}, 当前价差: {sr_open},当前threshold: {sr_open_threshold}, market1挂单价:{open_market1_maker_price}')
        # 如果market1成交了
        if is_open_market1_done and (not is_open_market2_done):
            if open_market1_maker_price is None:
                print(f'open_market1_maker_price is None at {idx}')
            if sr_open_threshold is None:
                print(f'sr_open_threshold is None at {idx}')
            # 判读market2是否要taker对冲掉
            if (market2_bid_price1/open_market1_maker_price - 1) <= (sr_open_threshold - market2_stop_loss_sr_delta):
                open_market2_taker_price = market2_bid_price1
                is_open_market2_done = True
                sr_open_real = open_market2_taker_price/open_market1_maker_price - 1
                market2_open_time = idx
                open_trades.append([idx, sr_open, sr_open_real, open_market1_maker_price, open_market2_taker_price,
                                    'market1_maker', 'market2_taker',
                                    market1_open_time, market2_open_time])
                # print(f'{idx}, market2Taker对冲， market2对冲价格：{open_market2_taker_price}')
            # 判断market2是否成交
            # elif market2_bid_price1 >= open_market2_maker_price:
            #     is_open_market2_done = True
            #     sr_open_real = open_market2_maker_price/open_market1_maker_price - 1
            #     market2_open_time = idx
            #     open_trades.append([idx, sr_open, sr_open_real, open_market1_maker_price, open_market2_maker_price,
            #                         'market1_maker', 'market2_maker',
            #                         market1_open_time, market2_open_time])
            #     print('sb')

        # 成交完成，重置
        if is_open_market1_done and is_open_market2_done:
            position_count += 1
            is_open_market1_done = False
            is_open_market2_done = False
            is_open_market1_maker = False
            open_market1_maker_price = None
            open_market2_maker_price = None
            open_market2_taker_price = None
            market1_open_time = None
            market2_open_time = None


        # 平仓
        # print(close_judgement,is_close_market1_maker,is_close_market2_maker)
        if close_judgement and (not is_any_trade_in_progress) and (not is_close_market1_maker) and (position_count > -pos_limit):
            close_market1_maker_price = round(market1_bid_price1 + tick_size1 * 1, round_num1)
            is_close_market1_maker = True
            # print(f'{idx}, 平仓挂单，当前价差: {sr_open}, {sr_close}, 当前threshold: {sr_open_threshold},{sr_close_threshold}, market1挂单价:{close_market1_maker_price}')

        # 如果价差消失且没成交，撤单
        if close_cancel_judgement and is_close_market1_maker:
            is_close_market1_maker = False
            # print(f'{idx},价差消失，平仓撤单，当前价差: {sr_close}, 当前threshold: {sr_close_threshold}')

        # 如果边都没有成交
        if (is_close_market1_maker and (not is_close_market1_done)):
            # 判断market1是否成交
            if (market1_bid_price1 >= close_market1_maker_price):
                is_close_market1_done = True
                is_close_market1_maker = False
                market1_close_time = idx
                # print(f'{idx}, market1Maker成交：{close_market1_maker_price}')
            # 判断是否重新挂单
            if (close_market1_maker_price - market1_bid_price1)> tick_size1 * close_replace_tick_num:
                # print(f'market1_bid_price1: {market1_bid_price1}, close_market1_maker_price: {close_market1_maker_price}, tick_size1: {tick_size1}, close_replace_tick_num: {close_replace_tick_num}')
                close_market1_maker_price = round(market1_bid_price1 + tick_size1 * close_replace_tick_num, round_num1)
                # print(f'{idx}, market1Maker重新挂单，当前行为: sell, 当前bid_price: {market1_bid_price1}, 当前价差: {sr_close},当前threshold: {sr_close_threshold}, market1新的挂单价:{close_market1_maker_price}')

        # 如果market1成交了
        if is_close_market1_done and (not is_close_market2_done):
            # 判断market2是否要taker对冲掉
            if (market2_ask_price1/close_market1_maker_price - 1) >= (sr_close_threshold + market2_stop_loss_sr_delta):
                close_market2_taker_price = market2_ask_price1
                is_close_market2_done = True
                is_close_market2_maker = False
                sr_close_real = close_market2_taker_price/close_market1_maker_price - 1
                close_trades.append([idx, sr_close, sr_close_real, close_market1_maker_price, close_market2_taker_price,
                                     'market1_maker', 'market2_taker',
                                     market1_close_time, market2_close_time])
            #     print(f'{idx}, marke2Taker成交：{close_market2_taker_price}')
            # # 判断market2是否成交
        #     elif market2_ask_price1 <= close_market2_maker_price:
        #         is_close_market2_done = True
        #         is_close_market2_maker = False
        #         sr_close_real = close_market2_maker_price/close_market1_maker_price - 1
        #         market2_close_time = idx
        #         close_trades.append([idx, sr_close, sr_close_real, close_market1_maker_price, close_market2_maker_price,
        #                              'market1_maker', 'market2_maker',
        #                              market1_close_time, market2_close_time])
        # ###        print(f'{index}, marke2Maker成交：{close_market2_maker_price}')
        #     # 判断market2是否重挂
        #     elif (market2_ask_price1 - close_market2_maker_price)> tick_size2 * close_replace_tick_num:
        #             close_market2_maker_price = round(market2_ask_price1 - tick_size2 * 1, round_num2)

        # 成交完成，重置
        if is_close_market1_done and is_close_market2_done:
            position_count -= 1
            is_close_market1_done = False
            is_close_market2_done = False
            is_close_market1_maker = False
            is_close_market2_maker = False
            close_market1_maker_price = None
            close_market2_maker_price = None
            close_market2_taker_price = None
            market1_close_time = None
            market2_close_time = None
            

        # except Exception as e:
                # traceback.print_exc()
    
    open_trades_df = pd.DataFrame(open_trades, columns=['index', 'sr_open', 'sr_open_real', 
                                                        'market1_traded_price', 'market2_traded_price',
                                                        'market1_traded_type', 'market2_traded_type',
                                                        'market1_open_time', 'market2_open_time'])
    open_trades_df.set_index('index', inplace=True)
    close_trades_df = pd.DataFrame(close_trades, columns=['index', 'sr_close', 'sr_close_real', 
                                                        'market1_traded_price', 'market2_traded_price',
                                                        'market1_traded_type', 'market2_traded_type',
                                                        'market1_close_time', 'market2_close_time'])
    close_trades_df.set_index('index', inplace=True)

    
    
    return open_trades_df, close_trades_df
   


### 2025-06-05 改成固定在一边maker 另一边taker； 2025-06-09优化了一下速度
def maker_maker_backtest_rolling_0605(cf_depth, mt_params, param_df, pos_limit = np.inf):


    open_replace_tick_num = mt_params["open_replace_tick_num"]
    close_replace_tick_num = mt_params["close_replace_tick_num"]


    market1_stop_loss_sr_delta = mt_params['market1_stop_loss_sr_delta']
    market2_stop_loss_sr_delta = mt_params['market2_stop_loss_sr_delta']

    

    tick_size1 = infer_tick_size_from_column(cf_depth['market1_bid_price0'])
    tick_ratio1 = round(tick_size1/cf_depth.iloc[-1]['market1_bid_price0'], 6)
    round_num1 = int(np.log10(1/tick_size1))
    tick_size2 = infer_tick_size_from_column(cf_depth['market2_bid_price0'])
    tick_ratio2 = round(tick_size2/cf_depth.iloc[-1]['market2_bid_price0'], 6)
    round_num2 = int(np.log10(1/tick_size2))
    print("交易所1精度信息", tick_size1, tick_ratio1, round_num1)
    print("交易所2精度信息", tick_size2, tick_ratio2, round_num2)
    
    is_open_market1_done = False
    is_open_market2_done = False
    is_open_market1_maker = False
    is_open_market2_maker = False
    open_market1_maker_price = None
    open_market1_taker_price = None
    open_market2_maker_price = None
    open_market2_taker_price = None
    market1_open_time = None
    market2_open_time = None

    is_close_market1_done = False
    is_close_market2_done = False
    is_close_market1_maker = False
    is_close_market2_maker = False
    close_market1_maker_price = None
    close_market1_taker_price = None
    close_market2_maker_price = None
    close_market2_taker_price = None
    market1_close_time = None
    market2_close_time = None
    
    sr_open = None
    sr_open_real = None
    sr_close = None
    sr_close_real = None


    # 回测输出变量
    open_trades = []
    close_trades = []
    position_count = 0

    m1_ask_arr = cf_depth['market1_ask_price0'].values
    m1_bid_arr = cf_depth['market1_bid_price0'].values
    m2_ask_arr = cf_depth['market2_ask_price0'].values
    m2_bid_arr = cf_depth['market2_bid_price0'].values
    idx_arr = cf_depth.index.to_numpy()

    for i in range(len(cf_depth)):
        idx = idx_arr[i]
        market1_ask_price1 = m1_ask_arr[i]
        market1_bid_price1 = m1_bid_arr[i]
        market2_ask_price1 = m2_ask_arr[i]
        market2_bid_price1 = m2_bid_arr[i]
    # # for idx, ds in tqdm(cf_depth.iterrows(), total=len(cf_depth), desc='Processing'):
    # #     #新加仓位限制

    #     market1_ask_price1 = ds['market1_ask_price0']
    #     market1_bid_price1 = ds['market1_bid_price0']
    #     market2_ask_price1 = ds['market2_ask_price0']
    #     market2_bid_price1 = ds['market2_bid_price0']
        # print(f'{idx}, market1 ask:{market1_ask_price1} , market1 bid: {market1_bid_price1}')

        current_time = idx
        # 新加动态threshold的逻辑
        update_time = param_df.index[param_df.index <= current_time].max()
        sr_open_threshold = param_df.loc[update_time, 'adjusted_open']/10000
        sr_close_threshold = param_df.loc[update_time, 'adjusted_close']/10000

        # 改成TM的价差会好一点
        sr_open = (market2_bid_price1)/(market1_ask_price1 - tick_size1 * 1) - 1
        sr_close = (market2_ask_price1)/(market1_bid_price1 + tick_size1 * 1) - 1


        # 把开平仓预设撤单价差作为撤单条件(实盘逻辑)
        sr_open_cancel_threshold = sr_open_threshold
        sr_close_cancel_threshold = sr_close_threshold
        
        # 下单判断
        open_judgement = (sr_open >= sr_open_threshold)
        close_judgement = (sr_close <= sr_close_threshold)
        # 撤单判断
        open_cancel_judgement = (sr_open < sr_open_cancel_threshold)
        close_cancel_judgement = (sr_close > sr_close_cancel_threshold)

        is_any_trade_in_progress = (
            is_open_market1_maker  or 
            is_close_market1_maker 
        )




        if open_judgement and (not is_any_trade_in_progress) and (not is_open_market1_maker) and position_count < pos_limit:
            open_market1_maker_price = round(market1_ask_price1 - tick_size1 * 1, round_num1)  #bid
            is_open_market1_maker = True
            # print(f'{idx},开仓挂单，当前价差: {sr_open}, {sr_close}, 当前threshold: {sr_open_threshold},{sr_close_threshold}, 开仓挂单，market1挂单价:{open_market1_maker_price}')
        # 如果价差消失且没成交，撤单
        if open_cancel_judgement and is_open_market1_maker:
            is_open_market1_maker = False
            # print(f'{idx},价差消失，开仓撤单，当前价差: {sr_open}, 当前threshold: {sr_open_threshold}')

        # 如果双边都没有成交
        if (is_open_market1_maker and (not is_open_market1_done)):
            # 判断market1是否成交
            if (market1_ask_price1 <= open_market1_maker_price):
                is_open_market1_done = True
                is_open_market1_maker = False
                market1_open_time = idx
                # print(f'{idx}, market1Maker成交：{open_market1_maker_price}')
            if (market1_ask_price1 - open_market1_maker_price)> tick_size1 * open_replace_tick_num:         # 这个地方等于的逻辑是不是有点问题？ 如果价格不变的话，好像也在重新挂单。。。
                # print(f'market1_ask_price1: {market1_ask_price1}, open_market1_maker_price: {open_market1_maker_price}, tick_size1: {tick_size1}, open_replace_tick_num: {open_replace_tick_num}')
                open_market1_maker_price = round(market1_ask_price1 - tick_size1 * 1, round_num1)           # 这个地方是不是应该再进行一下撤单判断？ 不然有可能直接成交了； 好像不需要，因为到这一步了就说明market1，2都没有成交，就直接continue了。
                # print(f'{idx}, market1Maker重新挂单，当前行为: buy, 当前ask_price: {market1_ask_price1}, 当前价差: {sr_open},当前threshold: {sr_open_threshold}, market1挂单价:{open_market1_maker_price}')
        # 如果market1成交了
        if is_open_market1_done and (not is_open_market2_done):
            if open_market1_maker_price is None:
                print(f'open_market1_maker_price is None at {idx}')
            if sr_open_threshold is None:
                print(f'sr_open_threshold is None at {idx}')
            # 判读market2是否要taker对冲掉
            if (market2_bid_price1/open_market1_maker_price - 1) <= (sr_open_threshold - market2_stop_loss_sr_delta):
                open_market2_taker_price = market2_bid_price1
                is_open_market2_done = True
                sr_open_real = open_market2_taker_price/open_market1_maker_price - 1
                market2_open_time = idx
                open_trades.append([idx, sr_open, sr_open_real, open_market1_maker_price, open_market2_taker_price,
                                    'market1_maker', 'market2_taker',
                                    market1_open_time, market2_open_time])
                # print(f'{idx}, market2Taker对冲， market2对冲价格：{open_market2_taker_price}')
            # 判断market2是否成交
            # elif market2_bid_price1 >= open_market2_maker_price:
            #     is_open_market2_done = True
            #     sr_open_real = open_market2_maker_price/open_market1_maker_price - 1
            #     market2_open_time = idx
            #     open_trades.append([idx, sr_open, sr_open_real, open_market1_maker_price, open_market2_maker_price,
            #                         'market1_maker', 'market2_maker',
            #                         market1_open_time, market2_open_time])
            #     print('sb')

        # 成交完成，重置
        if is_open_market1_done and is_open_market2_done:
            position_count += 1
            is_open_market1_done = False
            is_open_market2_done = False
            is_open_market1_maker = False
            open_market1_maker_price = None
            open_market2_maker_price = None
            open_market2_taker_price = None
            market1_open_time = None
            market2_open_time = None


        # 平仓
        # print(close_judgement,is_close_market1_maker,is_close_market2_maker)
        if close_judgement and (not is_any_trade_in_progress) and (not is_close_market1_maker) and (position_count > -pos_limit):
            close_market1_maker_price = round(market1_bid_price1 + tick_size1 * 1, round_num1)
            is_close_market1_maker = True
            # print(f'{idx}, 平仓挂单，当前价差: {sr_open}, {sr_close}, 当前threshold: {sr_open_threshold},{sr_close_threshold}, market1挂单价:{close_market1_maker_price}')

        # 如果价差消失且没成交，撤单
        if close_cancel_judgement and is_close_market1_maker:
            is_close_market1_maker = False
            # print(f'{idx},价差消失，平仓撤单，当前价差: {sr_close}, 当前threshold: {sr_close_threshold}')

        # 如果边都没有成交
        if (is_close_market1_maker and (not is_close_market1_done)):
            # 判断market1是否成交
            if (market1_bid_price1 >= close_market1_maker_price):
                is_close_market1_done = True
                is_close_market1_maker = False
                market1_close_time = idx
                # print(f'{idx}, market1Maker成交：{close_market1_maker_price}')
            # 判断是否重新挂单
            if (close_market1_maker_price - market1_bid_price1)> tick_size1 * close_replace_tick_num:
                # print(f'market1_bid_price1: {market1_bid_price1}, close_market1_maker_price: {close_market1_maker_price}, tick_size1: {tick_size1}, close_replace_tick_num: {close_replace_tick_num}')
                close_market1_maker_price = round(market1_bid_price1 + tick_size1 * close_replace_tick_num, round_num1)
                # print(f'{idx}, market1Maker重新挂单，当前行为: sell, 当前bid_price: {market1_bid_price1}, 当前价差: {sr_close},当前threshold: {sr_close_threshold}, market1新的挂单价:{close_market1_maker_price}')

        # 如果market1成交了
        if is_close_market1_done and (not is_close_market2_done):
            # 判断market2是否要taker对冲掉
            if (market2_ask_price1/close_market1_maker_price - 1) >= (sr_close_threshold + market2_stop_loss_sr_delta):
                close_market2_taker_price = market2_ask_price1
                is_close_market2_done = True
                is_close_market2_maker = False
                sr_close_real = close_market2_taker_price/close_market1_maker_price - 1
                close_trades.append([idx, sr_close, sr_close_real, close_market1_maker_price, close_market2_taker_price,
                                     'market1_maker', 'market2_taker',
                                     market1_close_time, market2_close_time])
            #     print(f'{idx}, marke2Taker成交：{close_market2_taker_price}')
            # # 判断market2是否成交
        #     elif market2_ask_price1 <= close_market2_maker_price:
        #         is_close_market2_done = True
        #         is_close_market2_maker = False
        #         sr_close_real = close_market2_maker_price/close_market1_maker_price - 1
        #         market2_close_time = idx
        #         close_trades.append([idx, sr_close, sr_close_real, close_market1_maker_price, close_market2_maker_price,
        #                              'market1_maker', 'market2_maker',
        #                              market1_close_time, market2_close_time])
        # ###        print(f'{index}, marke2Maker成交：{close_market2_maker_price}')
        #     # 判断market2是否重挂
        #     elif (market2_ask_price1 - close_market2_maker_price)> tick_size2 * close_replace_tick_num:
        #             close_market2_maker_price = round(market2_ask_price1 - tick_size2 * 1, round_num2)
            
        # 成交完成，重置
        if is_close_market1_done and is_close_market2_done:
            position_count -= 1
            is_close_market1_done = False
            is_close_market2_done = False
            is_close_market1_maker = False
            is_close_market2_maker = False
            close_market1_maker_price = None
            close_market2_maker_price = None
            close_market2_taker_price = None
            market1_close_time = None
            market2_close_time = None
            

        # except Exception as e:
                # traceback.print_exc()
    
    open_trades_df = pd.DataFrame(open_trades, columns=['index', 'sr_open', 'sr_open_real', 
                                                        'market1_traded_price', 'market2_traded_price',
                                                        'market1_traded_type', 'market2_traded_type',
                                                        'market1_open_time', 'market2_open_time'])
    open_trades_df.set_index('index', inplace=True)
    close_trades_df = pd.DataFrame(close_trades, columns=['index', 'sr_close', 'sr_close_real', 
                                                        'market1_traded_price', 'market2_traded_price',
                                                        'market1_traded_type', 'market2_traded_type',
                                                        'market1_close_time', 'market2_close_time'])
    close_trades_df.set_index('index', inplace=True)

    
    
    return open_trades_df, close_trades_df
   




# 如果要tm，就改变数据拼接顺序
def maker_maker_backtest(cf_depth, mt_params, pos_limit = np.inf):
    # 这一部分应该写在配置文件里，因为不同交易所精度不同，挂单参数可能不同

    # 新加的


    open_tick_num = mt_params["open_tick_num"]
    close_tick_num = mt_params["close_tick_num"]
    open_replace_tick_num = mt_params["open_replace_tick_num"]
    close_replace_tick_num = mt_params["close_replace_tick_num"]
    taker_traded_delay_seconds = mt_params["taker_traded_delay_seconds"]
    maker_ordered_delay_seconds = mt_params["maker_ordered_delay_seconds"]
    target_open_sr = mt_params["target_open_sr"]
    target_close_sr = mt_params["target_close_sr"]
    exchange1_type = mt_params['exchange1_type']
    market1_type = mt_params['market1_type']
    exchange2_type = mt_params['exchange2_type']
    market2_type = mt_params['market2_type']
    # slip_num = mt_params['slip_num']
    market1_stop_loss_sr_delta = mt_params['market1_stop_loss_sr_delta']
    market2_stop_loss_sr_delta = mt_params['market2_stop_loss_sr_delta']

    sr_open_threshold = target_open_sr
    sr_close_threshold = target_close_sr
    
    # 回测内部变量
    open_stable_st = cf_depth.index[0]
    open_stable_et = cf_depth.index[0]
    open_stable_dur = 0
    close_stable_st = cf_depth.index[0]
    close_stable_et = cf_depth.index[0]
    close_stable_dur = 0

    # 两个交易所精度可能不一样，双侧maker需要计算两个maker

    tick_size1 = infer_tick_size_from_column(cf_depth['market1_bid_price0'])
    # tick_size1 = 1/10**len(str(cf_depth.iloc[-1]['market1_bid_price1']).split('.')[1])
    tick_ratio1 = round(tick_size1/cf_depth.iloc[-1]['market1_bid_price0'], 6)
    round_num1 = int(np.log10(1/tick_size1))
    tick_size2 = infer_tick_size_from_column(cf_depth['market2_bid_price0'])
    # tick_size2 = 1/10**len(str(cf_depth.iloc[-1]['market2_bid_price1']).split('.')[1])
    tick_ratio2 = round(tick_size2/cf_depth.iloc[-1]['market2_bid_price0'], 6)
    round_num2 = int(np.log10(1/tick_size2))
    print("交易所1精度信息", tick_size1, tick_ratio1, round_num1)
    print("交易所2精度信息", tick_size2, tick_ratio2, round_num2)
    
    fee_save = 0
    if exchange1_type == 'binance':
        if market1_type == 'Spot':
            fee_save = 0.00025
        elif market1_type == 'Uperp':
            fee_save = 0.00025
    elif exchange1_type == 'okx':
        if market1_type == 'Spot':
            fee_save = 0.00025
        elif market1_type == 'Uperp':
            fee_save = 0.0002
    
    is_open_market1_done = False
    is_open_market2_done = False
    is_open_market1_maker = False
    is_open_market2_maker = False
    open_market1_maker_price = None
    open_market1_taker_price = None
    open_market2_maker_price = None
    open_market2_taker_price = None
    market1_open_time = None
    market2_open_time = None

    is_close_market1_done = False
    is_close_market2_done = False
    is_close_market1_maker = False
    is_close_market2_maker = False
    close_market1_maker_price = None
    close_market1_taker_price = None
    close_market2_maker_price = None
    close_market2_taker_price = None
    market1_close_time = None
    market2_close_time = None
    
    sr_open = None
    sr_open_order = None
    sr_open_real = None
    sr_close = None
    sr_close_order = None
    sr_close_real = None

    open_depth_volume = None
    close_depth_volume = None

    # 回测输出变量
    open_trades = []
    close_trades = []
    position_count = 0

    for index, ds in cf_depth.iterrows():
        #新加仓位限制
        
        #
        # try:
        sr_open = ds['sr_open']
        sr_close = ds['sr_close']
        market1_ask_price1 = ds['market1_ask_price0']
        market1_bid_price1 = ds['market1_bid_price0']
        market1_mid_price = market1_ask_price1/2 + market1_bid_price1/2
        market2_ask_price1 = ds['market2_ask_price0']
        market2_bid_price1 = ds['market2_bid_price0']
        market2_mid_price = market2_ask_price1/2 + market2_bid_price1/2
        market1_ask_size1 = ds['market1_ask_size0']
        market1_bid_size1 = ds['market1_bid_size0']
        market2_ask_size1 = ds['market2_ask_size0']
        market2_bid_size1 = ds['market2_bid_size0']
        
        # received_time_diff_1jump_later = ds['received_time_diff_1jump_later']

        
        sr_open = (market2_bid_price1+tick_size2 * 1)/(market1_ask_price1 - tick_size1 * 1) - 1
        sr_close = (market2_ask_price1-tick_size2 * 1)/(market1_bid_price1 + tick_size1 * 1) - 1

        # 把开平仓实时价差作为撤单条件(实盘测试逻辑)
        # sr_open_cancel_threshold = sr_open
        # sr_close_cancel_threshold = sr_close
        # 把开平仓预设撤单价差作为撤单条件(实盘逻辑)
        sr_open_cancel_threshold = target_open_sr
        sr_close_cancel_threshold = target_close_sr
        
        # 下单判断
        open_judgement = (sr_open >= sr_open_threshold)
        close_judgement = (sr_close <= sr_close_threshold)
        # 撤单判断
        open_cancel_judgement = (sr_open < sr_open_cancel_threshold)
        close_cancel_judgement = (sr_close > sr_close_cancel_threshold)

        # print(sr_close, sr_close_threshold, sr_close_cancel_threshold)

        # if open_judgement and (not is_open_market1_maker) and (not is_open_market2_maker):
        #     if abs(position_count) >= position_limit:
        #         continue  # 达到持仓上限，不开仓
        # 开仓
        if open_judgement and (not is_open_market1_maker) and (not is_open_market2_maker) and position_count < pos_limit:
            open_market1_maker_price = round(market1_ask_price1 - tick_size1 * 1, round_num1)  #bid
            open_market2_maker_price = round(market2_bid_price1 + tick_size2 * 1, round_num2)  #ask
            is_open_market1_maker = True
            is_open_market2_maker = True
            ### print(f'{index}, 开仓挂单，market1挂单价:{open_market1_maker_price}，market2挂单价：{open_market2_maker_price}')
        # 如果价差消失且没成交，撤单
        if open_cancel_judgement and is_open_market1_maker and is_open_market2_maker:
            is_open_market1_maker = False
            is_open_market2_maker = False

        # 如果双边都没有成交
        if (is_open_market1_maker and (not is_open_market1_done)) and (is_open_market2_maker and (not is_open_market2_done)):
            # 判断market1是否成交
            if (market1_ask_price1 <= open_market1_maker_price):
                is_open_market1_done = True
                is_open_market1_maker = False
                market1_open_time = index
              ###  print(f'{index}, market1Maker成交：{open_market1_maker_price}')
            # 判断market2是否成交
            elif (market2_bid_price1 >= open_market2_maker_price):
                is_open_market2_done = True
                is_open_market2_maker = False
                market2_open_time = index
            ###    print(f'{index}, market2Maker成交：{open_market1_maker_price}')
            # 判断是否重新挂单
            else:
                if (market1_ask_price1 - open_market1_maker_price)>= tick_size1 * open_replace_tick_num:         # 这个地方等于的逻辑是不是有点问题？ 如果价格不变的话，好像也在重新挂单。。。
                    open_market1_maker_price = round(market1_ask_price1 - tick_size1 * 1, round_num1)           # 这个地方是不是应该再进行一下撤单判断？ 不然有可能直接成交了； 好像不需要，因为到这一步了就说明market1，2都没有成交，就直接continue了。
                if (open_market2_maker_price - market2_bid_price1)>= tick_size2 * open_replace_tick_num:
                    open_market2_maker_price = round(market2_bid_price1 + tick_size2 * 1, round_num2)       # 同样，这个地方是不是应该再进行一下撤单判断？ 不然有可能直接成交了
                
        # 如果market1成交了
        if is_open_market1_done and is_open_market2_maker and (not is_open_market2_done):
            # 判读market2是否要taker对冲掉
            if (market2_bid_price1/open_market1_maker_price - 1) <= (sr_open_threshold - market2_stop_loss_sr_delta):
                open_market2_taker_price = market2_bid_price1
                is_open_market2_done = True
                is_open_market2_maker = False
                sr_open_real = open_market2_taker_price/open_market1_maker_price - 1
                market2_open_time = index
                open_trades.append([index, sr_open, sr_open_real, open_market1_maker_price, open_market2_taker_price,
                                    'market1_maker', 'market2_taker',
                                    market1_open_time, market2_open_time])
            ###    print(f'{index}, market2Taker对冲， market2对冲价格：{open_market2_taker_price}')
            # 判断market2是否成交
            elif market2_bid_price1 >= open_market2_maker_price:
                is_open_market2_done = True
                is_open_market2_maker = False
                sr_open_real = open_market2_maker_price/open_market1_maker_price - 1
                market2_open_time = index
                open_trades.append([index, sr_open, sr_open_real, open_market1_maker_price, open_market2_maker_price,
                                    'market1_maker', 'market2_maker',
                                    market1_open_time, market2_open_time])
            ###    print(f'{index}, market2Maker成交， market2成交价格：{open_market2_maker_price}')
            # 判断market2是否重挂
            elif (open_market2_maker_price - market2_bid_price1)>= tick_size2 * open_replace_tick_num:    # 这个地方等于的逻辑是不是有点问题？ 如果价格不变的话，好像也在重新挂单。。。
                    open_market2_maker_price = round(market2_bid_price1 + tick_size2 * 1, round_num2)

        # 如果market2成交了
        if is_open_market2_done and is_open_market1_maker and (not is_open_market1_done):
            # 判读market1是否要taker对冲掉
            if (open_market2_maker_price/market1_ask_price1 - 1) <= (sr_open_threshold - market1_stop_loss_sr_delta):
                open_market1_taker_price = market1_ask_price1
                is_open_market1_done = True
                is_open_market1_maker = False
                sr_open_real = open_market2_maker_price/open_market1_taker_price - 1
                market1_open_time = index
                open_trades.append([index, sr_open, sr_open_real, open_market1_taker_price, open_market2_maker_price,
                                    'market1_taker', 'market2_maker',
                                    market1_open_time, market2_open_time])
            ###    print(f'{index}, market1Taker对冲， maker1对冲价格{open_market1_taker_price}')
            # 判断market1是否成交
            elif market1_ask_price1 <= open_market1_maker_price:
                is_open_market1_done = True
                is_open_market1_maker = False
                sr_open_real = open_market2_maker_price/open_market1_maker_price - 1
                market1_open_time = index
                open_trades.append([index, sr_open, sr_open_real, open_market1_maker_price, open_market2_maker_price,
                                    'market1_maker', 'market2_maker',
                                    market1_open_time, market2_open_time])
            ###    print(f'{index}, market1Maker现货成交， market1Maker成交价格{open_market1_maker_price}')
            # 判断market1是否重挂
            elif (market1_ask_price1 - open_market1_maker_price)>= tick_size1 * open_replace_tick_num:    # 这个地方等于的逻辑是不是有点问题？ 如果价格不变的话，好像也在重新挂单。。。
                    open_market1_maker_price = round(market1_ask_price1 - tick_size1 * 1, round_num1)

        # 成交完成，重置
        if is_open_market1_done and is_open_market2_done:
            position_count += 1
            is_open_market1_done = False
            is_open_market2_done = False
            is_open_market1_maker = False
            is_open_market2_maker = False
            open_market1_maker_price = None
            open_market1_taker_price = None
            open_market2_maker_price = None
            open_market2_taker_price = None
            market1_open_time = None
            market2_open_time = None


        # 平仓
        # print(close_judgement,is_close_market1_maker,is_close_market2_maker)
        if close_judgement and (not is_close_market1_maker) and (not is_close_market2_maker) and (position_count > -pos_limit):
            close_market1_maker_price = round(market1_bid_price1 + tick_size1 * 1, round_num1)
            close_market2_maker_price = round(market2_ask_price1 - tick_size2 * 1, round_num2)
            is_close_market1_maker = True
            is_close_market2_maker = True
        ###    print(f'{index}, 平仓挂单，market1挂单价:{close_market1_maker_price}，market2挂单价：{close_market2_maker_price}')

        # 如果价差消失且没成交，撤单
        if close_cancel_judgement and is_close_market1_maker and is_close_market2_maker:
            is_close_market1_maker = False
            is_close_market2_maker = False
        ###    print(f'{index}, 平仓撤单，market1挂单价：{market1_bid_price1 + tick_size1 * 1},market2挂单价:{market2_ask_price1-tick_size2 * 1}')

        # 如果边都没有成交
        if (is_close_market1_maker and (not is_close_market1_done)) and (is_close_market2_maker and (not is_close_market2_done)):
            # 判断market1是否成交
            if (market1_bid_price1 >= close_market1_maker_price):
                is_close_market1_done = True
                is_close_market1_maker = False
                market1_close_time = index
        ###        print(f'{index}, market1Maker成交：{close_market1_maker_price}')
            # 判断market2是否成交
            elif (market2_ask_price1 <= close_market2_maker_price):
                is_close_market2_done = True
                is_close_market2_maker = False
                market2_close_time = index
        ###        print(f'{index}, market2Maker成交：{close_market2_maker_price}')
            # 判断是否重新挂单
            else:
                if (market2_ask_price1 - close_market2_maker_price)>= tick_size2 * close_replace_tick_num:
                    close_market2_maker_price = round(market2_ask_price1 - tick_size2 * close_replace_tick_num, round_num2)
                if (close_market1_maker_price - market1_bid_price1)>= tick_size1 * close_replace_tick_num:
                    close_market1_maker_price = round(market1_bid_price1 + tick_size1 * close_replace_tick_num, round_num1)

        # 如果market1成交了
        if is_close_market1_done and is_close_market2_maker and (not is_close_market2_done):
            # 判断market2是否要taker对冲掉
            if (market2_ask_price1/close_market1_maker_price - 1) >= (sr_close_threshold + market2_stop_loss_sr_delta):
                close_market2_taker_price = market2_ask_price1
                is_close_market2_done = True
                is_close_market2_maker = False
                sr_close_real = close_market2_taker_price/close_market1_maker_price - 1
                swap_close_time = index
                close_trades.append([index, sr_close, sr_close_real, close_market1_maker_price, close_market2_taker_price,
                                     'market1_maker', 'market2_taker',
                                     market1_close_time, market2_close_time])
        ###        print(f'{index}, marke2Taker成交：{close_market2_taker_price}')
            # 判断market2是否成交
            elif market2_ask_price1 <= close_market2_maker_price:
                is_close_market2_done = True
                is_close_market2_maker = False
                sr_close_real = close_market2_maker_price/close_market1_maker_price - 1
                market2_close_time = index
                close_trades.append([index, sr_close, sr_close_real, close_market1_maker_price, close_market2_maker_price,
                                     'market1_maker', 'market2_maker',
                                     market1_close_time, market2_close_time])
        ###        print(f'{index}, marke2Maker成交：{close_market2_maker_price}')
            # 判断market2是否重挂
            elif (market2_ask_price1 - close_market2_maker_price)>= tick_size2 * close_replace_tick_num:
                    close_market2_maker_price = round(market2_ask_price1 - tick_size2 * 1, round_num2)
            

        # 如果market2成交了
        if is_close_market2_done and is_close_market1_maker and (not is_close_market1_done):
            # 判断market1是否要taker对冲掉
            if (close_market2_maker_price/market1_bid_price1 - 1) >= (sr_close_threshold + market1_stop_loss_sr_delta):
                close_market1_taker_price = market1_bid_price1
                is_close_market1_done = True
                is_close_market1_maker = False
                sr_close_real = close_market2_maker_price/close_market1_taker_price - 1
                market1_close_time = index
                close_trades.append([index, sr_close, sr_close_real, close_market1_taker_price, close_market2_maker_price,
                                     'market1_taker', 'market2_maker',
                                     market1_close_time, market2_close_time])
        ###        print(f'{index}, marke1Taker成交：{close_market1_taker_price}')
            # 判断market1是否成交
            elif market1_bid_price1 >= close_market1_maker_price:
                is_close_market1_done = True
                is_close_market1_maker = False
                sr_close_real = close_market2_maker_price/close_market1_maker_price - 1
                market1_close_time = index
                close_trades.append([index, sr_close, sr_close_real, close_market1_maker_price, close_market2_maker_price,
                                     'market1_maker', 'market2_maker',
                                     market1_close_time, market2_close_time])
        ###        print(f'{index}, marke1Maker成交：{close_market1_maker_price}')
            # 判断market1是否重挂
            elif (close_market1_maker_price - market1_bid_price1)>= tick_size1 * close_replace_tick_num:
                    close_market1_maker_price = round(market1_bid_price1 + tick_size1 * 1, round_num1)

        # 成交完成，重置
        if is_close_market1_done and is_close_market2_done:
            position_count -= 1
            is_close_market1_done = False
            is_close_market2_done = False
            is_close_market1_maker = False
            is_close_market2_maker = False
            close_market1_maker_price = None
            close_market1_taker_price = None
            close_market2_maker_price = None
            close_market2_taker_price = None
            market1_close_time = None
            market2_close_time = None
            

        # except Exception as e:
                # traceback.print_exc()
    
    open_trades_df = pd.DataFrame(open_trades, columns=['index', 'sr_open', 'sr_open_real', 
                                                        'market1_traded_price', 'market2_traded_price',
                                                        'market1_traded_type', 'market2_traded_type',
                                                        'market1_open_time', 'market2_open_time'])
    open_trades_df.set_index('index', inplace=True)
    close_trades_df = pd.DataFrame(close_trades, columns=['index', 'sr_close', 'sr_close_real', 
                                                        'market1_traded_price', 'market2_traded_price',
                                                        'market1_traded_type', 'market2_traded_type',
                                                        'market1_close_time', 'market2_close_time'])
    close_trades_df.set_index('index', inplace=True)

    
    
    return open_trades_df, close_trades_df
    



import plotly.graph_objects as go

def plot_bid_price_spread_plotly(cf_depth, market2_price_col, market1_price_col='market1_bid_price0', q_10_bt=None, q_90_bt=None):
    """
    使用 Plotly 绘制两个市场 bid price 的相对差异（bps），去极值并标注 10% 和 90% 分位数。
    可选参数q_10_bt和q_90_bt允许在图上绘制额外的参考线。
    
    参数:
        cf_depth (pd.DataFrame): 含有价格数据的 DataFrame。
        market2_price_col (str): market2 的 bid price 列名。
        market1_price_col (str): market1 的 bid price 列名（默认为 'market1_bid_price0'）。
        q_10_bt (float, optional): 额外的10%分位数参考线。默认为None，不显示。
        q_90_bt (float, optional): 额外的90%分位数参考线。默认为None，不显示。
    """
    # 计算 spread（bps）
    spread = ((cf_depth[market2_price_col] / cf_depth[market1_price_col]) - 1) * 10000
    spread.index = cf_depth.index

    # 去除极端值
    lower = spread.quantile(0.001)
    upper = spread.quantile(0.999)
    spread_clipped = spread.clip(lower=lower, upper=upper)

    # 计算分位数
    q10 = spread_clipped.quantile(0.10)
    q90 = spread_clipped.quantile(0.90)

    # 创建图
    fig = go.Figure()

    # Spread 曲线
    fig.add_trace(go.Scatter(
        x=spread_clipped.index,
        y=spread_clipped,
        mode='lines',
        name='Spread (bps)',
        line=dict(color='purple')
    ))

    # 10% 分位线
    fig.add_trace(go.Scatter(
        x=[spread_clipped.index[0], spread_clipped.index[-1]],
        y=[q10, q10],
        mode='lines',
        name=f'10% Quantile: {q10:.2f}',
        line=dict(color='red', dash='dash')
    ))

    # 90% 分位线
    fig.add_trace(go.Scatter(
        x=[spread_clipped.index[0], spread_clipped.index[-1]],
        y=[q90, q90],
        mode='lines',
        name=f'90% Quantile: {q90:.2f}',
        line=dict(color='green', dash='dash')
    ))

    # 添加额外的10%分位数参考线（如果提供）
    if q_10_bt is not None:
        fig.add_trace(go.Scatter(
            x=[spread_clipped.index[0], spread_clipped.index[-1]],
            y=[q_10_bt, q_10_bt],
            mode='lines',
            name=f'10% Quantile (backtest): {q_10_bt:.2f}',
            line=dict(color='blue', dash='dash')
        ))

    # 添加额外的90%分位数参考线（如果提供）
    if q_90_bt is not None:
        fig.add_trace(go.Scatter(
            x=[spread_clipped.index[0], spread_clipped.index[-1]],
            y=[q_90_bt, q_90_bt],
            mode='lines',
            name=f'90% Quantile (backtest): {q_90_bt:.2f}',
            line=dict(color='orange', dash='dash')
        ))

    # 图表设置
    fig.update_layout(
        title=f'Spread between {market2_price_col} and {market1_price_col}',
        xaxis_title='Time',
        yaxis_title='Spread (bps)',
        hovermode='x unified',
        height=600,
        width=1400,
        legend=dict(x=0.01, y=0.99)
    )

    fig.show()
def plot_open_close_and_spread(cf_depth, open_df, close_df,
                                market2_price_col,
                                market1_price_col='market1_bid_price0',
                                target_open_sr=-0.0003,
                                target_close_sr=-0.00075):
    """
    绘制：
    - 每分钟 open 和 close 成交数量（左轴）
    - 去极值的价差 spread（右轴）
    - 加入开仓和平仓阈值线、分位数线
    """
    # 计算 spread（bps）
    spread = ((cf_depth[market2_price_col] / cf_depth[market1_price_col]) - 1) * 10000
    spread.index = cf_depth.index

    # 去除极端值
    # spread = spread.clip(lower=spread.quantile(0.001), upper=spread.quantile(0.999))
    q10 = spread.quantile(0.10)
    q90 = spread.quantile(0.90)

    # 成交统计
    open_counts = open_df.index.floor('min').value_counts().sort_index()
    close_counts = close_df.index.floor('min').value_counts().sort_index()
    all_times = open_counts.index.union(close_counts.index).union(spread.index.floor('min'))
    open_counts = open_counts.reindex(all_times, fill_value=0)
    close_counts = close_counts.reindex(all_times, fill_value=0)
    spread_resampled = spread.groupby(spread.index.floor('min')).mean().reindex(all_times)

    # 创建图
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # 成交量线条
    fig.add_trace(go.Scatter(x=open_counts.index, y=open_counts.values,
                             name="Open Trades", mode='lines', line=dict(color='blue')),
                  secondary_y=False)

    fig.add_trace(go.Scatter(x=close_counts.index, y=close_counts.values,
                             name="Close Trades", mode='lines', line=dict(color='red')),
                  secondary_y=False)

    # spread 曲线
    fig.add_trace(go.Scatter(x=spread_resampled.index, y=spread_resampled.values,
                             name="Spread (bps)", mode='lines', line=dict(color='green')),
                  secondary_y=True)

    # 分位数线
    for yval, label in zip([q10, q90], ['10% Quantile', '90% Quantile']):
        fig.add_trace(go.Scatter(x=[spread_resampled.index[0], spread_resampled.index[-1]],
                                 y=[yval, yval], name=label, mode='lines',
                                 line=dict(color='gray', dash='dash')),
                      secondary_y=True)

    # 阈值线（bps）
    fig.add_trace(go.Scatter(x=[spread_resampled.index[0], spread_resampled.index[-1]],
                             y=[target_open_sr * 10000] * 2,
                             name='Target Open Threshold',
                             mode='lines', line=dict(color='orange', dash='dot')),
                  secondary_y=True)

    fig.add_trace(go.Scatter(x=[spread_resampled.index[0], spread_resampled.index[-1]],
                             y=[target_close_sr * 10000] * 2,
                             name='Target Close Threshold',
                             mode='lines', line=dict(color='purple', dash='dot')),
                  secondary_y=True)

    # 图表设置：放大 + 美化
    fig.update_layout(
        title="Open/Close Trades & Spread (bps) with Thresholds",
        xaxis_title="Time",
        yaxis_title="Trade Count",
        legend=dict(x=0.01, y=0.99),
        hovermode='x unified',
        height=900,
        width=2800,
    )

    fig.update_yaxes(title_text="Trade Count", secondary_y=False)
    fig.update_yaxes(title_text="Spread (bps)", secondary_y=True)

    fig.show()
def plot_price_spread_with_trades_on_spread(cf_depth, open_df, close_df,
                                             market1_price_col='market1_bid_price0',
                                             market2_price_col='market2_bid_price0',
                                             market1_ask_col='market1_ask_price0',
                                             market2_ask_col='market2_ask_price0',
                                             spread_color='#6a0dad'):
    """
    双 y 轴图：
    - 左轴：Market1/2 的 bid & ask 价格（ask 为虚线）
    - 右轴：Spread（bps）+ 开平仓点（三角）
    """

    # 计算 spread（bps）
    spread = ((cf_depth[market2_price_col] / cf_depth[market1_price_col]) - 1) * 10000
    spread.index = cf_depth.index

    # 为 open 和 close df 添加成交时的 spread
    open_df = open_df.copy()
    close_df = close_df.copy()
    open_df['spread'] = ((open_df['market2_traded_price'] / open_df['market1_traded_price']) - 1) * 10000
    close_df['spread'] = ((close_df['market2_traded_price'] / close_df['market1_traded_price']) - 1) * 10000

    # 创建图
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # --- 左轴：Market1/2 Bid ---
    fig.add_trace(go.Scatter(
        x=cf_depth.index, y=cf_depth[market1_price_col],
        name='Market1 Bid Price',
        line=dict(color='blue', dash='solid')
    ), secondary_y=False)

    fig.add_trace(go.Scatter(
        x=cf_depth.index, y=cf_depth[market2_price_col],
        name='Market2 Bid Price',
        line=dict(color='orange', dash='solid')
    ), secondary_y=False)

    # --- 左轴：Market1/2 Ask ---
    fig.add_trace(go.Scatter(
        x=cf_depth.index, y=cf_depth[market1_ask_col],
        name='Market1 Ask Price',
        line=dict(color='blue', dash='dot')
    ), secondary_y=False)

    fig.add_trace(go.Scatter(
        x=cf_depth.index, y=cf_depth[market2_ask_col],
        name='Market2 Ask Price',
        line=dict(color='orange', dash='dot')
    ), secondary_y=False)

    # --- 右轴：Spread 曲线 ---
    fig.add_trace(go.Scatter(
        x=spread.index, y=spread,
        name='Spread (bps)',
        line=dict(color=spread_color, dash='solid')
    ), secondary_y=True)

    # --- 右轴：开仓点（三角向上）---
    fig.add_trace(go.Scatter(
        x=open_df.index, y=open_df['spread'],
        mode='markers',
        name='Open Trade (on spread)',
        marker=dict(symbol='triangle-up', color='green', size=10),
        hovertext=open_df['spread']
    ), secondary_y=True)

    # --- 右轴：平仓点（三角向下）---
    fig.add_trace(go.Scatter(
        x=close_df.index, y=close_df['spread'],
        mode='markers',
        name='Close Trade (on spread)',
        marker=dict(symbol='triangle-down', color='red', size=10),
        hovertext=close_df['spread']
    ), secondary_y=True)

    # --- 图表布局 ---
    fig.update_layout(
        title='Spread & Price Curve with Bid/Ask and Trade Markers',
        xaxis_title='Time',
        height=900,
        width=1800,
        hovermode='x unified',
        legend=dict(x=0.01, y=0.99)
    )

    fig.update_yaxes(title_text="Market Prices (Bid/Ask)", secondary_y=False)
    fig.update_yaxes(title_text="Spread (bps)", secondary_y=True)

    fig.show()


# 2025-05-06  ;2025-06-02 增加keep = last（因为改变了process_funding_time的逻辑,现在FundingTime指的是离现在最近的下一个时间节点)
def funding_df_to_series(df, rate_col: str = "FundingRate") -> pd.Series:
    """
    假设 df 已经过 `process_funding_time` 处理，
    返回以 FundingTime 为索引、FundingRate 为值的 Series（naive datetime）。
    """
    return (df
            .drop_duplicates("FundingTime", keep='last')
            .set_index("FundingTime")[rate_col]
            .astype(float)
            .sort_index())


#2025-06-02 Update
# dcdl里面的数据，E和T都是UTC时间，所以这里不能用Asia/Shanghai时间，而是全部转化为UTC时间；其中Time是东八区时间，所以要时区转化一下；FundingTime是UTC时间不用变。
def process_funding_time(csv_path, exchange, last_time = None):
    df = pd.read_csv(csv_path)
    if exchange == 'binance':
        # 币安的API里面'NextFundingTime'字段是离当前Time最近的下一个结算的时间节点，把NextFundingTime改名为FundingTime来统一命名
        df['FundingTime'] = pd.to_datetime(df['NextFundingTime'], unit='ms', utc=True).dt.tz_localize(None)
        df['Time'] = pd.to_datetime(df['Time']).dt.tz_convert('Asia/Shanghai').dt.tz_convert('UTC').dt.tz_localize(None)

    
    elif exchange == 'okx':
        # OKX的API里面'FundingTime'字段才是离当前Time最近的下一个结算的时间节点
        df['FundingTime'] = pd.to_datetime(df['FundingTime'],unit='ms',utc = True).dt.tz_localize(None)      
        df['Time'] = pd.to_datetime(df['Time']).dt.tz_convert('Asia/Shanghai').dt.tz_convert('UTC').dt.tz_localize(None)

    if last_time is not None:
        df = df[df["FundingTime"] <= last_time]
    
    return df

def run_arbitrage_workflow( 
    ccy: str,
    st: str,
    et: str,
    train_st: str,
    train_et: str,
    exchange1: str,
    market1: str,
    exchange2: str,
    market2: str,
    market1_stop_loss_sr_delta = -0.003,
    market2_stop_loss_sr_delta = -0.003,
    open_quantile = 0.9,
    close_quantile = 0.1,
    order_type = 'Combined',
    data_source = "inner_win", #或者ssd
    open_replace_tick_num = 1,
    close_replace_tick_num = 1,
    open_tick_num = 1,
    close_tick_num = 1,
    isPlot = True
    ):
    if data_source == 'inner_win':
        funding_okx_csv = f'/Users/rayxu/Desktop/Obentech/fundingRateData/okx/{ccy}-USDT-SWAP.csv'
        funding_binance_csv = f'/Users/rayxu/Desktop/Obentech/fundingRateData/binance/{ccy}USDT.csv'
    elif data_source == 'outer_ssd':
        funding_okx_csv = f'/Volumes/T7/Obentech/fundingRateData/okx/{ccy}-USDT-SWAP.csv'
        funding_binance_csv = f'/Volumes/T7/Obentech/fundingRateData/binance/{ccy}USDT.csv'        

    df_okx      = process_funding_time(funding_okx_csv,  exchange='okx')
    df_binance  = process_funding_time(funding_binance_csv,  exchange='binance')

    fr_okx      = funding_df_to_series(df_okx)       # OKX
    fr_binance  = funding_df_to_series(df_binance)   # Binance



    time_index = "local_time"
    # print('start reading cf_depth')
    cf_depth = read_cf_depth(ccy, st, et, exchange1, market1, exchange2, market2, data_source=data_source)
    # print('start reading cf_depth_train')

    cf_depth_train = read_cf_depth(ccy, train_st, train_et, exchange1, market1, exchange2, market2, data_source=data_source)
    spread_train = ((cf_depth_train['market2_bid_price0'] / cf_depth_train['market1_bid_price0']) - 1) * 10000
    capital = 0
    mean_price =0.5*(cf_depth_train['market2_bid_price0'].dropna().mean() + cf_depth_train['market1_bid_price0'].dropna().mean())

    capital = 10000
    single_order_amount = 100
    single_order_volume = 100/ mean_price
    notional = single_order_volume
    pos_limit = capital/(2*single_order_amount)



    q10_train = spread_train.quantile(close_quantile)
    q90_train = spread_train.quantile(open_quantile)

    print(f'thresholds: {q10_train},{q90_train}')
    print(f'pos_limit: {pos_limit}')

    target_open_sr = q90_train/10000
    target_close_sr = q10_train/10000

    # 设置为负的，全部用MT对冲


    taker_traded_delay_seconds = 0.02
    maker_ordered_delay_seconds = 0.01


    mt_params = {'open_tick_num':open_tick_num, 'open_replace_tick_num':open_replace_tick_num,
                'close_tick_num':close_tick_num, 'close_replace_tick_num':close_replace_tick_num, 
                'taker_traded_delay_seconds':taker_traded_delay_seconds, 'maker_ordered_delay_seconds':maker_ordered_delay_seconds,
                'market1_stop_loss_sr_delta':market1_stop_loss_sr_delta, 'market2_stop_loss_sr_delta':market2_stop_loss_sr_delta,
                'target_open_sr':target_open_sr, 'target_close_sr':target_close_sr,
                'exchange1_type':exchange1, 'market1_type':market1, 'exchange2_type':exchange2, 'market2_type':market2}

    # plot_bid_price_spread(cf_depth, 'market2_bid_price0','market1_bid_price0',q_10_bt=target_close_sr*10000,q_90_bt=target_open_sr*10000,ccy = ccy)
    # plot_bid_price_spread(cf_depth, 'market2_ask_price0','market1_bid_price0')
    if isPlot:
        plot_spread_and_funding_combined(cf_depth=cf_depth,market2_price_col='market2_bid_price0',market1_price_col='market1_bid_price0',fr_okx=fr_okx,fr_binance=fr_binance,ccy=ccy,q_10_bt=target_close_sr*1e4,q_90_bt=target_open_sr*1e4)
    # plot_spread_and_funding_combined(cf_depth=cf_depth,market2_price_col='market2_ask_price0',market1_price_col='market1_ask_price0',fr_okx=fr_okx,fr_binance=fr_binance,ccy=ccy,q_10_bt=target_close_sr*1e4,q_90_bt=target_open_sr*1e4)

    open_trades_df, close_trades_df = maker_maker_backtest_0502(cf_depth, mt_params,pos_limit= pos_limit)
    open_trades_df['is_market1_maker'] = (open_trades_df['market1_traded_type'] == 'market1_maker')
    open_trades_df['is_market2_maker'] = (open_trades_df['market2_traded_type'] == 'market2_maker')
    close_trades_df['is_market1_maker'] = (close_trades_df['market1_traded_type'] == 'market1_maker')
    close_trades_df['is_market2_maker'] = (close_trades_df['market2_traded_type'] == 'market2_maker')
    open_trades_df['is_mm'] = (open_trades_df['is_market1_maker'] & open_trades_df['is_market2_maker'])
    open_trades_df['is_mt'] = (open_trades_df['is_market1_maker'] & (~open_trades_df['is_market2_maker']))
    open_trades_df['is_tm'] = ((~open_trades_df['is_market1_maker']) & open_trades_df['is_market2_maker'])
    close_trades_df['is_mm'] = (close_trades_df['is_market1_maker'] & close_trades_df['is_market2_maker'])
    close_trades_df['is_mt'] = (close_trades_df['is_market1_maker'] & (~close_trades_df['is_market2_maker']))
    close_trades_df['is_tm'] = ((~close_trades_df['is_market1_maker']) & close_trades_df['is_market2_maker'])

    open_trades_df['market1_open_time'] = pd.to_datetime(open_trades_df['market1_open_time'])
    open_trades_df['market2_open_time'] = pd.to_datetime(open_trades_df['market2_open_time'])
    open_trades_df['is_mm_market1_first'] = open_trades_df['is_mm'] & (open_trades_df['market1_open_time'] <= open_trades_df['market2_open_time'])
    open_trades_df['is_mm_market2_first'] = open_trades_df['is_mm'] & (open_trades_df['market2_open_time'] < open_trades_df['market1_open_time'])

    close_trades_df['market1_close_time'] = pd.to_datetime(close_trades_df['market1_close_time'])
    close_trades_df['market2_close_time'] = pd.to_datetime(close_trades_df['market2_close_time'])
    close_trades_df['is_mm_market1_first'] = close_trades_df['is_mm'] & (close_trades_df['market1_close_time'] <= close_trades_df['market2_close_time'])
    close_trades_df['is_mm_market2_first'] = close_trades_df['is_mm'] & (close_trades_df['market2_close_time'] < close_trades_df['market1_close_time'])

    open_trades_df['fee_market1'] = -0.00003*open_trades_df['is_market1_maker'] + 0.00022*(1-open_trades_df['is_market1_maker'])
    open_trades_df['fee_market2'] = 0*open_trades_df['is_market2_maker'] + 0.0002*(1-open_trades_df['is_market2_maker'])
    close_trades_df['fee_market1'] = -0.00003*close_trades_df['is_market1_maker'] + 0.00022*(1-close_trades_df['is_market1_maker'])
    close_trades_df['fee_market2'] = 0*close_trades_df['is_market2_maker'] + 0.0002*(1-close_trades_df['is_market2_maker'])


    open_trades_df['slippage'] = open_trades_df['sr_open_real']-open_trades_df['sr_open']
    close_trades_df['slippage'] = close_trades_df['sr_close']-close_trades_df['sr_close_real']

    open_trades_df['slippage_with_fee'] = open_trades_df['slippage'] - open_trades_df['fee_market1'] - open_trades_df['fee_market2']
    close_trades_df['slippage_with_fee'] = close_trades_df['slippage'] - close_trades_df['fee_market1'] - close_trades_df['fee_market2']


    print(open_trades_df['is_mm_market1_first'].sum(), open_trades_df['is_mm_market2_first'].sum(), open_trades_df['is_mm'].sum(), open_trades_df['is_mt'].sum(), open_trades_df['is_tm'].sum(), open_trades_df.shape[0])
    print(close_trades_df['is_mm_market1_first'].sum(), close_trades_df['is_mm_market2_first'].sum(),close_trades_df['is_mm'].sum(), close_trades_df['is_mt'].sum(), close_trades_df['is_tm'].sum(), close_trades_df.shape[0])





    # 合并表格
    open_trades_df['side'] = 'open'
    close_trades_df['side'] = 'close'

    if order_type == 'MT': 
        open_trades_df = open_trades_df[open_trades_df['is_mt'] == True]    
        close_trades_df = close_trades_df[close_trades_df['is_mt'] == True]
    elif order_type == 'TM': 
        open_trades_df = open_trades_df[open_trades_df['is_tm'] == True]    
        close_trades_df = close_trades_df[close_trades_df['is_tm'] == True]



    trades_df = pd.concat([open_trades_df, close_trades_df]).sort_index()

    # 计算 PnL

    trade_pnl, funding_pnl, cum_pnl, pos = compute_dynamic_fifo_pnl_correct_fee_with_funding(trades_df,funding_okx_csv,funding_binance_csv, notional=notional)
    trade_pnl_2, funding_pnl_2, cum_pnl_2, pos, cum_df = compute_real_time_spread_pnl_correct_fee_with_funding(trades_df, funding_okx_csv,funding_binance_csv, notional=notional)


    def turnover_cycles(position: pd.Series, P_std: float) -> float:
        """
        计算完整开＋平轮次的换手率（cycles）。
        参数：
            position: 按时间排序的仓位序列（可以正负）。
            P_std:    标准仓位大小，定义一次满开的仓位量。
        返回：
            turnover_cycles: 完整开平轮次数量（1 轮次 = 满开 + 满平）。
        """
        V = position.diff().abs().sum()
        return V / (2 * P_std)


    turnover = turnover_cycles(pos,pos_limit)



    if isPlot:
        # 假设 cum_pnl, cum_pnl2, cum_df 已经在环境中定义
        fig, axes = plt.subplots(1, 3, figsize=(24, 4))  # 宽×高 = 24×4


        # 1) Cumulative PnL
        axes[0].plot(cum_pnl.index, cum_pnl.values,
                    marker='o', linestyle='-',
                    color='royalblue', label='Cumulative PnL')

        axes[0].plot(
            funding_pnl.index, funding_pnl.values,
            marker='x', linestyle='--',
            color='indianred', label='Funding Cumulative PnL'
        )

        axes[0].plot(
            trade_pnl.index, trade_pnl.values,
            marker='o', linestyle='-',
            color='pink', label='Total Trade PnL'
        )
        x = pd.to_datetime(cum_pnl.index)
        y = pd.to_numeric(cum_pnl.values, errors='coerce')  # 强制转换成 float，无法转换的设为 NaN
        mask = ~np.isnan(y)  # 去掉 NaN，避免画图报错
        axes[0].fill_between(x[mask], y[mask],
                            color='royalblue', alpha=0.1)
        # axes[0].fill_between(cum_pnl.index, cum_pnl.values,
        #                     color='royalblue', alpha=0.1)
        axes[0].set_title(f'{ccy} Cumulative PnL on {st}', fontsize=16, fontweight='bold')
        axes[0].set_xlabel('Time', fontsize=14)
        axes[0].set_ylabel('Cumulative PnL', fontsize=14)
        axes[0].grid(color='gray', linestyle='--', linewidth=0.5, alpha=0.7)
        axes[0].legend(fontsize=12)
        axes[0].tick_params(axis='both', labelsize=12)

        # 2) Cumulative PnL 2
        axes[1].plot(cum_pnl_2.index, cum_pnl_2.values,
                    marker='o', linestyle='-',
                    color='darkorange', label='Cumulative PnL 2')
        axes[1].fill_between(cum_pnl_2.index, cum_pnl_2.values,
                            color='darkorange', alpha=0.1)
        axes[1].set_title(f'{ccy} Real‐Time Spread PnL on {st}', fontsize=16, fontweight='bold')
        axes[1].set_xlabel('Time', fontsize=14)
        axes[1].set_ylabel('Cumulative PnL 2', fontsize=14)
        axes[1].grid(color='gray', linestyle='--', linewidth=0.5, alpha=0.7)
        axes[1].legend(fontsize=12)
        axes[1].tick_params(axis='both', labelsize=12)

        # 3) Position
        axes[2].plot(cum_df.index, cum_df['position'],
                    marker='o', linestyle='-',
                    color='seagreen', label='Position')
        axes[2].set_title(f'{ccy} Position on {st}', fontsize=16, fontweight='bold')
        axes[2].set_xlabel('Time', fontsize=14)
        axes[2].set_ylabel('Position', fontsize=14)
        axes[2].grid(color='gray', linestyle='--', linewidth=0.5, alpha=0.7)
        axes[2].legend(fontsize=12)
        axes[2].tick_params(axis='both', labelsize=12)

        plt.tight_layout()
        plt.show()

    if len(funding_pnl) > 0:
        funding_ret = funding_pnl[-1] / min(pos.abs().max() * open_trades_df['market1_traded_price'].mean(), capital * 0.05)
    else:
        funding_ret = 0  
    if len(cum_pnl) != 0:
        ret_rate_realized = cum_pnl[-1]/min( pos.abs().max()*open_trades_df['market1_traded_price'].mean(), capital*0.05)
        trade_ret_rate_realized = trade_pnl[-1]/min( pos.abs().max()*open_trades_df['market1_traded_price'].mean(), capital*0.05)
    else:
        ret_rate_realized = 0
        trade_ret_rate_realized = 0
        print('No Trades happens')
    ret_rate_on_record = cum_df['total_pnl'][-1]/min( pos.abs().max()*open_trades_df['market1_traded_price'].mean(), capital*0.05)
    # 统计 open_trades_df 的各类成交数量和比例
    open_total = open_trades_df.shape[0]
    open_stats = {
        "MM Market1 First Ratio": open_trades_df['is_mm_market1_first'].sum() / open_total if open_total > 0 else 0,
        "MM Market2 First Ratio": open_trades_df['is_mm_market2_first'].sum() / open_total if open_total > 0 else 0,
        "MM Ratio": open_trades_df['is_mm'].sum() / open_total if open_total > 0 else 0,
        "MT Ratio": open_trades_df['is_mt'].sum() / open_total if open_total > 0 else 0,
        "TM Ratio": open_trades_df['is_tm'].sum() / open_total if open_total > 0 else 0,
        "Total Trades": open_total
    }

    # 统计 close_trades_df 的各类成交数量和比例
    description_stats = {
        "Symbol": ccy,
        "Date": st,
        "Open Quantile": open_quantile,
        "Close Quantile": close_quantile,
        "Open SR Threshold": q90_train,
        "Close SR Threshold": q10_train
    }
    close_total = close_trades_df.shape[0]
    close_stats = {
        "MM Market1 First Ratio": close_trades_df['is_mm_market1_first'].sum() / close_total if close_total > 0 else 0,
        "MM Market2 First Ratio": close_trades_df['is_mm_market2_first'].sum() / close_total if close_total > 0 else 0,
        "MM Ratio": close_trades_df['is_mm'].sum() / close_total if close_total > 0 else 0,
        "MT Ratio": close_trades_df['is_mt'].sum() / close_total if close_total > 0 else 0,
        "TM Ratio": close_trades_df['is_tm'].sum() / close_total if close_total > 0 else 0,
        "Total Trades": close_total
    }


    d1 = pd.to_datetime(et)
    d2 = pd.to_datetime(st)

    days = 1 + (d1 - d2).total_seconds() / 86400


    # 计算其他指标
    additional_stats = {
        "Open SR Mean": open_trades_df['sr_open'].mean(),
        "Open SR Real Mean": open_trades_df['sr_open_real'].mean(),
        "Close SR Mean": close_trades_df['sr_close'].mean(),
        "Close SR Real Mean": close_trades_df['sr_close_real'].mean(),
        "Open Slippage Mean": open_trades_df['slippage'].mean(),
        "Open Slippage Std": open_trades_df['slippage'].std(),
        "Close Slippage Mean": close_trades_df['slippage'].mean(),
        "Slippage Std": close_trades_df['slippage'].std(),
        "Open Slippage With Fee Mean": open_trades_df['slippage_with_fee'].mean(),
        "Open Slippage With Fee Std": open_trades_df['slippage_with_fee'].std(),
        "Close Slippage With Fee Mean": close_trades_df['slippage_with_fee'].mean(),
        "Close Slippage With Fee Std": close_trades_df['slippage_with_fee'].std(),
        "Total Return Rate (Annualized%)": ret_rate_realized*365*100/days,
        "Trade Return Rate (Annualized%)": trade_ret_rate_realized*365*100/days,
        "Funding return rate (Annualized%)": funding_ret*365*100/days,
        "Total Return Rate on Record (Annualized%)": ret_rate_on_record*365*100/days,
        "Turnover Cycles": turnover/days
    }

    # 合并所有统计数据
    all_stats = {**description_stats, **close_stats, **additional_stats}

    # 打印表格
    return all_stats    


def run_arbitrage_workflow_tm(
    ccy: str,
    st: str,
    et: str,
    train_st: str,
    train_et: str,
    exchange1: str,
    market1: str,
    exchange2: str,
    market2: str,
    market1_stop_loss_sr_delta = -0.003,
    market2_stop_loss_sr_delta = -0.003,
    open_quantile = 0.9,
    close_quantile = 0.1,
    data_source = "inner_win", #或者ssd
    isBookTicker = False,
    isPlot = True
    ):

    if data_source == 'inner_win':
        funding_okx_csv = f'/Users/rayxu/Desktop/Obentech/fundingRateData/okx/{ccy}-USDT-SWAP.csv'
        funding_binance_csv = f'/Users/rayxu/Desktop/Obentech/fundingRateData/binance/{ccy}USDT.csv'
    elif data_source == 'outer_ssd':
        funding_okx_csv = f'/Volumes/T7/Obentech/fundingRateData/okx/{ccy}-USDT-SWAP.csv'
        funding_binance_csv = f'/Volumes/T7/Obentech/fundingRateData/binance/{ccy}USDT.csv'    

    df_okx      = process_funding_time(funding_okx_csv,  exchange='okx')
    df_binance  = process_funding_time(funding_binance_csv,  exchange='binance')

    fr_okx      = funding_df_to_series(df_okx)       # OKX
    fr_binance  = funding_df_to_series(df_binance)   # Binance




    if not isBookTicker:
        cf_depth = read_cf_depth(ccy, st, et, exchange1, market1, exchange2, market2, data_source=data_source)
        cf_depth_train = read_cf_depth(ccy, train_st, train_et, exchange1, market1, exchange2, market2, data_source=data_source)
    else:
        cf_depth = read_bookTicker_data(ccy, st, et, exchange1, market1, exchange2, market2, data_source=data_source)
        cf_depth_train = read_bookTicker_data(ccy, train_st, train_et, exchange1, market1, exchange2, market2, data_source=data_source)

    spread_train = ((cf_depth_train['market2_bid_price0'] / cf_depth_train['market1_bid_price0']) - 1) * 10000
    mean_price =0.5*(cf_depth_train['market2_bid_price0'].dropna().mean() + cf_depth_train['market1_bid_price0'].dropna().mean())

    capital = 10000
    single_order_amount = 100
    single_order_volume = 100/ mean_price
    notional = single_order_volume
    pos_limit = capital/(2*single_order_amount)


    q10_train = spread_train.quantile(close_quantile)
    q90_train = spread_train.quantile(open_quantile)

    print(f'thresholds: {q10_train},{q90_train}')
    print(f'pos_limit: {pos_limit}')

    target_open_sr = q90_train/10000
    target_close_sr = q10_train/10000

    # 设置为负的，全部用MT对冲


    open_tick_num = 1
    open_replace_tick_num = 1
    close_tick_num = 1
    close_replace_tick_num = 1
    taker_traded_delay_seconds = 0.02
    maker_ordered_delay_seconds = 0.01


    mt_params = {'open_tick_num':open_tick_num, 'open_replace_tick_num':open_replace_tick_num,
                'close_tick_num':close_tick_num, 'close_replace_tick_num':close_replace_tick_num, 
                'taker_traded_delay_seconds':taker_traded_delay_seconds, 'maker_ordered_delay_seconds':maker_ordered_delay_seconds,
                'market1_stop_loss_sr_delta':market1_stop_loss_sr_delta, 'market2_stop_loss_sr_delta':market2_stop_loss_sr_delta,
                'target_open_sr':target_open_sr, 'target_close_sr':target_close_sr,
                'exchange1_type':exchange1, 'market1_type':market1, 'exchange2_type':exchange2, 'market2_type':market2}

    # plot_bid_price_spread(cf_depth, 'market2_bid_price0','market1_bid_price0',q_10_bt=target_close_sr*10000,q_90_bt=target_open_sr*10000,ccy = ccy)
    # plot_bid_price_spread(cf_depth, 'market2_ask_price0','market1_bid_price0')
    if isPlot:
        plot_spread_and_funding_combined(cf_depth=cf_depth,market2_price_col='market2_bid_price0',market1_price_col='market1_bid_price0',fr_okx=fr_okx,fr_binance=fr_binance,ccy=ccy,q_10_bt=target_close_sr*1e4,q_90_bt=target_open_sr*1e4)
    open_trades_df, close_trades_df = maker_maker_backtest_0519(cf_depth, mt_params,pos_limit= pos_limit)
    open_trades_df['is_market1_maker'] = (open_trades_df['market1_traded_type'] == 'market1_maker')
    open_trades_df['is_market2_taker'] = (open_trades_df['market2_traded_type'] == 'market2_taker')

    close_trades_df['is_market1_maker'] = (close_trades_df['market1_traded_type'] == 'market1_maker')
    close_trades_df['is_market2_taker'] = (close_trades_df['market2_traded_type'] == 'market2_taker')

    open_trades_df['is_tt'] = ((~open_trades_df['is_market1_maker']) & open_trades_df['is_market2_taker'])
    open_trades_df['is_mt'] = (open_trades_df['is_market1_maker'] & open_trades_df['is_market2_taker'])

    close_trades_df['is_tt'] = ((~close_trades_df['is_market1_maker']) & close_trades_df['is_market2_taker'])
    close_trades_df['is_mt'] = (close_trades_df['is_market1_maker'] & close_trades_df['is_market2_taker'])


    open_trades_df['market1_open_time'] = pd.to_datetime(open_trades_df['market1_open_time'])
    open_trades_df['market2_open_time'] = pd.to_datetime(open_trades_df['market2_open_time'])

    close_trades_df['market1_close_time'] = pd.to_datetime(close_trades_df['market1_close_time'])
    close_trades_df['market2_close_time'] = pd.to_datetime(close_trades_df['market2_close_time'])

    open_trades_df['fee_market1'] = -0.00003*open_trades_df['is_market1_maker'] + 0.00022*(1-open_trades_df['is_market1_maker'])
    open_trades_df['fee_market2'] = 0*(1-open_trades_df['is_market2_taker']) + 0.0002*(open_trades_df['is_market2_taker'])

    close_trades_df['fee_market1'] = -0.00003*close_trades_df['is_market1_maker'] + 0.00022*(1-close_trades_df['is_market1_maker'])
    close_trades_df['fee_market2'] = 0*(1-close_trades_df['is_market2_taker']) + 0.0002*(close_trades_df['is_market2_taker'])


    open_trades_df['slippage'] = open_trades_df['sr_open_real']-open_trades_df['sr_open']
    close_trades_df['slippage'] = close_trades_df['sr_close']-close_trades_df['sr_close_real']

    open_trades_df['slippage_with_fee'] = open_trades_df['slippage'] - open_trades_df['fee_market1'] - open_trades_df['fee_market2']
    close_trades_df['slippage_with_fee'] = close_trades_df['slippage'] - close_trades_df['fee_market1'] - close_trades_df['fee_market2']

    # 合并表格
    open_trades_df['side'] = 'open'
    close_trades_df['side'] = 'close'



    trades_df = pd.concat([open_trades_df, close_trades_df]).sort_index()

    # 计算 PnL

    trade_pnl, funding_pnl, cum_pnl, pos = compute_dynamic_fifo_pnl_correct_fee_with_funding(trades_df,funding_okx_csv,funding_binance_csv)
    trade_pnl_2, funding_pnl_2, cum_pnl_2, pos, cum_df = compute_real_time_spread_pnl_correct_fee_with_funding(trades_df, funding_okx_csv,funding_binance_csv)


    turnover = turnover_cycles(pos,pos_limit)


    if isPlot:
        # 假设 cum_pnl, cum_pnl2, cum_df 已经在环境中定义
        fig, axes = plt.subplots(1, 3, figsize=(24, 4))  # 宽×高 = 24×4


        # 1) Cumulative PnL
        axes[0].plot(cum_pnl.index, cum_pnl.values,
                    marker='o', linestyle='-',
                    color='royalblue', label='Cumulative PnL')

        axes[0].plot(
            funding_pnl.index, funding_pnl.values,
            marker='x', linestyle='--',
            color='indianred', label='Funding Cumulative PnL'
        )

        axes[0].plot(
            trade_pnl.index, trade_pnl.values,
            marker='o', linestyle='-',
            color='pink', label='Total Trade PnL'
        )
        x = pd.to_datetime(cum_pnl.index)
        y = pd.to_numeric(cum_pnl.values, errors='coerce')  # 强制转换成 float，无法转换的设为 NaN
        mask = ~np.isnan(y)  # 去掉 NaN，避免画图报错
        axes[0].fill_between(x[mask], y[mask],
                            color='royalblue', alpha=0.1)
        # axes[0].fill_between(cum_pnl.index, cum_pnl.values,
        #                     color='royalblue', alpha=0.1)
        axes[0].set_title(f'{ccy} Cumulative PnL on {st}', fontsize=16, fontweight='bold')
        axes[0].set_xlabel('Time', fontsize=14)
        axes[0].set_ylabel('Cumulative PnL', fontsize=14)
        axes[0].grid(color='gray', linestyle='--', linewidth=0.5, alpha=0.7)
        axes[0].legend(fontsize=12)
        axes[0].tick_params(axis='both', labelsize=12)

        # 2) Cumulative PnL 2
        axes[1].plot(cum_pnl_2.index, cum_pnl_2.values,
                    marker='o', linestyle='-',
                    color='darkorange', label='Cumulative PnL 2')
        axes[1].fill_between(cum_pnl_2.index, cum_pnl_2.values,
                            color='darkorange', alpha=0.1)
        axes[1].set_title(f'{ccy} Real‐Time Spread PnL on {st}', fontsize=16, fontweight='bold')
        axes[1].set_xlabel('Time', fontsize=14)
        axes[1].set_ylabel('Cumulative PnL 2', fontsize=14)
        axes[1].grid(color='gray', linestyle='--', linewidth=0.5, alpha=0.7)
        axes[1].legend(fontsize=12)
        axes[1].tick_params(axis='both', labelsize=12)

        # 3) Position
        axes[2].plot(cum_df.index, cum_df['position'],
                    marker='o', linestyle='-',
                    color='seagreen', label='Position')
        axes[2].set_title(f'{ccy} Position on {st}', fontsize=16, fontweight='bold')
        axes[2].set_xlabel('Time', fontsize=14)
        axes[2].set_ylabel('Position', fontsize=14)
        axes[2].grid(color='gray', linestyle='--', linewidth=0.5, alpha=0.7)
        axes[2].legend(fontsize=12)
        axes[2].tick_params(axis='both', labelsize=12)

        plt.tight_layout()
        plt.show()

    if len(funding_pnl) > 0:
        funding_ret = funding_pnl[-1] / min(pos.abs().max() * open_trades_df['market1_traded_price'].mean(), capital * 0.05)
    else:
        funding_ret = 0  
    if len(cum_pnl) != 0:
        ret_rate_realized = cum_pnl[-1]/min( pos.abs().max()*open_trades_df['market1_traded_price'].mean(), capital*0.05)
        trade_ret_rate_realized = trade_pnl[-1]/min( pos.abs().max()*open_trades_df['market1_traded_price'].mean(), capital*0.05)
    else:
        ret_rate_realized = 0
        trade_ret_rate_realized = 0
        print('No Trades happens')
    ret_rate_on_record = cum_df['total_pnl'][-1]/min( pos.abs().max()*open_trades_df['market1_traded_price'].mean(), capital*0.05)
    # 统计 open_trades_df 的各类成交数量和比例
    open_total = open_trades_df.shape[0]
    open_stats = {
        "TT Ratio": open_trades_df['is_tt'].sum() / open_total if open_total > 0 else 0,
        "MT Ratio": open_trades_df['is_mt'].sum() / open_total if open_total > 0 else 0,
        "Total Trades": open_total
    }

    # 统计 close_trades_df 的各类成交数量和比例
    description_stats = {
        "Symbol": ccy,
        "Date": st,
        "Open Quantile": open_quantile,
        "Close Quantile": close_quantile,
        "Open SR Threshold": q90_train,
        "Close SR Threshold": q10_train
    }
    close_total = close_trades_df.shape[0]
    close_stats = {
        "TT Ratio": close_trades_df['is_tt'].sum() / close_total if close_total > 0 else 0,
        "MT Ratio": close_trades_df['is_mt'].sum() / close_total if close_total > 0 else 0,
        "Total Trades": close_total
    }



    TT_stats = {

        "TT Ratio overall": (close_trades_df['is_tt'].sum() + open_trades_df['is_tt'].sum()) / (close_total + open_total) if (close_total + open_total > 0) else 0,
        "MT Ratio overall": (close_trades_df['is_mt'].sum() + open_trades_df['is_mt'].sum()) / (close_total + open_total) if (close_total + open_total > 0) else 0,
        "Close Trades ": close_total
    }

    additional_stats = {
        "Open SR Mean": open_trades_df['sr_open'].mean(),
        "Open SR Real Mean": open_trades_df['sr_open_real'].mean(),
        "Close SR Mean": close_trades_df['sr_close'].mean(),
        "Close SR Real Mean": close_trades_df['sr_close_real'].mean(),
        "Open Slippage Mean": open_trades_df['slippage'].mean(),
        "Open Slippage Std": open_trades_df['slippage'].std(),
        "Close Slippage Mean": close_trades_df['slippage'].mean(),
        "Slippage Std": close_trades_df['slippage'].std(),
        "Total Return Rate (bps)": ret_rate_realized*10000,
        "Trade Return Rate (bps)": trade_ret_rate_realized*10000,
        "Funding return rate (bps)": funding_ret*10000,
        "Total Return Rate on Record (bps)": ret_rate_on_record*10000,
        "Turnover Cycles": turnover,
        "Open Slippage With Fee Mean": open_trades_df['slippage_with_fee'].mean(),
        "Open Slippage With Fee Std": open_trades_df['slippage_with_fee'].std(),
        "Close Slippage With Fee Mean": close_trades_df['slippage_with_fee'].mean(),
        "Close Slippage With Fee Std": close_trades_df['slippage_with_fee'].std(),
    }

    # 合并所有统计数据
    all_stats = {**description_stats, **TT_stats, **additional_stats}
    # 打印表格
    return all_stats    




# 2025-05-25 + 28 带因子的回测
def run_arbitrage_workflow_with_OIR(
    ccy: str,
    st: str,
    et: str,
    train_st: str,
    train_et: str,
    exchange1: str,
    market1: str,
    exchange2: str,
    market2: str,
    market1_stop_loss_sr_delta = -0.003,
    market2_stop_loss_sr_delta = -0.003,
    open_quantile = 0.9,
    close_quantile = 0.1,
    order_type = 'Combined',
    data_source = "inner_win",
    oir_threshold = 0.9,
    isBookTicker = False,
    isPreTakerMaker = False,
    isPlot = True
    ):

    funding_okx_csv = f'/Users/rayxu/Desktop/Obentech/fundingRateData/okx/{ccy}-USDT-SWAP.csv'
    funding_binance_csv = f'/Users/rayxu/Desktop/Obentech/fundingRateData/binance/{ccy}USDT.csv'


    df_okx      = process_funding_time(funding_okx_csv,  exchange='okx')
    df_binance  = process_funding_time(funding_binance_csv,  exchange='binance')

    fr_okx      = funding_df_to_series(df_okx)       # OKX
    fr_binance  = funding_df_to_series(df_binance)   # Binance


    if not isBookTicker:
        cf_depth = read_cf_depth(ccy, st, et, exchange1, market1, exchange2, market2, data_source=data_source)
        cf_depth_train = read_cf_depth(ccy, train_st, train_et, exchange1, market1, exchange2, market2, data_source=data_source)
    else:
        cf_depth = read_bookTicker_data(ccy, st, et, exchange1, market1, exchange2, market2, data_source=data_source)
        cf_depth_train = read_bookTicker_data(ccy, train_st, train_et, exchange1, market1, exchange2, market2, data_source=data_source)


    spread_train = ((cf_depth_train['market2_bid_price0'] / cf_depth_train['market1_bid_price0']) - 1) * 10000
    mean_price =0.5*(cf_depth_train['market2_bid_price0'].dropna().mean() + cf_depth_train['market1_bid_price0'].dropna().mean())

    capital = 10000
    single_order_amount = 100
    single_order_volume = 100/ mean_price
    notional = single_order_volume
    pos_limit = capital/(2*single_order_amount)


    q10_train = spread_train.quantile(close_quantile)
    q90_train = spread_train.quantile(open_quantile)

    print(f'thresholds: {q10_train},{q90_train}')
    print(f'pos_limit: {pos_limit}')

    target_open_sr = q90_train/10000
    target_close_sr = q10_train/10000
    open_tick_num = 1
    open_replace_tick_num = 1
    close_tick_num = 1
    close_replace_tick_num = 1
    taker_traded_delay_seconds = 0.02
    maker_ordered_delay_seconds = 0.01


    mt_params = {'open_tick_num':open_tick_num, 'open_replace_tick_num':open_replace_tick_num,
                'close_tick_num':close_tick_num, 'close_replace_tick_num':close_replace_tick_num, 
                'taker_traded_delay_seconds':taker_traded_delay_seconds, 'maker_ordered_delay_seconds':maker_ordered_delay_seconds,
                'market1_stop_loss_sr_delta':market1_stop_loss_sr_delta, 'market2_stop_loss_sr_delta':market2_stop_loss_sr_delta,
                'target_open_sr':target_open_sr, 'target_close_sr':target_close_sr,
                'exchange1_type':exchange1, 'market1_type':market1, 'exchange2_type':exchange2, 'market2_type':market2,"oir_threshold": oir_threshold}

    # plot_bid_price_spread(cf_depth, 'market2_bid_price0','market1_bid_price0',q_10_bt=target_close_sr*10000,q_90_bt=target_open_sr*10000,ccy = ccy)
    # plot_bid_price_spread(cf_depth, 'market2_ask_price0','market1_bid_price0')
    if isPlot:
        plot_spread_and_funding_combined(cf_depth=cf_depth,market2_price_col='market2_bid_price0',market1_price_col='market1_bid_price0',fr_okx=fr_okx,fr_binance=fr_binance,ccy=ccy,q_10_bt=target_close_sr*1e4,q_90_bt=target_open_sr*1e4)
    if not isPreTakerMaker: # 并非preMakerTaker, 而是普通TakerMaker
        open_trades_df, close_trades_df = maker_maker_backtest_0525(cf_depth, mt_params,pos_limit= pos_limit)

        open_trades_df['is_market1_maker'] = (open_trades_df['market1_traded_type'] == 'market1_maker')
        open_trades_df['is_market2_maker'] = (open_trades_df['market2_traded_type'] == 'market2_maker')
        close_trades_df['is_market1_maker'] = (close_trades_df['market1_traded_type'] == 'market1_maker')
        close_trades_df['is_market2_maker'] = (close_trades_df['market2_traded_type'] == 'market2_maker')
        open_trades_df['is_mm'] = (open_trades_df['is_market1_maker'] & open_trades_df['is_market2_maker'])
        open_trades_df['is_mt'] = (open_trades_df['is_market1_maker'] & (~open_trades_df['is_market2_maker']))
        open_trades_df['is_tm'] = ((~open_trades_df['is_market1_maker']) & open_trades_df['is_market2_maker'])
        close_trades_df['is_mm'] = (close_trades_df['is_market1_maker'] & close_trades_df['is_market2_maker'])
        close_trades_df['is_mt'] = (close_trades_df['is_market1_maker'] & (~close_trades_df['is_market2_maker']))
        close_trades_df['is_tm'] = ((~close_trades_df['is_market1_maker']) & close_trades_df['is_market2_maker'])

        open_trades_df['market1_open_time'] = pd.to_datetime(open_trades_df['market1_open_time'])
        open_trades_df['market2_open_time'] = pd.to_datetime(open_trades_df['market2_open_time'])
        open_trades_df['is_mm_market1_first'] = open_trades_df['is_mm'] & (open_trades_df['market1_open_time'] <= open_trades_df['market2_open_time'])
        open_trades_df['is_mm_market2_first'] = open_trades_df['is_mm'] & (open_trades_df['market2_open_time'] < open_trades_df['market1_open_time'])

        close_trades_df['market1_close_time'] = pd.to_datetime(close_trades_df['market1_close_time'])
        close_trades_df['market2_close_time'] = pd.to_datetime(close_trades_df['market2_close_time'])
        close_trades_df['is_mm_market1_first'] = close_trades_df['is_mm'] & (close_trades_df['market1_close_time'] <= close_trades_df['market2_close_time'])
        close_trades_df['is_mm_market2_first'] = close_trades_df['is_mm'] & (close_trades_df['market2_close_time'] < close_trades_df['market1_close_time'])


        open_trades_df['fee_market1'] = -0.00003*open_trades_df['is_market1_maker'] + 0.00022*(1-open_trades_df['is_market1_maker'])
        open_trades_df['fee_market2'] = 0*open_trades_df['is_market2_maker'] + 0.0002*(1-open_trades_df['is_market2_maker'])
        close_trades_df['fee_market1'] = -0.00003*close_trades_df['is_market1_maker'] + 0.00022*(1-close_trades_df['is_market1_maker'])
        close_trades_df['fee_market2'] = 0*close_trades_df['is_market2_maker'] + 0.0002*(1-close_trades_df['is_market2_maker'])

        if order_type == 'MT': 
            open_trades_df = open_trades_df[open_trades_df['is_mt'] == True]    
            close_trades_df = close_trades_df[close_trades_df['is_mt'] == True]
        elif order_type == 'TM': 
            open_trades_df = open_trades_df[open_trades_df['is_tm'] == True]    
            close_trades_df = close_trades_df[close_trades_df['is_tm'] == True]


        # 统计 open_trades_df 的各类成交数量和比例
        open_total = open_trades_df.shape[0]
        open_stats = {
            "MM Market1 First Ratio": open_trades_df['is_mm_market1_first'].sum() / open_total if open_total > 0 else 0,
            "MM Market2 First Ratio": open_trades_df['is_mm_market2_first'].sum() / open_total if open_total > 0 else 0,
            "MM Ratio": open_trades_df['is_mm'].sum() / open_total if open_total > 0 else 0,
            "MT Ratio": open_trades_df['is_mt'].sum() / open_total if open_total > 0 else 0,
            "TM Ratio": open_trades_df['is_tm'].sum() / open_total if open_total > 0 else 0,
            "Total Trades": open_total
        }
        close_total = close_trades_df.shape[0]
        close_stats = {
            "MM Market1 First Ratio": close_trades_df['is_mm_market1_first'].sum() / close_total if close_total > 0 else 0,
            "MM Market2 First Ratio": close_trades_df['is_mm_market2_first'].sum() / close_total if close_total > 0 else 0,
            "MM Ratio": close_trades_df['is_mm'].sum() / close_total if close_total > 0 else 0,
            "MT Ratio": close_trades_df['is_mt'].sum() / close_total if close_total > 0 else 0,
            "TM Ratio": close_trades_df['is_tm'].sum() / close_total if close_total > 0 else 0,
            "Total Trades": close_total
        }

    else:
        open_trades_df, close_trades_df = maker_maker_backtest_0528(cf_depth, mt_params,pos_limit= pos_limit)

        open_trades_df['is_market1_maker'] = (open_trades_df['market1_traded_type'] == 'market1_maker')
        open_trades_df['is_market2_taker'] = (open_trades_df['market2_traded_type'] == 'market2_taker')

        close_trades_df['is_market1_maker'] = (close_trades_df['market1_traded_type'] == 'market1_maker')
        close_trades_df['is_market2_taker'] = (close_trades_df['market2_traded_type'] == 'market2_taker')

        open_trades_df['is_tt'] = ((~open_trades_df['is_market1_maker']) & open_trades_df['is_market2_taker'])
        open_trades_df['is_mt'] = (open_trades_df['is_market1_maker'] & open_trades_df['is_market2_taker'])

        close_trades_df['is_tt'] = ((~close_trades_df['is_market1_maker']) & close_trades_df['is_market2_taker'])
        close_trades_df['is_mt'] = (close_trades_df['is_market1_maker'] & close_trades_df['is_market2_taker'])


        open_trades_df['fee_market1'] = -0.00003*open_trades_df['is_market1_maker'] + 0.00022*(1-open_trades_df['is_market1_maker'])
        open_trades_df['fee_market2'] = 0*(1-open_trades_df['is_market2_taker']) + 0.0002*(open_trades_df['is_market2_taker'])

        close_trades_df['fee_market1'] = -0.00003*close_trades_df['is_market1_maker'] + 0.00022*(1-close_trades_df['is_market1_maker'])
        close_trades_df['fee_market2'] = 0*(1-close_trades_df['is_market2_taker']) + 0.0002*(close_trades_df['is_market2_taker'])

        open_total = open_trades_df.shape[0]
        open_stats = {
            "TT Ratio": open_trades_df['is_tt'].sum() / open_total if open_total > 0 else 0,
            "MT Ratio": open_trades_df['is_mt'].sum() / open_total if open_total > 0 else 0,
            "Total Trades": open_total
        }
        close_total = close_trades_df.shape[0]
        close_stats = {
            "TT Ratio": close_trades_df['is_tt'].sum() / close_total if close_total > 0 else 0,
            "MT Ratio": close_trades_df['is_mt'].sum() / close_total if close_total > 0 else 0,
            "Total Trades": close_total
        }



        TT_stats = {

            "TT Ratio overall": (close_trades_df['is_tt'].sum() + open_trades_df['is_tt'].sum()) / (close_total + open_total) if (close_total + open_total > 0) else 0,
            "MT Ratio overall": (close_trades_df['is_mt'].sum() + open_trades_df['is_mt'].sum()) / (close_total + open_total) if (close_total + open_total > 0) else 0,
            "Total Trades overall": close_total + open_total

        }
    open_trades_df['slippage'] = open_trades_df['sr_open_real']-open_trades_df['sr_open']
    close_trades_df['slippage'] = close_trades_df['sr_close']-close_trades_df['sr_close_real']

    open_trades_df['slippage_with_fee'] = open_trades_df['slippage'] - open_trades_df['fee_market1'] - open_trades_df['fee_market2']
    close_trades_df['slippage_with_fee'] = close_trades_df['slippage'] - close_trades_df['fee_market1'] - close_trades_df['fee_market2']


    open_trades_df['side'] = 'open'
    close_trades_df['side'] = 'close'


    trades_df = pd.concat([open_trades_df, close_trades_df]).sort_index()

    # 计算 PnL

    trade_pnl, funding_pnl, cum_pnl, pos = compute_dynamic_fifo_pnl_correct_fee_with_funding(trades_df,funding_okx_csv,funding_binance_csv)
    trade_pnl_2, funding_pnl_2, cum_pnl_2, pos, cum_df = compute_real_time_spread_pnl_correct_fee_with_funding(trades_df, funding_okx_csv,funding_binance_csv)


    turnover = turnover_cycles(pos,pos_limit)
    if isPlot:
        # 假设 cum_pnl, cum_pnl2, cum_df 已经在环境中定义
        fig, axes = plt.subplots(1, 3, figsize=(24, 4))  # 宽×高 = 24×4


        # 1) Cumulative PnL
        axes[0].plot(cum_pnl.index, cum_pnl.values,
                    marker='o', linestyle='-',
                    color='royalblue', label='Cumulative PnL')

        axes[0].plot(
            funding_pnl.index, funding_pnl.values,
            marker='x', linestyle='--',
            color='indianred', label='Funding Cumulative PnL'
        )

        axes[0].plot(
            trade_pnl.index, trade_pnl.values,
            marker='o', linestyle='-',
            color='pink', label='Total Trade PnL'
        )
        x = pd.to_datetime(cum_pnl.index)
        y = pd.to_numeric(cum_pnl.values, errors='coerce')  # 强制转换成 float，无法转换的设为 NaN
        mask = ~np.isnan(y)  # 去掉 NaN，避免画图报错
        axes[0].fill_between(x[mask], y[mask],
                            color='royalblue', alpha=0.1)
        # axes[0].fill_between(cum_pnl.index, cum_pnl.values,
        #                     color='royalblue', alpha=0.1)
        axes[0].set_title(f'{ccy} Cumulative PnL on {st}', fontsize=16, fontweight='bold')
        axes[0].set_xlabel('Time', fontsize=14)
        axes[0].set_ylabel('Cumulative PnL', fontsize=14)
        axes[0].grid(color='gray', linestyle='--', linewidth=0.5, alpha=0.7)
        axes[0].legend(fontsize=12)
        axes[0].tick_params(axis='both', labelsize=12)

        # 2) Cumulative PnL 2
        axes[1].plot(cum_pnl_2.index, cum_pnl_2.values,
                    marker='o', linestyle='-',
                    color='darkorange', label='Cumulative PnL 2')
        axes[1].fill_between(cum_pnl_2.index, cum_pnl_2.values,
                            color='darkorange', alpha=0.1)
        axes[1].set_title(f'{ccy} Real‐Time Spread PnL on {st}', fontsize=16, fontweight='bold')
        axes[1].set_xlabel('Time', fontsize=14)
        axes[1].set_ylabel('Cumulative PnL 2', fontsize=14)
        axes[1].grid(color='gray', linestyle='--', linewidth=0.5, alpha=0.7)
        axes[1].legend(fontsize=12)
        axes[1].tick_params(axis='both', labelsize=12)

        # 3) Position
        axes[2].plot(cum_df.index, cum_df['position'],
                    marker='o', linestyle='-',
                    color='seagreen', label='Position')
        axes[2].set_title(f'{ccy} Position on {st}', fontsize=16, fontweight='bold')
        axes[2].set_xlabel('Time', fontsize=14)
        axes[2].set_ylabel('Position', fontsize=14)
        axes[2].grid(color='gray', linestyle='--', linewidth=0.5, alpha=0.7)
        axes[2].legend(fontsize=12)
        axes[2].tick_params(axis='both', labelsize=12)

        plt.tight_layout()
        plt.show()

    if len(funding_pnl) > 0:
        funding_ret = funding_pnl[-1] / min(pos.abs().max() * open_trades_df['market1_traded_price'].mean(), capital * 0.05)
    else:
        funding_ret = 0  
    if len(cum_pnl) != 0:
        ret_rate_realized = cum_pnl[-1]/min( pos.abs().max()*open_trades_df['market1_traded_price'].mean(), capital*0.05)
        trade_ret_rate_realized = trade_pnl[-1]/min( pos.abs().max()*open_trades_df['market1_traded_price'].mean(), capital*0.05)
    else:
        ret_rate_realized = 0
        trade_ret_rate_realized = 0
        print('No Trades happens')
    ret_rate_on_record = cum_df['total_pnl'][-1]/min( pos.abs().max()*open_trades_df['market1_traded_price'].mean(), capital*0.05)
    d1 = pd.to_datetime(et)
    d2 = pd.to_datetime(st)

    days = 1 + (d1 - d2).total_seconds() / 86400

    # 计算其他指标
    additional_stats = {
        "Open SR Mean": open_trades_df['sr_open'].mean(),
        "Open SR Real Mean": open_trades_df['sr_open_real'].mean(),
        "Close SR Mean": close_trades_df['sr_close'].mean(),
        "Close SR Real Mean": close_trades_df['sr_close_real'].mean(),
        "Open Slippage Mean": open_trades_df['slippage'].mean(),
        "Open Slippage Std": open_trades_df['slippage'].std(),
        "Close Slippage Mean": close_trades_df['slippage'].mean(),
        "Slippage Std": close_trades_df['slippage'].std(),
        "Total Return Rate (bps)": ret_rate_realized*10000,
        "Trade Return Rate (bps)": trade_ret_rate_realized*10000,
        "Funding return rate (bps)": funding_ret*10000,
        "Total Return Rate on Record (bps)": ret_rate_on_record*10000,
        "Total Return Rate (Annualized%)": ret_rate_realized*365*100/days,
        "Turnover Cycles": turnover,
        "Open Slippage With Fee Mean": open_trades_df['slippage_with_fee'].mean(),
        "Open Slippage With Fee Std": open_trades_df['slippage_with_fee'].std(),
        "Close Slippage With Fee Mean": close_trades_df['slippage_with_fee'].mean(),
        "Close Slippage With Fee Std": close_trades_df['slippage_with_fee'].std(),
    }
    # 统计 close_trades_df 的各类成交数量和比例
    description_stats = {
        "Symbol": ccy,
        "Date": st,
        "Open Quantile": open_quantile,
        "Close Quantile": close_quantile,
        "Open SR Threshold": q90_train,
        "Close SR Threshold": q10_train
    }    

    if not isPreTakerMaker:
        all_stats = {**description_stats,**close_stats, **additional_stats}
    else:
        all_stats = {**description_stats, **TT_stats, **additional_stats}
    # 打印表格
    return all_stats    


# 2025-05-27 增加TM或者MT
def run_arbitrage_workflow_0527(
    ccy: str,
    st: str,
    et: str,
    train_st: str,
    train_et: str,
    exchange1: str,
    market1: str,
    exchange2: str,
    market2: str,
    market1_stop_loss_sr_delta = -0.003,
    market2_stop_loss_sr_delta = -0.003,
    open_quantile = 0.9,
    close_quantile = 0.1,
    order_type = 'Combined',
    data_source = "inner_win" #或者ssd
    ):

    funding_okx_csv = f'/Users/rayxu/Desktop/Obentech/fundingRateData/okx/{ccy}-USDT-SWAP.csv'
    funding_binance_csv = f'/Users/rayxu/Desktop/Obentech/fundingRateData/binance/{ccy}USDT.csv'


    df_okx      = process_funding_time(funding_okx_csv,  exchange='okx')
    df_binance  = process_funding_time(funding_binance_csv,  exchange='binance')

    fr_okx      = funding_df_to_series(df_okx)       # OKX
    fr_binance  = funding_df_to_series(df_binance)   # Binance



    time_index = "local_time"
    # print('start reading cf_depth')
    cf_depth = read_cf_depth(ccy, st, et, exchange1, market1, exchange2, market2, data_source=data_source)
    # print('start reading cf_depth_train')

    cf_depth_train = read_cf_depth(ccy, train_st, train_et, exchange1, market1, exchange2, market2, data_source=data_source)
    spread_train = ((cf_depth_train['market2_bid_price0'] / cf_depth_train['market1_bid_price0']) - 1) * 10000
    capital = 0
    mean_price =0.5*(cf_depth_train['market2_bid_price0'].dropna().mean() + cf_depth_train['market1_bid_price0'].dropna().mean())
    if mean_price < 0.1: capital = 1000
    elif mean_price < 0.2: capital = 2000
    elif mean_price < 0.3: capital = 3000
    elif mean_price < 0.4: capital = 4000
    elif mean_price < 0.5: capital = 5000
    elif mean_price < 0.6: capital = 6000
    elif mean_price <1: capital = 12000
    elif mean_price < 5: capital = 60000
    elif mean_price < 10: capital = 120000
    elif mean_price < 50: capital = 200000
    elif mean_price < 100: capital = 300000
    elif mean_price < 250: capital = 450000
    elif mean_price < 500: capital = 600000
    elif mean_price < 1000: capital = 1000000
    elif mean_price < 10000: capital = 2000000
    else: capital = 50000000

    pos_limit = (capital*0.05/2)/mean_price


    q10_train = spread_train.quantile(close_quantile)
    q90_train = spread_train.quantile(open_quantile)

    print(f'thresholds: {q10_train},{q90_train}')
    print(f'pos_limit: {pos_limit}')

    target_open_sr = q90_train/10000
    target_close_sr = q10_train/10000

    # 设置为负的，全部用MT对冲


    open_tick_num = 1
    open_replace_tick_num = 1
    close_tick_num = 1
    close_replace_tick_num = 1
    taker_traded_delay_seconds = 0.02
    maker_ordered_delay_seconds = 0.01


    mt_params = {'open_tick_num':open_tick_num, 'open_replace_tick_num':open_replace_tick_num,
                'close_tick_num':close_tick_num, 'close_replace_tick_num':close_replace_tick_num, 
                'taker_traded_delay_seconds':taker_traded_delay_seconds, 'maker_ordered_delay_seconds':maker_ordered_delay_seconds,
                'market1_stop_loss_sr_delta':market1_stop_loss_sr_delta, 'market2_stop_loss_sr_delta':market2_stop_loss_sr_delta,
                'target_open_sr':target_open_sr, 'target_close_sr':target_close_sr,
                'exchange1_type':exchange1, 'market1_type':market1, 'exchange2_type':exchange2, 'market2_type':market2, 'order_type': order_type}

    # plot_bid_price_spread(cf_depth, 'market2_bid_price0','market1_bid_price0',q_10_bt=target_close_sr*10000,q_90_bt=target_open_sr*10000,ccy = ccy)
    # plot_bid_price_spread(cf_depth, 'market2_ask_price0','market1_bid_price0')
    plot_spread_and_funding_combined(cf_depth=cf_depth,market2_price_col='market2_bid_price0',market1_price_col='market1_bid_price0',fr_okx=fr_okx,fr_binance=fr_binance,ccy=ccy,q_10_bt=target_close_sr*1e4,q_90_bt=target_open_sr*1e4)
    plot_spread_and_funding_combined(cf_depth=cf_depth,market2_price_col='market2_ask_price0',market1_price_col='market1_ask_price0',fr_okx=fr_okx,fr_binance=fr_binance,ccy=ccy,q_10_bt=target_close_sr*1e4,q_90_bt=target_open_sr*1e4)
    # plot_spread_and_funding_combined(cf_depth=cf_depth,market2_price_col='market2_ask_price0',market1_price_col='market1_ask_price0',fr_okx=fr_okx,fr_binance=fr_binance,ccy=ccy,q_10_bt=target_close_sr*1e4,q_90_bt=target_open_sr*1e4)

    open_trades_df, close_trades_df = maker_maker_backtest_0527(cf_depth, mt_params,pos_limit= pos_limit)
    open_trades_df['is_market1_maker'] = (open_trades_df['market1_traded_type'] == 'market1_maker')
    open_trades_df['is_market2_maker'] = (open_trades_df['market2_traded_type'] == 'market2_maker')
    close_trades_df['is_market1_maker'] = (close_trades_df['market1_traded_type'] == 'market1_maker')
    close_trades_df['is_market2_maker'] = (close_trades_df['market2_traded_type'] == 'market2_maker')
    open_trades_df['is_mm'] = (open_trades_df['is_market1_maker'] & open_trades_df['is_market2_maker'])
    open_trades_df['is_mt'] = (open_trades_df['is_market1_maker'] & (~open_trades_df['is_market2_maker']))
    open_trades_df['is_tm'] = ((~open_trades_df['is_market1_maker']) & open_trades_df['is_market2_maker'])
    close_trades_df['is_mm'] = (close_trades_df['is_market1_maker'] & close_trades_df['is_market2_maker'])
    close_trades_df['is_mt'] = (close_trades_df['is_market1_maker'] & (~close_trades_df['is_market2_maker']))
    close_trades_df['is_tm'] = ((~close_trades_df['is_market1_maker']) & close_trades_df['is_market2_maker'])

    open_trades_df['market1_open_time'] = pd.to_datetime(open_trades_df['market1_open_time'])
    open_trades_df['market2_open_time'] = pd.to_datetime(open_trades_df['market2_open_time'])
    open_trades_df['is_mm_market1_first'] = open_trades_df['is_mm'] & (open_trades_df['market1_open_time'] <= open_trades_df['market2_open_time'])
    open_trades_df['is_mm_market2_first'] = open_trades_df['is_mm'] & (open_trades_df['market2_open_time'] < open_trades_df['market1_open_time'])

    close_trades_df['market1_close_time'] = pd.to_datetime(close_trades_df['market1_close_time'])
    close_trades_df['market2_close_time'] = pd.to_datetime(close_trades_df['market2_close_time'])
    close_trades_df['is_mm_market1_first'] = close_trades_df['is_mm'] & (close_trades_df['market1_close_time'] <= close_trades_df['market2_close_time'])
    close_trades_df['is_mm_market2_first'] = close_trades_df['is_mm'] & (close_trades_df['market2_close_time'] < close_trades_df['market1_close_time'])

    open_trades_df['fee_market1'] = -0.00003*open_trades_df['is_market1_maker'] + 0.00022*(1-open_trades_df['is_market1_maker'])
    open_trades_df['fee_market2'] = 0*open_trades_df['is_market2_maker'] + 0.0002*(1-open_trades_df['is_market2_maker'])
    close_trades_df['fee_market1'] = -0.00003*close_trades_df['is_market1_maker'] + 0.00022*(1-close_trades_df['is_market1_maker'])
    close_trades_df['fee_market2'] = 0*close_trades_df['is_market2_maker'] + 0.0002*(1-close_trades_df['is_market2_maker'])


    open_trades_df['slippage'] = open_trades_df['sr_open_real']-open_trades_df['sr_open']
    close_trades_df['slippage'] = close_trades_df['sr_close']-close_trades_df['sr_close_real']
    print(open_trades_df['is_mm_market1_first'].sum(), open_trades_df['is_mm_market2_first'].sum(), open_trades_df['is_mm'].sum(), open_trades_df['is_mt'].sum(), open_trades_df['is_tm'].sum(), open_trades_df.shape[0])
    print(close_trades_df['is_mm_market1_first'].sum(), close_trades_df['is_mm_market2_first'].sum(),close_trades_df['is_mm'].sum(), close_trades_df['is_mt'].sum(), close_trades_df['is_tm'].sum(), close_trades_df.shape[0])





    # 合并表格
    open_trades_df['side'] = 'open'
    close_trades_df['side'] = 'close'

    if order_type == 'MT': 
        open_trades_df = open_trades_df[open_trades_df['is_mt'] == True]    
        close_trades_df = close_trades_df[close_trades_df['is_mt'] == True]
    elif order_type == 'TM': 
        open_trades_df = open_trades_df[open_trades_df['is_tm'] == True]    
        close_trades_df = close_trades_df[close_trades_df['is_tm'] == True]



    trades_df = pd.concat([open_trades_df, close_trades_df]).sort_index()

    # 计算 PnL

    trade_pnl, funding_pnl, cum_pnl, pos = compute_dynamic_fifo_pnl_correct_fee_with_funding(trades_df,funding_okx_csv,funding_binance_csv)
    trade_pnl_2, funding_pnl_2, cum_pnl_2, pos, cum_df = compute_real_time_spread_pnl_correct_fee_with_funding(trades_df, funding_okx_csv,funding_binance_csv)


    def turnover_cycles(position: pd.Series, P_std: float) -> float:
        """
        计算完整开＋平轮次的换手率（cycles）。
        参数：
            position: 按时间排序的仓位序列（可以正负）。
            P_std:    标准仓位大小，定义一次满开的仓位量。
        返回：
            turnover_cycles: 完整开平轮次数量（1 轮次 = 满开 + 满平）。
        """
        V = position.diff().abs().sum()
        return V / (2 * P_std)


    turnover = turnover_cycles(pos,pos_limit)




    # 假设 cum_pnl, cum_pnl2, cum_df 已经在环境中定义
    fig, axes = plt.subplots(1, 3, figsize=(24, 4))  # 宽×高 = 24×4


    # 1) Cumulative PnL
    axes[0].plot(cum_pnl.index, cum_pnl.values,
                marker='o', linestyle='-',
                color='royalblue', label='Cumulative PnL')

    axes[0].plot(
        funding_pnl.index, funding_pnl.values,
        marker='x', linestyle='--',
        color='indianred', label='Funding Cumulative PnL'
    )

    axes[0].plot(
        trade_pnl.index, trade_pnl.values,
        marker='o', linestyle='-',
        color='pink', label='Total Trade PnL'
    )
    x = pd.to_datetime(cum_pnl.index)
    y = pd.to_numeric(cum_pnl.values, errors='coerce')  # 强制转换成 float，无法转换的设为 NaN
    mask = ~np.isnan(y)  # 去掉 NaN，避免画图报错
    axes[0].fill_between(x[mask], y[mask],
                        color='royalblue', alpha=0.1)
    # axes[0].fill_between(cum_pnl.index, cum_pnl.values,
    #                     color='royalblue', alpha=0.1)
    axes[0].set_title(f'{ccy} Cumulative PnL on {st}', fontsize=16, fontweight='bold')
    axes[0].set_xlabel('Time', fontsize=14)
    axes[0].set_ylabel('Cumulative PnL', fontsize=14)
    axes[0].grid(color='gray', linestyle='--', linewidth=0.5, alpha=0.7)
    axes[0].legend(fontsize=12)
    axes[0].tick_params(axis='both', labelsize=12)

    # 2) Cumulative PnL 2
    axes[1].plot(cum_pnl_2.index, cum_pnl_2.values,
                marker='o', linestyle='-',
                color='darkorange', label='Cumulative PnL 2')
    axes[1].fill_between(cum_pnl_2.index, cum_pnl_2.values,
                        color='darkorange', alpha=0.1)
    axes[1].set_title(f'{ccy} Real‐Time Spread PnL on {st}', fontsize=16, fontweight='bold')
    axes[1].set_xlabel('Time', fontsize=14)
    axes[1].set_ylabel('Cumulative PnL 2', fontsize=14)
    axes[1].grid(color='gray', linestyle='--', linewidth=0.5, alpha=0.7)
    axes[1].legend(fontsize=12)
    axes[1].tick_params(axis='both', labelsize=12)

    # 3) Position
    axes[2].plot(cum_df.index, cum_df['position'],
                marker='o', linestyle='-',
                color='seagreen', label='Position')
    axes[2].set_title(f'{ccy} Position on {st}', fontsize=16, fontweight='bold')
    axes[2].set_xlabel('Time', fontsize=14)
    axes[2].set_ylabel('Position', fontsize=14)
    axes[2].grid(color='gray', linestyle='--', linewidth=0.5, alpha=0.7)
    axes[2].legend(fontsize=12)
    axes[2].tick_params(axis='both', labelsize=12)

    plt.tight_layout()
    plt.show()

    if len(funding_pnl) > 0:
        funding_ret = funding_pnl[-1] / min(pos.abs().max() * open_trades_df['market1_traded_price'].mean(), capital * 0.05)
    else:
        funding_ret = 0  
    if len(cum_pnl) != 0:
        ret_rate_realized = cum_pnl[-1]/min( pos.abs().max()*open_trades_df['market1_traded_price'].mean(), capital*0.05)
        trade_ret_rate_realized = trade_pnl[-1]/min( pos.abs().max()*open_trades_df['market1_traded_price'].mean(), capital*0.05)
    else:
        ret_rate_realized = 0
        trade_ret_rate_realized = 0
        print('No Trades happens')
    ret_rate_on_record = cum_df['total_pnl'][-1]/min( pos.abs().max()*open_trades_df['market1_traded_price'].mean(), capital*0.05)
    # 统计 open_trades_df 的各类成交数量和比例
    open_total = open_trades_df.shape[0]
    open_stats = {
        "MM Market1 First Ratio": open_trades_df['is_mm_market1_first'].sum() / open_total if open_total > 0 else 0,
        "MM Market2 First Ratio": open_trades_df['is_mm_market2_first'].sum() / open_total if open_total > 0 else 0,
        "MM Ratio": open_trades_df['is_mm'].sum() / open_total if open_total > 0 else 0,
        "MT Ratio": open_trades_df['is_mt'].sum() / open_total if open_total > 0 else 0,
        "TM Ratio": open_trades_df['is_tm'].sum() / open_total if open_total > 0 else 0,
        "Total Trades": open_total
    }

    # 统计 close_trades_df 的各类成交数量和比例
    description_stats = {
        "Symbol": ccy,
        "Date": st,
        "Open Quantile": open_quantile,
        "Close Quantile": close_quantile,
        "Open SR Threshold": q90_train,
        "Close SR Threshold": q10_train
    }
    close_total = close_trades_df.shape[0]
    close_stats = {
        "MM Market1 First Ratio": close_trades_df['is_mm_market1_first'].sum() / close_total if close_total > 0 else 0,
        "MM Market2 First Ratio": close_trades_df['is_mm_market2_first'].sum() / close_total if close_total > 0 else 0,
        "MM Ratio": close_trades_df['is_mm'].sum() / close_total if close_total > 0 else 0,
        "MT Ratio": close_trades_df['is_mt'].sum() / close_total if close_total > 0 else 0,
        "TM Ratio": close_trades_df['is_tm'].sum() / close_total if close_total > 0 else 0,
        "Total Trades": close_total
    }

    # 计算其他指标
    additional_stats = {
        "Open SR Mean": open_trades_df['sr_open'].mean(),
        "Open SR Real Mean": open_trades_df['sr_open_real'].mean(),
        "Close SR Mean": close_trades_df['sr_close'].mean(),
        "Close SR Real Mean": close_trades_df['sr_close_real'].mean(),
        "Open Slippage Mean": open_trades_df['slippage'].mean(),
        "Open Slippage Std": open_trades_df['slippage'].std(),
        "Close Slippage Mean": close_trades_df['slippage'].mean(),
        "Slippage Std": close_trades_df['slippage'].std(),
        "Total Return Rate (bps)": ret_rate_realized*10000,
        "Trade Return Rate (bps)": trade_ret_rate_realized*10000,
        "Funding return rate (bps)": funding_ret*10000,
        "Total Return Rate on Record (bps)": ret_rate_on_record*10000,
        "Turnover Cycles": turnover
    }

    # 合并所有统计数据
    all_stats = {**description_stats, **open_stats, **close_stats, **additional_stats}

    # 打印表格
    return all_stats    




def run_arbitrage_workflow_mt(
    ccy: str,
    st: str,
    et: str,
    train_st: str,
    train_et: str,
    exchange1: str,
    market1: str,
    exchange2: str,
    market2: str,
    market1_stop_loss_sr_delta = -0.003,
    market2_stop_loss_sr_delta = -0.003,
    open_quantile = 0.95,
    close_quantile = 0.05,
    data_source = "inner_win", #或者ssd
    isBookTicker = False,
    isPlot = True
    ):

    if data_source == 'inner_win':
        funding_okx_csv = f'/Users/rayxu/Desktop/Obentech/fundingRateData/okx/{ccy}-USDT-SWAP.csv'
        funding_binance_csv = f'/Users/rayxu/Desktop/Obentech/fundingRateData/binance/{ccy}USDT.csv'
    elif data_source == 'outer_ssd':
        funding_okx_csv = f'/Volumes/T7/Obentech/fundingRateData/okx/{ccy}-USDT-SWAP.csv'
        funding_binance_csv = f'/Volumes/T7/Obentech/fundingRateData/binance/{ccy}USDT.csv' 

    df_okx      = process_funding_time(funding_okx_csv,  exchange='okx')
    df_binance  = process_funding_time(funding_binance_csv,  exchange='binance')

    fr_okx      = funding_df_to_series(df_okx)       # OKX
    fr_binance  = funding_df_to_series(df_binance)   # Binance




    if not isBookTicker:
        cf_depth = read_cf_depth(ccy, st, et, exchange1, market1, exchange2, market2, data_source="nuts_mm")
        cf_depth_train = read_cf_depth(ccy, train_st, train_et, exchange1, market1, exchange2, market2, data_source=data_source)
    else:
        cf_depth = read_bookTicker_data(ccy, st, et, exchange1, market1, exchange2, market2, data_source="nuts_mm")
        cf_depth_train = read_bookTicker_data(ccy, train_st, train_et, exchange1, market1, exchange2, market2, data_source=data_source)

    spread_train = ((cf_depth_train['market2_bid_price0'] / cf_depth_train['market1_bid_price0']) - 1) * 10000

    mean_price =0.5*(cf_depth_train['market2_bid_price0'].dropna().mean() + cf_depth_train['market1_bid_price0'].dropna().mean())

    capital = 10000
    single_order_amount = 100
    single_order_volume = 100/ mean_price
    notional = single_order_volume
    pos_limit = capital/(2*single_order_amount)


    q10_train = spread_train.quantile(close_quantile)
    q90_train = spread_train.quantile(open_quantile)

    print(f'thresholds: {q10_train},{q90_train}')
    print(f'pos_limit: {pos_limit}')

    target_open_sr = q90_train/10000
    target_close_sr = q10_train/10000

    # 设置为负的，全部用MT对冲


    open_tick_num = 1
    open_replace_tick_num = 1
    close_tick_num = 1
    close_replace_tick_num = 1
    taker_traded_delay_seconds = 0.02
    maker_ordered_delay_seconds = 0.01


    mt_params = {'open_tick_num':open_tick_num, 'open_replace_tick_num':open_replace_tick_num,
                'close_tick_num':close_tick_num, 'close_replace_tick_num':close_replace_tick_num, 
                'taker_traded_delay_seconds':taker_traded_delay_seconds, 'maker_ordered_delay_seconds':maker_ordered_delay_seconds,
                'market1_stop_loss_sr_delta':market1_stop_loss_sr_delta, 'market2_stop_loss_sr_delta':market2_stop_loss_sr_delta,
                'target_open_sr':target_open_sr, 'target_close_sr':target_close_sr,
                'exchange1_type':exchange1, 'market1_type':market1, 'exchange2_type':exchange2, 'market2_type':market2}
    
    if isPlot:
        plot_spread_and_funding_combined(cf_depth=cf_depth,market2_price_col='market2_bid_price0',market1_price_col='market1_bid_price0',fr_okx=fr_okx,fr_binance=fr_binance,ccy=ccy,q_10_bt=target_close_sr*1e4,q_90_bt=target_open_sr*1e4)
    open_trades_df, close_trades_df = maker_maker_backtest_0605(cf_depth, mt_params,pos_limit= pos_limit)
    open_trades_df['is_market1_maker'] = (open_trades_df['market1_traded_type'] == 'market1_maker')
    open_trades_df['is_market2_taker'] = (open_trades_df['market2_traded_type'] == 'market2_taker')

    close_trades_df['is_market1_maker'] = (close_trades_df['market1_traded_type'] == 'market1_maker')
    close_trades_df['is_market2_taker'] = (close_trades_df['market2_traded_type'] == 'market2_taker')

    open_trades_df['is_tt'] = ((~open_trades_df['is_market1_maker']) & open_trades_df['is_market2_taker'])
    open_trades_df['is_mt'] = (open_trades_df['is_market1_maker'] & open_trades_df['is_market2_taker'])

    close_trades_df['is_tt'] = ((~close_trades_df['is_market1_maker']) & close_trades_df['is_market2_taker'])
    close_trades_df['is_mt'] = (close_trades_df['is_market1_maker'] & close_trades_df['is_market2_taker'])


    open_trades_df['market1_open_time'] = pd.to_datetime(open_trades_df['market1_open_time'])
    open_trades_df['market2_open_time'] = pd.to_datetime(open_trades_df['market2_open_time'])

    close_trades_df['market1_close_time'] = pd.to_datetime(close_trades_df['market1_close_time'])
    close_trades_df['market2_close_time'] = pd.to_datetime(close_trades_df['market2_close_time'])

    open_trades_df['fee_market1'] = -0.00003*open_trades_df['is_market1_maker'] + 0.00022*(1-open_trades_df['is_market1_maker'])
    open_trades_df['fee_market2'] = 0*(1-open_trades_df['is_market2_taker']) + 0.0002*(open_trades_df['is_market2_taker'])

    close_trades_df['fee_market1'] = -0.00003*close_trades_df['is_market1_maker'] + 0.00022*(1-close_trades_df['is_market1_maker'])
    close_trades_df['fee_market2'] = 0*(1-close_trades_df['is_market2_taker']) + 0.0002*(close_trades_df['is_market2_taker'])


    open_trades_df['slippage'] = open_trades_df['sr_open_real']-open_trades_df['sr_open']
    close_trades_df['slippage'] = close_trades_df['sr_close']-close_trades_df['sr_close_real']

    open_trades_df['slippage_with_fee'] = open_trades_df['slippage'] - open_trades_df['fee_market1'] - open_trades_df['fee_market2']
    close_trades_df['slippage_with_fee'] = close_trades_df['slippage'] - close_trades_df['fee_market1'] - close_trades_df['fee_market2']

    # 合并表格
    open_trades_df['side'] = 'open'
    close_trades_df['side'] = 'close'



    trades_df = pd.concat([open_trades_df, close_trades_df]).sort_index()

    # 计算 PnL

    # trade_pnl, funding_pnl, cum_pnl, pos = compute_dynamic_fifo_pnl_correct_fee_with_funding(trades_df,funding_okx_csv,funding_binance_csv)
    # trade_pnl_2, funding_pnl_2, cum_pnl_2, pos, cum_df = compute_real_time_spread_pnl_correct_fee_with_funding(trades_df, funding_okx_csv,funding_binance_csv)
    trade_pnl, funding_pnl, cum_pnl, pos = compute_dynamic_fifo_pnl_correct_fee_with_funding(trades_df,funding_okx_csv,funding_binance_csv, notional=notional)
    trade_pnl_2, funding_pnl_2, cum_pnl_2, pos, cum_df = compute_real_time_spread_pnl_correct_fee_with_funding(trades_df, funding_okx_csv,funding_binance_csv, notional=notional)

    turnover = turnover_cycles(pos,pos_limit)


    if isPlot:
        # 假设 cum_pnl, cum_pnl2, cum_df 已经在环境中定义
        fig, axes = plt.subplots(1, 3, figsize=(24, 4))  # 宽×高 = 24×4


        # 1) Cumulative PnL
        axes[0].plot(cum_pnl.index, cum_pnl.values,
                    marker='o', linestyle='-',
                    color='royalblue', label='Cumulative PnL')

        axes[0].plot(
            funding_pnl.index, funding_pnl.values,
            marker='x', linestyle='--',
            color='indianred', label='Funding Cumulative PnL'
        )

        axes[0].plot(
            trade_pnl.index, trade_pnl.values,
            marker='o', linestyle='-',
            color='pink', label='Total Trade PnL'
        )
        x = pd.to_datetime(cum_pnl.index)
        y = pd.to_numeric(cum_pnl.values, errors='coerce')  # 强制转换成 float，无法转换的设为 NaN
        mask = ~np.isnan(y)  # 去掉 NaN，避免画图报错
        axes[0].fill_between(x[mask], y[mask],
                            color='royalblue', alpha=0.1)
        # axes[0].fill_between(cum_pnl.index, cum_pnl.values,
        #                     color='royalblue', alpha=0.1)
        axes[0].set_title(f'{ccy} Cumulative PnL on {st}', fontsize=16, fontweight='bold')
        axes[0].set_xlabel('Time', fontsize=14)
        axes[0].set_ylabel('Cumulative PnL', fontsize=14)
        axes[0].grid(color='gray', linestyle='--', linewidth=0.5, alpha=0.7)
        axes[0].legend(fontsize=12)
        axes[0].tick_params(axis='both', labelsize=12)

        # 2) Cumulative PnL 2
        axes[1].plot(cum_pnl_2.index, cum_pnl_2.values,
                    marker='o', linestyle='-',
                    color='darkorange', label='Cumulative PnL 2')
        axes[1].fill_between(cum_pnl_2.index, cum_pnl_2.values,
                            color='darkorange', alpha=0.1)
        axes[1].set_title(f'{ccy} Real‐Time Spread PnL on {st}', fontsize=16, fontweight='bold')
        axes[1].set_xlabel('Time', fontsize=14)
        axes[1].set_ylabel('Cumulative PnL 2', fontsize=14)
        axes[1].grid(color='gray', linestyle='--', linewidth=0.5, alpha=0.7)
        axes[1].legend(fontsize=12)
        axes[1].tick_params(axis='both', labelsize=12)

        # 3) Position
        axes[2].plot(cum_df.index, cum_df['position'],
                    marker='o', linestyle='-',
                    color='seagreen', label='Position')
        axes[2].set_title(f'{ccy} Position on {st}', fontsize=16, fontweight='bold')
        axes[2].set_xlabel('Time', fontsize=14)
        axes[2].set_ylabel('Position', fontsize=14)
        axes[2].grid(color='gray', linestyle='--', linewidth=0.5, alpha=0.7)
        axes[2].legend(fontsize=12)
        axes[2].tick_params(axis='both', labelsize=12)

        plt.tight_layout()
        plt.show()

    if len(funding_pnl) > 0:
        funding_ret = funding_pnl[-1] / min(pos.abs().max() * open_trades_df['market1_traded_price'].mean(), capital * 0.05)
    else:
        funding_ret = 0  
    if len(cum_pnl) != 0:
        ret_rate_realized = cum_pnl[-1]/min( pos.abs().max()*open_trades_df['market1_traded_price'].mean(), capital*0.05)
        trade_ret_rate_realized = trade_pnl[-1]/min( pos.abs().max()*open_trades_df['market1_traded_price'].mean(), capital*0.05)
    else:
        ret_rate_realized = 0
        trade_ret_rate_realized = 0
        print('No Trades happens')
    ret_rate_on_record = cum_df['total_pnl'][-1]/min( pos.abs().max()*open_trades_df['market1_traded_price'].mean(), capital*0.05)
    # 统计 open_trades_df 的各类成交数量和比例
    open_total = open_trades_df.shape[0]
    open_stats = {
        "Open Trades": open_total
    }

    # 统计 close_trades_df 的各类成交数量和比例
    description_stats = {
        "Symbol": ccy,
        "Date": st,
        "Open Quantile": open_quantile,
        "Close Quantile": close_quantile
    }
    close_total = close_trades_df.shape[0]
    close_stats = {
        "Total Trades": close_total
    }


    d1 = pd.to_datetime(et)
    d2 = pd.to_datetime(st)

    days = 1 + (d1 - d2).total_seconds() / 86400


    # 计算其他指标
    additional_stats = {
        "Open SR Mean": open_trades_df['sr_open'].mean(),
        "Open SR Real Mean": open_trades_df['sr_open_real'].mean(),
        "Close SR Mean": close_trades_df['sr_close'].mean(),
        "Close SR Real Mean": close_trades_df['sr_close_real'].mean(),
        "Open Slippage Mean": open_trades_df['slippage'].mean(),
        "Open Slippage Std": open_trades_df['slippage'].std(),
        "Close Slippage Mean": close_trades_df['slippage'].mean(),
        "Slippage Std": close_trades_df['slippage'].std(),
        "Open Slippage With Fee Mean": open_trades_df['slippage_with_fee'].mean(),
        "Open Slippage With Fee Std": open_trades_df['slippage_with_fee'].std(),
        "Close Slippage With Fee Mean": close_trades_df['slippage_with_fee'].mean(),
        "Close Slippage With Fee Std": close_trades_df['slippage_with_fee'].std(),
        "Total Return Rate (Annualized%)": ret_rate_realized*365*100/days,
        "Trade Return Rate (Annualized%)": trade_ret_rate_realized*365*100/days,
        "Funding return rate (Annualized%)": funding_ret*365*100/days,
        "Total Return Rate on Record (Annualized%)": ret_rate_on_record*365*100/days,
        "Turnover Cycles": turnover/days
    }

    # 合并所有统计数据
    all_stats = {**description_stats, **close_stats, **additional_stats}
    # 打印表格
    return all_stats    


# 2025-06-06 只能market1 maker， market2 taker，并且滚动回测。
def run_arbitrage_workflow_MT(
    ccy: str,
    st: str,
    et: str,
    train_st: str,
    train_et: str,
    exchange1: str,
    market1: str,
    exchange2: str,
    market2: str,
    market1_stop_loss_sr_delta = -0.003,
    market2_stop_loss_sr_delta = -0.003,
    open_quantile = 0.95,
    close_quantile = 0.05,
    data_source = "outer_ssd",
    open_replace_tick_num = 5,
    close_replace_tick_num = 5,
    open_tick_num = 2,
    close_tick_num = 2
    ):
    if data_source == 'inner_win':
        funding_okx_csv = f'/Users/rayxu/Desktop/Obentech/fundingRateData/okx/{ccy}-USDT-SWAP.csv'
        funding_binance_csv = f'/Users/rayxu/Desktop/Obentech/fundingRateData/binance/{ccy}USDT.csv'
    elif data_source == 'outer_ssd':
        funding_okx_csv = f'/Volumes/T7/Obentech/fundingRateData/okx/{ccy}-USDT-SWAP.csv'
        funding_binance_csv = f'/Volumes/T7/Obentech/fundingRateData/binance/{ccy}USDT.csv'        

    df_okx      = process_funding_time(funding_okx_csv,  exchange='okx')
    df_binance  = process_funding_time(funding_binance_csv,  exchange='binance')

    fr_okx      = funding_df_to_series(df_okx)       # OKX
    fr_binance  = funding_df_to_series(df_binance)   # Binance



    time_index = "local_time"
    cf_depth = read_cf_depth(ccy, train_st, et, exchange1, market1, exchange2, market2, data_source=data_source)


    # spread_train = ((cf_depth_train['market2_bid_price0'] / cf_depth_train['market1_bid_price0']) - 1) * 10000
    # capital = 0
    mean_price =0.5*(cf_depth['market2_bid_price0'].dropna().mean() + cf_depth['market1_bid_price0'].dropna().mean())

    capital = 4000
    single_order_amount = 100
    single_order_volume = 100/ mean_price
    notional = single_order_volume
    pos_limit = capital/(2*single_order_amount)
    look_back_days = 3
    z_threshold = 1.64


    param_df = generate_param_df(cf_depth,df_okx, df_binance,ccy,look_back_days,z_threshold,freq= '1D')

    # 让cf_depth的日期大于等于st 加上16小时
    cf_depth = cf_depth[cf_depth.index >= pd.to_datetime(st) - pd.Timedelta(hours=8)]
    cf_depth = cf_depth[~cf_depth.index.duplicated(keep='last')]

    taker_traded_delay_seconds = 0.02
    maker_ordered_delay_seconds = 0.01


    mt_params = {'open_tick_num':open_tick_num, 'open_replace_tick_num':open_replace_tick_num,
                'close_tick_num':close_tick_num, 'close_replace_tick_num':close_replace_tick_num, 
                'taker_traded_delay_seconds':taker_traded_delay_seconds, 'maker_ordered_delay_seconds':maker_ordered_delay_seconds,
                'market1_stop_loss_sr_delta':market1_stop_loss_sr_delta, 'market2_stop_loss_sr_delta':market2_stop_loss_sr_delta,
                'exchange1_type':exchange1, 'market1_type':market1, 'exchange2_type':exchange2, 'market2_type':market2}

    open_trades_df, close_trades_df = maker_maker_backtest_rolling_0605(cf_depth, mt_params,param_df,pos_limit= pos_limit)

    open_trades_df['is_market1_maker'] = (open_trades_df['market1_traded_type'] == 'market1_maker')
    open_trades_df['is_market2_maker'] = (open_trades_df['market2_traded_type'] == 'market2_maker')
    close_trades_df['is_market1_maker'] = (close_trades_df['market1_traded_type'] == 'market1_maker')
    close_trades_df['is_market2_maker'] = (close_trades_df['market2_traded_type'] == 'market2_maker')

    open_trades_df['fee_market1'] = -0.00003*open_trades_df['is_market1_maker'] + 0.00022*(1-open_trades_df['is_market1_maker'])
    open_trades_df['fee_market2'] = 0*open_trades_df['is_market2_maker'] + 0.0002*(1-open_trades_df['is_market2_maker'])
    close_trades_df['fee_market1'] = -0.00003*close_trades_df['is_market1_maker'] + 0.00022*(1-close_trades_df['is_market1_maker'])
    close_trades_df['fee_market2'] = 0*close_trades_df['is_market2_maker'] + 0.0002*(1-close_trades_df['is_market2_maker'])


    open_trades_df['slippage'] = open_trades_df['sr_open_real']-open_trades_df['sr_open']
    close_trades_df['slippage'] = close_trades_df['sr_close']-close_trades_df['sr_close_real']
    print(open_trades_df.shape[0])
    print(close_trades_df.shape[0])


    # 合并表格
    open_trades_df['side'] = 'open'
    close_trades_df['side'] = 'close'


    trades_df = pd.concat([open_trades_df, close_trades_df]).sort_index()

    # 计算 PnL
    end_time = pd.to_datetime(et) + pd.Timedelta(hours=16)
    trade_pnl, funding_pnl, cum_pnl, pos = compute_dynamic_fifo_pnl_correct_fee_with_funding(trades_df,funding_okx_csv,funding_binance_csv, notional=notional,end_time = end_time)
    trade_pnl_2, funding_pnl_2, cum_pnl_2, pos, cum_df = compute_real_time_spread_pnl_correct_fee_with_funding(trades_df, funding_okx_csv,funding_binance_csv, notional=notional,end_time=end_time)


    turnover = turnover_cycles(pos,pos_limit)




    # 假设 cum_pnl, cum_pnl2, cum_df 已经在环境中定义
    fig, axes = plt.subplots(1, 3, figsize=(24, 4))  # 宽×高 = 24×4


    # 1) Cumulative PnL
    axes[0].plot(cum_pnl.index, cum_pnl.values,
                marker='o', linestyle='-',
                color='royalblue', label='Cumulative PnL')

    axes[0].plot(
        funding_pnl.index, funding_pnl.values,
        marker='x', linestyle='--',
        color='indianred', label='Funding Cumulative PnL'
    )

    axes[0].plot(
        trade_pnl.index, trade_pnl.values,
        marker='o', linestyle='-',
        color='pink', label='Total Trade PnL'
    )
    x = pd.to_datetime(cum_pnl.index)
    y = pd.to_numeric(cum_pnl.values, errors='coerce')  # 强制转换成 float，无法转换的设为 NaN
    mask = ~np.isnan(y)  # 去掉 NaN，避免画图报错
    axes[0].fill_between(x[mask], y[mask],
                        color='royalblue', alpha=0.1)
    # axes[0].fill_between(cum_pnl.index, cum_pnl.values,
    #                     color='royalblue', alpha=0.1)
    axes[0].set_title(f'{ccy} Cumulative PnL on {st}', fontsize=16, fontweight='bold')
    axes[0].set_xlabel('Time', fontsize=14)
    axes[0].set_ylabel('Cumulative PnL', fontsize=14)
    axes[0].grid(color='gray', linestyle='--', linewidth=0.5, alpha=0.7)
    axes[0].legend(fontsize=12)
    axes[0].tick_params(axis='both', labelsize=12)

    # 2) Cumulative PnL 2
    axes[1].plot(cum_pnl_2.index, cum_pnl_2.values,
                marker='o', linestyle='-',
                color='darkorange', label='Cumulative PnL 2')
    axes[1].fill_between(cum_pnl_2.index, cum_pnl_2.values,
                        color='darkorange', alpha=0.1)
    axes[1].set_title(f'{ccy} Real‐Time Spread PnL on {st}', fontsize=16, fontweight='bold')
    axes[1].set_xlabel('Time', fontsize=14)
    axes[1].set_ylabel('Cumulative PnL 2', fontsize=14)
    axes[1].grid(color='gray', linestyle='--', linewidth=0.5, alpha=0.7)
    axes[1].legend(fontsize=12)
    axes[1].tick_params(axis='both', labelsize=12)

    # 3) Position
    axes[2].plot(cum_df.index, cum_df['position'],
                marker='o', linestyle='-',
                color='seagreen', label='Position')
    axes[2].set_title(f'{ccy} Position on {st}', fontsize=16, fontweight='bold')
    axes[2].set_xlabel('Time', fontsize=14)
    axes[2].set_ylabel('Position', fontsize=14)
    axes[2].grid(color='gray', linestyle='--', linewidth=0.5, alpha=0.7)
    axes[2].legend(fontsize=12)
    axes[2].tick_params(axis='both', labelsize=12)

    plt.tight_layout()
    plt.show()

    if len(funding_pnl) > 0:
        funding_ret = funding_pnl[-1] / capital
    else:
        funding_ret = 0  
    if len(cum_pnl) != 0:
        ret_rate_realized = cum_pnl[-1]/capital
        trade_ret_rate_realized = trade_pnl[-1]/capital
    else:
        ret_rate_realized = 0
        trade_ret_rate_realized = 0
        print('No Trades happens')
    ret_rate_on_record = cum_df['total_pnl'][-1]/capital
    # 统计 open_trades_df 的各类成交数量和比例
    open_total = open_trades_df.shape[0]
    open_stats = {
        "Open Trades": open_total
    }

    # 统计 close_trades_df 的各类成交数量和比例
    description_stats = {
        "Symbol": ccy,
        "Date": st,
        "Open Quantile": open_quantile,
        "Close Quantile": close_quantile
    }
    close_total = close_trades_df.shape[0]
    close_stats = {
        "Total Trades": close_total
    }


    d1 = pd.to_datetime(et)
    d2 = pd.to_datetime(st)

    days = 1 + (d1 - d2).total_seconds() / 86400


    # 计算其他指标
    additional_stats = {
        "Open SR Mean": open_trades_df['sr_open'].mean(),
        "Open SR Real Mean": open_trades_df['sr_open_real'].mean(),
        "Close SR Mean": close_trades_df['sr_close'].mean(),
        "Close SR Real Mean": close_trades_df['sr_close_real'].mean(),
        "Open Slippage Mean": open_trades_df['slippage'].mean(),
        "Open Slippage Std": open_trades_df['slippage'].std(),
        "Close Slippage Mean": close_trades_df['slippage'].mean(),
        "Slippage Std": close_trades_df['slippage'].std(),
        "Total Return Rate (Annualized%)": ret_rate_realized*365*100/days,
        "Trade Return Rate (Annualized%)": trade_ret_rate_realized*365*100/days,
        "Funding return rate (Annualized%)": funding_ret*365*100/days,
        "Total Return Rate on Record (Annualized%)": ret_rate_on_record*365*100/days,
        "Turnover Cycles": turnover/days
    }

    # 合并所有统计数据
    all_stats = {**description_stats, **close_stats, **additional_stats}

    return all_stats, cum_pnl, trade_pnl, funding_pnl



