import json
import requests
import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Tuple
from abc import ABC, abstractmethod

class DataLoader:
    """Utility class for loading and preparing market data"""
    
    @staticmethod
    def load_okx_data(filepath: str) -> pd.DataFrame:
        """Load and prepare OKX data from JSON file"""
        with open(filepath, 'r') as f:
            raw = json.load(f)
        
        return pd.concat(
            [pd.json_normalize(v).assign(symbol=k) for k, v in raw.items()],
            ignore_index=True
        )
    
    @staticmethod
    def load_binance_data(filepath: str) -> pd.DataFrame:
        """Load Binance data from JSON file"""
        return pd.read_json(filepath)
    
    @staticmethod
    def load_bybit_data(filepath: str) -> pd.DataFrame:
        """Load Bybit data from JSON file (top-level is a list of dicts)"""
        return pd.read_json(filepath)

    @staticmethod
    def parse_universe(universe_str: str) -> List[str]:
        """Parse universe string into list of symbols with USDT suffix"""
        return [f"{s}USDT" for s in universe_str.split(',')]

class OKXAPI:
    
    BASE_URL = "https://www.okx.com/api/v5"
    
    def __init__(self, timeout: int = 20):
        self.timeout = timeout
    
    def _make_request(self, url: str, params: Optional[Dict] = None) -> Dict:
        try:
            response = requests.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            raise RuntimeError(f"API request failed: {e}")
    
    def fetch_prices(self) -> Dict[str, float]:
        """Fetch all SWAP contract prices"""
        url = f"{self.BASE_URL}/market/tickers"
        data = self._make_request(url, params={"instType": "SWAP"})
        
        if data.get("code") != "0":
            raise RuntimeError(f"OKX API error: {data}")
        
        return {
            row["instId"].replace("-SWAP", ""): float(row["last"])
            for row in data["data"]
        }
    
    def fetch_contract_values(self) -> Dict[str, float]:
        """Fetch contract values (ctVal) for all SWAP contracts"""
        url = f"{self.BASE_URL}/public/instruments"
        data = self._make_request(url, params={"instType": "SWAP"})
        
        if data.get("code") != "0":
            raise RuntimeError(f"OKX API error: {data}")
        
        return {
            row["instId"].replace("-SWAP", ""): float(row["ctVal"])
            for row in data["data"]
        }

class MMRCalculator(ABC):
    """Abstract base class for MMR calculations"""
    
    def __init__(self, capital: float, position_limit_multiplier: float = 0.04):
        self.capital = capital
        self.pos_limit = capital * position_limit_multiplier
    
    @abstractmethod
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate real MMR for given data"""
        pass
    
    def _validate_dataframe(self, df: pd.DataFrame, required_columns: List[str]) -> None:
        """Validate DataFrame has required columns"""
        missing = set(required_columns) - set(df.columns)
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

class OKXMMRCalculator(MMRCalculator):
    """MMR Calculator for OKX Exchange"""
    
    def __init__(self, capital: float, position_limit_multiplier:float = 0.04, api: Optional[OKXAPI] = None, leverage_price_buffer: float = 0.15):
        super().__init__(capital,position_limit_multiplier)
        self.api = api or OKXAPI()
        self.leverage_price_buffer = leverage_price_buffer
    
    def calculate(self, df_mmr: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate real MMR for OKX contracts
        
        Args:
            df_mmr: DataFrame with columns ['symbol', 'minSz', 'maxSz', 'mmr', 'maxLever', 'tier']
        
        Returns:
            DataFrame with columns ['symbol', 'pos_limit', 'real_MMR_okx', 'maxLever']
        """
        self._validate_dataframe(df_mmr, ['symbol', 'minSz', 'maxSz', 'mmr'])
        
        # Fetch market data
        ctval_map = self.api.fetch_contract_values()
        price_map = self.api.fetch_prices()
        
        # Filter valid symbols
        df = self._filter_valid_symbols(df_mmr, ctval_map, price_map)
        if df.empty:
            return self._empty_result()
        
        # Prepare data
        df = self._prepare_data(df, ctval_map, price_map)
        
        # Calculate MMR
        real_mmr = self._calculate_mmr(df)
        
        # Get max leverage
        max_lever = self._get_max_leverage(df)
        
        return self._format_results(real_mmr, max_lever)
    
    def _filter_valid_symbols(
        self, 
        df: pd.DataFrame, 
        ctval_map: Dict[str, float], 
        price_map: Dict[str, float]
    ) -> pd.DataFrame:
        """Filter symbols with available market data"""
        symbols = df["symbol"].unique()
        valid_symbols = [
            s for s in symbols 
            if s in ctval_map and s in price_map
        ]
        return df[df["symbol"].isin(valid_symbols)].copy()
    
    def _prepare_data(
        self, 
        df: pd.DataFrame, 
        ctval_map: Dict[str, float], 
        price_map: Dict[str, float]
    ) -> pd.DataFrame:
        """Prepare data for MMR calculation"""
        df["ctVal"] = df["symbol"].map(ctval_map)
        df["price"] = df["symbol"].map(price_map)
        
        # Convert to numeric
        numeric_cols = ["minSz", "maxSz", "mmr", "maxLever"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        
        # Drop rows with missing critical data
        df = df.dropna(subset=["minSz", "maxSz", "mmr", "ctVal", "price"])
        
        # Calculate notional values
        df["notionalFloor"] = df["minSz"] * df["ctVal"] * df["price"]
        df["notionalCap"] = df["maxSz"] * df["ctVal"] * df["price"]
        
        return df
    
    def _calculate_mmr(self, df: pd.DataFrame) -> pd.Series:
        """Calculate real MMR using vectorized operations"""
        upper = np.minimum(df["notionalCap"].values, self.pos_limit)
        notion = np.clip(upper - df["notionalFloor"].values, 0, None)
        df["margin_contrib"] = notion * df["mmr"].values
        
        total_margin = df.groupby("symbol", observed=True)["margin_contrib"].sum()
        return total_margin / self.pos_limit
    
    def _get_max_leverage(self, df: pd.DataFrame) -> pd.Series:
        """Get maximum leverage for each symbol"""
        if "maxLever" not in df.columns:
            return pd.Series(dtype=float)
        
        stress_pos_limit = self.pos_limit /(1.0-self.leverage_price_buffer)
        # Symbols where pos_limit falls within a bracket
        hit_mask = (
            (stress_pos_limit > df["notionalFloor"]) & 
            (stress_pos_limit <= df["notionalCap"])
        )
        df_hit = (
            df[hit_mask]
            .sort_values(["symbol", "notionalCap"])
            .groupby("symbol", as_index=False)
            .tail(1)[["symbol", "maxLever"]]
        )
        
        # Symbols where pos_limit exceeds all brackets
        df_reached = (
            df.sort_values(["symbol", "notionalCap"])
            .groupby("symbol", as_index=False)
            .tail(1)[["symbol", "maxLever"]]
        )
        
        lever_map = df_reached.set_index("symbol")["maxLever"]
        if not df_hit.empty:
            lever_map.update(df_hit.set_index("symbol")["maxLever"])
        
        return lever_map
    
    def _format_results(
        self, 
        real_mmr: pd.Series, 
        max_lever: pd.Series
    ) -> pd.DataFrame:
        """Format final results"""
        return pd.DataFrame({
            "symbol": real_mmr.index.str.replace("-", "", regex=False),
            "pos_limit": self.pos_limit,
            "real_MMR_okx": real_mmr.values,
            "maxLever": max_lever.reindex(real_mmr.index).values
        })
    
    def _empty_result(self) -> pd.DataFrame:
        """Return empty result DataFrame"""
        return pd.DataFrame(
            columns=["symbol", "pos_limit", "real_MMR_okx", "maxLever"]
        )

class BinanceMMRCalculator(MMRCalculator):
    """MMR Calculator for Binance Exchange"""
    
    def calculate(self, df_mmr: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate real MMR for Binance contracts
        
        Args:
            df_mmr: DataFrame with 'data' column containing bracket information
        
        Returns:
            DataFrame with MMR results
        """
        self._validate_dataframe(df_mmr, ['data'])
        
        df = self._expand_brackets(df_mmr)
        df = self._filter_position_limit(df)
        df["real_MMR_bn"] = self._calculate_real_mmr(df)
        
        return df.rename(columns={'initialLeverage': 'maxLever_binance'})
    
    def _expand_brackets(self, df: pd.DataFrame) -> pd.DataFrame:
        """Expand nested bracket data"""
        df = df.copy()
        df['symbol'] = df['data'].apply(lambda x: x['symbol'])
        df["brackets"] = df["data"].apply(lambda x: x["brackets"])
        df = df[["symbol", "brackets"]].explode("brackets")
        
        # Extract bracket fields
        bracket_fields = [
            "notionalFloor", "notionalCap", 
            "maintMarginRatio", "cum", "initialLeverage"
        ]
        for field in bracket_fields:
            df[field] = df["brackets"].apply(lambda x: x[field])
        
        df = df.drop(columns=["brackets"])
        df['pos_limit'] = self.pos_limit
        
        return df
    
    def _filter_position_limit(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter brackets relevant to position limit"""
        return df[
            (df["pos_limit"] >= df["notionalFloor"]) &
            (df["pos_limit"] < df["notionalCap"])
        ]
    
    def _calculate_real_mmr(self, df: pd.DataFrame) -> pd.Series:
        """Calculate real MMR"""
        return np.where(
            df["pos_limit"] > 0,
            (df["pos_limit"] * df["maintMarginRatio"] - df["cum"]) / df["pos_limit"],
            1
        )

class BybitMMRCalculator(MMRCalculator):
    """MMR Calculator for Bybit Exchange (riskLimitValue unit = USDT notional cap)"""

    def calculate(self, df_mmr: pd.DataFrame) -> pd.DataFrame:
        self._validate_dataframe(df_mmr, ["symbol", "maintenanceMargin", "maxLeverage", "riskLimitValue"])

        df = df_mmr.copy()

        # numeric conversion (strings -> floats)
        for col in ["maintenanceMargin", "maxLeverage", "riskLimitValue", "mmDeduction", "id"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # mmDeduction can be empty string -> NaN, treat as 0
        if "mmDeduction" not in df.columns:
            df["mmDeduction"] = 0.0
        df["mmDeduction"] = df["mmDeduction"].fillna(0.0)

        # build brackets
        df = df.sort_values(["symbol", "riskLimitValue", "id"])
        df["notionalCap"] = df["riskLimitValue"]
        df["notionalFloor"] = df.groupby("symbol")["notionalCap"].shift(1).fillna(0.0)

        pos = float(self.pos_limit)
        hit = df[(pos > df["notionalFloor"]) & (pos <= df["notionalCap"])].copy()

        if hit.empty:
            # pos_limit exceeds all caps -> take last bracket per symbol
            hit = (df.sort_values(["symbol", "notionalCap"])
                     .groupby("symbol", as_index=False)
                     .tail(1)
                     .copy())
        else:
            # if multiple match (shouldn't), take smallest cap
            hit = (hit.sort_values(["symbol", "notionalCap"])
                     .groupby("symbol", as_index=False)
                     .head(1)
                     .copy())

        hit["pos_limit"] = pos
        hit["real_MMR_bybit"] = np.where(
            hit["pos_limit"] > 0,
            (hit["pos_limit"] * hit["maintenanceMargin"] - hit["mmDeduction"]) / hit["pos_limit"],
            1.0
        )

        return hit[["symbol", "pos_limit", "real_MMR_bybit", "maxLeverage"]].rename(
            columns={"maxLeverage": "maxLever_bybit"}
        )

class PairMultiPositionMMRAnalyzer:
    SUPPORTED = {"okx", "binance", "bybit"}

    def __init__(self, position_limits: List[float]):
        self.position_limits = position_limits

    def _calc_one_exchange(self, exchange: str, pos_limit: float, raw_df: pd.DataFrame) -> pd.DataFrame:
        exchange = exchange.lower()
        if exchange not in self.SUPPORTED:
            raise ValueError(f"Unsupported exchange={exchange}, supported={self.SUPPORTED}")

        if exchange == "okx":
            calc = OKXMMRCalculator(pos_limit, position_limit_multiplier=1.0)
            out = calc.calculate(raw_df)  # symbol,pos_limit,real_MMR_okx,maxLever
            out = out.rename(columns={"maxLever": "maxLever_okx"})
            return out[["symbol", "pos_limit", "real_MMR_okx", "maxLever_okx"]]

        if exchange == "binance":
            calc = BinanceMMRCalculator(pos_limit, position_limit_multiplier=1.0)
            out = calc.calculate(raw_df)  # symbol,pos_limit,real_MMR_bn,maxLever_binance,....
            # 只保留关心列
            keep = [c for c in ["symbol", "pos_limit", "real_MMR_bn", "maxLever_binance"] if c in out.columns]
            return out[keep]

        if exchange == "bybit":
            calc = BybitMMRCalculator(pos_limit, position_limit_multiplier=1.0)
            out = calc.calculate(raw_df)  # symbol,pos_limit,real_MMR_bybit,maxLever_bybit
            return out[["symbol", "pos_limit", "real_MMR_bybit", "maxLever_bybit"]]

        raise RuntimeError("unreachable")

    def analyze(
        self,
        exchange_left: str,
        df_left: pd.DataFrame,
        exchange_right: str,
        df_right: pd.DataFrame,
        universe: List[str],
        force_btc_eth_lev10: bool = True,
    ) -> pd.DataFrame:
        """
        universe: list like ["BTCUSDT","ETHUSDT",...]
        输出只包含这两个交易所的列 + average_real_MMR
        """
        base = pd.DataFrame({"symbol": sorted(set(universe))})
        all_results = []

        for pos_limit in self.position_limits:
            print(f"[{exchange_left}-{exchange_right}] pos_limit={pos_limit:,.0f}")

            left_out  = self._calc_one_exchange(exchange_left,  pos_limit, df_left)
            right_out = self._calc_one_exchange(exchange_right, pos_limit, df_right)

            # left merge on universe base -> 保证每个 symbol 都在
            merged = base.copy()
            merged = merged.merge(left_out.drop(columns=["pos_limit"]),  on="symbol", how="left")
            merged = merged.merge(right_out.drop(columns=["pos_limit"]), on="symbol", how="left")
            merged["pos_limit"] = pos_limit

            # average 只用这两个交易所里有的 real_MMR 列
            mmr_cols = [c for c in merged.columns if c.startswith("real_MMR_")]
            merged["average_real_MMR"] = merged[mmr_cols].mean(axis=1, skipna=True)

            all_results.append(merged)

        final_df = pd.concat(all_results, ignore_index=True)

        # 排序
        final_df = final_df.sort_values(["symbol", "pos_limit"])

        # 强制 BTC/ETH 杠杆=10：只作用在当前 pair 的 maxLever 列
        if force_btc_eth_lev10:
            special_symbols = {"BTCUSDT", "ETHUSDT", "BTC-USDT", "ETH-USDT"}
            mask = final_df["symbol"].isin(special_symbols)
            for col in [c for c in final_df.columns if c.startswith("maxLever_")]:
                final_df.loc[mask, col] = 10

        # 列顺序：symbol,pos_limit, maxLever*, real_MMR*, average
        max_cols = [c for c in final_df.columns if c.startswith("maxLever_")]
        mmr_cols = [c for c in final_df.columns if c.startswith("real_MMR_")]
        ordered = ["symbol", "pos_limit"] + max_cols + mmr_cols + ["average_real_MMR"]
        final_df = final_df[ordered]

        return final_df



if __name__ == "__main__":
    from common_utils.CONFIG import *
    verified_session, verified_cookies = login(prefix='mmadmin')
    verified_session1, verified_cookies1 = login(prefix='mmadminjp3')    


# BN 原始数据
    headers = {
        'Content-Type': 'application/json'
    }
    env = 'bn_v5_pm_pro26'
    url = f'https://mmadminjp3.digifinex.org/api/cf_strategy/raw_request'
    data = {
        'env':env,
        'method': 'GET',    # GET/POST
        'private': True,    # True/False
        'url': "https://fapi.binance.com/fapi/v1/leverageBracket"
    }

    print(f'{url}: {data}')
    response= verified_session1.request("POST", url, cookies = verified_cookies1,  headers=headers, data=json.dumps(data))
    print(response.json())


    f1 = open('/Volumes/T7/Obentech/leverage_and_MMR/binance阶梯质押率_2026-02-11.json', 'w')
    f1.write(json.dumps(response.json()))
    f1.close()

# OKX 原始数据
    url = f'https://mmadmin.digifinex.org/api/cf_strategy/raw_request'
    env = 'okx_v5_test12' 
    data = {
            'env':env,
            'method': 'GET',    # GET/POST
            'private': True,    # True/False
            'url': '/account/instruments',
            'params':{'instType':"SWAP"}
        }

    response= verified_session.request("POST", url, cookies = verified_cookies,  headers=headers, data=json.dumps(data))
    print(response.text)
    print(response.json()['data']['data'])
    pd.set_option('display.max_columns',200)
    okx_symbol_list = list(set(pd.DataFrame(response.json()['data']['data'])['ctValCcy']))

    # 获取okx阶梯保证金率

    symbol_list = okx_symbol_list

    symbol_list = [x+'-USDT' for x in symbol_list]

    env = "okx_v5_test12"

    output_dict = {}

    for symbol in symbol_list:
        url = f'https://mmadmin.digifinex.org/api/cf_strategy/raw_request'

        data = {
                'env':env,
                'method': 'GET',   
                'private': True,    
                'url': '/public/position-tiers',
                'params':{'instType':"SWAP", "instFamily":symbol, "tdMode":"cross"}
            }

        response= verified_session.request("POST", url, cookies = verified_cookies,  headers=headers, data=json.dumps(data))
        print(response.json()['data']['data'])
        output_dict[symbol] = response.json()['data']['data']
        time.sleep(1)

    f2 = open('/Volumes/T7/Obentech/leverage_and_MMR/okx阶梯质押率_2026-02-11.json', 'w')
    f2.write(json.dumps(output_dict))
    f2.close()


# Bybit 原始数据


    env = "bybit5_v5_by3"  # 你们系统里 Bybit 的 env 名
    mmadmin_url = "https://mmadmin.digifinex.org/api/cf_strategy/raw_request"
    bybit_url = "/v5/market/risk-limit"

    category = "linear"   # USDT 合约一般用 linear；反向合约用 inverse
    cursor = None

    all_rows = []   # 原始 list 直接 append
    pages = 0

    while True:
        params = {"category": category}
        if cursor:
            params["cursor"] = cursor  # 用 nextPageCursor 翻页 :contentReference[oaicite:1]{index=1}

        data = {
            "env": env,
            "method": "GET",
            "private": False,   # 这是 public 接口
            "url": bybit_url,
            "params": params
        }

        resp = verified_session.request(
            "POST",
            mmadmin_url,
            cookies=verified_cookies,
            headers=headers,
            data=json.dumps(data),
            timeout=30,
        )
        j = resp.json()

        # 兼容 raw_request 可能外面包了一层 data 的情况
        payload = j.get("data", j)

        if payload.get("retCode") != 0:
            raise RuntimeError(payload)

        result = payload.get("result", {})
        lst = result.get("list", []) or []
        all_rows.extend(lst)
        pages += 1
        print(f"[page {pages}] got {len(lst)} rows, total={len(all_rows)}")

        cursor = result.get("nextPageCursor")
        if not cursor:
            break

        time.sleep(0.15)

    with open('/Volumes/T7/Obentech/leverage_and_MMR/bybit阶梯质押率_2026-02-11.json', "w", encoding="utf-8") as f:
        json.dump(all_rows, f, ensure_ascii=False)

    print("Saved:",'/Volumes/T7/Obentech/leverage_and_MMR/bybit阶梯质押率_2026-02-11.json')


if __name__ == "__main__":
    date_str = '0211'

    # 定义position_limits列表
    position_limits = [
        100, 1000, 5000, 10000, 20000, 30000, 40000, 50000, 
        60000, 70000, 80000, 90000, 100000, 110000, 120000, 
        130000, 140000, 150000, 160000, 170000, 180000, 190000, 
        200000, 250000, 500000, 1000000, 1500000, 2000000
    ]
    loader = DataLoader()
    # 确定Universe
    universe_str_bn_okx = "0G,1INCH,2Z,A,AAVE,ACE,ACH,ACT,ACU,ADA,AERO,AEVO,AGLD,AIXBT,ALGO,ALLO,ANIME,APE,API3,APR,APT,AR,ARB,ARKM,ASTER,AT,ATH,ATOM,AUCTION,AVAX,AVNT,AXS,AZTEC,BABY,BAND,BARD,BAT,BCH,BEAT,BERA,BICO,BIGTIME,BIO,BLUR,BNB,BOME,BRETT,BREV,BTC,CC,CELO,CFX,CHZ,COAI,COMP,CRV,CVX,DASH,DOGE,DOOD,DOT,DYDX,EDEN,EGLD,EIGEN,ENA,ENJ,ENS,ENSO,ESP,ETC,ETH,ETHFI,ETHW,F,FARTCOIN,FIL,FLOW,FOGO,FUN,GALA,GAS,GIGGLE,GLM,GMT,GMX,GPS,GRASS,GRT,H,HBAR,HMSTR,HOME,HUMA,HYPE,ICP,ICX,IMX,INIT,INJ,IOST,IOTA,IP,JELLYJELLY,JTO,JUP,KAITO,KGEN,KMNO,KSM,LA,LAB,LAYER,LDO,LIGHT,LINEA,LINK,LIT,LPT,LQTY,LRC,LTC,MAGIC,MANA,MASK,ME,MEME,MERL,MET,METIS,MEW,MINA,MMT,MON,MOODENG,MORPHO,MOVE,MUBARAK,NEAR,NEIRO,NEO,NIGHT,NMR,NOT,OL,OM,ONDO,ONE,ONT,OP,ORDER,ORDI,PARTI,PENDLE,PENGU,PEOPLE,PIEVERSE,PIPPIN,PLUME,PNUT,POL,POPCAT,PROMPT,PROVE,PUMP,PYTH,QTUM,RAVE,RECALL,RENDER,RESOLV,RIVER,RLS,RSR,RVN,S,SAHARA,SAND,SAPIEN,SEI,SENT,SHELL,SIGN,SKY,SNX,SOL,SOON,SOPH,SPACE,SPK,SPX,SSV,STABLE,STRK,STX,SUI,SUSHI,SYRUP,TAO,THETA,TIA,TON,TRB,TRIA,TRUMP,TRUST,TRUTH,TRX,TURBO,UMA,UNI,USELESS,VANA,VIRTUAL,W,WAL,WCT,WET,WIF,WLD,WLFI,WOO,XAG,XAN,XAU,XLM,XPD,XPL,XPT,XRP,XTZ,YB,YFI,YGG,ZAMA,ZBT,ZEC,ZEN,ZETA,ZIL,ZK,ZKP,ZORA,ZRO,ZRX"  # Your universe string
    universe_str_bn_bybit = "0G,1INCH,2Z,4,A,A2Z,AAVE,ACE,ACH,ACT,ACU,ACX,ADA,AERGO,AERO,AEVO,AGLD,AIN,AIO,AIXBT,AKE,AKT,ALCH,ALGO,ALICE,ALLO,ALPINE,ALT,ANIME,ANKR,APE,API3,APR,APT,AR,ARB,ARC,ARIA,ARK,ARKM,ARPA,ASR,ASTER,ASTR,AT,ATH,ATOM,AUCTION,AVA,AVAAI,AVAX,AVNT,AWE,AXL,AXS,B,B2,B3,BABY,BAN,BANANA,BANANAS31,BAND,BANK,BARD,BAT,BB,BCH,BEAT,BEL,BERA,BICO,BIGTIME,BIO,BIRB,BLESS,BLUAI,BLUR,BMT,BNB,BNT,BOME,BR,BRETT,BREV,BSV,BTC,BTR,C,C98,CAKE,CARV,CATI,CC,CELO,CETUS,CFX,CGPT,CHILLGUY,CHR,CHZ,CKB,CLANKER,CLO,COAI,COMP,COOKIE,COTI,COW,CROSS,CRV,CTK,CTSI,CVC,CVX,CYBER,CYS,DASH,DEEP,DEGEN,DEXE,DIA,DOGE,DOLO,DOOD,DOT,DRIFT,DUSK,DYDX,DYM,EDEN,EDU,EGLD,EIGEN,ELSA,ENA,ENJ,ENS,ENSO,EPIC,ERA,ESPORTS,ETC,ETH,ETHFI,EUL,EVAA,F,FARTCOIN,FF,FHE,FIDA,FIGHT,FIL,FIO,FLOCK,FLOW,FLUID,FLUX,FOGO,FOLKS,FORM,GALA,GAS,GIGGLE,GLM,GMT,GMX,GOAT,GPS,GRASS,GRIFFAIN,GRT,GUN,H,HAEDAL,HANA,HBAR,HEI,HEMI,HFT,HIGH,HIPPO,HIVE,HMSTR,HOLO,HOME,HOOK,HUMA,HYPE,HYPER,ICNT,ICP,ICX,ID,ILV,IMX,IN,INIT,INJ,INX,IO,IOST,IOTA,IOTX,IP,IRYS,JASMY,JCT,JELLYJELLY,JST,JTO,JUP,KAIA,KAITO,KAS,KAVA,KERNEL,KGEN,KITE,KMNO,KNC,KSM,LA,LAB,LDO,LIGHT,LINEA,LINK,LISTA,LIT,LPT,LQTY,LRC,LSK,LTC,LUMIA,LUNA2,LYN,M,MAGIC,MAGMA,MANA,MANTA,MASK,MAV,MAVIA,MBOX,ME,MELANIA,MEME,MERL,MET,METIS,MEW,MINA,MIRA,MITO,MLN,MMT,MOCA,MON,MOODENG,MORPHO,MOVE,MOVR,MTL,MUBARAK,MYX,NAORIS,NEAR,NEO,NEWT,NFP,NIGHT,NIL,NMR,NOM,NOT,NTRN,NXPC,OG,OGN,OL,OM,ONDO,ONE,ONG,ONT,OP,OPEN,ORCA,ORDER,ORDI,OXT,PARTI,PAXG,PENDLE,PENGU,PEOPLE,PHA,PIEVERSE,PIPPIN,PIXEL,PLUME,PNUT,POL,POLYX,POPCAT,PORTAL,POWER,POWR,PROM,PROMPT,PROVE,PTB,PUFFER,PUMPBTC,PUNDIX,PYTH,Q,QNT,QTUM,RARE,RAVE,RDNT,RECALL,RED,RENDER,RESOLV,REZ,RIVER,RLC,RLS,RONIN,ROSE,RPL,RSR,RUNE,RVN,S,SAFE,SAGA,SAHARA,SAND,SAPIEN,SCR,SCRT,SEI,SENT,SFP,SHELL,SIGN,SIREN,SKL,SKR,SKY,SKYAI,SLP,SNX,SOL,SOLV,SOMI,SONIC,SOON,SOPH,SPACE,SPELL,SPK,SPORTFUN,SPX,SQD,SSV,STABLE,STBL,STEEM,STG,STO,STORJ,STRK,STX,SUI,SUN,SUPER,SUSHI,SWARMS,SXT,SYN,SYRUP,SYS,T,TA,TAC,TAIKO,TAO,THE,THETA,TIA,TLM,TNSR,TON,TOWNS,TRB,TREE,TRIA,TRU,TRUMP,TRUST,TRUTH,TRX,TURTLE,TUT,TWT,UAI,UB,UMA,UNI,US,USELESS,USUAL,VANA,VANRY,VELODROME,VELVET,VET,VFY,VINE,VIRTUAL,VVV,W,WAL,WAXP,WCT,WET,WIF,WLD,WLFI,WOO,XAI,XAN,XLM,XMR,XNY,XPIN,XPL,XRP,XTZ,XVG,XVS,YB,YFI,YGG,ZAMA,ZBT,ZEC,ZEN,ZEREBRO,ZETA,ZIL,ZK,ZKC,ZKP,ZORA,ZRO,ZRX"
    
    universe_bn_okx = loader.parse_universe(universe_str_bn_okx)
    universe_bn_bybit = loader.parse_universe(universe_str_bn_bybit)


    analyzer = PairMultiPositionMMRAnalyzer(position_limits=position_limits)
    
    # Load data
    df_okx = loader.load_okx_data('/Volumes/T7/Obentech/leverage_and_MMR/okx阶梯质押率_2026-02-11.json')
    df_bn = loader.load_binance_data('/Volumes/T7/Obentech/leverage_and_MMR/binance阶梯质押率_2026-02-11.json')
    df_by = loader.load_bybit_data('/Volumes/T7/Obentech/leverage_and_MMR/bybit阶梯质押率_2026-02-11.json')


    # Analyze

    results_bn_bybit = analyzer.analyze(exchange_left='binance',df_left=df_bn, exchange_right='bybit', df_right =  df_by, universe = universe_bn_bybit, force_btc_eth_lev10= True)
    results_bn_okx = analyzer.analyze(exchange_left='binance',df_left=df_bn, exchange_right='okx', df_right =  df_okx, universe = universe_bn_okx, force_btc_eth_lev10= True)


    results_bn_bybit_200k = results_bn_bybit[results_bn_bybit['pos_limit'] == 200000].drop(columns=['pos_limit'])
    results_bn_okx_200k = results_bn_okx[results_bn_okx['pos_limit'] == 200000].drop(columns=['pos_limit'])


    results_bn_bybit.to_csv(f'/Volumes/T7/Obentech/leverage_and_MMR/bn_bybit/limit_pos_leverage_map_bn_bybit_{date_str}.csv', index=False)
    results_bn_okx.to_csv( f'/Volumes/T7/Obentech/leverage_and_MMR/bn_okx/limit_pos_leverage_map_bn_okx_{date_str}.csv', index=False)

    results_bn_bybit_200k.to_csv(f'/Volumes/T7/Obentech/leverage_and_MMR/bn_bybit/average_real_MMR_bn_bybit_Pos200k_{date_str}.csv', index=False)
    results_bn_okx_200k.to_csv(f'/Volumes/T7/Obentech/leverage_and_MMR/bn_okx/average_real_MMR_bn_okx_Pos200k_{date_str}.csv', index=False)

