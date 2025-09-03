"""
swing_alert_bot.py

Modular swing-trade alert bot using Alpaca market data and Telegram notifications.
Supports both backtesting and live monitoring.

Usage:
  - Fill environment variables (APCA_*, TELEGRAM_*).
  - pip install dependencies: alpaca-trade-api, pandas, numpy, telegram, sqlalchemy, matplotlib
  - Toggle RUN_BACKTEST True/False
  - Run: python swing_alert_bot.py
"""

import os
import time
import math
import logging
from dataclasses import dataclass
from typing import List, Optional, Dict, Tuple
import datetime as dt

import numpy as np
import pandas as pd
import alpaca_trade_api as tradeapi
from telegram import Bot
from sqlalchemy import create_engine, text
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.data.historical import StockHistoricalDataClient
import uuid

RUN_ID = str(uuid.uuid4())

# --- CONFIG ---
RUN_BACKTEST = True  # True = run backtester, False = run live monitoring
CONFIG = {
    #"symbols": ["CCL", "NVDA", "FUBO"],
    "symbols": ["AACG", "AAME", "AAPI", "ABAT", "ABCL", "ABSI", "ABUS", "ABVC", "ACCO", "ACDC", "ACHV", "ACIU", "ACRE", "ACTG", "ACXAF", "ADAG", "ADCT", "ADVM", "AFCG", "AGEN", "AGH", "AIJTY", "AIMFF", "AIOT", "AIRG", "AIRJ", "AISP", "AKBA", "ALEC", "ALM", "ALMS", "ALNPY", "ALSMY", "ALT", "ALTI", "ALVOF", "AMBI", "AMBR", "AMC", "AMPG", "AMPY", "AMTX", "ANEB", "ANGH", "ANIX", "ANNA", "ANNX", "ANRO", "ANTE", "AP", "API", "APPS", "APT", "AQST", "ARAI", "AREC", "ARKO", "ARMP", "ARTV", "ASM", "ASOMY", "ASTL", "ATAI", "ATLN", "ATOM", "ATUS", "AUID", "AVIR", "AWP", "AXTI", "BAER", "BCHG", "BDMD", "BDN", "BDTX", "BEEP", "BGS", "BHR", "BIOA", "BIOX", "BIREF", "BLDE", "BLDP", "BLND", "BMHL", "BNEFF", "BORR", "BRAG", "BRBS", "BRIA", "BRLS", "BROGF", "BRY", "BSGM", "BTAI", "BTBIF", "BTBT", "BTCM", "BTCS", "BTE", "BTM", "BTMD", "BTQQF", "BW", "BYND", "BZAI", "BZUN", "CAAS", "CANG", "CATO", "CATX", "CBRA", "CCCC", "CCLD", "CDXS", "CDZI", "CELU", "CFWFF", "CGTX", "CHMI", "CHTH", "CIK", "CLAR", "CLBEY", "CLNE", "CLOV", "CLPR", "CLYM", "CMPS", "CMPX", "CMRB", "CMRC", "CMRF", "CMTG", "CMU", "CNDT", "CNTB", "CNTY", "CNVS", "CPIX", "CPSH", "CRDF", "CRGO", "CRNT", "CRON", "CTKB", "CTMX", "CTOR", "CTW", "CULP", "CURI", "CURV", "CV", "CXE", "CYBHF", "CYH", "DAIC", "DBI", "DC", "DDD", "DDEJF", "DDL", "DEFT", "DELHY", "DENN", "DGXX", "DH", "DHC", "DHF", "DHX", "DHY", "DIBS", "DLNG", "DLTH", "DNGDF", "DNUT", "DOUG", "DPRO", "DRTS", "DSWL", "DTI", "DTIL", "DVS", "DVSPF", "EB", "ECNCF", "ECSNF", "EDAP", "EDIT", "EGY", "EHTH", "EKTAY", "ELDN", "ELRNF", "ELUT", "EMX", "ENGN", "ENGS", "ESGL", "ESPR", "ETHZ", "EU", "EUDA", "EVC", "EVEX", "EVGO", "EVTL", "EWCZ", "FACO", "FBIO", "FCCN", "FCEL", "FEAM", "FECCF", "FF", "FFAI", "FIP", "FKWL", "FLD", "FLL", "FLNT", "FLX", "FLYX", "FNKO", "FOSL", "FRMUF", "FTCO", "FTEK", "FUBO", "FUFU", "GALT", "GAU", "GCI", "GCL", "GCV", "GDC", "GDRX", "GDRZF", "GEG", "GEODF", "GGN", "GGT", "GHG", "GLUE", "GLXZ", "GMM", "GNLX", "GNTA", "GOAI", "GOSS", "GOTU", "GPMT", "GRAN", "GROY", "GSIT", "GSM", "GTE", "GUYGF", "HAIVF", "HBNB", "HCAT", "HELFY", "HEPS", "HFFG", "HIO", "HIT", "HITI", "HIVE", "HIX", "HLLY", "HLSCF", "HLTRF", "HLVX", "HNST", "HPAI", "HPP", "HTLM", "HURA", "HUYA", "HYEX", "HYMC", "IAF", "ICTSF", "IH", "IHRT", "IMAB", "IMDX", "IMMX", "IMPP", "INDI", "INMB", "INNV", "INO", "INVE", "IOVA", "IPA", "IPMLF", "IRBT", "ISPR", "ITFS", "ITMSF", "ITRG", "IZEA", "JAGGF", "JRNGF", "KELTF", "KHTRF", "KLRS", "KOPN", "KRMD", "KRNGY", "KSIOF", "KULR", "KYTX", "LAC", "LAES", "LANV", "LAR", "LASE", "LCUT", "LDI", "LFT", "LHSW", "LIDR", "LIMN", "LIMX", "LITRF", "LOCL", "LODE", "LOT", "LPRO", "LRMR", "LSAK", "LTRX", "LVWR", "LWLG", "LXEO", "LZM", "LZMH", "MAAS", "MAMO", "MATH", "MAUTF", "MAXN", "MBOT", "MCCRF", "MDRX", "MDXH", "MEDXF", "MEHCQ", "MEIP", "MERC", "MGF", "MIN", "MMLP", "MMT", "MNKD", "MNTK", "MPU", "MRT", "MRVI", "MSC", "MVST", "MX", "MXGFF", "NAMM", "NAT", "NB", "NBBI", "NCMI", "NEOV", "NEXA", "NFE", "NGENF", "NHTC", "NIU", "NKRKY", "NKTX", "NNBR", "NNOX", "NNXPF", "NPWR", "NRGV", "NRO", "NRXP", "NSPR", "NTPIF", "NUTR", "NUVB", "NWHUF", "NXDR", "NXDT", "OBIO", "OCG", "ODV", "ODYS", "OKYO", "OMER", "OMI", "ONL", "OPAL", "OPHLY", "ORMP", "OSTX", "OSUR", "OXLC", "OXSQ", "PCOK", "PERF", "PETS", "PGEN", "PHK", "PHUN", "PIM", "PLTK", "PMETF", "PMI", "POAHY", "PPT", "PRME", "PROF", "PROK", "PROP", "PRQR", "PSEC", "PSNL", "PUMSY", "PXHI", "PYPD", "PYYX", "QD", "QDMI", "QIPT", "QTIH", "QTRX", "RBBN", "RC", "RCEL", "RCKT", "RDNW", "RECT", "RERE", "RES", "RGLXY", "RITR", "RLAY", "RMNI", "RMXI", "ROMA", "RPID", "RPT", "RR", "RRTS", "RSKD", "RSRBF", "RSSS", "RUPRF", "RZLV", "SANA", "SATL", "SAVA", "SB", "SBC", "SCAG", "SCRYY", "SDA", "SEER", "SEGG", "SERA", "SFRGY", "SGHT", "SHIM", "SHMD", "SIEB", "SKIN", "SKYE", "SLCJY", "SLDP", "SLI", "SLN", "SLND", "SLNG", "SLQT", "SLSN", "SLVYY", "SNDL", "SNT", "SOPH", "SORA", "SOTK", "SPCE", "SPRO", "SPWH", "SRFM", "SRG", "SRTS", "SSP", "STIM", "STKS", "STXS", "SURG", "SVC", "SVM", "SVRA", "SWIN", "SXGCF", "SY", "SYYNY", "TALK", "TARA", "TBLA", "TDIC", "TGE", "TGHL", "TGMPF", "THCH", "THTX", "TIXT", "TKNO", "TMOAY", "TOI", "TOLWF", "TRON", "TROX", "TRUE", "TRVG", "TSE", "TSHA", "TSI", "TTEC", "TTI", "TUSK", "TUYA", "TWNP", "UA", "UAA", "UAMY", "UBSFY", "UBXG", "UCL", "UEIC", "UFI", "UHG", "UIS", "ULCC", "UMICY", "UNCY", "UPLD", "UROY", "USAS", "USMT", "UURAF", "UXIN", "VELO", "VERI", "VFF", "VGAS", "VIOT", "VIR", "VLN", "VLOWY", "VMEO", "VNDA", "VOXR", "VRA", "VROYF", "VSME", "VSTA", "VSTS", "VTEX", "VTGN", "VTYX", "VUZI", "VVR", "VYGR", "VZLA", "WBX", "WHTCF", "WOOF", "WPFH", "WTF", "XBIT", "XFOR", "XRX", "XTKG", "YMT", "YSXT", "YYGH", "ZDCAF", "ZENA", "ZH", "ZIP", "ZJK", "ZKH", "ZSPC", "ZURA", "ZVIA" ],
    #"symbols": ["ABOS", "ABVE", "ACHFF", "ACRS", "ADV", "AEI", "AERG", "AFLYY", "AGL", "AGMRF", "AHG", "AIIO", "AINMF", "AKTAF", "ALLO", "ALTO", "ALXO", "ALYAF", "AMTD", "AMXEF", "ANL", "ANRGF", "AOIFF", "APXCF", "APYX", "ARAAF", "ARAY", "ARBE", "AREC", "ARREF", "ASCUF", "ASGOF", "ATNM", "ATPC", "AUGG", "AURX", "AUTL", "BEAT", "BEDU", "BFLY", "BGAOY", "BITF", "BLDP", "BLNK", "BRCC", "BTOC", "BUKS", "BYSI", "BZFD", "CABA", "CAPTF", "CBUS", "CCARF", "CCG", "CCO", "CDLX", "CERS", "CGC", "CGEN", "CHGG", "CHRS", "CIBEY", "CNNEF", "CNPRF", "COOK", "CORBF", "CPOP", "CRBU", "CRDF", "CRDL", "CRLBF", "CRNT", "CTM", "CURR", "CVGI", "DCGO", "DCMDF", "DRRX", "DSX", "DSY", "DTI", "DWSN", "DXLG", "ECRTF", "ECX", "EDTXF", "EGHT", "ELEMF", "EM", "EMOTF", "ENTX", "EQ", "ERAS", "EREUF", "ESVIF", "ETOLF", "EUDA", "EXFY", "FATE", "FAVO", "FNGR", "FSP", "GANX", "GCTS", "GERN", "GETY", "GEVO", "GGGOF", "GKOR", "GLGDF", "GMTLF", "GNSS", "GOTRF", "GPRO", "GRO", "GROV", "GRWG", "GRYP", "HAIN", "HBGRY", "HGBL", "HGRAF", "HKD", "HMENF", "HMR", "HNCUF", "HOVR", "HOWL", "HRTX", "HSTXF", "HUMA", "HYLN", "HYPR", "ICCM", "ICG", "IFRX", "IKT", "ILLMF", "INMB", "INVZ", "IOBT", "IONI", "IPA", "IRD", "IRWD", "ISTKF", "ITFY", "IVVD", "KBSX", "KLDCF", "KLTR", "KNDI", "KOS", "LAB", "LAZR", "LCTX", "LDI", "LGCFF", "LGO", "LGSXY", "LOOP", "LOVFF", "LRAXF", "LUCD", "LUCMF", "LUNG", "LVRO", "LXRX", "MAPS", "MCHX", "MCRP", "MDAI", "MDIA", "MESA", "MFGCF", "MGNX", "MGTE", "MGX", "MIST", "MLIZY", "MLPNF", "MNOV", "MNY", "MOBX", "MREO", "MRTMF", "MVIS", "MXCT", "NAMI", "NIPG", "NMRA", "NNDM", "NOTV", "NRDY", "NXXT", "NYWKF", "OABI", "OCGN", "OGI", "OGNNF", "OLPX", "ONCY", "OPK", "OPTX", "OVID", "PACB", "PDSB", "PLBY", "PLGDF", "PLRX", "PLUG", "PLX", "PMEC", "PMVP", "PNBK", "POSC", "POWW", "PRPL", "PSQH", "PTRUF", "PVL", "PXHI", "PYXS", "PZG", "QNCX", "QSI", "RAASY", "RCT", "REED", "REEMF", "REI", "REKR", "RGLSF", "RIOFF", "RMTI", "ROMA", "RPTX", "RTMAF", "RUBLF", "RXT", "SABR", "SACH", "SAFX", "SCZMF", "SES", "SGLA", "SHCAY", "SKYC", "SKYX", "SLS", "SMRT", "SMTGY", "SMXT", "SND", "SOL", "SPPJY", "SPWR", "SROYF", "STPGF", "SUUN", "SXTC", "TCRX", "TE", "TEAD", "TELA", "TGHLF", "THBRF", "THM", "THNCF", "THSGF", "TLRY", "TLSA", "TMQ", "TMRC", "TNYA", "TRNLY", "TROO", "TTNMF", "TYGO", "UAVS", "URG", "URLOF", "VANI", "VGZ", "VKSC", "VLN", "VRMTF", "VRNOF", "VSA", "WALD", "WDH", "WGRX", "WHWK", "WLDBF", "WOLF", "WRAP", "WRDLY", "WRN", "WTI", "WVMDF", "XCH", "XHG", "YOUL", "YTRA", "ZNTL"],
    "data_bar_timeframe": "1Day",
    "history_bars": 50,
    "loop_sleep_seconds": 60 * 30,
    #"loop_sleep_seconds": 60,
    "risk_per_trade": .02,
    "max_position_percent": 0.25,
    "min_cash_to_trade": 50.0,
    "no_shorting": True,
    "paper_mode": True,
    "db_path": "trading_alerts2.db",
}

# --- Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("swing_alert_bot")

# --- Alpaca client ---
APCA_KEY = os.getenv("APCA_API_KEY_ID", "PKF8NI39SP1SPIHSDDT0")
APCA_SECRET = os.getenv("APCA_API_SECRET_KEY", "tRCjNEpwVx05UUdopWiriLWLJ8aEhULfiMgcGWQv")
APCA_BASE = os.getenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets")
if not (APCA_KEY and APCA_SECRET):
    raise RuntimeError("Set APCA_API_KEY_ID and APCA_API_SECRET_KEY env vars")
alpaca = tradeapi.REST(APCA_KEY, APCA_SECRET, APCA_BASE, api_version='v2')

# --- Telegram notifier ---
TELE_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "7029677258:AAEuFXvreLE-6V0HYe4tuTPZaRpzAq3VWkU")
TELE_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "-4573985991")
if not (TELE_TOKEN and TELE_CHAT_ID):
    raise RuntimeError("Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID env vars")

telegram_bot = Bot(token=TELE_TOKEN)

class TelegramNotifier:
    def __init__(self, bot: Bot, chat_id: str):
        self.bot = bot
        self.chat_id = chat_id

    def send(self, text: str):
        logger.info("Telegram alert: " + text.replace("\n", " | "))
        try:
            self.bot.send_message(chat_id=self.chat_id, text=text)
        except Exception as e:
            logger.exception("Failed to send Telegram message: %s", e)

notifier = TelegramNotifier(telegram_bot, TELE_CHAT_ID)

# --- Database ---
engine = create_engine(f"sqlite:///{CONFIG['db_path']}", echo=False)
with engine.begin() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS trade_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            symbol TEXT,
            side TEXT,
            reason TEXT,
            entry_price REAL,
            stop_loss REAL,
            target REAL,
            qty INTEGER,
            status TEXT
        )
    """))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS positions (
            symbol TEXT PRIMARY KEY,
            entry_time TEXT,
            entry_price REAL,
            qty INTEGER,
            stop_loss REAL,
            target REAL,
            last_update TEXT
        )
    """))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS backtest_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT,
            symbol TEXT,
            strategy TEXT,
            entry REAL,
            exit REAL,
            entry_date TEXT,
            exit_date TEXT,
            qty INTEGER,
            pnl REAL,
            balance_after REAL
        )
    """))

# --- Indicators ---
def sma(series: pd.Series, period: int) -> pd.Series:
    return series.rolling(period).mean()

def ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()

def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    ma_up = up.ewm(com=(period-1), adjust=False).mean()
    ma_down = down.ewm(com=(period-1), adjust=False).mean()
    rs = ma_up / (ma_down + 1e-9)
    return 100 - (100 / (1 + rs))

# --- Account utilities ---
def get_account_value() -> Tuple[float, float]:
    acc = alpaca.get_account()
    return float(acc.cash), float(acc.portfolio_value)
    

def compute_position_size(cash: float, portfolio_value: float, entry_price: float, stop_loss: float) -> int:
    risk_amount = portfolio_value * CONFIG["risk_per_trade"]
    loss_per_share = max(entry_price - stop_loss, 1e-6)
    raw_qty = math.floor(risk_amount / loss_per_share)
    max_alloc = portfolio_value * CONFIG["max_position_percent"]
    max_qty_by_alloc = math.floor(max_alloc / entry_price)
    qty = min(raw_qty, max_qty_by_alloc)
    return max(qty, 0)

# --- Alpaca bars fetcher ---
import os
import pandas as pd
from pathlib import Path

CACHE_DIR = Path("alpaca_cache")
CACHE_DIR.mkdir(exist_ok=True)

def fetch_alpaca_bars(symbols: List[str], timeframe="1Day", start=None, end=None, limit=None, batch_size=150, use_cache=True, cache_expiry_days=1):
    client = StockHistoricalDataClient(APCA_KEY, APCA_SECRET)

    if start is None:
        if limit is not None:
            start = pd.Timestamp(datetime.now() - timedelta(days=limit * 2))
        else:
            start = pd.Timestamp(datetime.now() - timedelta(days=365))
    if end is None:
        end = pd.Timestamp(datetime.now())

    def chunked(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i+n]

    out = {}
    to_fetch = []

    # --- Try cache first ---
    for sym in symbols:
        cache_file = CACHE_DIR / f"{sym}_{timeframe}.parquet"
        if use_cache and cache_file.exists():
            mtime = datetime.fromtimestamp(cache_file.stat().st_mtime)
            if (datetime.now() - mtime).days < cache_expiry_days:
                try:
                    df = pd.read_parquet(cache_file)
                    if limit is not None:
                        df = df.tail(limit).reset_index(drop=True)
                    out[sym] = df
                    print(f"Loaded {sym} from cache")
                    continue  # no need to fetch
                except Exception as e:
                    logger.warning(f"Cache read failed for {sym}, refetching. Error: {e}")
        to_fetch.append(sym)

    # --- Fetch remaining from Alpaca ---
    all_dfs = []
    for chunk in chunked(to_fetch, batch_size):
        if not chunk:
            continue
        request_params = StockBarsRequest(
            symbol_or_symbols=chunk,
            timeframe=TimeFrame.Day if timeframe == "1Day" else TimeFrame.Minute,
            start=start,
            end=end
        )
        bars_resp = client.get_stock_bars(request_params)
        all_dfs.append(bars_resp.df.reset_index())

    if all_dfs:
        bars_df = pd.concat(all_dfs, ignore_index=True)
    else:
        bars_df = pd.DataFrame()   # no data

    # --- Split API data into per-symbol DFs ---
    for sym in to_fetch:
        if bars_df.empty or "symbol" not in bars_df.columns:
            logger.warning(f"No API data for {sym}")
            continue

        df = bars_df[bars_df['symbol'] == sym].sort_values('timestamp')
        if df.empty:
            logger.warning(f"No bars returned for {sym}")
            continue

        df = df.rename(columns={"timestamp": "time"})[['time', 'open', 'high', 'low', 'close', 'volume']]
        if limit is not None:
            df = df.tail(limit)
        df = df.reset_index(drop=True)
        out[sym] = df

        if use_cache:
            try:
                df.to_parquet(CACHE_DIR / f"{sym}_{timeframe}.parquet")
            except Exception as e:
                logger.warning(f"Failed to save cache for {sym}: {e}")

    return out


# --- Strategy base classes ---
@dataclass
class Signal:
    symbol: str
    side: str
    reason: str
    entry: float
    stop: float
    target: float
    qty: int

class Strategy:
    def analyze(self, symbol: str, df: pd.DataFrame) -> Optional[Signal]:
        raise NotImplementedError
    def should_exit(self, position_row: Dict, df: pd.DataFrame) -> bool:
        raise NotImplementedError

# --- Test strategy ---
class TestStrategy(Strategy):
    def __init__(self):
        self.entered = False

    def analyze(self, symbol, df):
        if not df.empty:
        #if not self.entered and not df.empty:
            print("TestStrategy analyzing", df['close'].iloc[-1])
            price = df['close'].iloc[-1]
            self.entered = True
            return Signal(symbol, "buy", "TestStrategy entry", price, price*0.99, price*1.01, 0)
        return None

    def should_exit(self, pos, df):
        return True if not df.empty else False

# --- Example strategies ---
class SMA_Crossover_Strategy(Strategy):
    def __init__(self, fast=20, slow=50):
        self.fast = fast
        self.slow = slow
    def analyze(self, symbol, df):
        if len(df) < self.slow + 5: return None
        fast_sma = sma(df['close'], self.fast)
        slow_sma = sma(df['close'], self.slow)
        if fast_sma.iloc[-2] < slow_sma.iloc[-2] and fast_sma.iloc[-1] > slow_sma.iloc[-1]:
            entry = df['close'].iloc[-1]
            swing_low = df['low'].iloc[-4:-1].min()
            stop = min(swing_low, entry*0.98)
            target = entry + (entry-stop)*2
            return Signal(symbol, "buy", f"SMA {self.fast}/{self.slow} crossover", entry, stop, target, 0)
        return None
    def should_exit(self, pos, df):
        close = df['close'].iloc[-1]
        slow_sma = sma(df['close'], self.slow).iloc[-1]
        return close < slow_sma or close <= pos['stop_loss'] or close >= pos['target']

class RSI_Pullback_Strategy(Strategy):
    def __init__(self, rsi_period=14, rsi_thresh=40, trend_sma=50):
        self.rsi_period = rsi_period
        self.rsi_thresh = rsi_thresh
        self.trend_sma = trend_sma
    def analyze(self, symbol, df):
        if len(df) < max(self.rsi_period, self.trend_sma)+2: return None
        close = df['close']
        r = rsi(close, self.rsi_period)
        trend = sma(close, self.trend_sma).iloc[-1]
        last_rsi = r.iloc[-1]
        if close.iloc[-1] > trend and last_rsi < self.rsi_thresh and r.iloc[-2] < last_rsi:
            entry = close.iloc[-1]
            recent_low = df['low'].iloc[-5:-1].min()
            stop = min(recent_low, entry*0.985)
            target = entry + (entry-stop)*2.5
            return Signal(symbol, "buy", f"RSI pullback (rsi={last_rsi:.1f})", entry, stop, target, 0)
        return None
    def should_exit(self, pos, df):
        close = df['close'].iloc[-1]
        r = rsi(df['close'], self.rsi_period).iloc[-1]
        return close >= pos['target'] or close <= pos['stop_loss'] or r > 70


class MA_HighLow_Pullback_Strategy(Strategy):
    def __init__(self, lookback=20, rsi_period=14, rsi_thresh=45, recovery_window=6):
        self.lookback = lookback
        self.rsi_period = rsi_period
        self.rsi_thresh = rsi_thresh
        self.recovery_window = recovery_window

    def analyze(self, symbol, df):
        if len(df) < self.lookback + self.recovery_window + 2:
            return None

        highs_ma = sma(df['high'], self.lookback)
        lows_ma = sma(df['low'], self.lookback)
        r = rsi(df['close'], self.rsi_period)

        # Conditions
        last5_above_high = (df['close'].iloc[-(self.lookback//4+1):-1] > highs_ma.iloc[-(self.lookback//4+1):-1]).all()

        # Break within last N days
        recent_break = (df['close'].iloc[-(self.recovery_window+1):-1] < lows_ma.iloc[-(self.recovery_window+1):-1]).any()
        # Recovery = today's close above lows_ma
        recovery = df['close'].iloc[-1] > lows_ma.iloc[-1]
        rsi_ok = r.iloc[-1] >= self.rsi_thresh

        if last5_above_high and recent_break and recovery and rsi_ok:
            entry = df['close'].iloc[-1]
            swing_low = df['low'].iloc[-5:-1].min()
            atr = (df['high'] - df['low']).rolling(14).mean().iloc[-1]
            stop = min(swing_low, entry - 2 * atr)
            target = entry + (entry - stop) * 2
            return Signal(
                symbol,
                "buy",
                f"MA High/Low Pullback + Recovery (RSI={r.iloc[-1]:.1f})",
                entry,
                stop,
                target,
                0
            )
        return None

    def should_exit(self, pos, df):
        close = df['close'].iloc[-1]
        lows_ma = sma(df['low'], self.lookback).iloc[-1]

        # 3-day consecutive negative closes
        if len(df) >= 9:  # need at least 4 closes to compare
            last3_pct_changes = df['close'].pct_change().iloc[-8:]
            three_red_days = (last3_pct_changes < 0).all()
        else:
            three_red_days = False

        return (
            close <= pos['stop_loss']
            or close >= pos['target']
            or close < lows_ma
            or three_red_days
        )






# --- Backtester ---
class PortfolioBacktester:
    def __init__(self, strategies: List[Strategy], symbols: List[str], initial_cash: float=500, risk_per_trade: float=0.01):
        self.strategies = strategies
        self.symbols = symbols
        self.initial_cash = initial_cash
        self.cash = initial_cash
        self.positions = {}
        self.trades = []
        self.equity_curve = []
        self.risk_per_trade = risk_per_trade

    def run(self, bars: Dict[str, pd.DataFrame]):
        # --- Build unified calendar of trading dates ---
        all_dates = sorted(set().union(*[df['time'] for df in bars.values()]))
        
        for day in all_dates:
            # Build a dictionary of available slices up to 'day'
            daily_data = {}
            for symbol, df in bars.items():
                # Slice all rows up to and including 'day'
                df_day = df[df['time'] <= day]
                if df_day.empty:
                    continue
                daily_data[symbol] = df_day

            # Skip if no data available for this date
            if not daily_data:
                continue

            # --- Process open positions ---
            for symbol, pos in list(self.positions.items()):
                if symbol not in daily_data:
                    continue
                df = daily_data[symbol]
                price = df['close'].iloc[-1]

                for strat in self.strategies:
                    if strat.should_exit(pos, df):
                        exit_price = price
                        pnl = (exit_price - pos['entry']) * pos['qty']
                        self.cash += pos['qty'] * exit_price
                        balance_after = self.cash
                        trade_record = {
                            "symbol": symbol,
                            "strategy": pos['strategy'],
                            "entry": float(pos['entry']),
                            "exit": float(exit_price),
                            "qty": int(pos['qty']),
                            "pnl": float(pnl),
                            "entry_date": str(pos['entry_date']),
                            "exit_date": str(day),
                            "balance_after": float(balance_after)
                        }
                        self.trades.append(trade_record)

                        with engine.begin() as conn:
                            conn.execute(text("""
                                INSERT INTO backtest_trades 
                                (run_id, symbol, strategy, entry, exit, entry_date, exit_date, qty, pnl, balance_after)
                                VALUES (:run_id, :symbol, :strategy, :entry, :exit, :entry_date, :exit_date, :qty, :pnl, :balance_after)
                            """), {**trade_record, "run_id": RUN_ID})
                        del self.positions[symbol]
                        break

            # --- Look for new entries ---
            for symbol, df in daily_data.items():
                if symbol in self.positions:
                    continue
                for strat in self.strategies:
                    sig = strat.analyze(symbol, df)
                    if sig:
                        risk_amt = self.cash * self.risk_per_trade
                        loss_per_share = max(sig.entry - sig.stop, 1e-6)
                        qty = int(risk_amt / loss_per_share)
                        if qty > 0 and qty * sig.entry <= self.cash:
                            self.cash -= qty * sig.entry
                            self.positions[symbol] = {
                                "symbol": symbol,
                                "entry": float(sig.entry),
                                "stop_loss": float(sig.stop),
                                "target": float(sig.target),
                                "qty": int(qty),
                                "strategy": strat.__class__.__name__,
                                "entry_date": str(day)
                            }
                        break  # only one strategy triggers per symbol per day

            # --- Update equity curve ---
            equity = self.cash
            for pos in self.positions.values():
                sym_df = daily_data.get(pos['symbol'])
                if sym_df is not None:
                    equity += pos['qty'] * sym_df['close'].iloc[-1]
            self.equity_curve.append((day, equity))

        # Convert equity_curve to a Series for plotting
        equity_series = pd.Series(
            [val for _, val in self.equity_curve],
            index=[day for day, _ in self.equity_curve]
        )
        return pd.DataFrame(self.trades), equity_series

    def summary(self):
        if not self.trades:
            return {"trades": 0}
        df = pd.DataFrame(self.trades)
        total_return = (self.equity_curve[-1][1] - self.initial_cash) / self.initial_cash * 100
        win_rate = (df['pnl'] > 0).mean() * 100
        avg_pnl = df['pnl'].mean()
        return {
            "overall": {
                "trades": len(df),
                "win_rate": win_rate,
                "avg_pnl": avg_pnl,
                "total_return_%": total_return
            }
        }


def process_symbol_live(symbol: str, strategies: List[Strategy]):

    # Fetch recent bars
    try:
        df = fetch_alpaca_bars([symbol], timeframe=CONFIG["data_bar_timeframe"], limit=CONFIG["history_bars"])[symbol]
        #print(symbol, "Data", df)
    except Exception as e:
        logger.exception("Error fetching bars for %s: %s", symbol, e)
        return
    if df.empty:
        logger.warning("No bars for %s", symbol)
        return

    # Fetch existing position
    try:
        with engine.connect() as conn:
            pos_row = conn.execute(text("SELECT * FROM positions WHERE symbol = :sym"), {"sym": symbol}).mappings().first()
            pos = dict(pos_row) if pos_row else None
    except Exception as e:
        logger.exception("Error fetching position for %s: %s", symbol, e)
        pos = None

    # --- Handle exit conditions ---
    if pos:
        print("Existing position for", symbol, pos)
        for strat in strategies:
            try:
                if strat.should_exit(pos, df):
                    msg = (f"EXIT SIGNAL for {symbol}\n"
                           f"Reason: strategy requests exit\n"
                           f"Current Price: {df['close'].iloc[-1]:.2f}\n"
                           f"Entry: {pos['entry_price']:.2f}  Qty: {pos['qty']}\n"
                           f"Stop: {pos['stop_loss']:.2f}  Target: {pos['target']:.2f}\n"
                           f"Action: SELL manually to close position.")
                    notifier.send(msg)
                    # Log exit
                    with engine.begin() as conn:
                        conn.execute(text("""
                            INSERT INTO trade_signals (timestamp, symbol, side, reason, entry_price, stop_loss, target, qty, status)
                            VALUES (:ts, :symbol, :side, :reason, :entry, :stop, :target, :qty, :status)
                        """), {
                            "ts": dt.datetime.now(dt.UTC).isoformat(),
                            "symbol": symbol,
                            "side": "sell",
                            "reason": "strategy_exit",
                            "entry": pos['entry_price'],
                            "stop": pos['stop_loss'],
                            "target": pos['target'],
                            "qty": pos['qty'],
                            "status": "closed_request"
                        })
                    remove_position(symbol)
                    break
            except Exception as e:
                logger.exception("Error in should_exit for %s: %s", symbol, e)

    # --- Handle new entry signals ---
    else:
        
        for strat in strategies:
            try:
                print("Analyzing", symbol, "with", strat.__class__.__name__)
                print("Data", df)
                sig = strat.analyze(symbol, df)
                if sig:
                    print("Signal For", symbol, sig)
                    try:
                        #cash, portfolio_value = get_account_value()
                        cash, portfolio_value = 500, 500
                        print("portfolio value", portfolio_value)
                    except Exception as e:
                        logger.exception("Error getting account value for %s: %s", symbol, e)
                        continue
                    portfolio_value = portfolio_value if portfolio_value > 0 else max(cash, 500)
                    qty = compute_position_size(cash, portfolio_value, sig.entry, sig.stop)
                    if qty < 1:
                        logger.info("Computed qty < 1 for %s, skipping signal", symbol)
                        continue
                    sig.qty = qty
                    # Log signal
                    try:
                        with engine.begin() as conn:
                            conn.execute(text("""
                                INSERT INTO trade_signals (timestamp, symbol, side, reason, entry_price, stop_loss, target, qty, status)
                                VALUES (:ts, :symbol, :side, :reason, :entry, :stop, :target, :qty, :status)
                            """), {
                                "ts": dt.datetime.now(dt.UTC).isoformat(),
                                "symbol": sig.symbol,
                                "side": "buy",
                                "reason": sig.reason,
                                "entry": sig.entry,
                                "stop": sig.stop,
                                "target": sig.target,
                                "qty": sig.qty,
                                "status": "open"
                            })
                    except Exception as e:
                        logger.exception("Error logging signal for %s: %s", symbol, e)
                        continue
                    # Send Telegram alert
                    msg = (f"BUY SIGNAL for {symbol}\n"
                           f"Strategy: {sig.reason}\n"
                           f"Entry: {sig.entry:.2f}\n"
                           f"Stop Loss: {sig.stop:.2f}\n"
                           f"Target: {sig.target:.2f}\n"
                           f"Qty: {sig.qty}\n"
                           f"Suggested action: BUY manually and mark as OPEN in DB if executed.")
                    #notifier.send(msg)
                    print(msg)
                    # Upsert position
                    #upsert_position(symbol, sig.entry, sig.qty, sig.stop, sig.target)
                    break
            except Exception as e:
                logger.exception("Error analyzing %s with strategy %s: %s", symbol, strat.__class__.__name__, e)

# --- Main live loop ---
def main_loop_live():
    logger.info("Starting live monitoring loop")
    while True:
        start_time = time.time()
        try:
            for symbol in CONFIG['symbols']:
                process_symbol_live(symbol, STRATEGIES)
                print("Processed", symbol)
        except Exception as e:
            logger.exception("Error in main loop: %s", e)
        elapsed = time.time() - start_time
        sleep_time = max(1, CONFIG['loop_sleep_seconds'] - elapsed)
        logger.info("Loop finished, sleeping %d seconds", int(sleep_time))
        time.sleep(sleep_time)

# --- Execution ---
if __name__ == "__main__":
    #STRATEGIES = [SMA_Crossover_Strategy(), RSI_Pullback_Strategy(), TestStrategy()]
    #STRATEGIES = [SMA_Crossover_Strategy(), RSI_Pullback_Strategy()]
    STRATEGIES = [MA_HighLow_Pullback_Strategy()]

    if RUN_BACKTEST:
        logger.info("Running backtest")
        bars = fetch_alpaca_bars(CONFIG['symbols'], timeframe="1Day")
        backtester = PortfolioBacktester(STRATEGIES, CONFIG['symbols'], initial_cash=500, risk_per_trade=0.02)
        trades, equity = backtester.run(bars)
        print("=== Trades ===")
        print(trades)
        print("=== Summary ===")
        print(backtester.summary())
        equity.plot(title="Portfolio Equity Curve")
        plt.show()
    else:
        logger.info("Running live monitoring")
        main_loop_live()
