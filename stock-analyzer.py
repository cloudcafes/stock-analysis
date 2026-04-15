import os
import time
import io
import sys
import random
from datetime import datetime
from zoneinfo import ZoneInfo

import yfinance as yf
import pandas as pd
import numpy as np
import requests
from sqlalchemy import create_engine
from google import genai

# ==============================
# CONFIG
# ==============================

GEMINI_API_KEY     = os.getenv("GEMINI_API_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")

DB_NAME = "sqlite:///stock_rotation.db"
engine = create_engine(DB_NAME)

STOCK_LIST_FILE = "stocks.txt"
SECTOR_FILE = "sector-map.txt"

# ==============================
# TELEGRAM
# ==============================

def send_telegram(msg):
    if not TELEGRAM_BOT_TOKEN:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": msg})

# ==============================
# LOAD SECTOR MAP
# ==============================

def load_sector_map(file_path):
    sector_map = {}
    with open(file_path, "r") as f:
        for line in f:
            line = line.strip()

            if not line or line.startswith("#"):
                continue

            line = line.rstrip(",")

            if ":" in line:
                try:
                    k, v = line.split(":", 1)
                    k = k.strip().replace('"','').replace("'","")
                    v = v.strip().replace('"','').replace("'","")
                    sector_map[k] = v
                except:
                    continue

    return sector_map

SECTOR_MAP = load_sector_map(SECTOR_FILE)

def get_sector(symbol):
    return SECTOR_MAP.get(symbol, "OTHER")

# ==============================
# LOAD STOCKS
# ==============================

def load_stocks():
    with open(STOCK_LIST_FILE) as f:
        return [s.strip() + ".NS" for s in f.readlines()]

# ==============================
# FETCH DATA
# ==============================

def fetch(symbol):
    try:
        df = yf.download(symbol, period="1y", interval="1d", progress=False)
        if df is None or df.empty or len(df) < 220:
            return None
        return df
    except:
        return None

# ==============================
# MARKET
# ==============================

def market_regime():
    df = fetch("^NSEI")
    c = df["Close"]
    dma50 = c.rolling(50).mean()
    dma200 = c.rolling(200).mean()

    if c.iloc[-1] > dma50.iloc[-1] > dma200.iloc[-1]:
        return "TREND"
    return "RANGE"

def nifty_ret():
    df = fetch("^NSEI")
    c = df["Close"]
    return c.iloc[-1] / c.iloc[-66] - 1

# ==============================
# FILTER
# ==============================

def valid(df):
    close = df["Close"].iloc[-1]
    vol = df["Volume"].rolling(20).mean().iloc[-1]

    if close < 100:
        return False

    if close * vol < 5e7:
        return False

    return True

# ==============================
# FEATURES
# ==============================

def features(df, nifty_ret):

    c = df["Close"]
    h = df["High"]
    l = df["Low"]

    dma20 = c.rolling(20).mean()
    dma50 = c.rolling(50).mean()
    dma200 = c.rolling(200).mean()

    ret_1m = c.iloc[-1] / c.iloc[-22] - 1
    ret_3m = c.iloc[-1] / c.iloc[-66] - 1

    rs = ret_3m - nifty_ret
    vol = c.pct_change().rolling(20).std().iloc[-1]
    slope = dma50.iloc[-1] - dma50.iloc[-5]

    pullback = (dma20.iloc[-1] - c.iloc[-1]) / dma20.iloc[-1]
    vol_ratio = df["Volume"].iloc[-1] / df["Volume"].rolling(20).mean().iloc[-1]

    hh = h.iloc[-1] > h.rolling(20).max().iloc[-2]
    hl = l.iloc[-1] > l.rolling(20).min().iloc[-2]

    stretch_200 = (c.iloc[-1] - dma200.iloc[-1]) / dma200.iloc[-1]

    range_pct = (h.rolling(20).max().iloc[-1] - l.rolling(20).min().iloc[-1]) / c.iloc[-1]

    return {
        "price": c.iloc[-1],
        "dma20": dma20.iloc[-1],
        "ret_1m": ret_1m,
        "ret_3m": ret_3m,
        "rs": rs,
        "vol": vol,
        "slope": slope,
        "pullback": pullback,
        "vol_ratio": vol_ratio,
        "structure": hh and hl,
        "stretch_200": stretch_200,
        "range_pct": range_pct
    }

# ==============================
# SCORING + SECTOR LOGIC
# ==============================

def score(df):

    df["sector"] = df["symbol"].apply(get_sector)

    # Remove indices
    df = df[df["sector"] != "INDEX"]

    # Sector strength
    sector_strength = df.groupby("sector")["ret_3m"].mean()
    df["sector_score"] = df["sector"].map(sector_strength)

    # Remove weak sectors
    df = df[df["sector_score"] > 0]

    df["ret_rank"] = df["ret_3m"].rank(pct=True)
    df["rs_rank"] = df["rs"].rank(pct=True)
    df["vol_rank"] = df["vol"].rank(pct=True)

    df["score"] = (
        df["ret_rank"] * 0.3 +
        df["rs_rank"] * 0.4 +
        (df["slope"] > 0).astype(int) * 0.2 -
        df["vol_rank"] * 0.1 +
        df["sector_score"] * 0.2
    )

    return df

# ==============================
# SIGNALS
# ==============================

def entry(row, regime):

    breakout = row["structure"] and row["vol_ratio"] > 1.5
    momentum = row["ret_3m"] > 0.15 and row["rs"] > 0
    pullback = 0 < row["pullback"] < 0.07

    if row["stretch_200"] > 0.5:
        return "AVOID"

    if regime == "TREND":
        if breakout or momentum:
            return "BUY"
    else:
        if pullback:
            return "BUY"

    return "NONE"

def exit(row):

    if row["price"] < row["dma20"] and row["slope"] < 0:
        return "EXIT"

    if row["ret_1m"] < 0 and row["rs"] < 0:
        return "EXIT"

    if row["range_pct"] < 0.05:
        return "EXIT"

    return "HOLD"

# ==============================
# SELECTION (SECTOR CONTROL)
# ==============================

def select_top(df, max_per_sector=2, top_n=10):

    df = df.sort_values("score", ascending=False)

    selected = []
    sector_count = {}

    for _, row in df.iterrows():

        sector = row["sector"]

        if sector_count.get(sector, 0) < max_per_sector:
            selected.append(row)
            sector_count[sector] = sector_count.get(sector, 0) + 1

        if len(selected) >= top_n:
            break

    return pd.DataFrame(selected)

# ==============================
# MAIN
# ==============================

def run():

    stocks = load_stocks()
    regime = market_regime()
    nret = nifty_ret()

    results = []

    print(f"Market Regime: {regime}")

    for s in stocks:
        df = fetch(s)
        if df is None or not valid(df):
            continue

        f = features(df, nret)
        f["symbol"] = s
        results.append(f)

        time.sleep(random.uniform(0.8, 1.2))

    df = pd.DataFrame(results)

    if df.empty:
        print("No data")
        return

    df = score(df)

    df["entry"] = df.apply(lambda x: entry(x, regime), axis=1)
    df["exit"] = df.apply(exit, axis=1)

    df["action"] = "HOLD"
    df.loc[df["entry"] == "BUY", "action"] = "BUY"
    df.loc[df["exit"] == "EXIT", "action"] = "EXIT"

    buys = select_top(df[df["action"] == "BUY"])
    exits = df[df["action"] == "EXIT"].head(10)
    holds = df[df["action"] == "HOLD"].head(10)

    # ==============================
    # OUTPUT
    # ==============================

    print("\n🔥 BUY\n")
    print(buys[["symbol", "sector", "score", "ret_3m", "rs"]])

    print("\n🚨 EXIT\n")
    print(exits[["symbol", "sector", "ret_1m", "rs"]])

    print("\n🟡 HOLD\n")
    print(holds[["symbol", "sector", "score"]])

    # ==============================
    # SAVE
    # ==============================

    df["date"] = datetime.now(ZoneInfo("Asia/Kolkata")).date()

    with engine.begin() as conn:
        df.to_sql("stock_signals", conn, if_exists="append", index=False)

    # ==============================
    # TELEGRAM AI
    # ==============================

    if GEMINI_API_KEY:
        client = genai.Client(api_key=GEMINI_API_KEY)

        summary = df.head(30).to_string()

        prompt = f"""
        You are a professional swing trader.

        Generate short Telegram output:

        🔥 Market view
        📈 BUY ideas
        ❌ EXIT warnings
        💡 Key insight

        Data:
        {summary}
        """

        res = client.models.generate_content(
            model="gemini-3.1-flash-lite-preview",
            contents=[prompt]
        )

        send_telegram(res.text)

# ==============================
# RUN
# ==============================

if __name__ == "__main__":
    run()