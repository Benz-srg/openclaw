# openclaw/handler.py
# Telegram Webhook Handler - Technical Indicator Calculation
# Compatible with: AWS Lambda (use Mangum for FastAPI/Vercel)
# Trigger: /calc SYMBOL TIMEFRAME  (e.g., /calc BTC/USDT 1h)

import os
import json
import re
import logging
from datetime import datetime, timezone

import requests
import pandas as pd
import pandas_ta as ta
from supabase import create_client, Client

logger = logging.getLogger("openclaw")
logger.setLevel(logging.INFO)

# ─── Supabase Clients ────────────────────────────────────────────────────────

# DB 1: Read-only - OHLCV historical market data source
data_client: Client = create_client(
    os.environ["SUPABASE_DATA_URL"],
    os.environ["SUPABASE_DATA_KEY"],
)

# DB 2: Write-only - calculated indicators destination
indicator_client: Client = create_client(
    os.environ["SUPABASE_INDICATOR_URL"],
    os.environ["SUPABASE_INDICATOR_KEY"],
)

TELEGRAM_API = f"https://api.telegram.org/bot{os.environ['TELEGRAM_BOT_TOKEN']}"


# ─── Telegram Helper ─────────────────────────────────────────────────────────

def send_message(chat_id: int, text: str) -> None:
    """Send a Markdown-formatted message back to the Telegram chat."""
    try:
        requests.post(
            f"{TELEGRAM_API}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"},
            timeout=10,
        )
    except Exception as e:
        logger.error(f"Failed to send Telegram message: {e}")


# ─── Core Logic ───────────────────────────────────────────────────────────────

def calculate_and_save(symbol: str, timeframe: str, chat_id: int) -> None:
    """
    1. Fetch OHLCV from DB 1
    2. Calculate RSI, EMA, MACD via pandas-ta
    3. Upsert results into DB 2
    4. Notify the Telegram user
    """

    # Step 1: Fetch OHLCV from DB 1
    try:
        response = (
            data_client.table("market_data")
            .select("timestamp, open, high, low, close, volume")
            .eq("symbol", symbol)
            .eq("timeframe", timeframe)
            .order("timestamp", desc=False)
            .limit(200)
            .execute()
        )
        ohlcv = response.data
    except Exception as e:
        raise RuntimeError(f"DB1 fetch failed: {e}") from e

    if not ohlcv or len(ohlcv) < 30:
        raise ValueError(
            f"Insufficient data: only {len(ohlcv) if ohlcv else 0} candles "
            f"found (minimum 30 required)."
        )

    # Step 2: Build DataFrame and calculate indicators
    df = pd.DataFrame(ohlcv)
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)

    df.ta.rsi(length=14, append=True)           # RSI_14
    df.ta.ema(length=21, append=True)           # EMA_21
    df.ta.ema(length=50, append=True)           # EMA_50
    df.ta.macd(fast=12, slow=26, signal=9, append=True)
    # Columns: MACD_12_26_9, MACDh_12_26_9, MACDs_12_26_9

    indicator_cols = [
        "RSI_14", "EMA_21", "EMA_50",
        "MACD_12_26_9", "MACDh_12_26_9", "MACDs_12_26_9",
    ]
    df_clean = df.dropna(subset=indicator_cols).copy()

    if df_clean.empty:
        raise ValueError("No complete indicator rows after NaN drop.")

    # Step 3: Build upsert rows
    now = datetime.now(timezone.utc).isoformat()
    rows = []
    for _, row in df_clean.iterrows():
        rows.append({
            "symbol":           symbol,
            "timeframe":        timeframe,
            "timestamp":        row["timestamp"],
            "rsi_14":           round(float(row["RSI_14"]), 4),
            "ema_21":           round(float(row["EMA_21"]), 4),
            "ema_50":           round(float(row["EMA_50"]), 4),
            "macd_line":        round(float(row["MACD_12_26_9"]), 4),
            "macd_signal":      round(float(row["MACDs_12_26_9"]), 4),
            "macd_histogram":   round(float(row["MACDh_12_26_9"]), 4),
            "updated_at":       now,
        })

    # Step 4: Upsert into DB 2
    try:
        indicator_client.table("indicators").upsert(
            rows,
            on_conflict="symbol,timeframe,timestamp"
        ).execute()
    except Exception as e:
        raise RuntimeError(f"DB2 upsert failed: {e}") from e

    # Step 5: Notify Telegram
    latest = rows[-1]
    send_message(
        chat_id,
        f"Indicators saved for {symbol} ({timeframe})\n"
        f"RSI(14): {latest['rsi_14']} | "
        f"EMA21: {latest['ema_21']} | "
        f"EMA50: {latest['ema_50']} | "
        f"MACD: {latest['macd_line']}\n"
        f"{len(rows)} rows upserted."
    )


# ─── AWS Lambda Handler ───────────────────────────────────────────────────────

def lambda_handler(event: dict, context) -> dict:
    """AWS Lambda entry point. Telegram sends POST requests here."""
    try:
        body = json.loads(event.get("body", "{}"))
    except json.JSONDecodeError:
        return {"statusCode": 200, "body": json.dumps({"ok": True})}

    message = body.get("message", {})
    text    = message.get("text", "").strip()
    chat_id = message.get("chat", {}).get("id")

    if not text or not chat_id:
        return {"statusCode": 200, "body": json.dumps({"ok": True})}

    match = re.match(r"^/calc\s+([A-Z]+/[A-Z]+)\s+(\w+)$", text, re.IGNORECASE)
    if not match:
        send_message(chat_id, "Usage: /calc BTC/USDT 1h")
        return {"statusCode": 200, "body": json.dumps({"ok": True})}

    symbol    = match.group(1).upper()
    timeframe = match.group(2).lower()

    send_message(chat_id, f"Calculating indicators for {symbol} ({timeframe})...")

    try:
        calculate_and_save(symbol, timeframe, chat_id)
    except Exception as err:
        logger.exception("openclaw error")
        send_message(chat_id, f"Error: {err}")

    return {"statusCode": 200, "body": json.dumps({"ok": True})}
