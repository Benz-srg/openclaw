// openclaw/handler.js
// Telegram Webhook Handler - Technical Indicator Calculation
// Compatible: AWS Lambda | Vercel | Cloudflare Workers
// Trigger: /calc SYMBOL TIMEFRAME (e.g., /calc BTC/USDT 1h)

import { createClient } from "@supabase/supabase-js";
import { RSI, MACD, EMA } from "technicalindicators";

// ─── Supabase Clients ────────────────────────────────────────────────────────

// DB 1: Read-only - source of OHLCV historical market data
const dataClient = createClient(
  process.env.SUPABASE_DATA_URL,
  process.env.SUPABASE_DATA_KEY
);

// DB 2: Write-only - destination for calculated indicators
const indicatorClient = createClient(
  process.env.SUPABASE_INDICATOR_URL,
  process.env.SUPABASE_INDICATOR_KEY
);

const TELEGRAM_API =
  "https://api.telegram.org/bot" + process.env.TELEGRAM_BOT_TOKEN;

// ─── Telegram Helper ─────────────────────────────────────────────────────────

async function sendMessage(chatId, text) {
  await fetch(TELEGRAM_API + "/sendMessage", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ chat_id: chatId, text, parse_mode: "Markdown" }),
  });
}

// ─── Main Handler ─────────────────────────────────────────────────────────────

export async function handler(req, res) {
  // Always respond 200 immediately so Telegram does not retry
  if (res) res.status(200).json({ ok: true });

  let chatId = null;

  try {
    const body =
      typeof req.body === "string" ? JSON.parse(req.body) : req.body;
    const message = body?.message;
    if (!message || !message.text) return;

    chatId = message.chat.id;
    const text = message.text.trim();

    // Parse /calc BTC/USDT 1h
    const match = text.match(/^\/calc\s+([A-Z]+\/[A-Z]+)\s+(\w+)$/i);
    if (!match) {
      await sendMessage(chatId, "Usage: /calc BTC/USDT 1h");
      return;
    }

    const symbol = match[1].toUpperCase();      // e.g. "BTC/USDT"
    const timeframe = match[2].toLowerCase();   // e.g. "1h"

    await sendMessage(
      chatId,
      "Calculating indicators for " + symbol + " (" + timeframe + ")..."
    );

    // ── Step 1: Fetch OHLCV from DB 1 ────────────────────────────────────────
    const { data: ohlcv, error: fetchError } = await dataClient
      .from("market_data")
      .select("timestamp, open, high, low, close, volume")
      .eq("symbol", symbol)
      .eq("timeframe", timeframe)
      .order("timestamp", { ascending: true })
      .limit(200);

    if (fetchError)
      throw new Error("DB1 fetch failed: " + fetchError.message);
    if (!ohlcv || ohlcv.length < 30)
      throw new Error(
        "Insufficient data: " + (ohlcv?.length ?? 0) + " candles (min 30)"
      );

    const closes = ohlcv.map((c) => parseFloat(c.close));

    // ── Step 2: Calculate Indicators ─────────────────────────────────────────
    const rsiArr  = RSI.calculate({ values: closes, period: 14 });
    const ema21Arr = EMA.calculate({ values: closes, period: 21 });
    const ema50Arr = EMA.calculate({ values: closes, period: 50 });
    const macdArr  = MACD.calculate({
      values: closes,
      fastPeriod: 12,
      slowPeriod: 26,
      signalPeriod: 9,
      SimpleMAOscillator: false,
      SimpleMASignal: false,
    });

    const count = Math.min(
      rsiArr.length, ema21Arr.length, ema50Arr.length, macdArr.length
    );

    // ── Step 3: Build upsert rows ─────────────────────────────────────────────
    const rows = Array.from({ length: count }, (_, i) => {
      const ci = ohlcv.length - count + i;
      const m  = macdArr[macdArr.length - count + i];
      return {
        symbol,
        timeframe,
        timestamp:      ohlcv[ci].timestamp,
        rsi_14:         rsiArr[rsiArr.length - count + i],
        ema_21:         ema21Arr[ema21Arr.length - count + i],
        ema_50:         ema50Arr[ema50Arr.length - count + i],
        macd_line:      m?.MACD       ?? null,
        macd_signal:    m?.signal     ?? null,
        macd_histogram: m?.histogram  ?? null,
        updated_at:     new Date().toISOString(),
      };
    });

    // ── Step 4: Upsert into DB 2 ──────────────────────────────────────────────
    const { error: upsertError } = await indicatorClient
      .from("indicators")
      .upsert(rows, {
        onConflict: "symbol,timeframe,timestamp", // unique constraint
        ignoreDuplicates: false,
      });

    if (upsertError)
      throw new Error("DB2 upsert failed: " + upsertError.message);

    // ── Step 5: Confirm to Telegram ───────────────────────────────────────────
    const latest = rows[rows.length - 1];
    await sendMessage(
      chatId,
      "Indicators saved for " + symbol + " (" + timeframe + ")\n" +
      "RSI(14): "    + latest.rsi_14?.toFixed(2)        + "\n" +
      "EMA(21): "    + latest.ema_21?.toFixed(4)        + "\n" +
      "EMA(50): "    + latest.ema_50?.toFixed(4)        + "\n" +
      "MACD Line: "  + latest.macd_line?.toFixed(4)     + "\n" +
      "MACD Signal: "+ latest.macd_signal?.toFixed(4)   + "\n" +
      "MACD Hist: "  + latest.macd_histogram?.toFixed(4)+ "\n" +
      count + " rows upserted."
    );
  } catch (err) {
    console.error("[openclaw]", err);
    if (chatId) await sendMessage(chatId, "Error: " + err.message);
  }
}

// ─── AWS Lambda Adapter ───────────────────────────────────────────────────────
export const lambdaHandler = async (event) => {
  await handler({ body: event.body }, null);
  return { statusCode: 200, body: '{"ok":true}' };
};
