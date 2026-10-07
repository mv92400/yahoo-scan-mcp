/*
 * ============================================================
 * YAHOO SCAN MCP
 * Version 1.3.0
 *
 * Architecture:
 *
 *   universe_s0.txt
 *        ↓
 *   UNIVERSE
 *        ↓
 *   Yahoo Spark 5d / 5m
 *        ↓
 *   S0 = ordinary stocks + price < $5
 *        ↓
 *   S1 metrics
 *        ↓
 *   Ranking
 *        ↓
 *   Lots of 20
 *
 * Existing files intentionally preserved:
 *   ./universe_s0.txt
 *   ./src/yahoo-ws.js
 *
 * No fabricated market data.
 * ============================================================
 */

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { z } from "zod";
import http from "node:http";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";

import { YahooWS } from "./src/yahoo-ws.js";


/* ============================================================
 * CONFIG
 * ============================================================
 */

const PORT = Number(process.env.PORT || 8080);

const APP_VERSION = "1.3.0";

const SERVICE_NAME = "yahoo-scan-mcp";

const UNIVERSE_FILE = new URL(
  "./universe_s0.txt",
  import.meta.url
);

const YAHOO_SPARK_URL =
  "https://query1.finance.yahoo.com/v7/finance/spark";


/* ============================================================
 * LOGGING
 * ============================================================
 */

function log(...args) {
  console.log(`[${SERVICE_NAME}]`, ...args);
}

function logError(...args) {
  console.error(`[${SERVICE_NAME}]`, ...args);
}


/* ============================================================
 * UNIVERSE CACHE
 * ============================================================
 */

let universeCache = null;

let universeLoadedAt = null;


/* ============================================================
 * SCAN STATE
 * ============================================================
 */

const scanState = {
  ok: true,

  version: APP_VERSION,

  stage: "IDLE",

  asof: null,

  elapsed_ms: 0,

  symbols_requested: 0,

  source: "Yahoo",

  s0: [],

  s1: [],

  lots: [],

  errors: [],

  universe_loaded_at: null
};


/* ============================================================
 * LOAD MATERIALIZED UNIVERSE
 * ============================================================
 */

async function loadUniverse() {

  if (universeCache) {
    return universeCache;
  }

  const raw = await readFile(
    UNIVERSE_FILE,
    "utf8"
  );

  const symbols = [];

  const seen = new Set();

  for (const line of raw.split(/\r?\n/)) {

    const clean = line.trim();

    if (!clean) {
      continue;
    }

    if (clean.startsWith("#")) {
      continue;
    }

    /*
     * Accept:
     *
     * AAPL
     * AAPL anything
     *
     * Only the first token is used.
     */

    const symbol =
      clean
        .split(/\s+/)[0]
        .trim()
        .toUpperCase();

    if (!symbol) {
      continue;
    }

    if (seen.has(symbol)) {
      continue;
    }

    seen.add(symbol);

    symbols.push(symbol);
  }

  universeCache = symbols;

  universeLoadedAt =
    new Date().toISOString();

  scanState.universe_loaded_at =
    universeLoadedAt;

  return universeCache;
}


/* ============================================================
 * GENERIC HELPERS
 * ============================================================
 */

function sleep(ms) {

  return new Promise(resolve => {
    setTimeout(resolve, ms);
  });
}


function finiteNumber(value) {

  const n = Number(value);

  return Number.isFinite(n)
    ? n
    : null;
}


function safeRatio(a, b) {

  const x = finiteNumber(a);
  const y = finiteNumber(b);

  if (
    x === null ||
    y === null ||
    y === 0
  ) {
    return null;
  }

  return x / y;
}


function mean(values) {

  const valid = values
    .map(Number)
    .filter(Number.isFinite);

  if (!valid.length) {
    return null;
  }

  return (
    valid.reduce(
      (sum, value) => sum + value,
      0
    ) / valid.length
  );
}


/* ============================================================
 * ORDINARY STOCK FILTER
 * ============================================================
 *
 * We intentionally reject obvious non-equity instruments.
 * The universe is already expected to be NASDAQ-oriented,
 * but the runtime filter remains defensive.
 * ============================================================
 */

function isOrdinaryStock(symbol, quote) {

  if (!symbol) {
    return false;
  }

  const upper =
    String(symbol).toUpperCase();

  /*
   * Obvious non-stock symbols.
   */

  const rejectedPatterns = [
    /\^/,          // indices
    /=F$/,         // FX
    /-USD$/,       // crypto
    /USD$/,        // crypto / FX variants
    /\.FI$/,       // futures / misc
    /\.CBT$/,
    /\.CME$/,
    /\.NYB$/,
    /\.COM$/,
    /\.CC$/,
    /\.TO$/,
    /\.V$/,
    /\.L$/,
    /\.DE$/,
    /\.PA$/,
    /\.MI$/,
    /\.AS$/,
    /\.SW$/
  ];

  if (
    rejectedPatterns.some(
      regex => regex.test(upper)
    )
  ) {
    return false;
  }

  /*
   * Yahoo quote metadata, when available.
   */

  const quoteType =
    quote?.quoteType ||
    quote?.typeDisp ||
    "";

  const qt =
    String(quoteType).toUpperCase();

  if (
    qt &&
    ![
      "EQUITY",
      "STOCK"
    ].includes(qt)
  ) {
    return false;
  }

  return true;
}


/* ============================================================
 * YAHOO SPARK
 * ============================================================
 */

async function yahooSpark(
  symbols,
  range = "5d",
  interval = "5m"
) {

  const unique = [
    ...new Set(
      symbols
        .map(s => String(s).trim().toUpperCase())
        .filter(Boolean)
    )
  ];

  const batches = [];

  for (
    let i = 0;
    i < unique.length;
    i += 20
  ) {
    batches.push(
      unique.slice(i, i + 20)
    );
  }

  const output = new Map();

  const errors = [];

  for (const batch of batches) {

    const url =
      new URL(YAHOO_SPARK_URL);

    url.searchParams.set(
      "symbols",
      batch.join(",")
    );

    url.searchParams.set(
      "range",
      range
    );

    url.searchParams.set(
      "interval",
      interval
    );

    url.searchParams.set(
      "indicators",
      "quote,close"
    );

    url.searchParams.set(
      "includeTimestamps",
      "true"
    );

    url.searchParams.set(
      "includePrePost",
      "false"
    );

    let success = false;

    let lastError = null;

    for (
      let attempt = 1;
      attempt <= 3;
      attempt++
    ) {

      const controller =
        new AbortController();

      const timeout =
        setTimeout(
          () => controller.abort(),
          20000
        );

      try {

        const response =
          await fetch(
            url,
            {
              method: "GET",

              headers: {
                "User-Agent":
                  "Mozilla/5.0 " +
                  "(Windows NT 10.0; Win64; x64) " +
                  "AppleWebKit/537.36 " +
                  "(KHTML, like Gecko) " +
                  "Chrome/120 Safari/537.36",

                "Accept":
                  "application/json"
              },

              signal:
                controller.signal
            }
          );

        if (!response.ok) {

          throw new Error(
            `Yahoo HTTP ${response.status}`
          );
        }

        const data =
          await response.json();

        const result =
          data?.spark?.result;

        if (!Array.isArray(result)) {

          throw new Error(
            "Yahoo response missing spark.result"
          );
        }

        for (const item of result) {

          const symbol =
            item?.symbol?.toUpperCase();

          if (!symbol) {
            continue;
          }

          output.set(
            symbol,
            item
          );
        }

        success = true;

        break;

      } catch (error) {

        lastError = error;

        if (attempt < 3) {
          await sleep(
            400 * attempt
          );
        }

      } finally {

        clearTimeout(timeout);
      }
    }

    if (!success) {

      errors.push({
        symbols: batch,
        error:
          lastError?.message ||
          "Yahoo request failed"
      });
    }
  }

  return {
    data: output,
    errors
  };
}


/* ============================================================
 * EXTRACT BARS
 * ============================================================
 */

function extractBars(item) {

  const timestamps =
    item?.timestamp || [];

  const quote =
    item?.indicators?.quote?.[0];

  const closes =
    item?.indicators?.quote?.[0]?.close ||
    item?.indicators?.close?.[0]?.close ||
    [];

  const opens =
    quote?.open || [];

  const highs =
    quote?.high || [];

  const lows =
    quote?.low || [];

  const volumes =
    quote?.volume || [];

  const bars = [];

  const length =
    timestamps.length;

  for (
    let i = 0;
    i < length;
    i++
  ) {

    const timestamp =
      Number(timestamps[i]);

    const close =
      finiteNumber(closes[i]);

    const volume =
      finiteNumber(volumes[i]);

    if (
      !Number.isFinite(timestamp) ||
      close === null ||
      volume === null
    ) {
      continue;
    }

    bars.push({
      timestamp,

      open:
        finiteNumber(opens[i]),

      high:
        finiteNumber(highs[i]),

      low:
        finiteNumber(lows[i]),

      close,

      volume
    });
  }

  bars.sort(
    (a, b) =>
      a.timestamp - b.timestamp
  );

  return bars;
}


/* ============================================================
 * SESSION KEY
 * ============================================================
 */

function sessionKey(timestamp) {

  const date =
    new Date(timestamp * 1000);

  /*
   * Yahoo timestamps are UTC.
   *
   * We use the NY market date by formatting
   * in America/New_York.
   */

  return new Intl.DateTimeFormat(
    "en-CA",
    {
      timeZone:
        "America/New_York",

      year: "numeric",

      month: "2-digit",

      day: "2-digit"
    }
  ).format(date);
}


/* ============================================================
 * VWAP
 * ============================================================
 */

function calculateVWAP(bars) {

  let cumulativePV = 0;

  let cumulativeVolume = 0;

  for (const bar of bars) {

    if (
      bar.high === null ||
      bar.low === null ||
      bar.close === null ||
      bar.volume === null
    ) {
      continue;
    }

    const typicalPrice =
      (
        bar.high +
        bar.low +
        bar.close
      ) / 3;

    cumulativePV +=
      typicalPrice *
      bar.volume;

    cumulativeVolume +=
      bar.volume;
  }

  if (
    cumulativeVolume <= 0
  ) {
    return null;
  }

  return (
    cumulativePV /
    cumulativeVolume
  );
}


/* ============================================================
 * S1 CALCULATION
 * ============================================================
 */

function calculateS1(
  symbol,
  bars
) {

  if (!Array.isArray(bars)) {
    return null;
  }

  /*
   * We need enough completed candles.
   */

  if (bars.length < 4) {
    return null;
  }

  /*
   * Last Yahoo candle can be incomplete.
   *
   * We deliberately exclude it.
   */

  const completed =
    bars.slice(0, -1);

  if (completed.length < 4) {
    return null;
  }

  const current =
    completed[completed.length - 1];

  const previous =
    completed[completed.length - 2];

  const previous2 =
    completed[completed.length - 3];

  if (
    !current ||
    !previous ||
    !previous2
  ) {
    return null;
  }

  /*
   * Last 15 minutes = 3 completed 5m bars.
   */

  const last3 =
    completed.slice(-3);

  const vol15m =
    last3.reduce(
      (sum, bar) =>
        sum +
        (
          finiteNumber(bar.volume) ||
          0
        ),
      0
    );

  /*
   * Acceleration:
   *
   * current 5m volume /
   * mean of preceding 3 completed 5m volumes
   */

  const preceding3 =
    completed.slice(
      Math.max(
        0,
        completed.length - 6
      ),
      completed.length - 3
    );

  const baseline5m =
    mean(
      preceding3.map(
        bar => bar.volume
      )
    );

  const accel5m =
    safeRatio(
      current.volume,
      baseline5m
    );

  /*
   * Current NY session.
   */

  const currentSession =
    sessionKey(
      current.timestamp
    );

  const currentSessionBars =
    completed.filter(
      bar =>
        sessionKey(
          bar.timestamp
        ) === currentSession
    );

  /*
   * VWAP only on the current session.
   */

  const vwap =
    calculateVWAP(
      currentSessionBars
    );

  const priceVsVWAP =
    vwap !== null
      ? (
          current.close /
          vwap
        ) - 1
      : null;

  /*
   * J-1 / J-2 volume.
   */

  const sessions = new Map();

  for (const bar of completed) {

    const key =
      sessionKey(
        bar.timestamp
      );

    if (!sessions.has(key)) {
      sessions.set(
        key,
        []
      );
    }

    sessions
      .get(key)
      .push(bar);
  }

  const sessionKeys =
    [...sessions.keys()]
      .sort()
      .reverse();

  const previousSessionKey =
    sessionKeys[1];

  const previous2SessionKey =
    sessionKeys[2];

  const previousSessionBars =
    previousSessionKey
      ? sessions.get(
          previousSessionKey
        ) || []
      : [];

  const previous2SessionBars =
    previous2SessionKey
      ? sessions.get(
          previous2SessionKey
        ) || []
      : [];

  const j1 =
    previousSessionBars.reduce(
      (sum, bar) =>
        sum +
        (
          finiteNumber(bar.volume) ||
          0
        ),
      0
    );

  const j2 =
    previous2SessionBars.reduce(
      (sum, bar) =>
        sum +
        (
          finiteNumber(bar.volume) ||
          0
        ),
      0
    );

  /*
   * Intraday RVOL.
   *
   * Compare the current 15m volume
   * with equivalent 15m windows from
   * prior sessions.
   */

  const currentIndex =
    currentSessionBars.findIndex(
      bar =>
        bar.timestamp ===
        current.timestamp
    );

  let rvol15m = null;

  if (
    currentIndex >= 2
  ) {

    const currentWindow =
      currentSessionBars.slice(
        Math.max(
          0,
          currentIndex - 2
        ),
        currentIndex + 1
      );

    const currentVol =
      currentWindow.reduce(
        (sum, bar) =>
          sum +
          (
            finiteNumber(bar.volume) ||
            0
          ),
        0
      );

    const priorSessionVolumes = [];

    const priorKeys =
      sessionKeys.slice(
        1,
        3
      );

    for (const key of priorKeys) {

      const sessionBars =
        sessions.get(key) || [];

      if (
        sessionBars.length >
        currentIndex
      ) {

        const comparable =
          sessionBars.slice(
            Math.max(
              0,
              currentIndex - 2
            ),
            currentIndex + 1
          );

        const volume =
          comparable.reduce(
            (sum, bar) =>
              sum +
              (
                finiteNumber(
                  bar.volume
                ) || 0
              ),
            0
          );

        if (volume > 0) {
          priorSessionVolumes.push(
            volume
          );
        }
      }
    }

    const baseline =
      mean(
        priorSessionVolumes
      );

    if (
      baseline !== null &&
      baseline > 0
    ) {

      rvol15m =
        currentVol /
        baseline;
    }
  }

  /*
   * HOD.
   */

  const hod =
    currentSessionBars.reduce(
      (max, bar) => {

        const high =
          finiteNumber(bar.high);

        if (high === null) {
          return max;
        }

        return Math.max(
          max,
          high
        );
      },
      0
    );

  const hodDistance =
    hod > 0
      ? (
          current.close /
          hod
        ) - 1
      : null;

  /*
   * Last 5m change.
   */

  const change5m =
    safeRatio(
      current.close,
      previous.close
    );

  /*
   * Previous 5m change.
   */

  const previousChange5m =
    safeRatio(
      previous.close,
      previous2.close
    );

  return {

    symbol,

    timestamp:
      current.timestamp,

    price:
      current.close,

    Vol15M:
      vol15m,

    RVOL15M:
      rvol15m,

    Accel5M:
      accel5m,

    VWAP:
      vwap,

    priceVsVWAP,

    J1:
      j1,

    J2:
      j2,

    HOD:
      hod,

    hodDistance,

    change5m:
      change5m !== null
        ? change5m - 1
        : null,

    previousChange5m:
      previousChange5m !== null
        ? previousChange5m - 1
        : null,

    bars:
      completed.length
  };
}


/* ============================================================
 * CURRENT PRICE
 * ============================================================
 */

function getCurrentPrice(item) {

  const meta =
    item?.meta;

  const regularMarketPrice =
    finiteNumber(
      meta?.regularMarketPrice
    );

  if (
    regularMarketPrice !== null
  ) {
    return regularMarketPrice;
  }

  const chartPreviousClose =
    finiteNumber(
      meta?.chartPreviousClose
    );

  if (
    chartPreviousClose !== null
  ) {
    return chartPreviousClose;
  }

  const bars =
    extractBars(item);

  if (bars.length) {

    return bars[
      bars.length - 1
    ].close;
  }

  return null;
}


/* ============================================================
 * LOT FORMATION
 * ============================================================
 */

function makeLots(
  rows,
  size = 20
) {

  const lots = [];

  for (
    let i = 0;
    i < rows.length;
    i += size
  ) {

    lots.push({
      lot:
        lots.length + 1,

      count:
        Math.min(
          size,
          rows.length - i
        ),

      symbols:
        rows
          .slice(i, i + size)
          .map(
            row => row.symbol
          )
    });
  }

  return lots;
}


/* ============================================================
 * YAHOO WS TEST
 * ============================================================
 */

async function runYahooWSTest() {

  const symbols = [
    "AAPL",
    "MSFT",
    "NVDA"
  ];

  const ws =
    new YahooWS();

  const started =
    Date.now();

  try {

    await ws.connect();

    await ws.subscribe(
      symbols
    );

    /*
     * Give Yahoo enough time to send
     * live messages.
     */

    await sleep(10000);

    const status =
      typeof ws.getStatus === "function"
        ? ws.getStatus()
        : null;

    const latest =
      typeof ws.getLatest === "function"
        ? ws.getLatest()
        : null;

    return {

      ok: true,

      connected:
        status?.connected ??
        true,

      subscribed:
        status?.subscribed ??
        true,

      symbols,

      messages:
        status?.messages ??
        null,

      ticks:
        status?.ticks ??
        null,

      errors:
        status?.errors ??
        0,

      decode_errors:
        status?.decode_errors ??
        0,

      latest,

      elapsed_ms:
        Date.now() - started
    };

  } finally {

    try {

      if (
        typeof ws.close === "function"
      ) {
        await ws.close();
      }

    } catch (error) {

      logError(
        "YahooWS close error:",
        error?.message ||
        error
      );
    }
  }
}


/* ============================================================
 * MAIN S0 / S1 SCAN
 * ============================================================
 */

async function runS0S1Scan({
  symbols = null,
  s1_limit = 50
} = {}) {

  const started =
    Date.now();

  scanState.ok = true;

  scanState.version =
    APP_VERSION;

  scanState.stage =
    "LOADING_UNIVERSE";

  scanState.asof =
    new Date().toISOString();

  scanState.elapsed_ms =
    0;

  scanState.symbols_requested =
    0;

  scanState.s0 = [];

  scanState.s1 = [];

  scanState.lots = [];

  scanState.errors = [];

  /*
   * Validate limit.
   */

  const limit =
    Math.min(
      200,
      Math.max(
        10,
        Number(s1_limit) || 50
      )
    );

  /*
   * Universe.
   */

  const requested =
    Array.isArray(symbols) &&
    symbols.length
      ? [
          ...new Set(
            symbols
              .map(
                s =>
                  String(s)
                    .trim()
                    .toUpperCase()
              )
              .filter(Boolean)
          )
        ]
      : await loadUniverse();

  scanState.symbols_requested =
    requested.length;

  /*
   * Yahoo data.
   */

  scanState.stage =
    "YAHOO_FETCH";

  const yahoo =
    await yahooSpark(
      requested,
      "5d",
      "5m"
    );

  scanState.errors.push(
    ...yahoo.errors
  );

  /*
   * S0.
   */

  scanState.stage =
    "S0";

  const s0 = [];

  for (
    const symbol of requested
  ) {

    const item =
      yahoo.data.get(symbol);

    if (!item) {
      continue;
    }

    const price =
      getCurrentPrice(item);

    if (
      price === null ||
      price <= 0 ||
      price >= 5
    ) {
      continue;
    }

    const quote =
      item?.meta || {};

    if (
      !isOrdinaryStock(
        symbol,
        quote
      )
    ) {
      continue;
    }

    s0.push({

      symbol,

      price,

      exchange:
        quote.exchangeName ||
        null,

      currency:
        quote.currency ||
        null,

      quoteType:
        quote.quoteType ||
        null
    });
  }

  scanState.s0 =
    s0;

  /*
   * S1.
   */

  scanState.stage =
    "S1";

  const s1 = [];

  for (
    const row of s0
  ) {

    const item =
      yahoo.data.get(
        row.symbol
      );

    if (!item) {
      continue;
    }

    const bars =
      extractBars(item);

    const metrics =
      calculateS1(
        row.symbol,
        bars
      );

    if (!metrics) {
      continue;
    }

    s1.push({
      ...row,
      ...metrics
    });
  }

  /*
   * Ranking.
   *
   * Primary:
   *   RVOL15M
   *
   * Secondary:
   *   Accel5M
   *
   * Tertiary:
   *   priceVsVWAP
   *
   * Nulls are always last.
   */

  s1.sort(
    (a, b) => {

      const ar =
        a.RVOL15M ?? -Infinity;

      const br =
        b.RVOL15M ?? -Infinity;

      if (br !== ar) {
        return br - ar;
      }

      const aa =
        a.Accel5M ?? -Infinity;

      const ba =
        b.Accel5M ?? -Infinity;

      if (ba !== aa) {
        return ba - aa;
      }

      const av =
        a.priceVsVWAP ?? -Infinity;

      const bv =
        b.priceVsVWAP ?? -Infinity;

      return bv - av;
    }
  );

  /*
   * Keep only requested ranking depth.
   */

  const ranked =
    s1.slice(
      0,
      limit
    );

  scanState.s1 =
    ranked;

  /*
   * Lots of 20.
   */

  scanState.stage =
    "LOTS";

  scanState.lots =
    makeLots(
      ranked,
      20
    );

  /*
   * Complete.
   */

  scanState.stage =
    "S0_S1";

  scanState.elapsed_ms =
    Date.now() - started;

  return {

    ok: true,

    stage:
      scanState.stage,

    asof:
      scanState.asof,

    elapsed_ms:
      scanState.elapsed_ms,

    universe_requested:
      requested.length,

    s0_count:
      s0.length,

    s0,

    s1_count:
      ranked.length,

    s1:
      ranked,

    lots:
      scanState.lots,

    errors:
      scanState.errors
  };
}


/* ============================================================
 * TOOL REGISTRATION
 * ============================================================
 */

function registerTools(server) {

  /*
   * ----------------------------------------------------------
   * PING
   * ----------------------------------------------------------
   */

  server.tool(

    "ping",

    "Check that Yahoo Scan MCP is alive.",

    {},

    async () => {

      return {

        content: [
          {
            type: "text",

            text:
              JSON.stringify({
                ok: true,

                service:
                  SERVICE_NAME,

                version:
                  APP_VERSION,

                timestamp:
                  new Date().toISOString()
              })
          }
        ]
      };
    }
  );


  /*
   * ----------------------------------------------------------
   * GET STATUS
   * ----------------------------------------------------------
   */

  server.tool(

    "get_status",

    "Return server, Yahoo and scan status.",

    {},

    async () => {

      return {

        content: [
          {
            type: "text",

            text:
              JSON.stringify({

                ok: true,

                service:
                  SERVICE_NAME,

                version:
                  APP_VERSION,

                yahoo: {
                  connected: false,

                  mode:
                    "on-demand",

                  note:
                    "Yahoo WebSocket is created by yahoo_ws_test."
                },

                scan:
                  scanState

              })
          }
        ]
      };
    }
  );


  /*
   * ----------------------------------------------------------
   * GET UNIVERSE
   * ----------------------------------------------------------
   */

  server.tool(

    "get_universe",

    "Return the materialized S0 universe loaded from universe_s0.txt.",

    {},

    async () => {

      try {

        const universe =
          await loadUniverse();

        return {

          content: [
            {
              type: "text",

              text:
                JSON.stringify({

                  ok: true,

                  count:
                    universe.length,

                  loaded_at:
                    universeLoadedAt,

                  source:
                    "universe_s0.txt",

                  symbols:
                    universe

                })
            }
          ]
        };

      } catch (error) {

        return {

          content: [
            {
              type: "text",

              text:
                JSON.stringify({

                  ok: false,

                  error:
                    error?.message ||
                    String(error)

                })
            }
          ],

          isError: true
        };
      }
    }
  );


  /*
   * ----------------------------------------------------------
   * YAHOO WS TEST
   * ----------------------------------------------------------
   */

  server.tool(

    "yahoo_ws_test",

    "Test the Yahoo WebSocket using AAPL, MSFT and NVDA.",

    {},

    async () => {

      try {

        const result =
          await runYahooWSTest();

        return {

          content: [
            {
              type: "text",

              text:
                JSON.stringify(result)
            }
          ]
        };

      } catch (error) {

        return {

          content: [
            {
              type: "text",

              text:
                JSON.stringify({

                  ok: false,

                  error:
                    error?.message ||
                    String(error)

                })
            }
          ],

          isError: true
        };
      }
    }
  );


  /*
   * ----------------------------------------------------------
   * S0 / S1 SCAN
   * ----------------------------------------------------------
   */

  server.tool(

    "yahoo_s0_s1_scan",

    "Run the Yahoo S0/S1 scan. If symbols are omitted, the materialized universe_s0.txt is used.",

    {
      symbols:
        z.array(
          z.string()
        )
        .optional(),

      s1_limit:
        z.number()
          .int()
          .min(10)
          .max(200)
          .optional()
    },

    async ({
      symbols,
      s1_limit
    }) => {

      try {

        const result =
          await runS0S1Scan({
            symbols:
              symbols ?? null,

            s1_limit:
              s1_limit ?? 50
          });

        return {

          content: [
            {
              type: "text",

              text:
                JSON.stringify(result)
            }
          ]
        };

      } catch (error) {

        scanState.ok = false;

        scanState.stage =
          "ERROR";

        scanState.elapsed_ms =
          0;

        scanState.errors.push({

          error:
            error?.message ||
            String(error)

        });

        return {

          content: [
            {
              type: "text",

              text:
                JSON.stringify({

                  ok: false,

                  stage:
                    "ERROR",

                  error:
                    error?.message ||
                    String(error)

                })
            }
          ],

          isError: true
        };
      }
    }
  );


  /*
   * ----------------------------------------------------------
   * GET SCAN STATE
   * ----------------------------------------------------------
   */

  server.tool(

    "get_scan_state",

    "Return the complete current S0/S1 scan state.",

    {},

    async () => {

      return {

        content: [
          {
            type: "text",

            text:
              JSON.stringify(
                scanState
              )
          }
        ]
      };
    }
  );


  log(
    "Tools registered:",
    [
      "ping",
      "get_status",
      "get_universe",
      "yahoo_ws_test",
      "yahoo_s0_s1_scan",
      "get_scan_state"
    ].join(", ")
  );
}


/* ============================================================
 * MCP SERVER FACTORY
 * ============================================================
 */

function createMcpServer() {

  const server =
    new McpServer({

      name:
        SERVICE_NAME,

      version:
        APP_VERSION

    });

  registerTools(
    server
  );

  return server;
}


/* ============================================================
 * SESSION STORE
 * ============================================================
 */

const sessions =
  new Map();


/* ============================================================
 * CREATE TRANSPORT
 * ============================================================
 */

function createTransport() {

  const transport =
    new StreamableHTTPServerTransport({

      /*
       * Stateful MCP sessions.
       */

      sessionIdGenerator:
        () => randomUUID(),

      /*
       * Normal JSON responses for ordinary
       * request/response tool calls.
       */

      enableJsonResponse:
        true,

      onsessioninitialized:
        sessionId => {

          sessions.set(
            sessionId,
            {
              transport
            }
          );

          log(
            "MCP session initialized:",
            sessionId
          );
        }
    });

  transport.onerror =
    error => {

      /*
       * Expected protocol negotiation errors
       * are logged, never swallowed.
       */

      logError(
        "MCP transport error:",
        error
      );
    };

  transport.onclose =
    () => {

      const sessionId =
        transport.sessionId;

      if (sessionId) {

        sessions.delete(
          sessionId
        );

        log(
          "MCP session closed:",
          sessionId
        );
      }
    };

  return transport;
}


/* ============================================================
 * HTTP HELPERS
 * ============================================================
 */

function setCors(res) {

  res.setHeader(
    "Access-Control-Allow-Origin",
    "*"
  );

  res.setHeader(
    "Access-Control-Allow-Methods",
    "GET,POST,DELETE,OPTIONS"
  );

  res.setHeader(
    "Access-Control-Allow-Headers",
    [
      "Content-Type",
      "Accept",
      "MCP-Session-Id",
      "MCP-Protocol-Version",
      "Last-Event-ID"
    ].join(", ")
  );

  res.setHeader(
    "Access-Control-Expose-Headers",
    "MCP-Session-Id"
  );
}


function sendJson(
  res,
  status,
  body
) {

  if (!res.headersSent) {

    res.statusCode =
      status;

    res.setHeader(
      "Content-Type",
      "application/json"
    );
  }

  res.end(
    JSON.stringify(body)
  );
}


/* ============================================================
 * MCP HTTP HANDLER
 * ============================================================
 */

async function handleMcp(
  req,
  res
) {

  /*
   * ----------------------------------------------------------
   * IMPORTANT:
   *
   * Do NOT manually parse the MCP body.
   *
   * StreamableHTTPServerTransport owns MCP request parsing.
   * ----------------------------------------------------------
   */

  const sessionId =
    req.headers[
      "mcp-session-id"
    ];


  /*
   * Existing session.
   */

  if (sessionId) {

    const session =
      sessions.get(
        String(sessionId)
      );

    if (!session) {

      sendJson(
        res,
        404,
        {
          jsonrpc:
            "2.0",

          error: {
            code:
              -32000,

            message:
              "Unknown MCP session ID"
          },

          id:
            null
        }
      );

      return;
    }

    await session
      .transport
      .handleRequest(
        req,
        res
      );

    return;
  }


  /*
   * New session.
   *
   * We only create a new MCP server for
   * an initialization request.
   */

  const contentType =
    String(
      req.headers[
        "content-type"
      ] || ""
    ).toLowerCase();

  const method =
    String(
      req.method || ""
    ).toUpperCase();

  /*
   * GET without an existing session is not
   * a valid way to initialize a stateful MCP
   * session.
   */

  if (
    method === "GET"
  ) {

    sendJson(
      res,
      400,
      {
        jsonrpc:
          "2.0",

        error: {
          code:
            -32000,

          message:
            "MCP session ID required"
        },

        id:
          null
      }
    );

    return;
  }

  /*
   * A POST without a session is allowed only
   * for initialization.
   *
   * We deliberately let the MCP transport
   * validate the JSON-RPC initialize request.
   */

  if (
    method !== "POST"
  ) {

    sendJson(
      res,
      405,
      {
        error:
          "Method Not Allowed"
      }
    );

    return;
  }

  /*
   * MCP requests must be JSON.
   */

  if (
    !contentType.includes(
      "application/json"
    )
  ) {

    sendJson(
      res,
      415,
      {
        error:
          "Content-Type must be application/json"
      }
    );

    return;
  }

  const transport =
    createTransport();

  const server =
    createMcpServer();

  /*
   * Connect exactly once.
   */

  await server.connect(
    transport
  );

  /*
   * Transport handles the request.
   *
   * No manual body parsing.
   */

  await transport.handleRequest(
    req,
    res
  );
}


/* ============================================================
 * HTTP SERVER
 * ============================================================
 */

const httpServer =
  http.createServer(
    async (req, res) => {

      setCors(res);

      /*
       * CORS preflight.
       */

      if (
        req.method ===
        "OPTIONS"
      ) {

        res.statusCode =
          204;

        res.end();

        return;
      }

      const url =
        new URL(
          req.url || "/",
          `http://${req.headers.host || "localhost"}`
        );


      /*
       * ------------------------------------------------------
       * ROOT
       * ------------------------------------------------------
       */

      if (
        url.pathname === "/"
      ) {

        sendJson(
          res,
          200,
          {
            ok: true,

            service:
              SERVICE_NAME,

            version:
              APP_VERSION,

            mcp:
              "/mcp",

            health:
              "/health",

            sessions:
              sessions.size
          }
        );

        return;
      }


      /*
       * ------------------------------------------------------
       * HEALTH
       * ------------------------------------------------------
       */

      if (
        url.pathname ===
        "/health"
      ) {

        sendJson(
          res,
          200,
          {
            ok: true,

            service:
              SERVICE_NAME,

            version:
              APP_VERSION,

            sessions:
              sessions.size,

            universe_loaded:
              Boolean(
                universeCache
              ),

            universe_count:
              universeCache
                ? universeCache.length
                : 0
          }
        );

        return;
      }


      /*
       * ------------------------------------------------------
       * MCP
       * ------------------------------------------------------
       */

      if (
        url.pathname === "/mcp"
      ) {

        try {

          await handleMcp(
            req,
            res
          );

        } catch (error) {

          logError(
            "MCP request error:",
            error
          );

          if (
            !res.headersSent
          ) {

            sendJson(
              res,
              500,
              {
                jsonrpc:
                  "2.0",

                error: {
                  code:
                    -32603,

                  message:
                    error?.message ||
                    "Internal MCP error"
                },

                id:
                  null
              }
            );

          } else {

            try {
              res.end();
            } catch {}
          }
        }

        return;
      }


      /*
       * ------------------------------------------------------
       * 404
       * ------------------------------------------------------
       */

      sendJson(
        res,
        404,
        {
          ok: false,

          error:
            "Not found"
        }
      );
    }
  );


/* ============================================================
 * SERVER ERROR
 * ============================================================
 */

httpServer.on(
  "error",
  error => {

    logError(
      "HTTP server error:",
      error
    );
  }
);


/* ============================================================
 * START
 * ============================================================
 */

httpServer.listen(
  PORT,
  "0.0.0.0",
  () => {

    log(
      `MCP server started v${APP_VERSION}`
    );

    log(
      `HTTP server listening on ${PORT}`
    );

    log(
      `MCP endpoint: /mcp`
    );

    log(
      `Health endpoint: /health`
    );
  }
);


/* ============================================================
 * GRACEFUL SHUTDOWN
 * ============================================================
 */

async function shutdown(
  signal
) {

  log(
    `Received ${signal}, shutting down...`
  );

  for (
    const [
      sessionId,
      session
    ] of sessions
  ) {

    try {

      await session
        .transport
        .close();

    } catch (error) {

      logError(
        `Error closing session ${sessionId}:`,
        error
      );
    }
  }

  sessions.clear();

  httpServer.close(
    () => {

      log(
        "HTTP server closed."
      );

      process.exit(0);
    }
  );
}


process.on(
  "SIGTERM",
  () => shutdown("SIGTERM")
);

process.on(
  "SIGINT",
  () => shutdown("SIGINT")
);
