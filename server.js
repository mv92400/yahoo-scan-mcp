import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { z } from "zod";
import http from "node:http";
import { readFile, writeFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import { YahooWS } from "./src/yahoo-ws.js";

const PORT = Number(process.env.PORT || 8080);
const APP_VERSION = "1.5.0";
const SERVICE_NAME = "yahoo-scan-mcp";

const UNIVERSE_FILE = new URL("./universe_s0.txt", import.meta.url);
const S0_CACHE_FILE = new URL("./s0_materialized.json", import.meta.url);
const S1_CACHE_FILE = new URL("./s1_materialized.json", import.meta.url);

const YAHOO_SPARK_URL = "https://query1.finance.yahoo.com/v7/finance/spark";

/* -------------------------------------------------------------------------- */
/* PERFORMANCE / RELIABILITY SETTINGS                                          */
/* -------------------------------------------------------------------------- */

const S0_LOOKBACK_RANGE = "5d";
const S0_INTERVAL = "5m";

const INTRADAY_RANGE = "1d";
const INTRADAY_INTERVAL = "5m";

const YAHOO_BATCH_SIZE = 20;
const YAHOO_CONCURRENCY = 4;
const YAHOO_RETRIES = 3;
const YAHOO_TIMEOUT_MS = 15_000;
const YAHOO_BACKOFF_MS = 600;

const MAX_BODY_BYTES = 1_000_000;
const BAR_MS = 5 * 60 * 1000;
const MAX_BAR_AGE_MS = 15 * 60 * 1000;
const LOT_SIZE = 20;

const DEFAULT_MAX_J1_RETURN = 0.05;
const DEFAULT_MAX_J2_RETURN = 0.05;

let universeCache = null;
let universeLoadedAt = null;
let activeOperation = null;

let s0Cache = {
  prepared_for_date: null,
  prepared_at: null,
  source: "none",
  universe_count: 0,
  eligible_count: 0,
  config: null,
  scope_fingerprint: null,
  rows: [],
  errors: [],
};

let s1Cache = {
  prepared_for_date: null,
  asof: null,
  source: "none",
  rows: [],
  lots: [],
  errors: [],
};

const scanState = {
  ok: true,
  version: APP_VERSION,
  stage: "IDLE",
  asof: null,
  elapsed_ms: 0,
  symbols_requested: 0,
  source: "Yahoo Spark",
  s0_count: 0,
  s1_count: 0,
  lots_count: 0,
  errors: [],
  universe_loaded_at: null,
  busy: false,
  data_fresh: null,
  s0_prepared_for_date: null,
  s1_prepared_for_date: null,
};

function log(...args) {
  console.log(`[${SERVICE_NAME}]`, ...args);
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function chunk(array, size) {
  const out = [];
  for (let i = 0; i < array.length; i += size) {
    out.push(array.slice(i, i + size));
  }
  return out;
}

async function mapWithConcurrency(items, concurrency, worker) {
  const results = new Array(items.length);
  let next = 0;

  async function runner() {
    while (true) {
      const index = next++;
      if (index >= items.length) return;
      results[index] = await worker(items[index], index);
    }
  }

  const count = Math.min(concurrency, Math.max(items.length, 1));

  await Promise.all(
    Array.from({ length: count }, () => runner())
  );

  return results;
}

function uniqueUpperSymbols(input) {
  return [...new Set(
    input
      .map(v => String(v || "").trim().toUpperCase())
      .filter(Boolean)
  )];
}

function getTimezoneParts(
  timestampMs,
  timeZone = "America/New_York"
) {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hourCycle: "h23",
  }).formatToParts(new Date(timestampMs));

  const obj = Object.fromEntries(
    parts.map(p => [p.type, p.value])
  );

  return {
    date: `${obj.year}-${obj.month}-${obj.day}`,
    hour: Number(obj.hour),
    minute: Number(obj.minute),
    second: Number(obj.second),
  };
}

function nowNyDate() {
  return getTimezoneParts(Date.now()).date;
}

function fingerprintSymbols(symbols) {
  return createHash("sha256")
    .update(symbols.join(","))
    .digest("hex");
}

function median(values) {
  const nums = values
    .filter(Number.isFinite)
    .slice()
    .sort((a, b) => a - b);

  if (!nums.length) return null;

  const mid = Math.floor(nums.length / 2);

  return nums.length % 2
    ? nums[mid]
    : (nums[mid - 1] + nums[mid]) / 2;
}

function mean(values) {
  const nums = values.filter(Number.isFinite);

  if (!nums.length) return null;

  return nums.reduce((a, b) => a + b, 0) / nums.length;
}

function safeRatio(a, b) {
  if (
    !Number.isFinite(a) ||
    !Number.isFinite(b) ||
    b === 0
  ) {
    return null;
  }

  return a / b;
}

function nonNullNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function normalizeReturn(value) {
  return Number.isFinite(value) ? value : null;
}

function extractBars(symbol, sparkEntry) {
  if (!sparkEntry) return [];

  const timestamps =
    Array.isArray(sparkEntry.timestamp)
      ? sparkEntry.timestamp
      : [];

  const quote =
    sparkEntry.indicators?.quote?.[0] || {};

  const bars = [];

  for (let i = 0; i < timestamps.length; i++) {
    const ts = Number(timestamps[i]);

    if (!Number.isFinite(ts)) continue;

    const bar = {
      symbol,
      ts: ts * 1000,
      open: nonNullNumber(quote.open?.[i]),
      high: nonNullNumber(quote.high?.[i]),
      low: nonNullNumber(quote.low?.[i]),
      close: nonNullNumber(quote.close?.[i]),
      volume: nonNullNumber(quote.volume?.[i]),
    };

    if (
      bar.close == null &&
      bar.open == null &&
      bar.volume == null
    ) {
      continue;
    }

    bars.push(bar);
  }

  return bars.sort((a, b) => a.ts - b.ts);
}

async function fetchYahooBatch(symbols, range, interval) {
  const controller = new AbortController();

  const timeout = setTimeout(
    () => controller.abort(),
    YAHOO_TIMEOUT_MS
  );

  const params = new URLSearchParams({
    symbols: symbols.join(","),
    range,
    interval,
    indicators: "quote,close",
    includeTimestamps: "true",
    includePrePost: "false",
  });

  try {
    const response = await fetch(
      `${YAHOO_SPARK_URL}?${params.toString()}`,
      {
        method: "GET",
        headers: {
          "Accept": "application/json",
          "User-Agent":
            "Mozilla/5.0 (compatible; yahoo-scan-mcp/1.5)",
        },
        signal: controller.signal,
      }
    );

    const text = await response.text();

    if (!response.ok) {
      const error = new Error(
        `Yahoo HTTP ${response.status}: ${text.slice(0, 180)}`
      );

      error.status = response.status;
      error.retryAfter =
        response.headers.get("retry-after");

      throw error;
    }

    let json;

    try {
      json = JSON.parse(text);
    } catch {
      throw new Error("Yahoo returned invalid JSON");
    }

    const results = json?.spark?.result;

    if (!Array.isArray(results)) {
      throw new Error(
        "Yahoo response missing spark.result"
      );
    }

    const out = new Map();

    for (const entry of results) {
      const symbol = String(
        entry?.symbol || ""
      ).toUpperCase();

      if (!symbol) continue;

      out.set(symbol, {
        meta: entry?.meta || {},
        bars: extractBars(symbol, entry),
      });
    }

    return out;
  } finally {
    clearTimeout(timeout);
  }
}

async function yahooSpark(symbols, range, interval) {
  const batches = chunk(
    symbols,
    YAHOO_BATCH_SIZE
  );

  const errors = [];

  const results = await mapWithConcurrency(
    batches,
    YAHOO_CONCURRENCY,
    async (batch, batchIndex) => {
      let lastError = null;

      for (
        let attempt = 1;
        attempt <= YAHOO_RETRIES;
        attempt++
      ) {
        try {
          return await fetchYahooBatch(
            batch,
            range,
            interval
          );
        } catch (error) {
          lastError = error;

          const retryAfter =
            Number(error?.retryAfter);

          const waitMs =
            Number.isFinite(retryAfter)
              ? Math.max(
                  500,
                  retryAfter * 1000
                )
              : YAHOO_BACKOFF_MS *
                (2 ** (attempt - 1));

          if (attempt < YAHOO_RETRIES) {
            log(
              `Yahoo ${range}/${interval} batch ` +
              `${batchIndex + 1}/${batches.length}`,
              `attempt ${attempt} failed:`,
              error?.message || error
            );

            await sleep(waitMs);
          }
        }
      }

      errors.push({
        batch: batchIndex,
        symbols: batch,
        range,
        interval,
        error:
          lastError?.message ||
          "Unknown Yahoo error",
      });

      return new Map();
    }
  );

  const merged = new Map();

  for (const map of results) {
    if (!map) continue;

    for (const [symbol, value] of map.entries()) {
      merged.set(symbol, value);
    }
  }

  return {
    data: merged,
    errors,
  };
}

function isOrdinaryStock(entry) {
  const quoteType =
    String(entry?.meta?.quoteType || "")
      .toUpperCase();

  if (quoteType && quoteType !== "EQUITY") {
    return false;
  }

  if (!quoteType) {
    return false;
  }

  const typeDisp =
    String(entry?.meta?.typeDisp || "")
      .toLowerCase();

  if (
    /fund|etf|trust|warrant|preferred|unit|index|future|option|bond/.test(
      typeDisp
    )
  ) {
    return false;
  }

  return true;
}

function groupBySession(bars) {
  const sessions = new Map();

  for (const bar of bars) {
    if (!Number.isFinite(bar.ts)) continue;

    const session =
      getTimezoneParts(bar.ts).date;

    if (!sessions.has(session)) {
      sessions.set(session, []);
    }

    sessions.get(session).push(bar);
  }

  return [...sessions.entries()]
    .sort((a, b) =>
      a[0].localeCompare(b[0])
    )
    .map(([date, sessionBars]) => [
      date,
      sessionBars
        .filter(b => Number.isFinite(b.ts))
        .sort((a, b) => a.ts - b.ts),
    ]);
}

function completedRegularSessions(
  bars,
  referenceDate = nowNyDate()
) {
  return groupBySession(bars)
    .filter(([date]) => date < referenceDate);
}

function calculateVWAP(bars) {
  let pv = 0;
  let vol = 0;

  for (const bar of bars) {
    if (
      !Number.isFinite(bar.volume) ||
      bar.volume <= 0 ||
      !Number.isFinite(bar.close)
    ) {
      continue;
    }

    const typical =
      Number.isFinite(bar.high) &&
      Number.isFinite(bar.low)
        ? (bar.high +
            bar.low +
            bar.close) / 3
        : bar.close;

    pv += typical * bar.volume;
    vol += bar.volume;
  }

  return vol > 0 ? pv / vol : null;
}

function dayDollarVolume(sessionBars) {
  let value = 0;

  for (const bar of sessionBars) {
    if (
      Number.isFinite(bar.volume) &&
      bar.volume > 0 &&
      Number.isFinite(bar.close)
    ) {
      value += bar.volume * bar.close;
    }
  }

  return value;
}

function buildRvolBaselineByIndex(
  historicalSessions
) {
  const maxLength = Math.max(
    0,
    ...historicalSessions.map(
      ([, bars]) => bars.length
    )
  );

  const out = new Array(maxLength)
    .fill(null);

  for (let idx = 0; idx < maxLength; idx++) {
    const values = [];

    for (const [, sessionBars] of historicalSessions) {
      if (sessionBars.length <= idx) continue;

      const start = idx - 2;

      if (start < 0) continue;

      const window = sessionBars.slice(
        start,
        idx + 1
      );

      if (window.length !== 3) continue;

      const vol15 =
        window.reduce(
          (sum, b) =>
            sum + (b.volume || 0),
          0
        );

      if (vol15 > 0) {
        values.push(vol15);
      }
    }

    if (values.length) {
      out[idx] = {
        mean: mean(values),
        median: median(values),
        samples: values.length,
      };
    }
  }

  return out;
}

function computeHistoricalS0(
  symbol,
  entry,
  options
) {
  if (!entry || !entry.bars?.length) {
    return null;
  }

  if (!isOrdinaryStock(entry)) {
    return null;
  }

  const bars = entry.bars;

  const nyToday = options.referenceDate;

  const sessions = groupBySession(bars);

  const completed =
    sessions.filter(
      ([date]) => date < nyToday
    );

  if (completed.length < 3) {
    return null;
  }

  const last3 = completed.slice(-3);

  const [j1Date, j1Bars] = last3[2] || [];
  const [j2Date, j2Bars] = last3[1] || [];
  const [, j3Bars] = last3[0] || [];

  if (
    !j1Bars?.length ||
    !j2Bars?.length ||
    !j3Bars?.length
  ) {
    return null;
  }

  const j1Close =
    j1Bars[j1Bars.length - 1]?.close;

  const j2Close =
    j2Bars[j2Bars.length - 1]?.close;

  const j3Close =
    j3Bars[j3Bars.length - 1]?.close;

  const j1Return =
    safeRatio(
      j1Close - j2Close,
      j2Close
    );

  const j2Return =
    safeRatio(
      j2Close - j3Close,
      j3Close
    );

  const referenceBars =
    bars[bars.length - 1];

  const referencePrice =
    referenceBars?.close;

  if (
    !Number.isFinite(referencePrice) ||
    referencePrice <= 0 ||
    referencePrice >= 5
  ) {
    return null;
  }

  const historicalForBaseline =
    completed.slice(
      -Math.max(
        3,
        options.baselineSessions
      )
    );

  const rvolBaseline15mByIndex =
    buildRvolBaselineByIndex(
      historicalForBaseline
    );

  const dailyDollarVolumes =
    historicalForBaseline.map(
      ([, sessionBars]) =>
        dayDollarVolume(sessionBars)
    );

  const avgDailyDollarVolume =
    mean(
      dailyDollarVolumes.filter(
        v => v > 0
      )
    );

  const j1TooHigh =
    options.maxJ1Return != null &&
    Number.isFinite(j1Return) &&
    j1Return >
      options.maxJ1Return;

  const j2TooHigh =
    options.maxJ2Return != null &&
    Number.isFinite(j2Return) &&
    j2Return >
      options.maxJ2Return;

  const liquidityTooLow =
    options.minAvgDailyDollarVolume != null &&
    (
      !Number.isFinite(
        avgDailyDollarVolume
      ) ||
      avgDailyDollarVolume <
        options.minAvgDailyDollarVolume
    );

  if (
    j1TooHigh ||
    j2TooHigh ||
    liquidityTooLow
  ) {
    return null;
  }

  return {
    symbol,
    ordinary_stock: true,
    prepared_for_date: nyToday,
    reference_price: referencePrice,
    J1: normalizeReturn(j1Return),
    J2: normalizeReturn(j2Return),
    J1_date: j1Date,
    J2_date: j2Date,
    prior_close: j1Close,
    prior2_close: j2Close,
    avgDailyDollarVolume:
      avgDailyDollarVolume,
    rvolBaseline15mByIndex,
    baseline_sessions:
      historicalForBaseline.map(
        ([date]) => date
      ),
    baseline_samples_max: Math.max(
      0,
      ...rvolBaseline15mByIndex
        .filter(Boolean)
        .map(v => v.samples)
    ),
  };
}

function getCurrentSessionBars(
  bars,
  referenceDate,
  nowMs
) {
  const sessions =
    groupBySession(bars);

  const current =
    sessions.find(
      ([date]) =>
        date === referenceDate
    )?.[1] || [];

  return current.filter(
    bar =>
      Number.isFinite(bar.ts) &&
      (bar.ts + BAR_MS) <=
        (nowMs + 1000)
  );
}

function calculateS1Intraday(
  symbol,
  entry,
  s0Row,
  nowMs
) {
  if (
    !entry ||
    !entry.bars?.length ||
    !s0Row
  ) {
    return null;
  }

  if (!isOrdinaryStock(entry)) {
    return null;
  }

  const currentDate =
    getTimezoneParts(nowMs).date;

  const currentSessionBars =
    getCurrentSessionBars(
      entry.bars,
      currentDate,
      nowMs
    );

  if (currentSessionBars.length < 4) {
    return null;
  }

  const currentIndex =
    currentSessionBars.length - 1;

  const latest =
    currentSessionBars[currentIndex];

  const latestAgeMs =
    nowMs - latest.ts;

  const barFresh =
    latestAgeMs >= 0 &&
    latestAgeMs <=
      MAX_BAR_AGE_MS;

  const last3 =
    currentSessionBars.slice(
      Math.max(
        0,
        currentIndex - 2
      ),
      currentIndex + 1
    );

  const previous3 =
    currentSessionBars.slice(
      Math.max(
        0,
        currentIndex - 3
      ),
      currentIndex
    );

  if (
    last3.length !== 3 ||
    previous3.length !== 3
  ) {
    return null;
  }

  const vol15m =
    last3.reduce(
      (sum, b) =>
        sum + (b.volume || 0),
      0
    );

  const accel5m =
    safeRatio(
      latest.volume || 0,
      mean(
        previous3.map(
          b => b.volume || 0
        )
      )
    );

  const baseline =
    s0Row.rvolBaseline15mByIndex?.[
      currentIndex
    ] || null;

  const rvol15m =
    baseline?.mean != null
      ? safeRatio(
          vol15m,
          baseline.mean
        )
      : null;

  const rvol15mMedian =
    baseline?.median != null
      ? safeRatio(
          vol15m,
          baseline.median
        )
      : null;

  const vwap =
    calculateVWAP(
      currentSessionBars
    );

  const price =
    latest.close;

  if (
    !Number.isFinite(price) ||
    price <= 0 ||
    price >= 5
  ) {
    return null;
  }

  const priceVsVWAP =
    safeRatio(
      price - vwap,
      vwap
    );

  const hod = Math.max(
    ...currentSessionBars.map(
      b =>
        b.high ??
        b.close ??
        0
    )
  );

  const hodDistance =
    hod > 0
      ? safeRatio(
          price - hod,
          hod
        )
      : null;

  const dollarVol15m =
    Number.isFinite(price)
      ? vol15m * price
      : null;

  const ny =
    getTimezoneParts(
      latest.ts
    );

  return {
    symbol,
    session: currentDate,
    asof:
      new Date(
        latest.ts
      ).toISOString(),
    bar_age_sec:
      Math.max(
        0,
        Math.round(
          latestAgeMs / 1000
        )
      ),
    bar_fresh: barFresh,
    bar_is_complete: true,
    ny_time:
      `${String(ny.hour).padStart(2, "0")}:` +
      `${String(ny.minute).padStart(2, "0")}:` +
      `${String(ny.second).padStart(2, "0")}`,
    bar_index: currentIndex,
    price,
    Vol15M: vol15m,
    DollarVol15M: dollarVol15m,
    RVOL15M: rvol15m,
    RVOL15M_median:
      rvol15mMedian,
    Accel5M: accel5m,
    VWAP: vwap,
    priceVsVWAP,
    HOD: hod,
    hodDistance,
    J1: s0Row.J1,
    J2: s0Row.J2,
    J1_date: s0Row.J1_date,
    J2_date: s0Row.J2_date,
    avgDailyDollarVolume:
      s0Row.avgDailyDollarVolume,
    s0_prepared_for_date:
      s0Row.prepared_for_date,
  };
}

function rankS1(rows) {
  return rows
    .filter(Boolean)
    .sort((a, b) => {
      const rvol =
        (b.RVOL15M ?? -Infinity) -
        (a.RVOL15M ?? -Infinity);

      if (rvol !== 0) {
        return rvol;
      }

      const accel =
        (b.Accel5M ?? -Infinity) -
        (a.Accel5M ?? -Infinity);

      if (accel !== 0) {
        return accel;
      }

      const vwap =
        (b.priceVsVWAP ?? -Infinity) -
        (a.priceVsVWAP ?? -Infinity);

      if (vwap !== 0) {
        return vwap;
      }

      const hod =
        (b.hodDistance ?? -Infinity) -
        (a.hodDistance ?? -Infinity);

      if (hod !== 0) {
        return hod;
      }

      return String(a.symbol)
        .localeCompare(
          String(b.symbol)
        );
    });
}

function buildLots(
  rows,
  size = LOT_SIZE
) {
  const lots = [];

  for (
    let i = 0;
    i < rows.length;
    i += size
  ) {
    const slice =
      rows.slice(
        i,
        i + size
      );

    lots.push({
      lot:
        Math.floor(i / size) + 1,
      count:
        slice.length,
      symbols:
        slice.map(
          r => r.symbol
        ),
    });
  }

  return lots;
}

function resetScanState() {
  scanState.ok = true;
  scanState.version =
    APP_VERSION;
  scanState.stage = "IDLE";
  scanState.asof = null;
  scanState.elapsed_ms = 0;
  scanState.symbols_requested = 0;
  scanState.source =
    "Yahoo Spark";
  scanState.s0_count =
    s0Cache.rows.length;
  scanState.s1_count =
    s1Cache.rows.length;
  scanState.lots_count =
    s1Cache.lots.length;
  scanState.errors = [];
  scanState.busy =
    Boolean(activeOperation);
  scanState.data_fresh = null;
  scanState.s0_prepared_for_date =
    s0Cache.prepared_for_date;
  scanState.s1_prepared_for_date =
    s1Cache.prepared_for_date;
}

async function withOperationLock(
  name,
  fn
) {
  if (activeOperation) {
    return {
      ok: false,
      busy: true,
      operation:
        activeOperation,
      error:
        `Operation already running: ${activeOperation}`,
    };
  }

  activeOperation = name;
  scanState.busy = true;
  scanState.stage =
    name.toUpperCase();

  try {
    return await fn();
  } finally {
    activeOperation = null;
    scanState.busy = false;

    if (
      scanState.stage !==
      "ERROR"
    ) {
      scanState.stage =
        "IDLE";
    }
  }
}

async function persistJson(
  url,
  value
) {
  await writeFile(
    url,
    JSON.stringify(value),
    "utf8"
  );
}

async function loadJsonIfValid(
  url
) {
  try {
    const raw =
      await readFile(
        url,
        "utf8"
      );

    return JSON.parse(raw);
  } catch {
    return null;
  }
}

async function loadUniverse(
  force = false
) {
  if (
    universeCache &&
    !force
  ) {
    return universeCache;
  }

  const raw =
    await readFile(
      UNIVERSE_FILE,
      "utf8"
    );

  const symbols =
    uniqueUpperSymbols(
      raw
        .split(/\r?\n/)
        .map(line =>
          line
            .replace(/#.*/, "")
            .trim()
        )
        .map(line =>
          line.split(
            /[\s,;]+/
          )[0]
        )
        .filter(Boolean)
    );

  if (!symbols.length) {
    throw new Error(
      "universe_s0.txt is empty or contains no valid ticker"
    );
  }

  universeCache =
    symbols;

  universeLoadedAt =
    new Date().toISOString();

  scanState.universe_loaded_at =
    universeLoadedAt;

  return symbols;
}

async function restoreCaches() {
  const today =
    nowNyDate();

  const diskS0 =
    await loadJsonIfValid(
      S0_CACHE_FILE
    );

  if (
    diskS0?.prepared_for_date === today &&
    Array.isArray(
      diskS0.rows
    ) &&
    diskS0?.config &&
    Number.isFinite(
      diskS0.config.maxJ1Return
    ) &&
    Number.isFinite(
      diskS0.config.maxJ2Return
    ) &&
    typeof diskS0.scope_fingerprint ===
      "string"
  ) {
    s0Cache =
      diskS0;

    log(
      `Restored S0 materialization for ${today}:`,
      `${s0Cache.rows.length} symbols`
    );
  }

  const diskS1 =
    await loadJsonIfValid(
      S1_CACHE_FILE
    );

  if (
    diskS1?.prepared_for_date === today &&
    Array.isArray(
      diskS1.rows
    ) &&
    Array.isArray(
      diskS1.lots
    )
  ) {
    s1Cache =
      diskS1;

    log(
      `Restored S1 materialization for ${today}:`,
      `${s1Cache.rows.length} symbols`
    );
  }

  resetScanState();
}

async function prepareS0({
  symbols,
  force,
  maxJ1Return,
  maxJ2Return,
  minAvgDailyDollarVolume,
}) {
  return withOperationLock(
    "S0_PREPARE",
    async () => {
      const started =
        Date.now();

      const preparedForDate =
        nowNyDate();

      const universe =
        uniqueUpperSymbols(
          symbols?.length
            ? symbols
            : await loadUniverse()
        );

      const scopeFingerprint =
        fingerprintSymbols(
          universe
        );

      const requestedConfig = {
        maxJ1Return:
          maxJ1Return ?? null,
        maxJ2Return:
          maxJ2Return ?? null,
        minAvgDailyDollarVolume:
          minAvgDailyDollarVolume ??
          null,
      };

      const sameConfig =
        JSON.stringify(
          s0Cache.config
        ) ===
        JSON.stringify(
          requestedConfig
        );

      const sameScope =
        s0Cache.scope_fingerprint ===
        scopeFingerprint;

      if (
        !force &&
        s0Cache.prepared_for_date ===
          preparedForDate &&
        s0Cache.rows.length &&
        sameConfig &&
        sameScope
      ) {
        scanState.stage =
          "S0_READY";

        scanState.elapsed_ms =
          Date.now() -
          started;

        return {
          ok: true,
          reused_cache: true,
          stage:
            "S0_READY",
          version:
            APP_VERSION,
          prepared_for_date:
            preparedForDate,
          elapsed_ms:
            scanState.elapsed_ms,
          universe_count:
            universe.length,
          eligible_count:
            s0Cache.rows.length,
          s0:
            s0Cache.rows,
          errors:
            s0Cache.errors,
        };
      }

      scanState.stage =
        "S0_YAHOO_FETCH";

      scanState.symbols_requested =
        universe.length;

      const {
        data,
        errors,
      } = await yahooSpark(
        universe,
        S0_LOOKBACK_RANGE,
        S0_INTERVAL
      );

      const rows = [];

      scanState.stage =
        "S0_CALC_FILTER";

      for (const symbol of universe) {
        const row =
          computeHistoricalS0(
            symbol,
            data.get(symbol),
            {
              referenceDate:
                preparedForDate,
              baselineSessions:
                4,
              maxJ1Return,
              maxJ2Return,
              minAvgDailyDollarVolume,
            }
          );

        if (row) {
          rows.push(row);
        }
      }

      s0Cache = {
        prepared_for_date:
          preparedForDate,
        prepared_at:
          new Date().toISOString(),
        source:
          "Yahoo Spark 5d/5m",
        universe_count:
          universe.length,
        eligible_count:
          rows.length,
        config:
          requestedConfig,
        scope_fingerprint:
          scopeFingerprint,
        rows,
        errors,
      };

      await persistJson(
        S0_CACHE_FILE,
        s0Cache
      );

      scanState.stage =
        "S0_READY";

      scanState.s0_count =
        rows.length;

      scanState.s0_prepared_for_date =
        preparedForDate;

      scanState.elapsed_ms =
        Date.now() -
        started;

      scanState.errors =
        errors;

      return {
        ok: true,
        reused_cache:
          false,
        stage:
          "S0_READY",
        version:
          APP_VERSION,
        prepared_for_date:
          preparedForDate,
        prepared_at:
          s0Cache.prepared_at,
        elapsed_ms:
          scanState.elapsed_ms,
        universe_count:
          universe.length,
        eligible_count:
          rows.length,
        s0:
          rows,
        errors,
      };
    }
  );
}

async function runS1({
  limit,
  symbols,
}) {
  return withOperationLock(
    "S1_SCAN",
    async () => {
      const started =
        Date.now();

      const today =
        nowNyDate();

      if (
        s0Cache.prepared_for_date !==
          today ||
        !s0Cache.rows.length
      ) {
        scanState.stage =
          "ERROR";

        scanState.ok =
          false;

        scanState.errors = [
          {
            error:
              `S0 is not prepared for ${today}`,
          },
        ];

        return {
          ok: false,
          stage:
            "S1_BLOCKED",
          version:
            APP_VERSION,
          error:
            `S0 must be prepared before S1 for ${today}`,
          needs_s0:
            true,
        };
      }

      const allowed =
        new Set(
          symbols?.length
            ? uniqueUpperSymbols(
                symbols
              )
            : s0Cache.rows.map(
                r => r.symbol
              )
        );

      const candidates =
        s0Cache.rows.filter(
          r =>
            allowed.has(
              r.symbol
            )
        );

      if (!candidates.length) {
        return {
          ok: true,
          stage:
            "S1_READY",
          version:
            APP_VERSION,
          elapsed_ms:
            Date.now() -
            started,
          s0_count: 0,
          s1_count: 0,
          s1: [],
          lots: [],
          errors: [],
        };
      }

      scanState.stage =
        "S1_INTRADAY_FETCH";

      scanState.symbols_requested =
        candidates.length;

      const {
        data,
        errors,
      } = await yahooSpark(
        candidates.map(
          r => r.symbol
        ),
        INTRADAY_RANGE,
        INTRADAY_INTERVAL
      );

      scanState.stage =
        "S1_CALC";

      const nowMs =
        Date.now();

      const rows = [];

      let staleCount =
        0;

      for (const s0Row of candidates) {
        const row =
          calculateS1Intraday(
            s0Row.symbol,
            data.get(
              s0Row.symbol
            ),
            s0Row,
            nowMs
          );

        if (!row) {
          continue;
        }

        if (!row.bar_fresh) {
          staleCount++;
        }

        rows.push(row);
      }

      const freshRows =
        rows.filter(
          r => r.bar_fresh
        );

      scanState.data_fresh = {
        computed:
          rows.length,
        fresh:
          freshRows.length,
        stale:
          staleCount,
        max_bar_age_sec:
          MAX_BAR_AGE_MS / 1000,
      };

      scanState.stage =
        "S1_RANKING";

      const ranked =
        rankS1(
          freshRows
        ).slice(
          0,
          limit
        );

      const lots =
        buildLots(
          ranked,
          LOT_SIZE
        );

      s1Cache = {
        prepared_for_date:
          today,
        asof:
          new Date(
            nowMs
          ).toISOString(),
        source:
          "Yahoo Spark 1d/5m",
        rows:
          ranked,
        lots,
        errors,
      };

      await persistJson(
        S1_CACHE_FILE,
        s1Cache
      );

      scanState.stage =
        "S1_READY_SF";

      scanState.ok =
        true;

      scanState.s0_count =
        candidates.length;

      scanState.s1_count =
        ranked.length;

      scanState.lots_count =
        lots.length;

      scanState.s1_prepared_for_date =
        today;

      scanState.errors =
        errors;

      scanState.elapsed_ms =
        Date.now() -
        started;

      return {
        ok: true,
        stage:
          "S1_READY_SF",
        version:
          APP_VERSION,
        prepared_for_date:
          today,
        asof:
          s1Cache.asof,
        elapsed_ms:
          scanState.elapsed_ms,
        s0_count:
          candidates.length,
        s1_count:
          ranked.length,
        s1:
          ranked,
        lots,
        data_fresh:
          scanState.data_fresh,
        errors,
      };
    }
  );
}

function passesWinnerGate(
  row,
  gate
) {
  if (!row) return false;

  if (!row.bar_fresh) {
    return false;
  }

  if (
    !Number.isFinite(row.price) ||
    row.price <= 0 ||
    row.price >= 5
  ) {
    return false;
  }

  if (
    gate.require_price_above_vwap &&
    !(row.priceVsVWAP > 0)
  ) {
    return false;
  }

  if (
    gate.min_rvol15m != null &&
    !(row.RVOL15M >=
      gate.min_rvol15m)
  ) {
    return false;
  }

  if (
    gate.min_accel5m != null &&
    !(row.Accel5M >=
      gate.min_accel5m)
  ) {
    return false;
  }

  if (
    gate.min_price_vs_vwap != null &&
    !(row.priceVsVWAP >=
      gate.min_price_vs_vwap)
  ) {
    return false;
  }

  if (
    gate.max_hod_distance != null &&
    !(row.hodDistance >=
      gate.max_hod_distance)
  ) {
    return false;
  }

  if (
    gate.max_j1_return != null &&
    Number.isFinite(row.J1) &&
    row.J1 >
      gate.max_j1_return
  ) {
    return false;
  }

  if (
    gate.max_j2_return != null &&
    Number.isFinite(row.J2) &&
    row.J2 >
      gate.max_j2_return
  ) {
    return false;
  }

  return true;
}

async function runSF({
  startLot,
  endLot,
  gate,
}) {
  return withOperationLock(
    "SF_SCAN",
    async () => {
      const started =
        Date.now();

      const today =
        nowNyDate();

      if (
        s1Cache.prepared_for_date !==
          today ||
        !s1Cache.lots.length
      ) {
        scanState.stage =
          "ERROR";

        scanState.ok =
          false;

        scanState.errors = [
          {
            error:
              `S1 cache is not ready for ${today}`,
          },
        ];

        return {
          ok: false,
          stage:
            "SF_BLOCKED",
          version:
            APP_VERSION,
          error:
            `S1 must be completed before SF for ${today}`,
          needs_s1:
            true,
        };
      }

      const firstLot =
        Math.max(
          1,
          startLot
        );

      const lastLot =
        Math.min(
          s1Cache.lots.length,
          Math.max(
            firstLot,
            endLot
          )
        );

      const refreshedLots =
        [];

      const allErrors =
        [];

      let winner = null;

      scanState.stage =
        "SF_READY";

      for (
        let lotNumber =
          firstLot;
        lotNumber <=
          lastLot;
        lotNumber++
      ) {
        const lot =
          s1Cache.lots[
            lotNumber - 1
          ];

        if (!lot) continue;

        const {
          data,
          errors,
        } = await yahooSpark(
          lot.symbols,
          INTRADAY_RANGE,
          INTRADAY_INTERVAL
        );

        allErrors.push(
          ...errors
        );

        const nowMs =
          Date.now();

        const lotRows =
          [];

        for (const symbol of lot.symbols) {
          const s0Row =
            s0Cache.rows.find(
              r =>
                r.symbol ===
                symbol
            );

          const refreshed =
            calculateS1Intraday(
              symbol,
              data.get(symbol),
              s0Row,
              nowMs
            );

          if (refreshed) {
            lotRows.push(
              refreshed
            );
          }
        }

        const ranked =
          rankS1(
            lotRows
          );

        const gatePassed =
          ranked.find(
            row =>
              passesWinnerGate(
                row,
                gate
              )
          );

        refreshedLots.push({
          lot:
            lotNumber,
          count:
            ranked.length,
          rows:
            ranked,
          winner:
            gatePassed || null,
        });

        if (gatePassed) {
          winner =
            gatePassed;
          break;
        }
      }

      scanState.stage =
        winner
          ? "WINNER_FOUND"
          : "SF_DONE";

      scanState.elapsed_ms =
        Date.now() -
        started;

      scanState.errors =
        allErrors;

      scanState.ok =
        true;

      return {
        ok: true,
        stage:
          scanState.stage,
        version:
          APP_VERSION,
        prepared_for_date:
          today,
        elapsed_ms:
          scanState.elapsed_ms,
        start_lot:
          firstLot,
        end_lot_checked:
          refreshedLots.at(-1)?.lot ||
          null,
        lots_checked:
          refreshedLots.length,
        winner,
        stop_triggered:
          Boolean(winner),
        gate,
        refreshed_lots:
          refreshedLots,
        errors:
          allErrors,
      };
    }
  );
}

async function runYahooWSTest() {
  const symbols = [
    "AAPL",
    "MSFT",
    "NVDA",
  ];

  const started =
    Date.now();

  const ws =
    new YahooWS();

  try {
    await ws.connect();

    await ws.subscribe(
      symbols
    );

    await sleep(
      10_000
    );

    const status =
      typeof ws.getStatus ===
      "function"
        ? await ws.getStatus()
        : null;

    const latest =
      typeof ws.getLatest ===
      "function"
        ? await ws.getLatest(
            symbols
          )
        : null;

    return {
      ok: true,
      version:
        APP_VERSION,
      elapsed_ms:
        Date.now() -
        started,
      symbols,
      status,
      latest,
    };
  } finally {
    try {
      await ws.close?.();
    } catch (error) {
      log(
        "YahooWS close error:",
        error?.message ||
          error
      );
    }
  }
}

function registerTools(
  server
) {
  server.tool(
    "ping",
    "Health check of the Yahoo Scan MCP service.",
    {},
    async () => ({
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
                new Date().toISOString(),
            }),
        },
      ],
    })
  );

  server.tool(
    "get_status",
    "Return server, universe, S0, S1 and SF status.",
    {},
    async () => ({
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
              architecture:
                "UNIVERSE -> S0_PREPARE -> S1_15:55 -> RANKING/LOTS -> SF_15:59",
              universe: {
                loaded:
                  Boolean(
                    universeCache
                  ),
                count:
                  universeCache?.length ||
                  0,
                loaded_at:
                  universeLoadedAt,
              },
              caches: {
                s0: {
                  prepared_for_date:
                    s0Cache.prepared_for_date,
                  prepared_at:
                    s0Cache.prepared_at,
                  count:
                    s0Cache.rows.length,
                  source:
                    s0Cache.source,
                },
                s1: {
                  prepared_for_date:
                    s1Cache.prepared_for_date,
                  asof:
                    s1Cache.asof,
                  count:
                    s1Cache.rows.length,
                  lots:
                    s1Cache.lots.length,
                  source:
                    s1Cache.source,
                },
              },
              active_operation:
                activeOperation,
              scan:
                scanState,
            }),
        },
      ],
    })
  );

  server.tool(
    "get_universe",
    "Return the materialized ticker universe from universe_s0.txt.",
    {
      limit:
        z.number()
          .int()
          .min(1)
          .max(2500)
          .optional()
          .default(2500),

      reload:
        z.boolean()
          .optional()
          .default(false),
    },
    async ({
      limit,
      reload,
    }) => {
      const universe =
        await loadUniverse(
          Boolean(reload)
        );

      return {
        content: [
          {
            type: "text",
            text:
              JSON.stringify({
                ok: true,
                count:
                  universe.length,
                returned:
                  Math.min(
                    limit,
                    universe.length
                  ),
                loaded_at:
                  universeLoadedAt,
                symbols:
                  universe.slice(
                    0,
                    limit
                  ),
              }),
          },
        ],
      };
    }
  );

  server.tool(
    "get_scan_state",
    "Return the latest S0/S1/SF state without launching a new scan.",
    {},
    async () => ({
      content: [
        {
          type: "text",
          text:
            JSON.stringify(
              scanState
            ),
        },
      ],
    })
  );

  server.tool(
    "yahoo_s0_prepare",
    "PRE-15:55 S0 preparation. Fetch historical Yahoo 5d/5m once, compute J-1/J-2, historical 15m RVOL baseline and liquidity context, apply the S0 price/ordinary-stock/explosion filters, and materialize the compact S0 cache for S1.",
    {
      symbols:
        z.array(
          z.string()
            .trim()
            .min(1)
            .max(15)
        )
        .max(2500)
        .optional(),

      force:
        z.boolean()
          .optional()
          .default(false),

      max_j1_return:
        z.number()
          .min(-1)
          .max(5)
          .optional()
          .default(
            DEFAULT_MAX_J1_RETURN
          ),

      max_j2_return:
        z.number()
          .min(-1)
          .max(5)
          .optional()
          .default(
            DEFAULT_MAX_J2_RETURN
          ),

      min_avg_daily_dollar_volume:
        z.number()
          .min(0)
          .optional(),
    },

    async ({
      symbols,
      force,
      max_j1_return,
      max_j2_return,
      min_avg_daily_dollar_volume,
    }) => {
      const result =
        await prepareS0({
          symbols,
          force,
          maxJ1Return:
            max_j1_return,
          maxJ2Return:
            max_j2_return,
          minAvgDailyDollarVolume:
            min_avg_daily_dollar_volume,
        });

      return {
        isError:
          !result.ok &&
          !result.busy,
        content: [
          {
            type: "text",
            text:
              JSON.stringify(
                result
              ),
          },
        ],
      };
    }
  );

  server.tool(
    "yahoo_s1_scan",
    "15:55 S1 only. Requires a same-day precomputed S0. Fetches current-session Yahoo 1d/5m only, calculates Vol15M/RVOL15M/Accel5M/VWAP/HOD, ranks locally, materializes lots of 20 and caches the result for SF.",
    {
      limit:
        z.number()
          .int()
          .min(20)
          .max(200)
          .optional()
          .default(50),

      symbols:
        z.array(
          z.string()
            .trim()
            .min(1)
            .max(15)
        )
        .max(2500)
        .optional(),
    },

    async ({
      limit,
      symbols,
    }) => {
      const result =
        await runS1({
          limit,
          symbols,
        });

      return {
        isError:
          !result.ok &&
          !result.busy,
        content: [
          {
            type: "text",
            text:
              JSON.stringify(
                result
              ),
          },
        ],
      };
    }
  );

  server.tool(
    "yahoo_sf_scan",
    "15:59 SF. Uses the already-materialized S1 lots, refreshes only one lot at a time with current Yahoo 1d/5m data, re-ranks locally, and STOPs immediately on the first candidate that passes the supplied Winner Gate.",
    {
      start_lot:
        z.number()
          .int()
          .min(1)
          .optional()
          .default(1),

      end_lot:
        z.number()
          .int()
          .min(1)
          .optional()
          .default(999),

      min_rvol15m:
        z.number()
          .min(0)
          .optional(),

      min_accel5m:
        z.number()
          .min(0)
          .optional(),

      min_price_vs_vwap:
        z.number()
          .min(-1)
          .max(5)
          .optional(),

      max_hod_distance:
        z.number()
          .min(-1)
          .max(1)
          .optional(),

      require_price_above_vwap:
        z.boolean()
          .optional()
          .default(false),

      max_j1_return:
        z.number()
          .min(-1)
          .max(5)
          .optional(),

      max_j2_return:
        z.number()
          .min(-1)
          .max(5)
          .optional(),
    },

    async ({
      start_lot,
      end_lot,
      min_rvol15m,
      min_accel5m,
      min_price_vs_vwap,
      max_hod_distance,
      require_price_above_vwap,
      max_j1_return,
      max_j2_return,
    }) => {
      const gate = {
        min_rvol15m:
          min_rvol15m ??
          null,

        min_accel5m:
          min_accel5m ??
          null,

        min_price_vs_vwap:
          min_price_vs_vwap ??
          null,

        max_hod_distance:
          max_hod_distance ??
          null,

        require_price_above_vwap,

        max_j1_return:
          max_j1_return ??
          null,

        max_j2_return:
          max_j2_return ??
          null,
      };

      const result =
        await runSF({
          startLot:
            start_lot,

          endLot:
            end_lot,

          gate,
        });

      return {
        isError:
          !result.ok &&
          !result.busy,
        content: [
          {
            type: "text",
            text:
              JSON.stringify(
                result
              ),
          },
        ],
      };
    }
  );

  server.tool(
    "yahoo_s0_s1_scan",
    "Compatibility wrapper: run S1 on the prepared S0 cache. For the production workflow use yahoo_s0_prepare, then yahoo_s1_scan, then yahoo_sf_scan.",
    {
      symbols:
        z.array(
          z.string()
            .trim()
            .min(1)
            .max(15)
        )
        .max(2500)
        .optional(),

      s1_limit:
        z.number()
          .int()
          .min(20)
          .max(200)
          .optional()
          .default(50),
    },

    async ({
      symbols,
      s1_limit,
    }) => {
      const result =
        await runS1({
          limit:
            s1_limit,
          symbols,
        });

      return {
        isError:
          !result.ok &&
          !result.busy,
        content: [
          {
            type: "text",
            text:
              JSON.stringify(
                result
              ),
          },
        ],
      };
    }
  );

  server.tool(
    "yahoo_ws_test",
    "Test live Yahoo WebSocket connectivity with a few liquid symbols.",
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
                JSON.stringify(
                  result
                ),
            },
          ],
        };
      } catch (error) {
        return {
          isError:
            true,

          content: [
            {
              type: "text",
              text:
                JSON.stringify({
                  ok: false,
                  version:
                    APP_VERSION,
                  error:
                    error?.message ||
                    String(error),
                }),
            },
          ],
        };
      }
    }
  );
}

function createMcpServer() {
  const server =
    new McpServer({
      name:
        SERVICE_NAME,
      version:
        APP_VERSION,
    });

  registerTools(
    server
  );

  return server;
}

async function parseJsonBody(
  req
) {
  const chunks = [];
  let size = 0;

  for await (
    const chunk of req
  ) {
    size += chunk.length;

    if (
      size >
      MAX_BODY_BYTES
    ) {
      throw new Error(
        "Request body too large"
      );
    }

    chunks.push(chunk);
  }

  if (!size) {
    return undefined;
  }

  const raw =
    Buffer.concat(
      chunks
    ).toString(
      "utf8"
    );

  try {
    return JSON.parse(
      raw
    );
  } catch {
    throw new Error(
      "Invalid JSON request body"
    );
  }
}

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
    "Content-Type, Accept, Authorization, Origin, MCP-Session-Id, MCP-Protocol-Version, Last-Event-ID"
  );

  res.setHeader(
    "Access-Control-Expose-Headers",
    "MCP-Session-Id, MCP-Protocol-Version, Last-Event-ID"
  );
}

async function handleMcp(
  req,
  res
) {
  setCors(res);

  if (
    req.method ===
    "OPTIONS"
  ) {
    res.writeHead(
      204
    );
    res.end();
    return;
  }

  if (
    req.method !==
    "POST"
  ) {
    res.writeHead(
      405,
      {
        "Content-Type":
          "application/json",
        "Allow":
          "POST, OPTIONS",
      }
    );

    res.end(
      JSON.stringify({
        ok: false,
        error:
          "MCP endpoint accepts POST requests",
      })
    );

    return;
  }

  let body;

  try {
    body =
      await parseJsonBody(
        req
      );
  } catch (error) {
    res.writeHead(
      400,
      {
        "Content-Type":
          "application/json",
      }
    );

    res.end(
      JSON.stringify({
        jsonrpc:
          "2.0",
        error: {
          code:
            -32700,
          message:
            error?.message ||
            "Invalid JSON",
        },
        id: null,
      })
    );

    return;
  }

  if (!body) {
    res.writeHead(
      400,
      {
        "Content-Type":
          "application/json",
      }
    );

    res.end(
      JSON.stringify({
        jsonrpc:
          "2.0",
        error: {
          code:
            -32600,
          message:
            "Empty MCP request body",
        },
        id: null,
      })
    );

    return;
  }

  const transport =
    new StreamableHTTPServerTransport(
      {
        sessionIdGenerator:
          undefined,
        enableJsonResponse:
          true,
      }
    );

  const server =
    createMcpServer();

  try {
    await server.connect(
      transport
    );

    await transport.handleRequest(
      req,
      res,
      body
    );
  } catch (error) {
    log(
      "MCP transport error:",
      error
    );

    if (
      !res.headersSent
    ) {
      res.writeHead(
        500,
        {
          "Content-Type":
            "application/json",
        }
      );

      res.end(
        JSON.stringify({
          jsonrpc:
            "2.0",
          error: {
            code:
              -32603,
            message:
              error?.message ||
              "Internal MCP error",
          },
          id: null,
        })
      );
    }
  }
}

async function main() {
  await loadUniverse();

  await restoreCaches();

  const server =
    http.createServer(
      async (
        req,
        res
      ) => {
        try {
          const url =
            new URL(
              req.url ||
                "/",
              `http://${req.headers.host || "localhost"}`
            );

          if (
            url.pathname ===
            "/health"
          ) {
            res.writeHead(
              200,
              {
                "Content-Type":
                  "application/json",
              }
            );

            res.end(
              JSON.stringify({
                ok: true,
                service:
                  SERVICE_NAME,
                version:
                  APP_VERSION,
                universe_count:
                  universeCache.length,
                s0_count:
                  s0Cache.rows.length,
                s0_prepared_for_date:
                  s0Cache.prepared_for_date,
                s1_count:
                  s1Cache.rows.length,
                s1_prepared_for_date:
                  s1Cache.prepared_for_date,
                active_operation:
                  activeOperation,
                timestamp:
                  new Date().toISOString(),
              })
            );

            return;
          }

          if (
            url.pathname ===
            "/"
          ) {
            res.writeHead(
              200,
              {
                "Content-Type":
                  "application/json",
              }
            );

            res.end(
              JSON.stringify({
                ok: true,
                service:
                  SERVICE_NAME,
                version:
                  APP_VERSION,
                endpoint:
                  "/mcp",
                transport:
                  "Streamable HTTP (stateless)",
                architecture:
                  "UNIVERSE -> S0_PREPARE -> S1 -> RANKING/LOTS -> SF",
              })
            );

            return;
          }

          if (
            url.pathname ===
            "/mcp"
          ) {
            await handleMcp(
              req,
              res
            );

            return;
          }

          res.writeHead(
            404,
            {
              "Content-Type":
                "application/json",
            }
          );

          res.end(
            JSON.stringify({
              ok: false,
              error:
                "Not found",
            })
          );
        } catch (error) {
          log(
            "HTTP request error:",
            error
          );

          if (
            !res.headersSent
          ) {
            res.writeHead(
              500,
              {
                "Content-Type":
                  "application/json",
              }
            );

            res.end(
              JSON.stringify({
                ok: false,
                error:
                  "Internal server error",
              })
            );
          } else {
            res.end();
          }
        }
      }
    );

  server.requestTimeout =
    10 * 60 * 1000;

  server.headersTimeout =
    65 * 1000;

  server.keepAliveTimeout =
    70 * 1000;

  server.listen(
    PORT,
    "0.0.0.0",
    () => {
      log(
        `HTTP server listening on 0.0.0.0:${PORT}`
      );

      log(
        `MCP endpoint: /mcp`
      );

      log(
        `Health endpoint: /health`
      );

      log(
        `Universe loaded: ${universeCache.length} symbols`
      );

      log(
        `S0 cache date: ${s0Cache.prepared_for_date || "none"}`
      );

      log(
        `S1 cache date: ${s1Cache.prepared_for_date || "none"}`
      );

      log(
        `Version: ${APP_VERSION}`
      );
    }
  );
}

main().catch(
  error => {
    console.error(
      `[${SERVICE_NAME}] FATAL STARTUP ERROR:`,
      error
    );

    process.exit(1);
  }
);
