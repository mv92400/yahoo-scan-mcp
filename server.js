/*
 * Yahoo Scan MCP — Version 2.0.0
 * Rebuilt against the existing package.json, universe_s0.txt and src/yahoo-ws.js.
 *
 * Pipeline: S0 -> S1 -> frozen DV ranking -> SF lots of 20 -> Winner Gate.
 * S0 uses true 5-minute Yahoo OHLCV bars for every historical volume baseline.
 * Accel5M is informational only. Locked thresholds are constants below.
 */

import { createServer } from "node:http";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { z } from "zod";
import { YahooWS } from "./src/yahoo-ws.js";

const APP_VERSION = "2.0.0";
const CACHE_SCHEMA_VERSION = 2; // Forces a refresh of materialized caches produced before the baseline fixes.
const PORT = Number(process.env.PORT || 8080);
const ROOT = path.dirname(fileURLToPath(import.meta.url));
const UNIVERSE_FILE = process.env.UNIVERSE_FILE || path.join(ROOT, "universe_s0.txt");
const S0_CACHE_FILE = process.env.S0_CACHE_FILE || path.join(ROOT, "s0_materialized.json");
const S1_CACHE_FILE = process.env.S1_CACHE_FILE || path.join(ROOT, "s1_materialized.json");
const S0_INTERVAL = "5m";
const S0_RANGE = "1mo";
const SPARK_BATCH_SIZE = 10; // Yahoo Spark batches support up to 10 symbols in the current public schema.
const SPARK_BATCH_CONCURRENCY = 6;
const SPARK_TIMEOUT_MS = 7000;
const S1_INTERVAL = "5m";
const MARKET_TZ = "America/New_York";
const DATA_CONCURRENCY = Math.max(1, Math.min(16, Number(process.env.DATA_CONCURRENCY || 10)));
const SF_LOT_SIZE = 20;
const REQUEST_TIMEOUT_MS = 12000;
const REQUEST_RETRIES = 3;
const RETRY_DELAY_MS = 350;
const MIN_HISTORY_SESSIONS = 6; // Six completed prior sessions; never count the current session as history.
const MIN_REFERENCE_SAMPLES = 5; // A slot reference needs at least five historical session samples.
const MAX_WS_TICK_AGE_MS = 120000;
const MAX_WS_RECEIPT_AGE_MS = 120000;

// Do not change these thresholds without an explicit strategy decision.
const LOCKED = Object.freeze({
  maxPrice: 5.00,
  maxAbsJ1Pct: 5.00,
  maxAbsJ2Pct: 5.00,
  s1Rvol15Min: 1.30,
  s1Vol5MinRatio: 0.20,
  s1PriceVsVwapMinPct: -1.00,
  rvol15Min: 1.50,
  vol5MinRatio: 0.20,
  delta60MaxPct: 10.00,
  priceVsVwapMinPct: -1.00,
  delta1555to1559MinPct: 0.00,
  lotSize: 20
});
const NON_BLOCKING = Object.freeze({ accel5m: true });

const scanState = {
  started_at: null,
  completed_at: null,
  asof: null,
  session_date: null,
  status: "idle",
  s0_count: 0,
  s1_count: 0,
  sf_count: 0,
  s0_elapsed_ms: null,
  s1_elapsed_ms: null,
  sf_elapsed_ms: null,
  elapsed_ms: null,
  last_error: null,
  websocket: { connected: false, subscribed: false, ticks: 0 }
};

let yahooWS = null;
let wsConnectPromise = null;

function sleep(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }
function finite(value) { return typeof value === "number" && Number.isFinite(value); }
function round(value, digits = 4) {
  if (!finite(value)) return null;
  const p = 10 ** digits;
  return Math.round(value * p) / p;
}
function median(values) {
  const a = values.filter(finite).sort((x, y) => x - y);
  if (!a.length) return null;
  const m = Math.floor(a.length / 2);
  return a.length % 2 ? a[m] : (a[m - 1] + a[m]) / 2;
}
function pctChange(from, to) {
  if (!finite(from) || !finite(to) || from <= 0) return null;
  return ((to / from) - 1) * 100;
}
function chunk(items, size) {
  const out = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
}
function getNYParts(timestamp) {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: MARKET_TZ, year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", second: "2-digit", hourCycle: "h23"
  }).formatToParts(new Date(timestamp));
  const out = {};
  for (const p of parts) if (p.type !== "literal") out[p.type] = Number(p.value);
  return out;
}
function etDate(timestamp) {
  const p = getNYParts(timestamp);
  return `${p.year}-${String(p.month).padStart(2, "0")}-${String(p.day).padStart(2, "0")}`;
}
function etClock(timestamp) {
  const p = getNYParts(timestamp);
  return `${String(p.hour).padStart(2, "0")}:${String(p.minute).padStart(2, "0")}`;
}
function floor5Clock(timestamp) {
  const p = getNYParts(timestamp);
  const minute = Math.floor(p.minute / 5) * 5;
  return `${String(p.hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}`;
}
function isRegularSessionBar(timestamp) {
  const p = getNYParts(timestamp);
  const minute = p.hour * 60 + p.minute;
  return minute >= 570 && minute < 960; // 09:30 inclusive, 16:00 exclusive ET
}
function isContinuousWindow(day, endIndex, count) {
  if (endIndex < count - 1) return false;
  const first = day[endIndex - count + 1];
  const last = day[endIndex];
  if (!first || !last) return false;
  if (last.ts - first.ts !== (count - 1) * 5 * 60_000) return false;
  for (let i = endIndex - count + 2; i <= endIndex; i++) {
    if (day[i].ts - day[i - 1].ts !== 5 * 60_000) return false;
  }
  return true;
}
function atomicWriteJson(filename, value) {
  const tmp = `${filename}.${process.pid}.${Date.now()}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(value, null, 2), "utf8");
  fs.renameSync(tmp, filename);
}
function readJson(filename) {
  try { return JSON.parse(fs.readFileSync(filename, "utf8")); }
  catch { return null; }
}
function readUniverse() {
  if (!fs.existsSync(UNIVERSE_FILE)) throw new Error(`UNIVERSE_FILE_NOT_FOUND: ${UNIVERSE_FILE}`);
  const raw = fs.readFileSync(UNIVERSE_FILE, "utf8");
  return [...new Set(raw.split(/\r?\n/).map(s => s.trim().toUpperCase())
    .filter(s => s && !s.startsWith("#") && /^[A-Z0-9.^=_-]{1,20}$/.test(s)))];
}
function normalizeSymbols(symbols) {
  return [...new Set((symbols || []).map(s => String(s).trim().toUpperCase()).filter(Boolean))];
}
function etWallTimeToEpoch(year, month, day, hour = 0, minute = 0, second = 0, millisecond = 0) {
  const targetWallMs = Date.UTC(year, month - 1, day, hour, minute, second, millisecond);
  // Reject calendar overflow (for example February 30) before applying the NY offset.
  const check = new Date(targetWallMs);
  if (check.getUTCFullYear() !== year || check.getUTCMonth() + 1 !== month || check.getUTCDate() !== day ||
      check.getUTCHours() !== hour || check.getUTCMinutes() !== minute || check.getUTCSeconds() !== second) {
    throw new Error("INVALID_ASOF_CALENDAR_DATE");
  }
  let guess = targetWallMs;
  // Solve wall-clock ET = UTC + offset; iterating handles EST/EDT without assuming a fixed offset.
  for (let i = 0; i < 4; i++) {
    const p = getNYParts(guess);
    const representedWallMs = Date.UTC(p.year, p.month - 1, p.day, p.hour, p.minute, p.second, millisecond);
    const correction = targetWallMs - representedWallMs;
    guess += correction;
    if (correction === 0) break;
  }
  const resolved = getNYParts(guess);
  if (resolved.year !== year || resolved.month !== month || resolved.day !== day ||
      resolved.hour !== hour || resolved.minute !== minute || resolved.second !== second) {
    throw new Error("INVALID_ASOF_ET_WALL_TIME");
  }
  return guess;
}
function validateAsOf(asof) {
  if (asof == null || asof === "") return Date.now();
  const value = String(asof).trim();
  // An ISO datetime without Z/offset is interpreted as New York wall-clock time, not Railway's timezone.
  const localIso = /^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,3}))?)?)?$/.exec(value);
  if (localIso) {
    const [, ys, mos, ds, hs = "00", mis = "00", ss = "00", mss = "0"] = localIso;
    const year = Number(ys), month = Number(mos), day = Number(ds);
    const hour = Number(hs), minute = Number(mis), second = Number(ss);
    const millisecond = Number(mss.padEnd(3, "0"));
    if (hour > 23 || minute > 59 || second > 59) throw new Error(`INVALID_ASOF: ${asof}`);
    try { return etWallTimeToEpoch(year, month, day, hour, minute, second, millisecond); }
    catch { throw new Error(`INVALID_ASOF: ${asof}`); }
  }
  const n = Date.parse(value);
  if (!Number.isFinite(n)) throw new Error(`INVALID_ASOF: ${asof}`);
  return n;
}

async function fetchYahooChart(symbol, interval = "5m", range = "1d") {
  let lastError = null;
  const hosts = ["query1.finance.yahoo.com", "query2.finance.yahoo.com"];
  for (let attempt = 1; attempt <= REQUEST_RETRIES; attempt++) {
    const host = hosts[(attempt - 1) % hosts.length];
    const url = `https://${host}/v8/finance/chart/${encodeURIComponent(symbol)}` +
      `?range=${encodeURIComponent(range)}&interval=${encodeURIComponent(interval)}` +
      "&includePrePost=false&events=div%2Csplits";
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
    try {
      const response = await fetch(url, {
        signal: controller.signal,
        headers: { "User-Agent": "Mozilla/5.0 (compatible; YahooScanMCP/2.0.0)", "Accept": "application/json" }
      });
      if (!response.ok) throw new Error(`Yahoo HTTP ${response.status} (${host})`);
      const json = await response.json();
      if (json?.chart?.error) throw new Error(`Yahoo chart error: ${json.chart.error.description || "unknown"}`);
      const result = json?.chart?.result?.[0];
      if (!result) throw new Error("YAHOO_EMPTY_CHART_RESULT");
      const normalized = normalizeYahooChart(symbol, result);
      if (!normalized.bars.length) throw new Error("YAHOO_CHART_HAS_NO_VALID_OHLCV_BARS");
      return normalized;
    } catch (error) {
      lastError = error;
      if (attempt < REQUEST_RETRIES) await sleep(RETRY_DELAY_MS * attempt);
    } finally {
      clearTimeout(timer);
    }
  }
  throw new Error(`${symbol}: ${lastError?.name === "AbortError" ? "YAHOO_TIMEOUT" : (lastError?.message || "YAHOO_REQUEST_FAILED")}`);
}

async function fetchYahooSparkBatch(symbols, interval = S0_INTERVAL, range = S0_RANGE) {
  const list = normalizeSymbols(symbols);
  if (!list.length) return new Map();
  if (list.length > SPARK_BATCH_SIZE) throw new Error(`SPARK_BATCH_LIMIT_${SPARK_BATCH_SIZE}`);
  const params = new URLSearchParams({
    symbols: list.join(","), range, interval,
    includeTimestamps: "true", includePrePost: "false",
    corsDomain: "finance.yahoo.com", ".tsrc": "finance"
  });
  let lastError = null;
  for (const host of ["query1.finance.yahoo.com", "query2.finance.yahoo.com"]) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), SPARK_TIMEOUT_MS);
    try {
      const response = await fetch(`https://${host}/v7/finance/spark?${params}`, {
        signal: controller.signal,
        headers: { "User-Agent": "Mozilla/5.0 (compatible; YahooScanMCP/2.0.0)", "Accept": "application/json" }
      });
      if (!response.ok) throw new Error(`Yahoo Spark HTTP ${response.status} (${host})`);
      const json = await response.json();
      const entries = json?.spark?.result;
      if (!Array.isArray(entries)) throw new Error("YAHOO_SPARK_RESULT_MISSING");
      const out = new Map();
      for (const entry of entries) {
        const responses = Array.isArray(entry?.response) ? entry.response : [entry];
        for (const responseItem of responses) {
          const merged = {
            ...entry, ...responseItem,
            meta: responseItem?.meta || entry?.meta || {},
            timestamp: responseItem?.timestamp || entry?.timestamp,
            indicators: responseItem?.indicators || entry?.indicators
          };
          const symbol = String(merged?.symbol || merged?.meta?.symbol || entry?.symbol || "").toUpperCase();
          if (!list.includes(symbol)) continue;
          const quote = merged?.indicators?.quote?.[0];
          const timestamps = merged?.timestamp;
          if (!Array.isArray(timestamps) || !quote || !["open", "high", "low", "close", "volume"].every(key => Array.isArray(quote[key]))) continue;
          const aligned = timestamps.length > 0 && ["open", "high", "low", "close", "volume"].every(key => quote[key].length === timestamps.length);
          if (!aligned) continue;
          const normalized = normalizeYahooChart(symbol, merged);
          // Only accept Spark results with full, aligned OHLCV arrays; otherwise fall back to Chart.
          if (normalized.bars.length) out.set(symbol, normalized);
        }
      }
      return out;
    } catch (error) {
      lastError = error;
    } finally {
      clearTimeout(timer);
    }
  }
  throw new Error(lastError?.name === "AbortError" ? "YAHOO_SPARK_TIMEOUT" : (lastError?.message || "YAHOO_SPARK_FAILED"));
}
function normalizeYahooChart(symbol, result) {
  const timestamps = result.timestamp || [];
  const quote = result.indicators?.quote?.[0] || {};
  const bars = [];
  for (let i = 0; i < timestamps.length; i++) {
    const ts = Number(timestamps[i]) * 1000;
    const open = Number(quote.open?.[i]);
    const high = Number(quote.high?.[i]);
    const low = Number(quote.low?.[i]);
    const close = Number(quote.close?.[i]);
    const volRaw = quote.volume?.[i];
    const volume = volRaw == null ? 0 : Number(volRaw);
    if (![ts, open, high, low, close].every(Number.isFinite) || open <= 0 || high <= 0 || low <= 0 || close <= 0) continue;
    if (!Number.isFinite(volume) || volume < 0) continue;
    bars.push({ ts, open, high, low, close, volume });
  }
  bars.sort((a, b) => a.ts - b.ts);
  const unique = [];
  for (const bar of bars) {
    if (!unique.length || unique[unique.length - 1].ts !== bar.ts) unique.push(bar);
  }
  return { symbol, meta: result.meta || {}, bars: unique };
}
function completedBarsAt(bars, asofMs, intervalMs = 5 * 60_000) {
  return bars.filter(b => b.ts + intervalMs <= asofMs);
}
function barsAtOrBefore(bars, asofMs) { return bars.filter(b => b.ts <= asofMs); }
function groupBySession(bars) {
  const map = new Map();
  for (const bar of bars) {
    if (!isRegularSessionBar(bar.ts)) continue;
    const date = etDate(bar.ts);
    if (!map.has(date)) map.set(date, []);
    map.get(date).push(bar);
  }
  for (const day of map.values()) day.sort((a, b) => a.ts - b.ts);
  return map;
}
function getSessionSlotIndex(timestamp) {
  const p = getNYParts(timestamp);
  const minute = p.hour * 60 + p.minute;
  if (minute < 570 || minute >= 960) return null;
  return Math.floor((minute - 570) / 5);
}
function buildSessionGrid(date, bars = []) {
  const bySlot = new Map();
  for (const bar of bars || []) {
    const index = getSessionSlotIndex(bar.ts);
    if (index == null || index < 0 || index >= 78) continue;
    // Only real, validated OHLCV bars enter the map. An absent/malformed candle becomes volume 0.
    bySlot.set(index, bar);
  }
  const [year, month, day] = date.split("-").map(Number);
  return Array.from({ length: 78 }, (_, index) => {
    const minutes = 570 + index * 5;
    const hour = Math.floor(minutes / 60);
    const minute = minutes % 60;
    const actual = bySlot.get(index) || null;
    return {
      ts: etWallTimeToEpoch(year, month, day, hour, minute),
      volume: actual && finite(actual.volume) && actual.volume >= 0 ? actual.volume : 0,
      bar: actual,
      index
    };
  });
}
function sessionPerformance(bars) {
  if (!bars?.length) return null;
  const first = bars.find(b => floor5Clock(b.ts) === "09:30");
  const last = bars.find(b => floor5Clock(b.ts) === "15:55");
  // J-1/J-2 use a genuine opening candle and final regular-session candle, never a partial session.
  if (!first || !last || first.open <= 0 || last.close <= 0 || last.ts < first.ts) return null;
  return pctChange(first.open, last.close);
}
function classifyInstrument(meta, symbol) {
  const quoteType = String(meta?.quoteType || "").toUpperCase().trim();
  const exchangeRaw = String(meta?.exchangeName || meta?.fullExchangeName || meta?.exchange || "").toUpperCase().trim();
  const exchange = exchangeRaw.replace(/[^A-Z]/g, "");
  const name = `${meta?.shortName || ""} ${meta?.longName || ""}`.toUpperCase().trim();
  const forbiddenTypes = new Set(["ETF", "MUTUALFUND", "INDEX", "OPTION", "FUTURE", "CRYPTOCURRENCY", "WARRANT", "RIGHT", "UNIT", "PREFERRED_STOCK", "MONEYMARKET"]);
  if (quoteType && (forbiddenTypes.has(quoteType) || quoteType !== "EQUITY")) return "non_ordinary";
  if (/\b(?:WARRANTS?|RIGHTS?|UNITS?|PREFERRED(?:\s+STOCK|\s+SHARES?)?|PREFERENCE SHARES?|DEPOSITARY SHARES?|EXCHANGE[- ]TRADED FUND|ETF)\b/.test(name)) return "non_ordinary";
  // Do not infer security type from ticker suffixes alone: legitimate equities can end in U, W, R, etc.
  const nasdaqCodes = new Set(["NMS", "NGM", "NCM", "NAS", "NASDAQ", "NASDAQGS", "NASDAQGM", "NASDAQCM"]);
  const nasdaqExchange = exchange.includes("NASDAQ") || nasdaqCodes.has(exchange);
  // Missing type/exchange metadata is ambiguous, not positive evidence of a common stock.
  if (!quoteType || !exchangeRaw) return "ambiguous";
  if (!nasdaqExchange) return "non_ordinary";
  return "ordinary";
}
function isOrdinaryStock(meta, symbol) {
  return classifyInstrument(meta, symbol) === "ordinary";
}
function isCompletedSession(bars) {
  return finite(sessionPerformance(bars));
}
function volumeWindowFromGrid(grid, endIndex, count) {
  if (!Array.isArray(grid) || endIndex < count - 1 || endIndex >= grid.length) return null;
  let total = 0;
  for (let i = endIndex - count + 1; i <= endIndex; i++) {
    const slot = grid[i];
    if (!slot || !finite(slot.volume) || slot.volume < 0) return null;
    total += slot.volume;
  }
  return total;
}
function sumVolumes(day, endIndex, count) {
  if (!isContinuousWindow(day, endIndex, count)) return null;
  let total = 0;
  for (let i = endIndex - count + 1; i <= endIndex; i++) total += day[i].volume;
  return total;
}
function mapMedians(map) {
  const out = {};
  for (const [slot, values] of map) {
    const value = median(values);
    if (finite(value) && value > 0) out[slot] = value;
  }
  return out;
}
function buildHistoricalReferences(sessionMap, historicalDates) {
  const volume5BySlot = new Map();
  const volume15BySlot = new Map();
  const volume60BySlot = new Map();
  const medianBarVolumes = [];
  const dailyDollarVolumes = [];
  let sessionsUsed = 0;
  for (const date of historicalDates) {
    const day = sessionMap.get(date) || [];
    if (!isCompletedSession(day)) continue;
    const grid = buildSessionGrid(date, day); // Exactly 78 slots; missing/malformed OHLC candles contribute volume 0.
    sessionsUsed++;
    const dayMedian = median(grid.map(slot => slot.volume));
    if (finite(dayMedian)) medianBarVolumes.push(dayMedian);
    const dailyDollar = day.reduce((sum, b) => sum + (finite(b.close) && finite(b.volume) ? b.close * b.volume : 0), 0);
    if (finite(dailyDollar) && dailyDollar > 0) dailyDollarVolumes.push(dailyDollar);
    for (let i = 0; i < grid.length; i++) {
      const slot = floor5Clock(grid[i].ts);
      if (!volume5BySlot.has(slot)) volume5BySlot.set(slot, []);
      volume5BySlot.get(slot).push(grid[i].volume); // Zero-volume slots remain part of the median/sample count.
      // Three aligned 5m slots, including zero-filled missing candles, ending at this slot.
      const v15 = volumeWindowFromGrid(grid, i, 3);
      if (finite(v15)) {
        if (!volume15BySlot.has(slot)) volume15BySlot.set(slot, []);
        volume15BySlot.get(slot).push(v15);
      }
      // Twelve aligned 5m slots, including zero-filled missing candles, ending at this slot.
      const v60 = volumeWindowFromGrid(grid, i, 12);
      if (finite(v60)) {
        if (!volume60BySlot.has(slot)) volume60BySlot.set(slot, []);
        volume60BySlot.get(slot).push(v60);
      }
    }
  }
  return {
    rvol15BySlot: mapMedians(volume15BySlot),
    vol5BySlot: mapMedians(volume5BySlot),
    vol60BySlot: mapMedians(volume60BySlot),
    median5mBarVolume: median(medianBarVolumes),
    medianDailyDollarVolume: median(dailyDollarVolumes),
    historicalSessionsUsed: sessionsUsed,
    samplesBySlot: {
      rvol15: Object.fromEntries([...volume15BySlot].map(([k, v]) => [k, v.length])),
      vol5: Object.fromEntries([...volume5BySlot].map(([k, v]) => [k, v.length])),
      vol60: Object.fromEntries([...volume60BySlot].map(([k, v]) => [k, v.length]))
    }
  };
}
function getJ1J2(sessionMap, historicalDates) {
  const j1Date = historicalDates.at(-1);
  const j2Date = historicalDates.at(-2);
  return {
    j1: j1Date ? sessionPerformance(sessionMap.get(j1Date)) : null,
    j2: j2Date ? sessionPerformance(sessionMap.get(j2Date)) : null,
    j1Date: j1Date || null,
    j2Date: j2Date || null
  };
}
function categorizeRejections(items) {
  const counts = {};
  for (const item of items) if (!item?.ok) {
    const key = item?.reason || "UNKNOWN";
    counts[key] = (counts[key] || 0) + 1;
  }
  return Object.fromEntries(Object.entries(counts).sort((a, b) => a[0].localeCompare(b[0])));
}
async function mapConcurrent(items, worker, concurrency = DATA_CONCURRENCY) {
  const results = new Array(items.length);
  let cursor = 0;
  async function runner() {
    while (true) {
      const index = cursor++;
      if (index >= items.length) return;
      try { results[index] = await worker(items[index], index); }
      catch (error) { results[index] = { ok: false, symbol: items[index]?.symbol || String(items[index]), reason: "UNHANDLED_WORKER_ERROR", error: error?.message || String(error) }; }
    }
  }
  await Promise.all(Array.from({ length: Math.min(concurrency, items.length) }, () => runner()));
  return results;
}

async function processS0Symbol(symbol, asofMs, prefetchedChart = null) {
  try {
    let chart = prefetchedChart || await fetchYahooChart(symbol, S0_INTERVAL, S0_RANGE);
    let classification = classifyInstrument(chart.meta, symbol);
    // Spark often omits chart metadata; retry that symbol against Chart before treating it as ambiguous.
    if (classification === "ambiguous" && prefetchedChart) {
      const chartFallback = await fetchYahooChart(symbol, S0_INTERVAL, S0_RANGE);
      const fallbackClassification = classifyInstrument(chartFallback.meta, symbol);
      if (fallbackClassification !== "ambiguous") { chart = chartFallback; classification = fallbackClassification; }
    }
    if (classification !== "ordinary") return { ok: false, symbol, reason: classification === "ambiguous" ? "AMBIGUOUS_INSTRUMENT_METADATA" : "NOT_ORDINARY_STOCK", quote_type: chart.meta?.quoteType || null, exchange: chart.meta?.exchangeName || chart.meta?.fullExchangeName || chart.meta?.exchange || null };
    const regular = chart.bars.filter(b => isRegularSessionBar(b.ts));
    const completed = completedBarsAt(regular, asofMs);
    if (!completed.length) return { ok: false, symbol, reason: "NO_COMPLETED_HISTORY" };
    const sessions = groupBySession(completed);
    const dates = [...sessions.keys()].sort();
    const todayET = etDate(asofMs);
    // Count six completed historical sessions strictly before today ET; the current session never counts.
    const historicalDates = dates.filter(d => d < todayET && isCompletedSession(sessions.get(d)));
    if (historicalDates.length < MIN_HISTORY_SESSIONS) return { ok: false, symbol, reason: "INSUFFICIENT_COMPLETED_SESSIONS", sessions: historicalDates.length, required: MIN_HISTORY_SESSIONS };
    const latestDate = dates.at(-1);
    const lastBar = (sessions.get(latestDate) || []).at(-1);
    if (!lastBar || !finite(lastBar.close)) return { ok: false, symbol, reason: "NO_CURRENT_PRICE" };
    // Source universe is curated below $5, but this live/history check guards stale source files.
    if (lastBar.close >= LOCKED.maxPrice) return { ok: false, symbol, reason: "PRICE_GE_5", price: lastBar.close };
    const { j1, j2, j1Date, j2Date } = getJ1J2(sessions, historicalDates);
    if (!finite(j1) || !finite(j2)) return { ok: false, symbol, reason: "NO_J1_J2" };
    if (Math.abs(j1) > LOCKED.maxAbsJ1Pct || Math.abs(j2) > LOCKED.maxAbsJ2Pct) return { ok: false, symbol, reason: "J1_J2_EXCESSIVE", j1, j2, j1Date, j2Date };
    const refs = buildHistoricalReferences(sessions, historicalDates);
    if (!finite(refs.median5mBarVolume) || refs.median5mBarVolume <= 0 || !finite(refs.medianDailyDollarVolume) || refs.medianDailyDollarVolume <= 0) {
      return { ok: false, symbol, reason: "NO_LIQUIDITY_BASELINE" };
    }
    if (!Object.keys(refs.rvol15BySlot).length || !Object.keys(refs.vol5BySlot).length || !Object.keys(refs.vol60BySlot).length) {
      return { ok: false, symbol, reason: "INCOMPLETE_HISTORICAL_REFERENCES" };
    }
    return {
      ok: true, symbol, price: round(lastBar.close), asof: lastBar.ts + 5 * 60_000,
      latest_session: latestDate, j1: round(j1), j2: round(j2), j1_date: j1Date, j2_date: j2Date,
      median_5m_bar_volume: round(refs.median5mBarVolume, 2),
      median_daily_dollar_volume: round(refs.medianDailyDollarVolume, 2),
      historical_sessions_used: refs.historicalSessionsUsed,
      references: {
        rvol15_by_slot: refs.rvol15BySlot,
        vol5_by_slot: refs.vol5BySlot,
        vol60_by_slot: refs.vol60BySlot,
        samples_by_slot: refs.samplesBySlot
      },
      // Compatibility scalars only. S1/SF must select reference by its actual 5m slot.
      rvol15_reference: round(refs.rvol15BySlot[floor5Clock(lastBar.ts)], 2),
      vol5_reference: round(refs.vol5BySlot[floor5Clock(lastBar.ts)], 2),
      vol60_reference: round(refs.vol60BySlot[floor5Clock(lastBar.ts)], 2)
    };
  } catch (error) {
    return { ok: false, symbol, reason: "FETCH_ERROR", error: error?.message || String(error) };
  }
}
async function runS0(symbols, asofMs = Date.now()) {
  const started = Date.now();
  const sparkStarted = Date.now();
  const batches = chunk(symbols, SPARK_BATCH_SIZE);
  const sparkResults = [];
  const prefetched = new Map();
  let sparkAvailable = false;
  // Probe one batch first. If Spark is blocked or returns no usable OHLCV, skip repeated failed probes.
  if (batches.length) {
    try {
      const map = await fetchYahooSparkBatch(batches[0], S0_INTERVAL, S0_RANGE);
      if (map.size > 0) {
        sparkAvailable = true;
        for (const [symbol, chart] of map) prefetched.set(symbol, chart);
        sparkResults.push({ requested: batches[0], map, ok: true, returned: map.size });
      } else {
        sparkResults.push({ requested: batches[0], map, ok: false, returned: 0, error: "SPARK_PROBE_RETURNED_NO_USABLE_OHLCV" });
      }
    } catch (error) {
      sparkResults.push({ requested: batches[0], map: new Map(), ok: false, returned: 0, error: error?.message || String(error) });
    }
  }
  if (sparkAvailable && batches.length > 1) {
    const remaining = await mapConcurrent(batches.slice(1), async batch => {
      try {
        const map = await fetchYahooSparkBatch(batch, S0_INTERVAL, S0_RANGE);
        return { requested: batch, map, ok: true, returned: map.size };
      } catch (error) {
        return { requested: batch, map: new Map(), ok: false, returned: 0, error: error?.message || String(error) };
      }
    }, SPARK_BATCH_CONCURRENCY);
    sparkResults.push(...remaining);
    for (const batchResult of remaining) for (const [symbol, chart] of batchResult.map) prefetched.set(symbol, chart);
  }
  const sparkElapsedMs = Date.now() - sparkStarted;
  const missing = symbols.filter(symbol => !prefetched.has(symbol));
  const fallbackStarted = Date.now();
  const raw = await mapConcurrent(symbols, symbol => processS0Symbol(symbol, asofMs, prefetched.get(symbol) || null));
  const chartFallbackMs = Date.now() - fallbackStarted;
  const candidates = raw.filter(x => x?.ok);
  const result = {
    stage: "S0", version: APP_VERSION, cache_schema_version: CACHE_SCHEMA_VERSION, asof: asofMs, session_date: etDate(asofMs),
    universe_requested: symbols.length, processed: raw.length, count: candidates.length,
    rejected: raw.length - candidates.length, rejection_counts: categorizeRejections(raw),
    data_sources: {
      preferred: "Yahoo Spark multi-symbol OHLCV, batches of 10",
      spark_available: sparkAvailable,
      spark_batches_requested: sparkResults.length,
      spark_batches_with_results: sparkResults.filter(x => x.ok && x.returned > 0).length,
      spark_batch_errors: sparkResults.filter(x => !x.ok).map(x => ({ symbols: x.requested, error: x.error })),
      symbols_returned_by_spark: prefetched.size,
      symbols_fallback_to_chart: missing.length,
      fallback_reason: "Spark unavailable for a symbol, or response lacked complete OHLCV arrays; Chart API fallback used"
    },
    timings: {
      total_ms: Date.now() - started,
      spark_batches_ms: sparkElapsedMs,
      per_symbol_chart_fallback_phase_ms: chartFallbackMs,
      mean_wall_ms_per_symbol: symbols.length ? round((Date.now() - started) / symbols.length, 2) : 0
    },
    elapsed_ms: Date.now() - started, candidates
  };
  atomicWriteJson(S0_CACHE_FILE, result);
  scanState.s0_count = candidates.length;
  scanState.s0_elapsed_ms = result.elapsed_ms;
  scanState.session_date = result.session_date;
  return result;
}
function loadS0CacheFor(asofMs) {
  const s0 = readJson(S0_CACHE_FILE);
  if (!s0?.candidates || !Array.isArray(s0.candidates)) return { ok: false, error: "S0_CACHE_EMPTY_OR_INVALID" };
  if (s0.session_date !== etDate(asofMs)) return { ok: false, error: "S0_CACHE_WRONG_SESSION_DATE", cache_session_date: s0.session_date, requested_session_date: etDate(asofMs) };
  if (s0.version !== APP_VERSION) return { ok: false, error: "S0_CACHE_VERSION_MISMATCH", cache_version: s0.version };
  if (s0.cache_schema_version !== CACHE_SCHEMA_VERSION) return { ok: false, error: "S0_CACHE_SCHEMA_MISMATCH", cache_schema_version: s0.cache_schema_version ?? null, required_schema_version: CACHE_SCHEMA_VERSION, hint: "Run yahoo_s0_prepare to rebuild references." };
  return { ok: true, cache: s0 };
}

function calculateVWAP(bars) {
  let pv = 0, volume = 0;
  for (const bar of bars) {
    if (!finite(bar.volume) || bar.volume <= 0) continue;
    const typical = (bar.high + bar.low + bar.close) / 3;
    pv += typical * bar.volume;
    volume += bar.volume;
  }
  return volume > 0 ? pv / volume : null;
}
function calculateHOD(bars) { return bars.length ? Math.max(...bars.map(b => b.high)) : null; }
function windowVolume(bars, count) {
  if (bars.length < count) return null;
  const window = bars.slice(-count);
  if (!isContinuousWindow(window, window.length - 1, count)) return null;
  return window.reduce((sum, b) => sum + b.volume, 0);
}
function calculateDelta60(bars, priceOverride = null) {
  if (bars.length < 13) return null;
  const cur = bars.at(-1), prev = bars.at(-13);
  if (cur.ts - prev.ts !== 60 * 60_000) return null;
  return pctChange(prev.close, finite(priceOverride) ? priceOverride : cur.close);
}
function calculateAccel5m(bars) {
  if (bars.length < 2 || bars.at(-2).volume <= 0) return null;
  return pctChange(bars.at(-2).volume, bars.at(-1).volume);
}
function calculateDollarVolume(price, rollingVolume15m) {
  if (!finite(price) || price <= 0 || !finite(rollingVolume15m) || rollingVolume15m < 0) return null;
  return price * rollingVolume15m;
}
function evaluateS1Checks({ price, priceVsVWAP, rvol15, vol5Ratio }) {
  return {
    price_under_5: finite(price) && price < LOCKED.maxPrice,
    price_vs_vwap: finite(priceVsVWAP) && priceVsVWAP >= LOCKED.s1PriceVsVwapMinPct,
    rvol15: finite(rvol15) && rvol15 >= LOCKED.s1Rvol15Min,
    vol5_ratio: finite(vol5Ratio) && vol5Ratio >= LOCKED.s1Vol5MinRatio
  };
}
function evaluateSFChecks({ price, rvol15, vol5Ratio, delta60, priceVsVWAP, delta1555toAsOf }) {
  return {
    rvol15: finite(rvol15) && rvol15 >= LOCKED.rvol15Min,
    vol5_ratio: finite(vol5Ratio) && vol5Ratio >= LOCKED.vol5MinRatio,
    delta60: finite(delta60) && delta60 <= LOCKED.delta60MaxPct,
    price_vs_vwap: finite(priceVsVWAP) && priceVsVWAP >= LOCKED.priceVsVwapMinPct,
    delta1555toAsOf: finite(delta1555toAsOf) && delta1555toAsOf >= LOCKED.delta1555to1559MinPct,
    price_under_5: finite(price) && price < LOCKED.maxPrice
  };
}
function getSlotReference(candidate, key, slot) {
  const references = candidate?.references;
  const map = references?.[key];
  const value = map?.[slot];
  const sampleKeyByReferenceKey = {
    rvol15_by_slot: "rvol15",
    vol5_by_slot: "vol5",
    vol60_by_slot: "vol60"
  };
  const sampleKey = sampleKeyByReferenceKey[key];
  const sampleCount = sampleKey ? references?.samples_by_slot?.[sampleKey]?.[slot] : null;
  if (!finite(sampleCount) || sampleCount < MIN_REFERENCE_SAMPLES) return null;
  return finite(value) && value > 0 ? value : null;
}
function rejectCountsFromResults(items) {
  const counts = {};
  for (const item of items) if (!item?.ok) {
    const reason = item.reason || "UNKNOWN";
    counts[reason] = (counts[reason] || 0) + 1;
  }
  return Object.fromEntries(Object.entries(counts).sort((a, b) => a[0].localeCompare(b[0])));
}

async function processS1Symbol(candidate, asofMs) {
  const symbol = candidate.symbol;
  try {
    const chart = await fetchYahooChart(symbol, S1_INTERVAL, "1d");
    const regular = chart.bars.filter(b => isRegularSessionBar(b.ts));
    const bars = completedBarsAt(regular, asofMs);
    if (!bars.length) return { ok: false, symbol, reason: "NO_COMPLETED_BARS" };
    if (bars.length < 13) return { ok: false, symbol, reason: "INSUFFICIENT_INTRADAY" };
    const current = bars.at(-1);
    if (asofMs - (current.ts + 5 * 60_000) > 10 * 60_000) return { ok: false, symbol, reason: "STALE_5M_BAR" };
    if (current.close >= LOCKED.maxPrice) return { ok: false, symbol, reason: "PRICE_GE_5", price: current.close };
    const slot = floor5Clock(current.ts);
    const liveGrid = buildSessionGrid(etDate(current.ts), bars);
    const slotIndex = getSessionSlotIndex(current.ts);
    const vol15 = volumeWindowFromGrid(liveGrid, slotIndex, 3);
    const vol60 = volumeWindowFromGrid(liveGrid, slotIndex, 12);
    if (!finite(vol15) || !finite(vol60)) return { ok: false, symbol, reason: "INSUFFICIENT_VOLUME_WINDOW" };
    const rvolRef = getSlotReference(candidate, "rvol15_by_slot", slot);
    const vol5Ref = getSlotReference(candidate, "vol5_by_slot", slot);
    const vol60Ref = getSlotReference(candidate, "vol60_by_slot", slot);
    if (!finite(rvolRef) || !finite(vol5Ref) || !finite(vol60Ref)) return { ok: false, symbol, reason: "NO_SLOT_ALIGNED_REFERENCE", slot };
    const vwap = calculateVWAP(bars);
    const hod = calculateHOD(bars);
    const price = current.close;
    const priceVsVWAP = finite(vwap) && vwap > 0 ? pctChange(vwap, price) : null;
    const rvol15 = vol15 / rvolRef;
    const vol5Ratio = current.volume / vol5Ref;
    const delta60 = calculateDelta60(bars);
    const accel5m = calculateAccel5m(bars);
    const checks = evaluateS1Checks({ price, priceVsVWAP, rvol15, vol5Ratio });
    const failed = Object.entries(checks).filter(([, pass]) => !pass).map(([key]) => key);
    if (failed.length) return {
      ok: false, symbol, reason: "S1_FILTER_REJECT", failed_checks: failed,
      checks, price: round(price), rvol15: round(rvol15), vol5_ratio: round(vol5Ratio), slot
    };
    const dv = calculateDollarVolume(price, vol15); // Locked ranking: price × cumulative 15m volume only.
    if (!finite(dv)) return { ok: false, symbol, reason: "INVALID_DOLLAR_VOLUME" };
    return {
      ok: true, symbol, asof: current.ts + 5 * 60_000, bar_ts: current.ts, slot,
      price: round(price), vwap: round(vwap), price_vs_vwap_pct: round(priceVsVWAP),
      hod: round(hod), hod_distance_pct: finite(hod) && hod > 0 ? round(pctChange(hod, price)) : null,
      vol5: current.volume, vol15, vol60, rvol15: round(rvol15), rvol15_reference: round(rvolRef),
      vol5_reference: round(vol5Ref), vol5_ratio: round(vol5Ratio), vol60_reference: round(vol60Ref),
      vol60_ratio: round(vol60 / vol60Ref), delta60_pct: round(delta60), accel5m_pct: round(accel5m),
      accel5m_blocking: false, dv, dv_display: round(dv, 2), dv_formula: "price * volume15m",
      j1: candidate.j1, j2: candidate.j2, checks,
      references: candidate.references
    };
  } catch (error) {
    return { ok: false, symbol, reason: "FETCH_ERROR", error: error?.message || String(error) };
  }
}
async function ensureWsSubscriptions(symbols) {
  const list = normalizeSymbols(symbols);
  if (!list.length) return { ok: false, reason: "EMPTY_SUBSCRIPTION" };
  try {
    if (!yahooWS) yahooWS = new YahooWS({ log: (...args) => console.log(...args) });
    if (!yahooWS.connected) {
      if (!wsConnectPromise) wsConnectPromise = yahooWS.connect().finally(() => { wsConnectPromise = null; });
      await wsConnectPromise;
    }
    const subscribed = yahooWS.subscribe(list);
    scanState.websocket = { ...yahooWS.status(), subscribed_count: subscribed };
    return { ok: true, subscribed_count: subscribed, status: scanState.websocket };
  } catch (error) {
    scanState.websocket = { ...(yahooWS?.status?.() || {}), connected: false, error: error?.message || String(error) };
    return { ok: false, error: error?.message || String(error), status: scanState.websocket };
  }
}
async function runS1(candidates, asofMs) {
  const started = Date.now();
  const raw = await mapConcurrent(candidates, candidate => processS1Symbol(candidate, asofMs));
  const qualified = raw.filter(x => x?.ok);
  // Preserve the ranking as calculated at S1. SF must never recalculate or reorder by new DV.
  qualified.sort((a, b) => (b.dv - a.dv) || a.symbol.localeCompare(b.symbol));
  const computationMs = Date.now() - started;
  const result = {
    stage: "S1", version: APP_VERSION, cache_schema_version: CACHE_SCHEMA_VERSION, asof: asofMs, session_date: etDate(asofMs),
    count: qualified.length, rejected: raw.length - qualified.length, rejection_counts: rejectCountsFromResults(raw),
    ranking_formula: "price * volume15m", ranking_frozen: true, candidates: qualified,
    elapsed_ms: computationMs, timings: { compute_ms: computationMs, total_ms: computationMs, mean_wall_ms_per_symbol: raw.length ? round(computationMs / raw.length, 2) : 0 }
  };
  scanState.s1_count = qualified.length;
  const ws = qualified.length ? await ensureWsSubscriptions(qualified.map(x => x.symbol)) : { ok: false, reason: "NO_S1_CANDIDATES" };
  result.websocket_subscription = { ok: ws.ok, subscribed_count: ws.subscribed_count || 0, error: ws.error || null };
  result.elapsed_ms = Date.now() - started;
  result.timings.total_ms = result.elapsed_ms;
  result.timings.websocket_connect_and_subscribe_ms = Math.max(0, result.elapsed_ms - computationMs);
  scanState.s1_elapsed_ms = result.elapsed_ms;
  // Persist diagnostic updates without changing the frozen ranking or DV values.
  atomicWriteJson(S1_CACHE_FILE, result);
  return result;
}
function loadS1CacheFor(asofMs) {
  const s1 = readJson(S1_CACHE_FILE);
  if (!s1?.candidates || !Array.isArray(s1.candidates)) return { ok: false, error: "S1_CACHE_EMPTY_OR_INVALID" };
  if (s1.session_date !== etDate(asofMs)) return { ok: false, error: "S1_CACHE_WRONG_SESSION_DATE", cache_session_date: s1.session_date, requested_session_date: etDate(asofMs) };
  if (s1.version !== APP_VERSION || s1.ranking_frozen !== true) return { ok: false, error: "S1_CACHE_VERSION_OR_RANKING_INVALID" };
  if (s1.cache_schema_version !== CACHE_SCHEMA_VERSION) return { ok: false, error: "S1_CACHE_SCHEMA_MISMATCH", cache_schema_version: s1.cache_schema_version ?? null, required_schema_version: CACHE_SCHEMA_VERSION, hint: "Run S1 again to rebuild the frozen ranking." };
  return { ok: true, cache: s1 };
}

function getFreshWsTick(symbol, referenceMs = Date.now(), expectedSessionDate = etDate(referenceMs)) {
  const tick = yahooWS?.latest?.get?.(symbol);
  if (!tick) return { ok: false, reason: "NO_WS_TICK" };
  let tickMs = Date.parse(tick.timestamp || "");
  if (!Number.isFinite(tickMs) && finite(Number(tick.time))) tickMs = Number(tick.time) < 1e12 ? Number(tick.time) * 1000 : Number(tick.time);
  const receivedMs = Date.parse(tick.received_at || "");
  if (!Number.isFinite(tickMs) || !Number.isFinite(receivedMs) || !finite(tick.price) || tick.price <= 0) return { ok: false, reason: "INVALID_WS_TICK" };
  const tickAge = referenceMs - tickMs;
  const receiptAge = referenceMs - receivedMs;
  if (tickAge < -5000 || tickAge > MAX_WS_TICK_AGE_MS || receiptAge < -5000 || receiptAge > MAX_WS_RECEIPT_AGE_MS) {
    return { ok: false, reason: "STALE_WS_TICK", tick_age_ms: tickAge, receipt_age_ms: receiptAge };
  }
  if (etDate(tickMs) !== expectedSessionDate || !isRegularSessionBar(tickMs)) return { ok: false, reason: "WS_TICK_OUTSIDE_REGULAR_SESSION" };
  return { ok: true, tick, tick_ms: tickMs, received_ms: receivedMs, tick_age_ms: tickAge, receipt_age_ms: receiptAge };
}
async function processSFCandidate(candidate, asofMs) {
  const symbol = candidate.symbol;
  try {
    const chart = await fetchYahooChart(symbol, S1_INTERVAL, "1d");
    const regular = chart.bars.filter(b => isRegularSessionBar(b.ts) && etDate(b.ts) === etDate(asofMs));
    const available = barsAtOrBefore(regular, asofMs);
    const targetBar = available.find(b => etClock(b.ts) === "15:55");
    if (!targetBar) return { ok: false, symbol, reason: "NO_1555_BAR" };
    if (asofMs < targetBar.ts || asofMs >= targetBar.ts + 5 * 60_000) return { ok: false, symbol, reason: "OUTSIDE_1555_TO_1600_WINDOW" };
    const partialBars = available.filter(b => b.ts >= (regular[0]?.ts ?? b.ts));
    if (partialBars.length < 13) return { ok: false, symbol, reason: "INSUFFICIENT_SF_HISTORY" };
    if (Date.now() >= targetBar.ts + 5 * 60_000) return { ok: false, symbol, reason: "SF_WINDOW_EXPIRED" };
    const tickResult = getFreshWsTick(symbol, Date.now(), etDate(asofMs));
    // A chart timestamp alone is not proof of a fresh live trade. This gate remains strict.
    if (!tickResult.ok) return { ok: false, symbol, reason: tickResult.reason, ws_diagnostics: tickResult };
    if (tickResult.tick_ms < targetBar.ts || tickResult.tick_ms >= targetBar.ts + 5 * 60_000) return { ok: false, symbol, reason: "NO_FRESH_TICK_IN_SF_BAR", ws_diagnostics: tickResult };
    const livePrice = tickResult.tick.price;
    const evaluationMs = tickResult.tick_ms;
    const liveGrid = buildSessionGrid(etDate(asofMs), partialBars);
    const slotIndex = getSessionSlotIndex(targetBar.ts);
    const vol15 = volumeWindowFromGrid(liveGrid, slotIndex, 3);
    if (!finite(vol15)) return { ok: false, symbol, reason: "INSUFFICIENT_SF_VOL15" };
    const slot = "15:55";
    const rvolRef = getSlotReference(candidate, "rvol15_by_slot", slot);
    const vol5Ref = getSlotReference(candidate, "vol5_by_slot", slot);
    if (!finite(rvolRef) || !finite(vol5Ref)) return { ok: false, symbol, reason: "NO_SF_SLOT_REFERENCE" };
    const rvol15 = vol15 / rvolRef;
    const vol5 = targetBar.volume; // Yahoo Chart's observed partial 15:55 bar volume; no fabricated multiplier.
    const vol5Ratio = vol5 / vol5Ref;
    const vwap = calculateVWAP(partialBars);
    const hod = calculateHOD(partialBars);
    const priceVsVWAP = finite(vwap) && vwap > 0 ? pctChange(vwap, livePrice) : null;
    // Delta60 compares live price to the last available close at least 60m before as-of.
    const oneHourAgo = evaluationMs - 60 * 60_000;
    const baselineBar = [...partialBars].reverse().find(b => b.ts <= oneHourAgo);
    const delta60 = baselineBar ? pctChange(baselineBar.close, livePrice) : null;
    const delta1555toAsOf = targetBar.open > 0 ? pctChange(targetBar.open, livePrice) : null;
    const accel5m = partialBars.length >= 2 && partialBars.at(-2).volume > 0 ? pctChange(partialBars.at(-2).volume, partialBars.at(-1).volume) : null;
    const checks = evaluateSFChecks({ price: livePrice, rvol15, vol5Ratio, delta60, priceVsVWAP, delta1555toAsOf });
    const pass = Object.values(checks).every(Boolean);
    return {
      ok: true, symbol, asof: evaluationMs, scan_started_asof: asofMs, price: round(livePrice), chart_partial_price: round(targetBar.close),
      target_bar_ts: targetBar.ts, target_bar_open: targetBar.open,
      delta60_reference_price: baselineBar?.close ?? null,
      vwap: round(vwap), price_vs_vwap_pct: round(priceVsVWAP), hod: round(hod),
      hod_distance_pct: finite(hod) && hod > 0 ? round(pctChange(hod, livePrice)) : null,
      vol5, vol5_reference: round(vol5Ref), vol5_ratio: round(vol5Ratio), vol15: round(vol15),
      rvol15: round(rvol15), rvol15_reference: round(rvolRef), delta60_pct: round(delta60),
      delta1555toAsOf_pct: round(delta1555toAsOf), accel5m_pct: round(accel5m), accel5m_blocking: false,
      ws_tick_timestamp: tickResult.tick.timestamp, ws_tick_age_ms: tickResult.tick_age_ms,
      ws_receipt_age_ms: tickResult.receipt_age_ms, checks, pass
    };
  } catch (error) {
    return { ok: false, symbol, reason: "FETCH_ERROR", error: error?.message || String(error) };
  }
}
async function runWinnerGate(sfCandidate, asofMs) {
  if (!sfCandidate?.pass) return { pass: false, stop: false, reason: "SF_FILTERS_FAILED" };
  const now = Date.now();
  if (!finite(sfCandidate.target_bar_ts) || now >= sfCandidate.target_bar_ts + 5 * 60_000) {
    return { pass: false, stop: false, reason: "SF_WINDOW_EXPIRED" };
  }
  const fresh = getFreshWsTick(sfCandidate.symbol, now, etDate(asofMs));
  if (!fresh.ok) return { pass: false, stop: false, reason: fresh.reason, diagnostics: fresh };
  if (fresh.tick_ms < sfCandidate.target_bar_ts || fresh.tick_ms >= sfCandidate.target_bar_ts + 5 * 60_000) {
    return { pass: false, stop: false, reason: "NO_FRESH_TICK_IN_SF_BAR", diagnostics: fresh };
  }
  // Recheck price-sensitive locked gates against the latest tick immediately before declaring a winner.
  const p = fresh.tick.price;
  const recheck = {
    price_under_5: p < LOCKED.maxPrice,
    price_vs_vwap: finite(sfCandidate.vwap) && sfCandidate.vwap > 0 && pctChange(sfCandidate.vwap, p) >= LOCKED.priceVsVwapMinPct,
    delta60: finite(sfCandidate.delta60_reference_price) && sfCandidate.delta60_reference_price > 0 && pctChange(sfCandidate.delta60_reference_price, p) <= LOCKED.delta60MaxPct,
    delta1555toAsOf: finite(sfCandidate.target_bar_open) && sfCandidate.target_bar_open > 0 && pctChange(sfCandidate.target_bar_open, p) >= LOCKED.delta1555to1559MinPct
  };
  const failed = Object.entries(recheck).filter(([, pass]) => !pass).map(([key]) => key);
  if (failed.length) return { pass: false, stop: false, reason: "WINNER_GATE_RECHECK_FAILED", failed_checks: failed, recheck };
  return {
    pass: true, stop: true, reason: "FIRST_VALID_FRESH_WINNER", winner: { ...sfCandidate, price: round(p), checks: { ...sfCandidate.checks, ...recheck } },
    tick: { price: p, timestamp: fresh.tick.timestamp, received_at: fresh.tick.received_at },
    tick_age_ms: fresh.tick_age_ms, receipt_age_ms: fresh.receipt_age_ms, recheck
  };
}
async function runSF(candidates, asofMs) {
  const started = Date.now();
  const evaluated = [];
  const gates = [];
  let winner = null;
  let lotsProcessed = 0;
  // Input array is the immutable S1 DV order. Work in real concurrent lots of 20.
  for (const lot of chunk(candidates, SF_LOT_SIZE)) {
    lotsProcessed++;
    const lotResults = await mapConcurrent(lot, candidate => processSFCandidate(candidate, asofMs), SF_LOT_SIZE);
    // mapConcurrent preserves input indices; gate candidates strictly in frozen S1 order.
    for (const result of lotResults) {
      evaluated.push(result);
      if (!result?.ok || !result.pass) continue;
      const gate = await runWinnerGate(result, asofMs);
      gates.push({ symbol: result.symbol, ...gate });
      const resultIndex = evaluated.length - 1;
      evaluated[resultIndex] = { ...result, winner_gate: { pass: gate.pass, reason: gate.reason }, pass: Boolean(gate.pass) };
      if (gate.pass) { winner = { ...(gate.winner || result), winner_gate: gate }; break; }
    }
    if (winner) break;
  }
  const result = {
    stage: "SF", version: APP_VERSION, asof: asofMs, session_date: etDate(asofMs),
    candidate_count: candidates.length, evaluated_count: evaluated.length,
    count: winner ? 1 : 0, winner, winners: winner ? [winner] : [], evaluated,
    winner_gate_checks: gates, lots_processed: lotsProcessed, lot_size: SF_LOT_SIZE,
    locked_params: LOCKED, ranking_source: "frozen S1 ranking; no SF reranking",
    rejection_counts: rejectCountsFromResults(evaluated.map(x => x?.ok && !x.pass ? { ...x, ok: false, reason: "SF_FILTER_REJECT" } : x)),
    elapsed_ms: Date.now() - started, timings: { total_ms: Date.now() - started }
  };
  scanState.sf_count = result.count;
  scanState.sf_elapsed_ms = result.elapsed_ms;
  return result;
}
function runDeterministicSelfTests() {
  const checks = [];
  const check = (name, ok, detail = null) => checks.push({ name, ok: Boolean(ok), detail });
  check("locked_S1_RVOL15", LOCKED.s1Rvol15Min === 1.30, LOCKED.s1Rvol15Min);
  check("locked_SF_RVOL15", LOCKED.rvol15Min === 1.50, LOCKED.rvol15Min);
  check("locked_Vol5_ratio", LOCKED.vol5MinRatio === 0.20 && LOCKED.s1Vol5MinRatio === 0.20, { s1: LOCKED.s1Vol5MinRatio, sf: LOCKED.vol5MinRatio });
  check("locked_VWAP_minimum", LOCKED.priceVsVwapMinPct === -1 && LOCKED.s1PriceVsVwapMinPct === -1, { s1: LOCKED.s1PriceVsVwapMinPct, sf: LOCKED.priceVsVwapMinPct });
  check("locked_Delta60_maximum", LOCKED.delta60MaxPct === 10, LOCKED.delta60MaxPct);
  check("locked_lot_size", LOCKED.lotSize === 20 && SF_LOT_SIZE === 20, SF_LOT_SIZE);
  check("locked_S0_price_and_J1J2", LOCKED.maxPrice === 5 && LOCKED.maxAbsJ1Pct === 5 && LOCKED.maxAbsJ2Pct === 5, { maxPrice: LOCKED.maxPrice, J1: LOCKED.maxAbsJ1Pct, J2: LOCKED.maxAbsJ2Pct });
  const s1BoundaryChecks = evaluateS1Checks({ price: 4.99, priceVsVWAP: -1, rvol15: 1.30, vol5Ratio: 0.20 });
  check("S1_exact_locked_boundaries_pass", Object.values(s1BoundaryChecks).every(Boolean), s1BoundaryChecks);
  const s1FailChecks = evaluateS1Checks({ price: 4.99, priceVsVWAP: -1.01, rvol15: 1.29, vol5Ratio: 0.19 });
  check("S1_below_locked_boundaries_rejected", Object.values(s1FailChecks).some(v => !v), s1FailChecks);
  const sfBoundaryChecks = evaluateSFChecks({ price: 4.99, rvol15: 1.50, vol5Ratio: 0.20, delta60: 10, priceVsVWAP: -1, delta1555toAsOf: 0 });
  check("SF_exact_locked_boundaries_pass", Object.values(sfBoundaryChecks).every(Boolean), sfBoundaryChecks);
  const sfFailChecks = evaluateSFChecks({ price: 4.99, rvol15: 1.50, vol5Ratio: 0.20, delta60: 10.01, priceVsVWAP: -1, delta1555toAsOf: 0 });
  check("SF_Delta60_above_plus10_rejected", !sfFailChecks.delta60, sfFailChecks);
  check("Accel5M_does_not_change_S1_checks", JSON.stringify(evaluateS1Checks({ price: 4.99, priceVsVWAP: -1, rvol15: 1.30, vol5Ratio: 0.20 })) === JSON.stringify(s1BoundaryChecks), "Accel5M is not an input to blocking checks");
  const bars3 = [0, 1, 2].map(i => ({ ts: 1000000 + i * 300000, volume: i + 1 }));
  check("rolling_Vol15_sum", windowVolume(bars3, 3) === 6, windowVolume(bars3, 3));
  const gapBars = [{ ts: 1000000, volume: 1 }, { ts: 1000000 + 300000, volume: 2 }, { ts: 1000000 + 900000, volume: 3 }];
  check("reject_noncontiguous_Vol15", windowVolume(gapBars, 3) === null, windowVolume(gapBars, 3));
  const sessions = new Map();
  const dates = ["2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08", "2026-01-09"];
  for (const date of dates) {
    const day = [];
    const start = Date.parse(`${date}T14:30:00Z`); // 09:30 ET in January
    for (let i = 0; i < 78; i++) {
      const px = 1 + i / 10000;
      day.push({ ts: start + i * 300000, open: px, high: px + 0.01, low: px - 0.01, close: px, volume: i + 1 });
    }
    sessions.set(date, day);
  }
  // Add the final 15:55 bar to each synthetic session so it qualifies as a completed session.
  for (const date of dates) {
    const day = sessions.get(date);
    const px = 1.01;
    day[77] = { ...day[77], ts: Date.parse(`${date}T20:55:00Z`), open: px, high: px + 0.01, low: px - 0.01, close: px, volume: 78 };
  }
  const refs = buildHistoricalReferences(sessions, dates);
  check("baseline_RVOL15_from_3x5m", refs.rvol15BySlot["15:50"] === 228, refs.rvol15BySlot["15:50"]);
  check("baseline_Vol5_from_5m", refs.vol5BySlot["15:50"] === 77, refs.vol5BySlot["15:50"]);
  check("baseline_Vol60_from_12x5m", refs.vol60BySlot["15:50"] === 858, refs.vol60BySlot["15:50"]);
  const missingDay = sessions.get(dates[0]).filter((_, i) => i !== 70);
  const zeroFilled = buildSessionGrid(dates[0], missingDay);
  check("missing_historical_slot_zero_filled", zeroFilled.length === 78 && zeroFilled[70].volume === 0 && zeroFilled[70].bar === null, { length: zeroFilled.length, missing_slot_volume: zeroFilled[70].volume });
  const malformed = normalizeYahooChart("TEST", { timestamp: [Date.parse("2026-01-05T14:30:00Z") / 1000, Date.parse("2026-01-05T14:35:00Z") / 1000], indicators: { quote: [{ open: [1, null], high: [1.1, null], low: [0.9, null], close: [1, null], volume: [10, 20] }] } });
  const malformedGrid = buildSessionGrid(dates[0], malformed.bars);
  check("malformed_OHLC_candle_becomes_zero_volume_slot", malformed.bars.length === 1 && malformedGrid[1].volume === 0, { valid_bars: malformed.bars.length, malformed_slot_volume: malformedGrid[1].volume });
  check("slot_sample_counts_include_missing_volume_days", refs.samplesBySlot.vol5["15:50"] === dates.length, refs.samplesBySlot.vol5["15:50"]);
  check("reference_sample_key_mapping", getSlotReference({ references: { rvol15_by_slot: { "15:50": 100 }, samples_by_slot: { rvol15: { "15:50": 5 } } } }, "rvol15_by_slot", "15:50") === 100, getSlotReference({ references: { rvol15_by_slot: { "15:50": 100 }, samples_by_slot: { rvol15: { "15:50": 5 } } } }, "rvol15_by_slot", "15:50"));
  check("reference_minimum_samples_enforced", getSlotReference({ references: { vol5_by_slot: { "15:50": 100 }, samples_by_slot: { vol5: { "15:50": 4 } } } }, "vol5_by_slot", "15:50") === null, "four samples must be rejected");
  check("asof_timezone_less_string_is_ET", validateAsOf("2026-10-02T15:55:00") === Date.parse("2026-10-02T19:55:00Z"), new Date(validateAsOf("2026-10-02T15:55:00")).toISOString());
  check("asof_explicit_UTC_remains_UTC", validateAsOf("2026-10-02T15:55:00Z") === Date.parse("2026-10-02T15:55:00Z"), "explicit Z honored");
  check("Accel5M_nonblocking", NON_BLOCKING.accel5m === true, NON_BLOCKING.accel5m);
  check("ordinary_stock_EQUITY_NMS", isOrdinaryStock({ quoteType: "EQUITY", exchangeName: "NMS", shortName: "Sample Inc." }, "SAMP"), "regular NASDAQ equity allowed");
  check("reject_ETF_metadata", !isOrdinaryStock({ quoteType: "ETF", exchangeName: "NMS", shortName: "Sample Fund" }, "SAMP"), "ETF rejected");
  check("reject_warrant_name", !isOrdinaryStock({ quoteType: "EQUITY", exchangeName: "NMS", shortName: "Sample Inc. Warrant" }, "SAMPW"), "warrant rejected by name");
  check("ticker_suffix_alone_not_rejected", isOrdinaryStock({ quoteType: "EQUITY", exchangeName: "NMS", shortName: "Sample U Inc." }, "SAMPU"), "U suffix alone is not proof of a unit");
  check("reject_unit_name", !isOrdinaryStock({ quoteType: "EQUITY", exchangeName: "NMS", shortName: "Sample Acquisition Units" }, "SAMPU"), "unit rejected by metadata name");
  check("ambiguous_missing_quote_type_rejected", classifyInstrument({ exchangeName: "NMS", shortName: "Sample Inc." }, "SAMP") === "ambiguous", "missing quote type is not assumed ordinary");
  check("J1J2_requires_open_and_close_slots", sessionPerformance([{ ts: Date.parse("2026-01-05T14:35:00Z"), open: 1, close: 1.1 }, { ts: Date.parse("2026-01-05T20:55:00Z"), open: 1.1, close: 1.2 }]) === null, "session missing 09:30 slot is incomplete");
  check("DV_is_price_times_Vol15", calculateDollarVolume(2.5, 100) === 250, calculateDollarVolume(2.5, 100));
  check("DV_rejects_invalid_inputs", calculateDollarVolume(0, 100) === null && calculateDollarVolume(2.5, -1) === null, "invalid price/volume rejected");
  const failures = checks.filter(c => !c.ok);
  return { ok: failures.length === 0, version: APP_VERSION, checks, passed: checks.length - failures.length, failed: failures.length };
}

function mcpText(data) { return { content: [{ type: "text", text: JSON.stringify(data) }] }; }
function createMcpServer() {
  const server = new McpServer({ name: "yahoo-scan-mcp", version: APP_VERSION });
  server.tool("ping", "Health check", {}, async () => mcpText({ ok: true, service: "yahoo-scan-mcp", version: APP_VERSION, timestamp: new Date().toISOString() }));
  server.tool("get_status", "Return scanner status", {}, async () => mcpText({ ok: true, version: APP_VERSION, scanState, locked: LOCKED, non_blocking: NON_BLOCKING, cache_schema_version: CACHE_SCHEMA_VERSION, universe_file: UNIVERSE_FILE }));
  server.tool("diagnose_filesystem", "Diagnose local scanner files", {}, async () => {
    let universeCount = null, universeError = null;
    try { universeCount = readUniverse().length; } catch (e) { universeError = e.message; }
    return mcpText({ ok: !universeError, cwd: ROOT, universe_file: UNIVERSE_FILE, universe_exists: fs.existsSync(UNIVERSE_FILE), universe_count: universeCount, universe_error: universeError, s0_cache_exists: fs.existsSync(S0_CACHE_FILE), s1_cache_exists: fs.existsSync(S1_CACHE_FILE), websocket: yahooWS?.status?.() || { connected: false } });
  });
  server.tool("get_universe", "Return materialized NASDAQ <$5 universe", {}, async () => {
    try { const symbols = readUniverse(); return mcpText({ ok: true, count: symbols.length, universe_file: UNIVERSE_FILE, symbols }); }
    catch (e) { return mcpText({ ok: false, error: e.message }); }
  });
  server.tool("yahoo_spark_test", "Test the Yahoo Spark multi-symbol OHLCV endpoint", {
    symbols: z.array(z.string()).optional(), range: z.string().optional(), interval: z.string().optional()
  }, async ({ symbols, range, interval }) => {
    const list = normalizeSymbols(symbols?.length ? symbols : ["AAPL", "MSFT", "NVDA", "AMD", "INTC"]);
    const results = [];
    for (const batch of chunk(list, SPARK_BATCH_SIZE)) {
      try {
        const map = await fetchYahooSparkBatch(batch, interval || "5m", range || "1d");
        for (const symbol of batch) {
          const data = map.get(symbol);
          results.push(data ? { symbol, ok: true, data_source: "yahoo_spark", bars: data.bars.length, last: data.bars.at(-1) || null, meta: data.meta } : { symbol, ok: false, reason: "SPARK_SYMBOL_MISSING_OR_INCOMPLETE_OHLCV" });
        }
      } catch (error) {
        for (const symbol of batch) results.push({ symbol, ok: false, error: error?.message || String(error) });
      }
    }
    return mcpText({ ok: results.every(x => x.ok), batch_size: SPARK_BATCH_SIZE, fallback_available_in_s0: true, results });
  });
  server.tool("yahoo_ws_test", "Connect to Yahoo WebSocket and report observed tick coverage", {
    symbols: z.array(z.string()).optional(), wait_ms: z.number().int().min(0).max(10000).optional()
  }, async ({ symbols, wait_ms }) => {
    const list = normalizeSymbols(symbols?.length ? symbols : ["AAPL", "MSFT", "NVDA", "AMD", "INTC"]);
    const sub = await ensureWsSubscriptions(list);
    if (sub.ok && (wait_ms || 0) > 0) await sleep(wait_ms);
    const status = yahooWS?.status?.() || {};
    const receivedSymbols = Object.keys(status.latest || {});
    const received = list.filter(s => receivedSymbols.includes(s));
    return mcpText({ ok: sub.ok, connection: sub, requested_count: list.length, subscribed_count: sub.subscribed_count || 0, received_tick_count: received.length, received_symbols: received, coverage_pct: list.length ? round(received.length / list.length * 100, 2) : 0, status });
  });
  server.tool("yahoo_s0_prepare", "Build materialized S0 universe (explicit refresh)", {
    symbols: z.array(z.string()).optional(), asof: z.string().optional()
  }, async ({ symbols, asof }) => {
    try {
      const asofMs = validateAsOf(asof);
      const list = symbols?.length ? normalizeSymbols(symbols) : readUniverse();
      if (!list.length) return mcpText({ ok: false, error: "EMPTY_UNIVERSE" });
      const result = await runS0(list, asofMs);
      return mcpText({ ok: true, ...result });
    } catch (e) { scanState.last_error = e.message; return mcpText({ ok: false, error: e.message }); }
  });
  server.tool("yahoo_s1_scan", "Run S1 using same-session materialized S0", {
    asof: z.string().optional(), symbols: z.array(z.string()).optional()
  }, async ({ asof, symbols }) => {
    try {
      const asofMs = validateAsOf(asof);
      const loaded = loadS0CacheFor(asofMs);
      if (!loaded.ok) return mcpText({ ok: false, ...loaded, hint: "Run yahoo_s0_prepare explicitly to refresh S0." });
      let candidates = loaded.cache.candidates;
      if (symbols?.length) {
        const selected = new Set(normalizeSymbols(symbols));
        candidates = candidates.filter(c => selected.has(c.symbol));
        if (!candidates.length) return mcpText({ ok: false, error: "NO_REQUESTED_SYMBOLS_IN_S0_CACHE" });
      }
      scanState.status = "s1_running";
      const result = await runS1(candidates, asofMs);
      scanState.asof = asofMs; scanState.status = "s1_completed";
      return mcpText({ ok: true, ...result });
    } catch (e) { scanState.last_error = e.message; scanState.status = "error"; return mcpText({ ok: false, error: e.message }); }
  });
  server.tool("yahoo_s0_s1_scan", "Run explicit S0 refresh then S1", {
    asof: z.string().optional(), symbols: z.array(z.string()).optional()
  }, async ({ asof, symbols }) => {
    const started = Date.now();
    try {
      const hasExplicitAsOf = asof != null && String(asof).trim() !== "";
      const s0AsOfMs = validateAsOf(asof);
      scanState.started_at = new Date().toISOString(); scanState.status = "running"; scanState.last_error = null;
      const list = symbols?.length ? normalizeSymbols(symbols) : readUniverse();
      if (!list.length) throw new Error("EMPTY_UNIVERSE");
      // Freeze S0 to its start time, but do not make S1 stale by reusing that time
      // after a long S0. Explicit asof remains deterministic for diagnostic tests.
      const s0 = await runS0(list, s0AsOfMs);
      const s1AsOfMs = hasExplicitAsOf ? s0AsOfMs : Date.now();
      const s1 = await runS1(s0.candidates, s1AsOfMs);
      scanState.asof = s1AsOfMs; scanState.elapsed_ms = Date.now() - started;
      scanState.completed_at = new Date().toISOString(); scanState.status = "completed";
      return mcpText({ ok: true, stage: "S0_S1", asof: s1AsOfMs, s0_asof: s0.asof, s1_asof: s1.asof, elapsed_ms: scanState.elapsed_ms, universe_requested: list.length, s0_count: s0.count, s1_count: s1.count, s0: { count: s0.count, rejected: s0.rejected, rejection_counts: s0.rejection_counts, elapsed_ms: s0.elapsed_ms }, s1: { count: s1.count, rejected: s1.rejected, rejection_counts: s1.rejection_counts, elapsed_ms: s1.elapsed_ms, ranking_frozen: true, websocket_subscription: s1.websocket_subscription }, s1_candidates: s1.candidates });
    } catch (e) { scanState.last_error = e.message; scanState.status = "error"; scanState.elapsed_ms = Date.now() - started; return mcpText({ ok: false, error: e.message, elapsed_ms: scanState.elapsed_ms }); }
  });
  server.tool("yahoo_sf_scan", "Run locked SF filters in real lots of 20 and stop at the first fresh winner", {
    asof: z.string().optional(), symbols: z.array(z.string()).optional()
  }, async ({ asof, symbols }) => {
    try {
      const asofMs = validateAsOf(asof);
      const loaded = loadS1CacheFor(asofMs);
      if (!loaded.ok) return mcpText({ ok: false, ...loaded, hint: "Run S1 for this ET session first. S1 ranking is frozen and is not rebuilt by SF." });
      let candidates = loaded.cache.candidates;
      if (symbols?.length) {
        const selected = new Set(normalizeSymbols(symbols));
        candidates = candidates.filter(c => selected.has(c.symbol));
        if (!candidates.length) return mcpText({ ok: false, error: "NO_REQUESTED_SYMBOLS_IN_S1_CACHE" });
      }
      // Preserve the S1 cache order even for subset requests.
      scanState.status = "sf_running";
      const result = await runSF(candidates, asofMs);
      scanState.asof = asofMs; scanState.status = result.winner ? "winner_found" : "sf_completed_no_winner";
      return mcpText({ ok: true, ...result });
    } catch (e) { scanState.last_error = e.message; scanState.status = "error"; return mcpText({ ok: false, error: e.message }); }
  });
  server.tool("get_locked_params", "Return locked scanner thresholds", {}, async () => mcpText({ ok: true, version: APP_VERSION, locked: LOCKED, non_blocking: NON_BLOCKING, notes: ["S0 J-1/J-2 absolute move <= 5%", "Historical Vol5/Vol60/RVOL15 baselines use a 78-slot ET grid; missing/malformed bars contribute volume 0", "S1 RVOL15 >= 1.30", "S1 Vol5/reference >= 0.20", "S1 price vs VWAP >= -1%", "DV = price * 15m rolling volume, descending", "SF RVOL15 >= 1.50", "SF Vol5/reference >= 0.20 with no partial-bar multiplier", "SF Delta60 <= +10%", "SF price vs VWAP >= -1%", "SF Delta 15:55 to scan-time >= 0% within 15:55-16:00 ET", "Lots of 20; Winner Gate requires an actually received fresh Yahoo WS tick", "Accel5M is informational only"] }));
  server.tool("get_scan_state", "Return current scan state and live data coverage", {}, async () => mcpText({ ok: true, scanState, websocket: yahooWS?.status?.() || { connected: false } }));
  server.tool("yahoo_self_test", "Run deterministic internal regression checks; does not call Yahoo", {}, async () => mcpText(runDeterministicSelfTests()));
  return server;
}

async function readJsonBody(req, maxBytes = 2 * 1024 * 1024) {
  const chunks = [];
  let total = 0;
  for await (const chunk of req) {
    total += chunk.length;
    if (total > maxBytes) {
      req.resume();
      const error = new Error("REQUEST_BODY_TOO_LARGE");
      error.statusCode = 413;
      throw error;
    }
    chunks.push(chunk);
  }
  if (!chunks.length) return undefined;
  const raw = Buffer.concat(chunks).toString("utf8");
  try { return JSON.parse(raw); }
  catch {
    const error = new Error("INVALID_JSON_BODY");
    error.statusCode = 400;
    throw error;
  }
}
function sendJson(res, statusCode, value) {
  if (res.headersSent || res.writableEnded) return;
  const body = JSON.stringify(value);
  res.statusCode = statusCode;
  res.setHeader("content-type", "application/json; charset=utf-8");
  res.setHeader("cache-control", "no-store");
  res.setHeader("content-length", Buffer.byteLength(body));
  res.end(body);
}
async function handleHttpRequest(req, res) {
  const host = req.headers.host || "localhost";
  const url = new URL(req.url || "/", `http://${host}`);
  const method = String(req.method || "GET").toUpperCase();
  if (method === "GET" && url.pathname === "/health") {
    let universeCount = null, universeError = null;
    try { universeCount = readUniverse().length; } catch (e) { universeError = e.message; }
    sendJson(res, universeError ? 503 : 200, { ok: !universeError, service: "yahoo-scan-mcp", version: APP_VERSION, timestamp: new Date().toISOString(), universe_count: universeCount, universe_error: universeError, status: scanState.status });
    return;
  }
  if (method === "GET" && url.pathname === "/") {
    sendJson(res, 200, { ok: true, service: "yahoo-scan-mcp", version: APP_VERSION, endpoints: { health: "/health", mcp: "/mcp" } });
    return;
  }
  if (url.pathname === "/mcp" && method === "GET") {
    sendJson(res, 405, { ok: false, error: "MCP endpoint requires POST" });
    return;
  }
  if (url.pathname === "/mcp" && method === "DELETE") {
    sendJson(res, 405, { ok: false, error: "Stateless MCP transport" });
    return;
  }
  if (url.pathname === "/mcp" && method === "POST") {
    let server, transport;
    try {
      const body = await readJsonBody(req);
      server = createMcpServer();
      transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined });
      res.on("close", () => {
        transport?.close?.().catch(() => {});
        server?.close?.().catch(() => {});
      });
      await server.connect(transport);
      await transport.handleRequest(req, res, body);
    } catch (error) {
      console.error("[MCP ERROR]", error);
      const status = Number(error?.statusCode) || 500;
      sendJson(res, status, { ok: false, error: error?.message || String(error) });
      try { transport?.close?.().catch(() => {}); } catch {}
      try { server?.close?.().catch(() => {}); } catch {}
    }
    return;
  }
  sendJson(res, 404, { ok: false, error: "NOT_FOUND" });
}

const httpServer = createServer((req, res) => {
  handleHttpRequest(req, res).catch(error => {
    console.error("[HTTP ERROR]", error);
    sendJson(res, 500, { ok: false, error: error?.message || String(error) });
  });
});
httpServer.listen(PORT, "0.0.0.0", () => {
  console.log(`[yahoo-scan-mcp] started v${APP_VERSION} on port ${PORT}`);
  console.log(`[yahoo-scan-mcp] universe file: ${UNIVERSE_FILE}`);
  try { console.log(`[yahoo-scan-mcp] universe symbols: ${readUniverse().length}`); }
  catch (e) { console.error(`[yahoo-scan-mcp] universe problem: ${e.message}`); }
});
function shutdown(signal) {
  console.log(`[yahoo-scan-mcp] shutting down on ${signal}`);
  try { yahooWS?.close?.(); } catch {}
  httpServer.close(() => process.exit(0));
  setTimeout(() => process.exit(1), 5000).unref();
}
process.once("SIGTERM", () => shutdown("SIGTERM"));
process.once("SIGINT", () => shutdown("SIGINT"));

