#!/usr/bin/env node
// yahoo-scan-mcp: Small Cap EOD Scanner exposed as MCP tools (stdio).
import fs from "node:fs";
import path from "node:path";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import { LiveCache, etToday, etDateString } from "./src/cache.js";
import { YahooWS } from "./src/ws.js";
import { Scanner, LOCKED } from "./src/engine.js";

const log = m => process.stderr.write(`[yahoo-scan] ${m}\n`);
const LOG_DIR = path.resolve("logs");

const cache = new LiveCache();
const yws = new YahooWS(cache, { log });
let universe = new Map(); // symbol -> {type, exchange}
let refs = new Map();     // symbol -> {vol15, vol5, vol60}
let scanner = new Scanner(cache, universe, refs);
const timers = [];
let lastDay = { s1: null, sf: null };

const text = obj => ({ content: [{ type: "text", text: JSON.stringify(obj, null, 2) }] });
const sleep = ms => new Promise(r => setTimeout(r, ms));

function loadUniverseFile(p) {
  const raw = fs.readFileSync(p, "utf8").trim();
  const m = new Map();
  if (p.endsWith(".json")) {
    const j = JSON.parse(raw);
    for (const x of Array.isArray(j) ? j : Object.keys(j).map(k => ({ symbol: k, ...j[k] })))
      m.set(typeof x === "string" ? x : x.symbol, { type: x.type || "CS", exchange: x.exchange || "NASDAQ" });
  } else {
    const lines = raw.split(/\r?\n/);
    const sep = lines[0].includes(";") ? ";" : ",";
    const head = lines[0].toLowerCase().split(sep).map(s => s.trim());
    const hasHeader = head.some(h => ["symbol", "ticker"].includes(h));
    const idx = hasHeader ? Math.max(head.indexOf("symbol"), head.indexOf("ticker")) : 0;
    for (const line of lines.slice(hasHeader ? 1 : 0)) {
      const sym = line.split(sep)[idx]?.trim().replace(/"/g, "");
      if (sym) m.set(sym, { type: "CS", exchange: "NASDAQ" });
    }
  }
  return m;
}

function saveLog(name, data) {
  fs.mkdirSync(LOG_DIR, { recursive: true });
  const f = path.join(LOG_DIR, `${etDateString()}_${name}.json`);
  fs.writeFileSync(f, JSON.stringify(data, null, 2));
  return f;
}

const server = new McpServer({ name: "yahoo-scan-mcp", version: "0.1.0" });

server.tool(
  "load_universe",
  "Load the S0 universe (NASDAQ common stocks, price < $5 already applied) and optional reference volumes (JSON {SYM:{vol15,vol5,vol60}}).",
  { universe_path: z.string(), refs_path: z.string().optional() },
  async ({ universe_path, refs_path }) => {
    universe = loadUniverseFile(universe_path);
    refs = new Map();
    if (refs_path) for (const [k, v] of Object.entries(JSON.parse(fs.readFileSync(refs_path, "utf8")))) refs.set(k, v);
    scanner = new Scanner(cache, universe, refs);
    return text({ universe: universe.size, refs: refs.size });
  }
);

server.tool(
  "yahoo_ws_test",
  "Connect to the Yahoo WebSocket, subscribe to N symbols (from the universe, or a default list) and measure coverage over `seconds`.",
  { n: z.number().int().min(1).max(5000).default(5), seconds: z.number().min(2).max(300).default(20), symbols: z.array(z.string()).optional() },
  async ({ n, seconds, symbols }) => {
    const list = symbols?.length ? symbols : universe.size ? [...universe.keys()].slice(0, n) : ["AAPL", "MSFT", "NVDA", "AMD", "INTC"].slice(0, n);
    const t0 = Date.now();
    try { if (!yws.status().open) await yws.connect(); }
    catch (e) { return text({ ok: false, error: `connect failed: ${e.message}` }); }
    const tConnect = Date.now() - t0;
    const t1 = Date.now();
    await yws.subscribe(list);
    const tSub = Date.now() - t1;
    cache.resetCoverage();
    const before = cache.ticksReceived;
    let firstTick = null;
    const end = Date.now() + seconds * 1000;
    while (Date.now() < end) {
      if (firstTick == null && cache.ticksReceived > before) firstTick = Date.now() - t1;
      await sleep(250);
    }
    const covered = list.filter(s => cache.get(s)?.last).length;
    return text({
      ok: true, requested: list.length, covered, coverage_pct: +(100 * covered / list.length).toFixed(1),
      connect_ms: tConnect, subscribe_ms: tSub, first_tick_ms: firstTick,
      ticks_during_test: cache.ticksReceived - before,
      ticks_per_sec: +((cache.ticksReceived - before) / seconds).toFixed(1),
      ws: yws.status(), cache: cache.stats(),
      note: "Coverage depends on trading activity: off-hours or illiquid tickers produce no ticks.",
    });
  }
);

server.tool("ws_status", "WebSocket, cache and universe diagnostics.", {}, async () =>
  text({ ws: yws.status(), cache: cache.stats(), universe: universe.size, refs: refs.size, last_day: lastDay })
);

server.tool(
  "start_stream",
  "Connect and subscribe to the whole S0 universe (run well before 15:55 ET).",
  {},
  async () => {
    if (!universe.size) return text({ ok: false, error: "load_universe first" });
    if (!yws.status().open) await yws.connect();
    const t0 = Date.now();
    await yws.subscribe([...universe.keys()]);
    return text({ ok: true, subscribed: yws.subscribed.size, subscribe_ms: Date.now() - t0 });
  }
);

server.tool(
  "run_s1",
  "Run S1 now (or as of an ISO timestamp): metrics, shortlist, DV ranking, lots of 20.",
  { as_of: z.string().optional(), top: z.number().int().default(20) },
  async ({ as_of, top }) => {
    const r = scanner.runS1(as_of ? Date.parse(as_of) : Date.now());
    lastDay.s1 = saveLog("s1", r);
    return text({ ...r, shortlist: r.shortlist.slice(0, top) });
  }
);

server.tool(
  "run_sf",
  "Run SF over the pre-formed lots in DV order; stops at the first Winner Gate PASS.",
  { as_of: z.string().optional() },
  async ({ as_of }) => {
    const r = scanner.runSF(as_of ? Date.parse(as_of) : Date.now());
    lastDay.sf = saveLog("sf", r);
    return text(r);
  }
);

server.tool(
  "arm_day",
  "Schedule S1 at 15:55:00 ET and SF at 15:59:00 ET today. Stream must be running (start_stream).",
  {},
  async () => {
    timers.splice(0).forEach(clearTimeout);
    const now = Date.now();
    const t1 = etToday(15, 55), t2 = etToday(15, 59);
    const out = { s1_at: new Date(t1).toISOString(), sf_at: new Date(t2).toISOString() };
    if (t1 > now) timers.push(setTimeout(() => { lastDay.s1 = saveLog("s1", scanner.runS1(t1)); log("S1 done"); }, t1 - now));
    else out.s1_skipped = "15:55 ET already passed";
    if (t2 > now) timers.push(setTimeout(() => { lastDay.sf = saveLog("sf", scanner.runSF(t2)); log("SF done"); }, t2 - now));
    else out.sf_skipped = "15:59 ET already passed";
    return text(out);
  }
);

server.tool(
  "get_locked_params",
  "Return the locked SF thresholds in use.",
  {},
  async () => text(LOCKED)
);

await server.connect(new StdioServerTransport());
log("ready");
      
