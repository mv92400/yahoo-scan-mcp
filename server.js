#!/usr/bin/env node

import http from "node:http";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { z } from "zod";
import { YahooWS } from "./src/yahoo-ws.js";

const PORT = Number(process.env.PORT || 8080);

function log(...args) {
  console.error("[yahoo-scan-mcp]", ...args);
}

function json(res, status, body) {
  const data = JSON.stringify(body);
  res.writeHead(status, {
    "Content-Type": "application/json; charset=utf-8",
    "Content-Length": Buffer.byteLength(data),
    "Cache-Control": "no-store"
  });
  res.end(data);
}

const yahoo = new YahooWS({ log });

const scanState = {
  asof: null,
  symbols_requested: 0,
  s0: [],
  s1: [],
  lots: [],
  errors: [],
  elapsed_ms: 0
};

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function chunk(list, size) {
  const out = [];
  for (let i = 0; i < list.length; i += size) {
    out.push(list.slice(i, i + size));
  }
  return out;
}

function nyParts(unixSeconds) {
  const d = new Date(Number(unixSeconds) * 1000);

  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false
  }).formatToParts(d);

  const m = Object.fromEntries(parts.map(p => [p.type, p.value]));

  return {
    date: `${m.year}-${m.month}-${m.day}`,
    hour: Number(m.hour),
    minute: Number(m.minute),
    second: Number(m.second),
    hm: `${m.hour}:${m.minute}`
  };
}

function cleanNumber(v) {
  return Number.isFinite(Number(v)) ? Number(v) : null;
}

function isNasdaqExchange(exchange) {
  return new Set([
    "NMS",
    "NGM",
    "NCM",
    "NAS"
  ]).has(String(exchange || "").toUpperCase());
}

/*
============================================================
YAHOO SPARK
============================================================
*/

async function yahooSpark(symbols, range = "10d", interval = "5m") {
  const results = [];
  const batches = chunk(symbols, 100);

  for (const batch of batches) {
    const url =
      "https://query1.finance.yahoo.com/v7/finance/spark" +
      `?symbols=${encodeURIComponent(batch.join(","))}` +
      `&range=${encodeURIComponent(range)}` +
      `&interval=${encodeURIComponent(interval)}` +
      "&indicators=quote,close";

    let success = false;
    let lastError = null;

    for (let attempt = 1; attempt <= 3 && !success; attempt++) {
      try {
        const response = await fetch(url, {
          headers: {
            "User-Agent": "Mozilla/5.0"
          },
          signal: AbortSignal.timeout(20000)
        });

        if (!response.ok) {
          throw new Error(`Yahoo HTTP ${response.status}`);
        }

        const body = await response.json();
        const rows = body?.spark?.result || [];

        results.push(...rows);
        success = true;
      } catch (err) {
        lastError = err;

        if (attempt < 3) {
          await sleep(500 * attempt);
        }
      }
    }

    if (!success) {
      results.push({
        _error: true,
        _batch: batch,
        _message: lastError?.message || String(lastError)
      });
    }

    await sleep(100);
  }

  return results;
}

/*
============================================================
S1 METRICS
============================================================
*/

function calculateS1(result) {
  const meta = result?.meta || {};
  const timestamps = result?.timestamp || [];
  const quote = result?.indicators?.quote?.[0] || {};

  if (!timestamps.length) {
    return null;
  }

  const rows = [];

  for (let i = 0; i < timestamps.length; i++) {
    const ts = timestamps[i];
    const p = nyParts(ts);

    const close = cleanNumber(quote.close?.[i]);

    if (close == null) {
      continue;
    }

    rows.push({
      ts,
      date: p.date,
      hour: p.hour,
      minute: p.minute,
      open: cleanNumber(quote.open?.[i]),
      high: cleanNumber(quote.high?.[i]),
      low: cleanNumber(quote.low?.[i]),
      close,
      volume: cleanNumber(quote.volume?.[i]) || 0
    });
  }

  if (!rows.length) {
    return null;
  }

  const now = Date.now();
  const today = nyParts(now / 1000).date;

  const todayRows = rows
    .filter(r =>
      r.date === today &&
      r.hour >= 9 &&
      r.hour <= 16
    )
    .sort((a, b) => a.ts - b.ts);

  if (!todayRows.length) {
    return null;
  }

  /*
  ----------------------------------------------------------
  COMPLETED 5-MINUTE BARS
  ----------------------------------------------------------
  */

  const latest = todayRows[todayRows.length - 1];

  const completed = todayRows.filter(r =>
    r.ts < latest.ts
  );

  if (!completed.length) {
    return null;
  }

  /*
  ----------------------------------------------------------
  VOL15M
  ----------------------------------------------------------
  */

  const trailing15 = completed.slice(-3);

  const vol15m = trailing15.reduce(
    (sum, r) => sum + (r.volume || 0),
    0
  );

  /*
  ----------------------------------------------------------
  VOL5M
  ----------------------------------------------------------
  */

  const last5 = completed.at(-1);

  const vol5m = last5?.volume || 0;

  /*
  ----------------------------------------------------------
  ACCEL5M
  ----------------------------------------------------------
  */

  const previous5 = completed.slice(-7, -1);

  const previous5Average =
    previous5.length > 0
      ? previous5.reduce(
          (sum, r) => sum + r.volume,
          0
        ) / previous5.length
      : null;

  const accel5m =
    previous5Average &&
    previous5Average > 0
      ? vol5m / previous5Average
      : null;

  /*
  ----------------------------------------------------------
  VWAP + HOD
  ----------------------------------------------------------
  */

  let priceVolume = 0;
  let totalVolume = 0;
  let hod = -Infinity;

  for (const r of completed) {
    const typical =
      Number.isFinite(r.high) &&
      Number.isFinite(r.low)
        ? (r.high + r.low + r.close) / 3
        : r.close;

    const volume = r.volume || 0;

    priceVolume += typical * volume;
    totalVolume += volume;

    if (Number.isFinite(r.high)) {
      hod = Math.max(hod, r.high);
    }
  }

  const vwap =
    totalVolume > 0
      ? priceVolume / totalVolume
      : null;

  const price = latest.close;

  const vwapPct =
    vwap &&
    vwap > 0
      ? price / vwap - 1
      : null;

  const hodDistance =
    Number.isFinite(hod) &&
    hod > 0
      ? price / hod - 1
      : null;

  /*
  ----------------------------------------------------------
  PREVIOUS SESSIONS
  ----------------------------------------------------------
  */

  const byDate = new Map();

  for (const r of rows) {
    if (!byDate.has(r.date)) {
      byDate.set(r.date, []);
    }

    byDate.get(r.date).push(r);
  }

  const dates = [...byDate.keys()].sort((a, b) =>
    a < b ? 1 : a > b ? -1 : 0
  );

  const previousDate =
    dates.find(d => d < today) || null;

  const previous2Date =
    dates.find(d => d < (previousDate || today)) || null;

  const previousClose =
    previousDate &&
    byDate.get(previousDate)?.length
      ? byDate.get(previousDate).at(-1).close
      : cleanNumber(meta.previousClose);

  const previous2Close =
    previous2Date &&
    byDate.get(previous2Date)?.length
      ? byDate.get(previous2Date).at(-1).close
      : null;

  const j1Pct =
    previousClose &&
    previousClose > 0
      ? price / previousClose - 1
      : null;

  const j2Pct =
    previous2Close &&
    previous2Close > 0
      ? previousClose / previous2Close - 1
      : null;

  /*
  ----------------------------------------------------------
  RVOL15M
  ----------------------------------------------------------
  */

  const targetMinutes = trailing15.map(
    r => r.hour * 60 + r.minute
  );

  const historical15mVolumes = [];

  for (const date of dates) {
    if (date >= today) {
      continue;
    }

    const dayRows = byDate.get(date) || [];

    const matching = dayRows.filter(r =>
      targetMinutes.includes(r.hour * 60 + r.minute)
    );

    if (matching.length >= 2) {
      const volume = matching
        .slice(-3)
        .reduce(
          (sum, r) => sum + r.volume,
          0
        );

      historical15mVolumes.push(volume);
    }
  }

  const baseline15m =
    historical15mVolumes.length
      ? historical15mVolumes.reduce(
          (sum, v) => sum + v,
          0
        ) / historical15mVolumes.length
      : null;

  const rvol15m =
    baseline15m &&
    baseline15m > 0
      ? vol15m / baseline15m
      : null;

  /*
  ----------------------------------------------------------
  LAST 5M SHARE
  ----------------------------------------------------------
  */

  const last5Share =
    vol15m > 0
      ? vol5m / vol15m
      : null;

  /*
  ----------------------------------------------------------
  SIMPLE RANK SCORE
  ----------------------------------------------------------
  */

  let score = 0;

  if (rvol15m != null) {
    score += Math.max(0, rvol15m - 1) * 3;
  }

  if (accel5m != null) {
    score += Math.max(0, accel5m - 1) * 2;
  }

  if (vwapPct != null) {
    score += Math.max(-0.01, vwapPct) * 50;
  }

  if (hodDistance != null) {
    score += Math.max(-0.20, hodDistance) * -1;
  }

  return {
    symbol: meta.symbol || null,
    exchange: meta.exchange || null,
    quoteType: meta.quoteType || null,

    price,

    day_volume:
      cleanNumber(meta.regularMarketVolume) ||
      todayRows.reduce(
        (sum, r) => sum + r.volume,
        0
      ),

    change_pct:
      cleanNumber(meta.regularMarketChangePercent) ??
      j1Pct,

    vol15m,
    baseline15m,
    rvol15m,

    vol5m,
    accel5m,

    vwap,
    vwap_pct: vwapPct,

    hod,
    hod_distance: hodDistance,

    j1_pct: j1Pct,
    j2_pct: j2Pct,

    last5_share_15m: last5Share,

    bars_today: todayRows.length,

    data_timestamp:
      new Date(latest.ts * 1000).toISOString(),

    score
  };
}

/*
============================================================
LOT FORMATION
============================================================
*/

function createLots(items, size = 20) {
  const lots = [];

  for (let i = 0; i < items.length; i += size) {
    lots.push({
      lot: lots.length + 1,
      symbols: items
        .slice(i, i + size)
        .map(x => x.symbol)
    });
  }

  return lots;
}

/*
============================================================
MCP SERVER
============================================================
*/

function buildServer() {
  const server = new McpServer({
    name: "yahoo-scan-mcp",
    version: "1.1.0"
  });

  /*
  ----------------------------------------------------------
  PING
  ----------------------------------------------------------
  */

  server.tool(
    "ping",
    "Health check.",
    {},
    async () => ({
      content: [{
        type: "text",
        text: JSON.stringify({
          ok: true,
          service: "yahoo-scan-mcp",
          version: "1.1.0",
          timestamp: new Date().toISOString()
        })
      }]
    })
  );

  /*
  ----------------------------------------------------------
  STATUS
  ----------------------------------------------------------
  */

  server.tool(
    "get_status",
    "Returns scanner and Yahoo WebSocket status.",
    {},
    async () => ({
      content: [{
        type: "text",
        text: JSON.stringify({
          ok: true,
          service: "yahoo-scan-mcp",
          stage: "live_scan",

          yahoo_websocket: yahoo.status(),

          scan_state: {
            asof: scanState.asof,
            symbols_requested:
              scanState.symbols_requested,
            s0_count:
              scanState.s0.length,
            s1_count:
              scanState.s1.length,
            lots:
              scanState.lots.length,
            errors:
              scanState.errors.length,
            elapsed_ms:
              scanState.elapsed_ms
          },

          timestamp:
            new Date().toISOString()
        }, null, 2)
      }]
    })
  );

  /*
  ----------------------------------------------------------
  YAHOO WEBSOCKET TEST
  ----------------------------------------------------------
  */

  server.tool(
    "yahoo_ws_test",
    "Tests Yahoo live WebSocket data.",
    {
      symbols:
        z.array(z.string()).optional(),

      seconds:
        z.number()
          .int()
          .min(5)
          .max(120)
          .optional()
    },

    async ({ symbols, seconds }) => {
      const list =
        symbols?.length
          ? symbols
          : [
              "AAPL",
              "MSFT",
              "NVDA",
              "AMD",
              "INTC"
            ];

      const duration =
        seconds ?? 15;

      const started = Date.now();

      try {
        await yahoo.connect();

        const subscribed =
          yahoo.subscribe(list);

        await new Promise(resolve =>
          setTimeout(
            resolve,
            duration * 1000
          )
        );

        return {
          content: [{
            type: "text",
            text: JSON.stringify({
              ok: true,
              requested_symbols: list,
              requested_count: list.length,
              subscribed_count: subscribed,
              duration_seconds: duration,
              elapsed_ms:
                Date.now() - started,
              yahoo:
                yahoo.status()
            }, null, 2)
          }]
        };
      } catch (err) {
        return {
          content: [{
            type: "text",
            text: JSON.stringify({
              ok: false,
              error:
                err?.message ||
                String(err),
              yahoo:
                yahoo.status()
            }, null, 2)
          }]
        };
      }
    }
  );

  /*
  ==========================================================
  S0 + S1
  ==========================================================
  */

  server.tool(
    "yahoo_s0_s1_scan",
    "Builds S0 and S1 from Yahoo 5-minute intraday data.",
    {
      symbols:
        z.array(z.string())
          .min(1)
          .max(2500),

      s1_limit:
        z.number()
          .int()
          .min(10)
          .max(200)
          .optional()
    },

    async ({ symbols, s1_limit }) => {
      const started = Date.now();

      const list = [
        ...new Set(
          symbols
            .map(s =>
              String(s)
                .trim()
                .toUpperCase()
            )
            .filter(Boolean)
        )
      ];

      scanState.symbols_requested =
        list.length;

      scanState.errors = [];

      /*
      --------------------------------------------------------
      FETCH
      --------------------------------------------------------
      */

      const intraday =
        await yahooSpark(
          list,
          "10d",
          "5m"
        );

      /*
      --------------------------------------------------------
      S0
      --------------------------------------------------------
      */

      const valid = [];

      for (const row of intraday) {
        if (row?._error) {
          scanState.errors.push({
            type: "spark_batch",
            message: row._message,
            symbols: row._batch
          });

          continue;
        }

        const meta =
          row?.meta;

        const price =
          cleanNumber(
            meta?.regularMarketPrice
          );

        if (!meta || price == null) {
          continue;
        }

        const quoteType =
          String(
            meta.quoteType || ""
          ).toUpperCase();

        const exchange =
          String(
            meta.exchange || ""
          ).toUpperCase();

        const isEquity =
          quoteType === "EQUITY";

        const isNasdaq =
          isNasdaqExchange(exchange);

        const under5 =
          price > 0 &&
          price < 5;

        /*
        S0:
        ordinary equity
        NASDAQ
        price < $5
        */

        if (
          isEquity &&
          isNasdaq &&
          under5
        ) {
          valid.push(row);
        }
      }

      const s0 =
        valid
          .map(row => {
            const meta =
              row.meta || {};

            return {
              symbol:
                meta.symbol,

              price:
                cleanNumber(
                  meta.regularMarketPrice
                ),

              exchange:
                meta.exchange || null,

              quoteType:
                meta.quoteType || null,

              name:
                meta.longName ||
                meta.shortName ||
                null
            };
          })
          .sort(
            (a, b) =>
              a.symbol.localeCompare(
                b.symbol
              )
          );

      /*
      --------------------------------------------------------
      S1
      --------------------------------------------------------
      */

      const s1raw = [];

      for (const row of valid) {
        try {
          const metric =
            calculateS1(row);

          if (metric) {
            s1raw.push(metric);
          }
        } catch (err) {
          scanState.errors.push({
            symbol:
              row?.meta?.symbol ||
              null,

            type: "metric",

            message:
              err?.message ||
              String(err)
          });
        }
      }

      /*
      --------------------------------------------------------
      PRIMARY RANKING
      --------------------------------------------------------
      RVOL15M
      then Accel5M
      then VWAP
      --------------------------------------------------------
      */

      const s1 =
        s1raw
          .filter(x =>
            x.price != null &&
            x.vol15m > 0 &&
            x.rvol15m != null
          )
          .sort((a, b) => {

            const rvolA =
              a.rvol15m ??
              -Infinity;

            const rvolB =
              b.rvol15m ??
              -Infinity;

            if (
              rvolB !== rvolA
            ) {
              return (
                rvolB -
                rvolA
              );
            }

            const accelA =
              a.accel5m ??
              -Infinity;

            const accelB =
              b.accel5m ??
              -Infinity;

            if (
              accelB !== accelA
            ) {
              return (
                accelB -
                accelA
              );
            }

            return (
              (b.vwap_pct ??
                -Infinity) -
              (a.vwap_pct ??
                -Infinity)
            );
          });

      const limit =
        s1_limit ?? 60;

      const finalS1 =
        s1.slice(
          0,
          limit
        );

      /*
      --------------------------------------------------------
      LOTS OF 20
      --------------------------------------------------------
      */

      const lots =
        createLots(
          finalS1,
          20
        );

      /*
      --------------------------------------------------------
      SAVE STATE
      --------------------------------------------------------
      */

      scanState.asof =
        new Date().toISOString();

      scanState.s0 =
        s0;

      scanState.s1 =
        finalS1;

      scanState.lots =
        lots;

      scanState.elapsed_ms =
        Date.now() -
        started;

      /*
      --------------------------------------------------------
      RESULT
      --------------------------------------------------------
      */

      return {
        content: [{
          type: "text",
          text: JSON.stringify({
            ok: true,

            stage: "S0_S1",

            asof:
              scanState.asof,

            elapsed_ms:
              scanState.elapsed_ms,

            universe_requested:
              list.length,

            s0_count:
              s0.length,

            s0,

            s1_count:
              finalS1.length,

            s1:
              finalS1,

            lots_count:
              lots.length,

            lots,

            errors_count:
              scanState.errors.length,

            errors:
              scanState.errors,

            methodology: {
              S0:
                "NASDAQ EQUITY, live price < $5",

              S1:
                "5-minute intraday data",

              Vol15M:
                "last 3 completed 5m bars",

              RVOL15M:
                "15m volume versus historical same-time 15m baseline",

              Accel5M:
                "last completed 5m volume versus preceding 5m average",

              VWAP:
                "session VWAP",

              J1:
                "current price versus previous close",

              J2:
                "previous close versus previous-2 close",

              ranking:
                "RVOL15M > Accel5M > VWAP",

              lots:
                "20 symbols per lot"
            }
          }, null, 2)
        }]
      };
    }
  );

  /*
  ==========================================================
  GET LAST SCAN
  ==========================================================
  */

  server.tool(
    "get_scan_state",
    "Returns the latest S0/S1 state.",
    {
      s1_limit:
        z.number()
          .int()
          .min(1)
          .max(200)
          .optional()
    },

    async ({ s1_limit }) => {
      const limit =
        s1_limit ?? 60;

      return {
        content: [{
          type: "text",
          text: JSON.stringify({
            ok: true,

            asof:
              scanState.asof,

            symbols_requested:
              scanState.symbols_requested,

            s0_count:
              scanState.s0.length,

            s0:
              scanState.s0,

            s1_count:
              Math.min(
                limit,
                scanState.s1.length
              ),

            s1:
              scanState.s1.slice(
                0,
                limit
              ),

            lots_count:
              scanState.lots.length,

            lots:
              scanState.lots,

            errors_count:
              scanState.errors.length,

            errors:
              scanState.errors,

            elapsed_ms:
              scanState.elapsed_ms
          }, null, 2)
        }]
      };
    }
  );

  return server;
}

/*
============================================================
HTTP
============================================================
*/

async function readBody(req) {
  const chunks = [];
  let size = 0;

  for await (const chunk of req) {
    chunks.push(chunk);

    size += chunk.length;

    if (size > 4 * 1024 * 1024) {
      throw new Error(
        "Request body too large"
      );
    }
  }

  if (!chunks.length) {
    return undefined;
  }

  return JSON.parse(
    Buffer.concat(chunks)
      .toString("utf8")
  );
}

const httpServer =
  http.createServer(
    async (req, res) => {

      try {

        /*
        ------------------------------------------------------
        HEALTH
        ------------------------------------------------------
        */

        if (
          req.method === "GET" &&
          req.url === "/health"
        ) {
          return json(
            res,
            200,
            {
              ok: true,
              service:
                "yahoo-scan-mcp",
              timestamp:
                new Date().toISOString()
            }
          );
        }

        /*
        ------------------------------------------------------
        MCP
        ------------------------------------------------------
        */

        if (
          req.url === "/mcp"
        ) {

          if (
            ![
              "POST",
              "GET",
              "DELETE"
            ].includes(req.method)
          ) {
            res.writeHead(
              405,
              {
                Allow:
                  "GET, POST, DELETE"
              }
            );

            return res.end();
          }

          const mcpServer =
            buildServer();

          const transport =
            new StreamableHTTPServerTransport({
              sessionIdGenerator:
                undefined
            });

          res.on(
            "close",
            () => {
              transport
                .close()
                .catch(() => {});

              mcpServer
                .close()
                .catch(() => {});
            }
          );

          await mcpServer.connect(
            transport
          );

          let body;

          if (
            req.method === "POST"
          ) {
            body =
              await readBody(req);
          }

          await transport.handleRequest(
            req,
            res,
            body
          );

          return;
        }

        /*
        ------------------------------------------------------
        ROOT
        ------------------------------------------------------
        */

        if (
          req.method === "GET" &&
          req.url === "/"
        ) {
          return json(
            res,
            200,
            {
              ok: true,
              service:
                "yahoo-scan-mcp",
              version:
                "1.1.0",
              authentication:
                "none",
              endpoints: {
                health:
                  "/health",
                mcp:
                  "/mcp"
              }
            }
          );
        }

        return json(
          res,
          404,
          {
            error:
              "Not found"
          }
        );

      } catch (err) {

        log(
          "HTTP error:",
          err
        );

        if (
          !res.headersSent
        ) {
          return json(
            res,
            500,
            {
              error:
                err?.message ||
                String(err)
            }
          );
        }

        res.end();
      }
    }
  );

httpServer.on(
  "error",
  err => {
    log(
      "HTTP server error:",
      err
    );

    process.exit(1);
  }
);

httpServer.listen(
  PORT,
  "0.0.0.0",
  () => {
    log(
      `Listening on 0.0.0.0:${PORT}`
    );

    log(
      "Health: /health"
    );

    log(
      "MCP: /mcp"
    );
  }
);
