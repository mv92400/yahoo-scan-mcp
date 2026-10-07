import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { z } from "zod";
import http from "node:http";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";

import { YahooWS } from "./src/yahoo-ws.js";

/*
========================================================
CONFIG
========================================================
*/

const PORT = Number(process.env.PORT || 8080);
const APP_VERSION = "1.2.1";

const UNIVERSE_FILE =
  new URL("./universe_s0.txt", import.meta.url);

const log = (...args) =>
  console.error("[yahoo-scan-mcp]", ...args);


/*
========================================================
HELPERS
========================================================
*/

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function chunk(array, size) {
  const result = [];

  for (let i = 0; i < array.length; i += size) {
    result.push(array.slice(i, i + size));
  }

  return result;
}

function cleanNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function normalizeSymbols(input) {
  if (!Array.isArray(input)) return [];

  return [
    ...new Set(
      input
        .map(x =>
          String(x || "")
            .trim()
            .toUpperCase()
        )
        .filter(Boolean)
    )
  ];
}


/*
========================================================
UNIVERSE
========================================================
*/

let universeCache = null;
let universeLoadedAt = null;

async function loadUniverse() {
  if (universeCache) {
    return universeCache;
  }

  const raw =
    await readFile(
      UNIVERSE_FILE,
      "utf8"
    );

  const symbols = [];

  for (const line of raw.split(/\r?\n/)) {
    const clean = line.trim();

    if (!clean) continue;
    if (clean.startsWith("#")) continue;

    const symbol =
      clean
        .split(/[,\s;]+/)[0]
        .trim()
        .toUpperCase();

    if (!symbol) continue;

    symbols.push(symbol);
  }

  universeCache = [
    ...new Set(symbols)
  ];

  universeLoadedAt =
    new Date().toISOString();

  return universeCache;
}


/*
========================================================
SCAN STATE
========================================================
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


/*
========================================================
YAHOO SPARK
========================================================
*/

async function yahooSpark(
  symbols,
  range = "5d",
  interval = "5m"
) {
  const batches =
    chunk(symbols, 20);

  const all = [];

  for (const batch of batches) {

    const querySymbols =
      batch
        .map(s => encodeURIComponent(s))
        .join(",");

    const url =
      "https://query1.finance.yahoo.com/v7/finance/spark" +
      `?symbols=${querySymbols}` +
      `&range=${range}` +
      `&interval=${interval}` +
      "&indicators=quote,close" +
      "&includeTimestamps=true" +
      "&includePrePost=false";

    let success = false;
    let lastError = null;

    for (
      let attempt = 1;
      attempt <= 3;
      attempt++
    ) {

      try {

        const controller =
          new AbortController();

        const timer =
          setTimeout(
            () => controller.abort(),
            20000
          );

        const response =
          await fetch(
            url,
            {
              method: "GET",

              headers: {
                "User-Agent":
                  "Mozilla/5.0"
              },

              signal:
                controller.signal
            }
          );

        clearTimeout(timer);

        if (!response.ok) {
          throw new Error(
            `Yahoo HTTP ${response.status}`
          );
        }

        const json =
          await response.json();

        for (const symbol of batch) {

          const data =
            json?.spark?.result?.find(
              x =>
                String(
                  x?.symbol || ""
                ).toUpperCase() === symbol
            );

          if (!data) {

            all.push({
              symbol,
              error:
                "Yahoo symbol missing"
            });

            continue;
          }

          all.push({
            symbol,
            data
          });
        }

        success = true;
        break;

      } catch (error) {

        lastError = error;

        if (attempt < 3) {
          await sleep(
            500 * attempt
          );
        }
      }
    }

    if (!success) {

      for (const symbol of batch) {

        all.push({
          symbol,

          error:
            lastError?.message ||
            "Yahoo request failed"
        });
      }
    }
  }

  return all;
}


/*
========================================================
EXTRACT BARS
========================================================
*/

function extractBars(data) {

  const timestamps =
    data?.timestamp || [];

  const quote =
    data?.indicators?.quote?.[0] || {};

  const opens =
    quote.open || [];

  const highs =
    quote.high || [];

  const lows =
    quote.low || [];

  const closes =
    quote.close || [];

  const volumes =
    quote.volume || [];

  const bars = [];

  for (
    let i = 0;
    i < timestamps.length;
    i++
  ) {

    const timestamp =
      cleanNumber(
        timestamps[i]
      );

    const open =
      cleanNumber(
        opens[i]
      );

    const high =
      cleanNumber(
        highs[i]
      );

    const low =
      cleanNumber(
        lows[i]
      );

    const close =
      cleanNumber(
        closes[i]
      );

    const volume =
      cleanNumber(
        volumes[i]
      );

    if (
      timestamp == null ||
      close == null
    ) {
      continue;
    }

    bars.push({
      timestamp,
      open,
      high,
      low,
      close,
      volume: volume || 0
    });
  }

  return bars;
}


/*
========================================================
SESSION KEY
========================================================
*/

function sessionKey(timestamp) {

  const d =
    new Date(
      timestamp * 1000
    );

  return [
    d.getUTCFullYear(),

    String(
      d.getUTCMonth() + 1
    ).padStart(2, "0"),

    String(
      d.getUTCDate()
    ).padStart(2, "0")
  ].join("-");
}


/*
========================================================
VWAP
========================================================
*/

function calculateVWAP(bars) {

  let pv = 0;
  let volume = 0;

  for (const bar of bars) {

    if (
      bar.close == null ||
      !Number.isFinite(
        bar.volume
      )
    ) {
      continue;
    }

    const typical =
      (
        (bar.high ?? bar.close) +
        (bar.low ?? bar.close) +
        bar.close
      ) / 3;

    pv +=
      typical * bar.volume;

    volume +=
      bar.volume;
  }

  if (volume <= 0) {
    return null;
  }

  return pv / volume;
}


/*
========================================================
S1
========================================================
*/

function calculateS1(
  symbol,
  bars
) {

  if (
    !Array.isArray(bars) ||
    bars.length < 4
  ) {
    return null;
  }

  const sorted =
    [...bars].sort(
      (a, b) =>
        a.timestamp -
        b.timestamp
    );

  /*
  Last bar can still be incomplete.
  */

  const completed =
    sorted.slice(0, -1);

  if (completed.length < 4) {
    return null;
  }

  const current =
    completed[
      completed.length - 1
    ];

  const previous =
    completed[
      completed.length - 2
    ];

  const previous2 =
    completed[
      completed.length - 3
    ];

  if (
    !current ||
    !previous ||
    !previous2
  ) {
    return null;
  }


  /*
  ------------------------------------------------------
  VOL15M
  ------------------------------------------------------
  */

  const last3 =
    completed.slice(-3);

  const Vol15M =
    last3.reduce(
      (sum, bar) =>
        sum + (bar.volume || 0),
      0
    );


  /*
  ------------------------------------------------------
  ACCEL5M
  ------------------------------------------------------
  */

  const previous3 =
    completed.slice(
      Math.max(
        0,
        completed.length - 4
      ),
      completed.length - 1
    );

  const previous3Mean =
    previous3.length
      ? previous3.reduce(
          (sum, bar) =>
            sum +
            (bar.volume || 0),
          0
        ) / previous3.length
      : 0;

  const Accel5M =
    previous3Mean > 0
      ? current.volume /
        previous3Mean
      : null;


  /*
  ------------------------------------------------------
  RVOL15M
  ------------------------------------------------------
  */

  const currentSession =
    sessionKey(
      current.timestamp
    );

  const currentIndex =
    completed.findIndex(
      x =>
        x.timestamp ===
        current.timestamp
    );

  const historical = [];

  for (
    let i = 0;
    i < completed.length;
    i++
  ) {

    const bar =
      completed[i];

    if (
      sessionKey(
        bar.timestamp
      ) === currentSession
    ) {
      continue;
    }

    const distance =
      Math.abs(
        i - currentIndex
      );

    if (distance <= 2) {
      historical.push(
        bar.volume || 0
      );
    }
  }

  let baseline = 0;

  if (
    historical.length > 0
  ) {

    baseline =
      historical.reduce(
        (a, b) =>
          a + b,
        0
      ) /
      historical.length;

    baseline *= 3;
  }

  const RVOL15M =
    baseline > 0
      ? Vol15M / baseline
      : null;


  /*
  ------------------------------------------------------
  VWAP
  ------------------------------------------------------
  */

  const VWAP =
    calculateVWAP(
      completed
    );

  const priceVsVWAP =
    VWAP &&
    current.close != null
      ? current.close / VWAP - 1
      : null;


  /*
  ------------------------------------------------------
  J1 / J2
  ------------------------------------------------------
  */

  const J1 =
    previous.close > 0
      ? current.close /
          previous.close -
        1
      : null;

  const J2 =
    previous2.close > 0
      ? previous.close /
          previous2.close -
        1
      : null;


  /*
  ------------------------------------------------------
  HOD
  ------------------------------------------------------
  */

  const highs =
    completed
      .map(x => x.high)
      .filter(
        x =>
          x != null &&
          Number.isFinite(x)
      );

  const HOD =
    highs.length
      ? Math.max(...highs)
      : null;

  const hodDistance =
    HOD > 0
      ? current.close / HOD - 1
      : null;


  return {

    symbol,

    timestamp:
      current.timestamp,

    price:
      current.close,

    Vol15M,

    RVOL15M,

    Accel5M,

    VWAP,

    priceVsVWAP,

    J1,

    J2,

    HOD,

    hodDistance
  };
}


/*
========================================================
ORDINARY STOCK
========================================================
*/

function isOrdinaryStock(data) {

  if (!data) {
    return false;
  }

  const meta =
    data?.meta || {};

  const quoteType =
    String(
      meta.quoteType ||
      meta.instrumentType ||
      ""
    ).toUpperCase();

  if (
    quoteType &&
    quoteType !== "EQUITY"
  ) {
    return false;
  }

  const name =
    String(
      meta.longName ||
      meta.shortName ||
      ""
    ).toUpperCase();

  if (
    name.includes(" ETF") ||
    name.includes("ETF ") ||
    name.includes(" FUND") ||
    name.includes("FUND ")
  ) {
    return false;
  }

  const exchange =
    String(
      meta.exchange ||
      ""
    ).toUpperCase();

  if (exchange) {

    const allowed = [
      "NMS",
      "NGM",
      "NCM",
      "NAS",
      "NASDAQ",
      "NYSE",
      "NYQ",
      "ASE",
      "AMEX",
      "BATS"
    ];

    if (
      !allowed.includes(
        exchange
      )
    ) {
      return false;
    }
  }

  return true;
}


/*
========================================================
CURRENT PRICE
========================================================
*/

function getCurrentPrice(data) {

  const meta =
    data?.meta || {};

  return (
    cleanNumber(
      meta.regularMarketPrice
    ) ??
    cleanNumber(
      meta.postMarketPrice
    ) ??
    null
  );
}


/*
========================================================
WEBSOCKET TEST
========================================================
*/

let wsTestRunning = false;

async function runYahooWSTest(
  symbols,
  seconds
) {

  if (wsTestRunning) {
    throw new Error(
      "Yahoo WebSocket test already running"
    );
  }

  wsTestRunning = true;

  const started =
    Date.now();

  const yahoo =
    new YahooWS({
      log
    });

  try {

    await yahoo.connect();

    await yahoo.subscribe(
      symbols
    );

    await sleep(
      seconds * 1000
    );

    const status =
      typeof yahoo.status ===
      "function"
        ? yahoo.status()
        : null;

    let ticks = {};

    if (
      typeof yahoo.getLatest ===
      "function"
    ) {

      ticks =
        await yahoo.getLatest();

    } else if (
      yahoo.latest
    ) {

      ticks =
        yahoo.latest;

    } else if (
      yahoo.recent_ticks
    ) {

      ticks =
        yahoo.recent_ticks;
    }

    return {

      ok: true,

      elapsed_ms:
        Date.now() -
        started,

      status,

      ticks
    };

  } finally {

    try {

      if (
        typeof yahoo.close ===
        "function"
      ) {
        await yahoo.close();
      }

    } catch {}

    wsTestRunning = false;
  }
}


/*
========================================================
REGISTER TOOLS
========================================================
*/

function registerTools(server) {

  /*
  ------------------------------------------------------
  PING
  ------------------------------------------------------
  */

  server.tool(
    "ping",
    "Test Yahoo Scan MCP connectivity.",
    {},
    async () => ({
      content: [{
        type: "text",

        text:
          JSON.stringify({
            ok: true,

            service:
              "yahoo-scan-mcp",

            version:
              APP_VERSION,

            timestamp:
              new Date().toISOString()
          })
      }]
    })
  );


  /*
  ------------------------------------------------------
  STATUS
  ------------------------------------------------------
  */

  server.tool(
    "get_status",
    "Return MCP and scanner status.",
    {},
    async () => {

      return {
        content: [{
          type: "text",

          text:
            JSON.stringify({
              ok: true,

              service:
                "yahoo-scan-mcp",

              version:
                APP_VERSION,

              yahoo: {
                connected: false,

                mode:
                  "on-demand",

                note:
                  "Yahoo WebSocket is created by yahoo_ws_test"
              },

              scan:
                scanState
            })
        }]
      };
    }
  );


  /*
  ------------------------------------------------------
  UNIVERSE
  ------------------------------------------------------
  */

  server.tool(
    "get_universe",
    "Return the materialized S0 universe.",
    {
      limit:
        z
          .number()
          .int()
          .min(1)
          .max(2500)
          .optional()
    },

    async ({ limit }) => {

      const universe =
        await loadUniverse();

      scanState.universe_loaded_at =
        universeLoadedAt;

      const selected =
        universe.slice(
          0,
          limit ||
          universe.length
        );

      return {
        content: [{
          type: "text",

          text:
            JSON.stringify({
              ok: true,

              count:
                universe.length,

              returned:
                selected.length,

              universe:
                selected
            })
        }]
      };
    }
  );


  /*
  ------------------------------------------------------
  YAHOO WS TEST
  ------------------------------------------------------
  */

  server.tool(
    "yahoo_ws_test",
    "Test Yahoo WebSocket connectivity.",
    {
      symbols:
        z
          .array(z.string())
          .max(2500)
          .optional(),

      seconds:
        z
          .number()
          .int()
          .min(5)
          .max(120)
          .optional()
    },

    async ({
      symbols,
      seconds
    }) => {

      const requested =
        normalizeSymbols(
          symbols || [
            "AAPL",
            "MSFT",
            "NVDA"
          ]
        );

      const duration =
        seconds || 10;

      const result =
        await runYahooWSTest(
          requested,
          duration
        );

      const filteredTicks =
        {};

      for (
        const symbol of requested
      ) {

        if (
          result.ticks &&
          result.ticks[symbol]
        ) {

          filteredTicks[symbol] =
            result.ticks[symbol];
        }
      }

      return {
        content: [{
          type: "text",

          text:
            JSON.stringify({
              ok: true,

              symbols:
                requested,

              seconds:
                duration,

              elapsed_ms:
                result.elapsed_ms,

              status:
                result.status,

              ticks:
                filteredTicks
            })
        }]
      };
    }
  );


  /*
  ------------------------------------------------------
  S0 / S1 SCAN
  ------------------------------------------------------
  */

  server.tool(
    "yahoo_s0_s1_scan",
    "Run Yahoo S0/S1 scanner.",
    {
      symbols:
        z
          .array(z.string())
          .max(2500)
          .optional(),

      s1_limit:
        z
          .number()
          .int()
          .min(10)
          .max(200)
          .optional()
    },

    async ({
      symbols,
      s1_limit
    }) => {

      const started =
        Date.now();

      scanState.stage =
        "S0_S1";

      scanState.errors = [];
      scanState.s0 = [];
      scanState.s1 = [];
      scanState.lots = [];

      let list;

      if (
        Array.isArray(symbols) &&
        symbols.length
      ) {

        list =
          normalizeSymbols(
            symbols
          );

      } else {

        list =
          await loadUniverse();

        scanState.universe_loaded_at =
          universeLoadedAt;
      }

      scanState.symbols_requested =
        list.length;


      /*
      ----------------------------------------------------
      YAHOO DATA
      ----------------------------------------------------
      */

      const yahooData =
        await yahooSpark(
          list,
          "5d",
          "5m"
        );


      /*
      ----------------------------------------------------
      S0
      ----------------------------------------------------
      */

      const s0 = [];

      for (
        const item of yahooData
      ) {

        if (item.error) {

          scanState.errors.push({
            symbol:
              item.symbol,

            stage:
              "S0",

            error:
              item.error
          });

          continue;
        }

        const data =
          item.data;

        if (
          !isOrdinaryStock(
            data
          )
        ) {
          continue;
        }

        const price =
          getCurrentPrice(
            data
          );

        if (
          price == null ||
          price <= 0 ||
          price >= 5
        ) {
          continue;
        }

        s0.push({
          symbol:
            item.symbol,

          price
        });
      }

      scanState.s0 =
        s0;


      /*
      ----------------------------------------------------
      S1
      ----------------------------------------------------
      */

      const s1 = [];

      for (
        const candidate of s0
      ) {

        const item =
          yahooData.find(
            x =>
              x.symbol ===
              candidate.symbol
          );

        if (!item?.data) {
          continue;
        }

        const bars =
          extractBars(
            item.data
          );

        const metrics =
          calculateS1(
            candidate.symbol,
            bars
          );

        if (!metrics) {
          continue;
        }

        s1.push(
          metrics
        );
      }


      /*
      ----------------------------------------------------
      RANKING
      ----------------------------------------------------
      */

      s1.sort(
        (a, b) => {

          const rvolA =
            a.RVOL15M ??
            -Infinity;

          const rvolB =
            b.RVOL15M ??
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
            a.Accel5M ??
            -Infinity;

          const accelB =
            b.Accel5M ??
            -Infinity;

          if (
            accelB !== accelA
          ) {
            return (
              accelB -
              accelA
            );
          }

          const vwapA =
            a.priceVsVWAP ??
            -Infinity;

          const vwapB =
            b.priceVsVWAP ??
            -Infinity;

          return (
            vwapB -
            vwapA
          );
        }
      );


      const limited =
        s1.slice(
          0,
          s1_limit || 50
        );


      /*
      ----------------------------------------------------
      LOTS OF 20
      ----------------------------------------------------
      */

      const lots =
        chunk(
          limited,
          20
        );

      scanState.s1 =
        limited;

      scanState.lots =
        lots;

      scanState.elapsed_ms =
        Date.now() -
        started;

      scanState.asof =
        new Date().toISOString();

      scanState.ok =
        true;


      /*
      ----------------------------------------------------
      RESULT
      ----------------------------------------------------
      */

      return {
        content: [{
          type: "text",

          text:
            JSON.stringify({

              ok: true,

              stage:
                "S0_S1",

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
                limited.length,

              s1:
                limited,

              lots,

              errors:
                scanState.errors
            })
        }]
      };
    }
  );


  /*
  ------------------------------------------------------
  SCAN STATE
  ------------------------------------------------------
  */

  server.tool(
    "get_scan_state",
    "Return latest scanner state.",
    {},
    async () => ({
      content: [{
        type: "text",

        text:
          JSON.stringify(
            scanState
          )
      }]
    })
  );
}


/*
========================================================
MCP SESSION STORE
========================================================
*/

const sessions =
  new Map();


/*
========================================================
CREATE SERVER
========================================================
*/

function createMcpServer() {

  const server =
    new McpServer({
      name:
        "yahoo-scan-mcp",

      version:
        APP_VERSION
    });

  registerTools(
    server
  );

  return server;
}


/*
========================================================
CREATE TRANSPORT
========================================================
*/

function createTransport() {

  let transport;

  transport =
    new StreamableHTTPServerTransport({

      sessionIdGenerator:
        () => randomUUID(),

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
            `MCP session initialized: ${sessionId}`
          );
        }
    });

  transport.onerror =
    error => {

      log(
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
          `MCP session closed: ${sessionId}`
        );
      }
    };

  return transport;
}


/*
========================================================
MCP HTTP HANDLER
========================================================
*/

async function handleMcp(
  req,
  res
) {

  const sessionId =
    req.headers[
      "mcp-session-id"
    ];

  /*
  ------------------------------------------------------
  EXISTING SESSION
  ------------------------------------------------------
  */

  if (
    sessionId &&
    sessions.has(sessionId)
  ) {

    const session =
      sessions.get(
        sessionId
      );

    log(
      `MCP request -> session ${sessionId}`
    );

    /*
      IMPORTANT:
      No manual body parsing here.
      The SDK transport reads the request.
    */

    await session.transport.handleRequest(
      req,
      res
    );

    return;
  }


  /*
  ------------------------------------------------------
  NEW SESSION
  ------------------------------------------------------
  */

  if (!sessionId) {

    /*
      The Streamable HTTP transport itself
      handles the initialize request.
    */

    const transport =
      createTransport();

    const server =
      createMcpServer();

    await server.connect(
      transport
    );

    log(
      `MCP new connection: ${req.method} ${req.url}`
    );

    await transport.handleRequest(
      req,
      res
    );

    return;
  }


  /*
  ------------------------------------------------------
  UNKNOWN SESSION
  ------------------------------------------------------
  */

  res.writeHead(
    404,
    {
      "Content-Type":
        "application/json"
    }
  );

  res.end(
    JSON.stringify({
      jsonrpc:
        "2.0",

      error: {
        code:
          -32001,

        message:
          "MCP session not found"
      },

      id:
        null
    })
  );
}


/*
========================================================
HTTP SERVER
========================================================
*/

const httpServer =
  http.createServer(
    async (req, res) => {

      try {

        /*
        --------------------------------------------------
        CORS
        --------------------------------------------------
        */

        res.setHeader(
          "Access-Control-Allow-Origin",
          "*"
        );

        res.setHeader(
          "Access-Control-Allow-Headers",
          "Content-Type, Accept, MCP-Session-Id, Last-Event-ID"
        );

        res.setHeader(
          "Access-Control-Expose-Headers",
          "MCP-Session-Id"
        );


        /*
        --------------------------------------------------
        OPTIONS
        --------------------------------------------------
        */

        if (
          req.method === "OPTIONS"
        ) {

          res.writeHead(
            204
          );

          res.end();

          return;
        }


        /*
        --------------------------------------------------
        HEALTH
        --------------------------------------------------
        */

        if (
          req.url === "/" ||
          req.url === "/health"
        ) {

          res.writeHead(
            200,
            {
              "Content-Type":
                "application/json"
            }
          );

          res.end(
            JSON.stringify({

              ok: true,

              service:
                "yahoo-scan-mcp",

              version:
                APP_VERSION,

              sessions:
                sessions.size

            })
          );

          return;
        }


        /*
        --------------------------------------------------
        MCP
        --------------------------------------------------
        */

        if (
          req.url === "/mcp"
        ) {

          await handleMcp(
            req,
            res
          );

          return;
        }


        /*
        --------------------------------------------------
        404
        --------------------------------------------------
        */

        res.writeHead(
          404,
          {
            "Content-Type":
              "application/json"
          }
        );

        res.end(
          JSON.stringify({

            ok: false,

            error:
              "Not found"

          })
        );

      } catch (error) {

        log(
          "HTTP error:",
          error
        );

        if (
          !res.headersSent
        ) {

          res.writeHead(
            500,
            {
              "Content-Type":
                "application/json"
            }
          );

          res.end(
            JSON.stringify({

              ok: false,

              error:
                error?.message ||
                String(error)

            })
          );
        }
      }
    }
  );


/*
========================================================
START
========================================================
*/

httpServer.listen(
  PORT,
  "0.0.0.0",
  () => {

    log(
      `HTTP server listening on ${PORT}`
    );

    log(
      `MCP endpoint: /mcp`
    );

    log(
      `Health endpoint: /health`
    );

    log(
      `MCP server started v${APP_VERSION}`
    );
  }
);
