import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { z } from "zod";
import http from "node:http";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";

import { YahooWS } from "./src/yahoo-ws.js";

/*
============================================================
CONFIG
============================================================
*/

const PORT =
  Number(process.env.PORT || 8080);

const APP_VERSION = "1.2.0";

const UNIVERSE_FILE =
  new URL("./universe_s0.txt", import.meta.url);

const log = (...args) => {
  console.error(
    "[yahoo-scan-mcp]",
    ...args
  );
};

/*
============================================================
GENERIC HELPERS
============================================================
*/

function sleep(ms) {
  return new Promise(
    resolve => setTimeout(resolve, ms)
  );
}

function chunk(array, size) {
  const out = [];

  for (
    let i = 0;
    i < array.length;
    i += size
  ) {
    out.push(
      array.slice(i, i + size)
    );
  }

  return out;
}

function cleanNumber(value) {
  if (
    value === null ||
    value === undefined ||
    value === ""
  ) {
    return null;
  }

  const n = Number(value);

  return Number.isFinite(n)
    ? n
    : null;
}

function normalizeSymbols(symbols) {
  return [
    ...new Set(
      (symbols || [])
        .map(s =>
          String(s)
            .trim()
            .toUpperCase()
        )
        .filter(s =>
          /^[A-Z0-9._$=-]{1,20}$/.test(s) &&
          s !== "SYMBOL"
        )
    )
  ];
}

/*
============================================================
LOCAL UNIVERSE
============================================================
*/

let universeCache = null;
let universeLoadedAt = null;

async function loadUniverse() {
  if (
    Array.isArray(universeCache) &&
    universeCache.length
  ) {
    return universeCache;
  }

  const raw =
    await readFile(
      UNIVERSE_FILE,
      "utf8"
    );

  const symbols =
    normalizeSymbols(
      raw
        .split(/\r?\n/)
        .map(line => {
          const cleaned =
            line.trim();

          if (
            !cleaned ||
            cleaned.startsWith("#")
          ) {
            return "";
          }

          /*
          Supports:

          GRAB
          GRAB,3.07
          GRAB;3.07
          GRAB 3.07
          */
          return cleaned
            .split(/[;,\s]+/)[0];
        })
    );

  if (!symbols.length) {
    throw new Error(
      "Universe file is empty: universe_s0.txt"
    );
  }

  universeCache = symbols;

  universeLoadedAt =
    new Date().toISOString();

  return universeCache;
}

/*
============================================================
SCAN STATE
============================================================
*/

const scanState = {
  ok: true,

  version:
    APP_VERSION,

  stage:
    "idle",

  asof:
    null,

  elapsed_ms:
    0,

  symbols_requested:
    0,

  source:
    null,

  s0: [],

  s1: [],

  lots: [],

  errors: [],

  universe_loaded_at:
    null
};

/*
============================================================
YAHOO SPARK
============================================================

Yahoo Spark returns:

spark.result[]
  -> response[]
      -> timestamp
      -> meta
      -> indicators
          -> quote[]
          -> close
============================================================
*/

async function yahooSpark(
  symbols,
  range = "5d",
  interval = "5m"
) {
  const results = [];

  /*
  Yahoo has shown HTTP 400 with larger batches.
  Keep batches deliberately small.
  */
  const batches =
    chunk(symbols, 20);

  for (const batch of batches) {

    const url =
      "https://query1.finance.yahoo.com/v7/finance/spark" +
      `?symbols=${encodeURIComponent(
        batch.join(",")
      )}` +
      `&range=${encodeURIComponent(
        range
      )}` +
      `&interval=${encodeURIComponent(
        interval
      )}` +
      "&indicators=quote,close" +
      "&includeTimestamps=true" +
      "&includePrePost=false";

    let success = false;
    let lastError = null;

    for (
      let attempt = 1;
      attempt <= 3 &&
      !success;
      attempt++
    ) {

      try {

        const response =
          await fetch(
            url,
            {
              headers: {
                "User-Agent":
                  "Mozilla/5.0"
              },

              signal:
                AbortSignal.timeout(
                  20000
                )
            }
          );

        if (!response.ok) {
          throw new Error(
            `Yahoo HTTP ${response.status}`
          );
        }

        const body =
          await response.json();

        const apiError =
          body?.spark?.error;

        if (apiError) {
          throw new Error(
            apiError.description ||
            apiError.message ||
            "Yahoo Spark error"
          );
        }

        const rows =
          body?.spark?.result || [];

        /*
        Spark result rows contain response[0].
        */
        for (const item of rows) {

          const responseRow =
            item?.response?.[0] ??
            item;

          if (
            !responseRow ||
            !Array.isArray(
              responseRow.timestamp
            ) ||
            !responseRow.timestamp.length
          ) {

            results.push({
              _invalid:
                true,

              _symbol:
                item?.symbol ||
                responseRow?.meta?.symbol ||
                null,

              _message:
                "Yahoo Spark returned no intraday response"
            });

            continue;
          }

          const meta =
            responseRow.meta || {};

          if (
            !meta.symbol &&
            item?.symbol
          ) {
            meta.symbol =
              item.symbol;
          }

          responseRow.meta =
            meta;

          results.push(
            responseRow
          );
        }

        /*
        Successful HTTP response with zero rows
        must not silently become S0=0.
        */
        if (!rows.length) {

          results.push({
            _invalid:
              true,

            _batch:
              batch,

            _message:
              "Yahoo Spark returned zero result rows"
          });
        }

        success = true;

      } catch (err) {

        lastError = err;

        if (attempt < 3) {
          await sleep(
            500 * attempt
          );
        }
      }
    }

    if (!success) {

      results.push({
        _error:
          true,

        _batch:
          batch,

        _message:
          lastError?.message ||
          String(lastError)
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

function extractBars(row) {

  const timestamps =
    row?.timestamp || [];

  const quote =
    row?.indicators
      ?.quote?.[0] || {};

  const closes =
    quote.close || [];

  const opens =
    quote.open || [];

  const highs =
    quote.high || [];

  const lows =
    quote.low || [];

  const volumes =
    quote.volume || [];

  const bars = [];

  for (
    let i = 0;
    i < timestamps.length;
    i++
  ) {

    const close =
      cleanNumber(
        closes[i]
      );

    const volume =
      cleanNumber(
        volumes[i]
      );

    if (
      close === null ||
      volume === null
    ) {
      continue;
    }

    bars.push({
      timestamp:
        timestamps[i],

      open:
        cleanNumber(
          opens[i]
        ),

      high:
        cleanNumber(
          highs[i]
        ),

      low:
        cleanNumber(
          lows[i]
        ),

      close,

      volume
    });
  }

  return bars;
}

function sessionKey(timestamp) {

  return new Date(
    timestamp * 1000
  )
    .toISOString()
    .slice(0, 10);
}

function calculateVWAP(bars) {

  let pv = 0;
  let volume = 0;

  for (const bar of bars) {

    const typical =
      (
        (bar.high ??
          bar.close) +
        (bar.low ??
          bar.close) +
        bar.close
      ) / 3;

    pv +=
      typical *
      bar.volume;

    volume +=
      bar.volume;
  }

  if (!volume) {
    return null;
  }

  return pv / volume;
}

function calculateS1(row) {

  const symbol =
    String(
      row?.meta?.symbol ||
      row?.symbol ||
      ""
    ).toUpperCase();

  const bars =
    extractBars(row);

  if (bars.length < 4) {

    return {
      ok: false,

      symbol,

      reason:
        "Not enough 5m bars"
    };
  }

  /*
  Only completed bars.
  Yahoo may return the current incomplete
  5-minute bar.
  */
  const completed =
    bars.slice(
      0,
      Math.max(
        0,
        bars.length - 1
      )
    );

  if (
    completed.length < 4
  ) {

    return {
      ok: false,

      symbol,

      reason:
        "Not enough completed 5m bars"
    };
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

  const last3 =
    completed.slice(-3);

  const vol15 =
    last3.reduce(
      (sum, bar) =>
        sum + bar.volume,
      0
    );

  const preceding =
    completed.slice(0, -3);

  /*
  Historical same-time-of-day baseline.
  */
  const currentDate =
    sessionKey(
      current.timestamp
    );

  const sameTimeVolumes =
    preceding
      .filter(bar =>
        sessionKey(
          bar.timestamp
        ) !== currentDate
      )
      .filter(bar => {

        const d =
          new Date(
            bar.timestamp * 1000
          );

        const c =
          new Date(
            current.timestamp * 1000
          );

        return (
          d.getUTCHours() ===
            c.getUTCHours() &&
          d.getUTCMinutes() ===
            c.getUTCMinutes()
        );
      })
      .map(
        bar =>
          bar.volume
      );

  const baseline =
    sameTimeVolumes.length
      ? sameTimeVolumes.reduce(
          (a, b) =>
            a + b,
          0
        ) /
        sameTimeVolumes.length
      : null;

  const rvol15 =
    baseline &&
    baseline > 0
      ? vol15 / baseline
      : null;

  const accelBase =
    completed.slice(
      Math.max(
        0,
        completed.length - 6
      ),
      -3
    );

  const accelMean =
    accelBase.length
      ? accelBase.reduce(
          (sum, bar) =>
            sum + bar.volume,
          0
        ) /
        accelBase.length
      : null;

  const accel5m =
    accelMean &&
    accelMean > 0
      ? current.volume /
        accelMean
      : null;

  const vwap =
    calculateVWAP(
      completed
    );

  const currentPrice =
    current.close;

  const j1 =
    previous.close &&
    previous.close > 0
      ? currentPrice /
        previous.close
      : null;

  const j2 =
    previous2.close &&
    previous2.close > 0
      ? previous.close /
        previous2.close
      : null;

  const hod =
    Math.max(
      ...completed.map(
        bar =>
          bar.high ??
          bar.close
      )
    );

  const hodDistance =
    hod &&
    hod > 0
      ? currentPrice /
          hod -
        1
      : null;

  const priceVsVWAP =
    vwap &&
    vwap > 0
      ? currentPrice /
          vwap -
        1
      : null;

  return {

    ok: true,

    symbol,

    price:
      currentPrice,

    timestamp:
      current.timestamp,

    vol15m:
      vol15,

    rvol15m:
      rvol15,

    accel5m:
      accel5m,

    vwap:
      vwap,

    price_vs_vwap:
      priceVsVWAP,

    j1:
      j1,

    j2:
      j2,

    hod:
      hod,

    hod_distance:
      hodDistance,

    bars:
      completed.length
  };
}

/*
============================================================
S0 FILTER
============================================================
*/

function isOrdinaryStock(row) {

  const meta =
    row?.meta || {};

  const quoteType =
    String(
      meta.quoteType ??
      meta.instrumentType ??
      ""
    ).toUpperCase();

  const exchange =
    String(
      meta.exchange ??
      meta.exchangeName ??
      ""
    ).toUpperCase();

  /*
  If Yahoo explicitly tells us it is not equity,
  reject it.
  */
  if (
    quoteType &&
    quoteType !== "EQUITY"
  ) {
    return false;
  }

  /*
  Explicitly reject ETFs/funds.
  */
  const shortName =
    String(
      meta.shortName ||
      meta.longName ||
      ""
    ).toUpperCase();

  if (
    shortName.includes("ETF") ||
    shortName.includes("FUND")
  ) {
    return false;
  }

  /*
  If exchange information is available,
  require NASDAQ/NYSE/AMEX-type US equity venues.
  */
  if (
    exchange &&
    ![
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
    ].includes(exchange)
  ) {
    return false;
  }

  return true;
}

function getCurrentPrice(row) {

  const meta =
    row?.meta || {};

  return (
    cleanNumber(
      meta.regularMarketPrice
    ) ??
    cleanNumber(
      meta.postMarketPrice
    )
  );
}

/*
============================================================
WS TEST LOCK
============================================================
*/

let wsTestQueue =
  Promise.resolve();

function withWsTestLock(task) {

  const run =
    wsTestQueue.then(
      task,
      task
    );

  wsTestQueue =
    run.catch(
      () => {}
    );

  return run;
}

/*
============================================================
MCP SERVER
============================================================
*/

const server =
  new McpServer({
    name:
      "yahoo-scan-mcp",

    version:
      APP_VERSION
  });

/*
============================================================
PING
============================================================
*/

server.tool(
  "ping",

  "Health check",

  {},

  async () => ({
    content: [{
      type:
        "text",

      text:
        JSON.stringify({
          ok: true,

          service:
            "yahoo-scan-mcp",

          version:
            APP_VERSION,

          timestamp:
            new Date()
              .toISOString()

        }, null, 2)
    }]
  })
);

/*
============================================================
GET STATUS
============================================================
*/

server.tool(
  "get_status",

  "Return current scanner state and Yahoo diagnostic status",

  {},

  async () => {

    /*
    There is intentionally no global YahooWS connection.

    yahoo_ws_test creates an isolated YahooWS instance for
    each diagnostic test. Therefore get_status must not call
    an undefined global "yahoo" object.
    */

    const yahooStatus = {
      connected:
        false,

      mode:
        "on-demand",

      note:
        "Yahoo WebSocket is created by yahoo_ws_test"
    };

    return {
      content: [{
        type:
          "text",

        text:
          JSON.stringify({

            ok: true,

            version:
              APP_VERSION,

            scanner:
              scanState,

            yahoo:
              yahooStatus

          }, null, 2)
      }]
    };
  }
);

/*
============================================================
GET UNIVERSE
============================================================
*/

server.tool(
  "get_universe",

  "Return the locally materialized filtered ticker universe",

  {
    limit:
      z.number()
        .int()
        .min(1)
        .max(2500)
        .optional()
  },

  async ({
    limit
  }) => {

    try {

      const symbols =
        await loadUniverse();

      const out =
        limit
          ? symbols.slice(
              0,
              limit
            )
          : symbols;

      return {
        content: [{
          type:
            "text",

          text:
            JSON.stringify({

              ok: true,

              source:
                "universe_s0.txt",

              count:
                symbols.length,

              returned:
                out.length,

              loaded_at:
                universeLoadedAt,

              symbols:
                out

            }, null, 2)
        }]
      };

    } catch (err) {

      return {
        content: [{
          type:
            "text",

          text:
            JSON.stringify({

              ok: false,

              error:
                err?.message ||
                String(err)

            }, null, 2)
        }]
      };
    }
  }
);

/*
============================================================
YAHOO WS TEST
============================================================
*/

server.tool(
  "yahoo_ws_test",

  "Test Yahoo Finance live WebSocket",

  {
    symbols:
      z.array(
        z.string()
      )
      .min(1)
      .max(2500)
      .optional(),

    seconds:
      z.number()
        .int()
        .min(5)
        .max(120)
        .optional()
  },

  async ({
    symbols,
    seconds
  }) =>
    withWsTestLock(
      async () => {

        const list =
          normalizeSymbols(
            symbols?.length
              ? symbols
              : [
                  "AAPL",
                  "MSFT",
                  "NVDA",
                  "AMD",
                  "INTC"
                ]
          );

        const duration =
          seconds ?? 15;

        const started =
          Date.now();

        let testYahoo =
          null;

        try {

          /*
          Do NOT reuse the global YahooWS instance.
          This keeps each diagnostic isolated.
          */

          testYahoo =
            new YahooWS({
              log
            });

          await testYahoo.connect();

          const subscribed =
            testYahoo.subscribe(
              list
            );

          await new Promise(
            resolve =>
              setTimeout(
                resolve,
                duration * 1000
              )
          );

          const status =
            testYahoo.status();

          const requestedSet =
            new Set(list);

          status.latest =
            Object.fromEntries(
              Object.entries(
                status.latest || {}
              ).filter(
                ([symbol]) =>
                  requestedSet.has(
                    symbol
                  )
              )
            );

          status.recent_ticks =
            (
              status.recent_ticks ||
              []
            ).filter(
              tick =>
                requestedSet.has(
                  tick.symbol
                )
            );

          return {
            content: [{
              type:
                "text",

              text:
                JSON.stringify({

                  ok: true,

                  requested_symbols:
                    list,

                  requested_count:
                    list.length,

                  subscribed_count:
                    subscribed,

                  duration_seconds:
                    duration,

                  elapsed_ms:
                    Date.now() -
                    started,

                  yahoo:
                    status

                }, null, 2)
            }]
          };

        } catch (err) {

          return {
            content: [{
              type:
                "text",

              text:
                JSON.stringify({

                  ok: false,

                  error:
                    err?.message ||
                    String(err),

                  yahoo:
                    testYahoo
                      ? testYahoo.status()
                      : null

                }, null, 2)
            }]
          };

        } finally {

          try {
            testYahoo?.close();
          } catch {}

        }
      }
    )
);

/*
============================================================
S0 / S1 SCAN
============================================================
*/

server.tool(
  "yahoo_s0_s1_scan",

  "Build S0/S1 from Yahoo 5-minute intraday data",

  {
    symbols:
      z.array(
        z.string()
      )
      .min(1)
      .max(2500)
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

    const started =
      Date.now();

    try {

      const supplied =
        symbols?.length
          ? symbols
          : await loadUniverse();

      const list =
        normalizeSymbols(
          supplied
        );

      if (!list.length) {
        throw new Error(
          "No valid symbols supplied"
        );
      }

      /*
      --------------------------------------------------------
      RESET STATE
      --------------------------------------------------------
      */

      scanState.stage =
        "S0_S1";

      scanState.asof =
        null;

      scanState.elapsed_ms =
        0;

      scanState.symbols_requested =
        list.length;

      scanState.source =
        symbols?.length
          ? "request"
          : "universe_s0.txt";

      scanState.s0 =
        [];

      scanState.s1 =
        [];

      scanState.lots =
        [];

      scanState.errors =
        [];

      /*
      --------------------------------------------------------
      YAHOO DATA
      --------------------------------------------------------
      */

      const rows =
        await yahooSpark(
          list,
          "5d",
          "5m"
        );

      /*
      --------------------------------------------------------
      PROCESS
      --------------------------------------------------------
      */

      for (const row of rows) {

        if (row?._error) {

          scanState.errors.push({
            type:
              "yahoo",

            batch:
              row._batch,

            message:
              row._message
          });

          continue;
        }

        if (row?._invalid) {

          scanState.errors.push({
            type:
              "invalid",

            symbol:
              row._symbol,

            message:
              row._message
          });

          continue;
        }

        const symbol =
          String(
            row?.meta?.symbol ||
            row?.symbol ||
            ""
          ).toUpperCase();

        if (!symbol) {

          scanState.errors.push({
            type:
              "missing_symbol",

            message:
              "Yahoo response has no symbol"
          });

          continue;
        }

        const price =
          getCurrentPrice(
            row
          );

        /*
        S0:
        ordinary stock,
        live price < $5.
        */

        if (
          price === null ||
          price >= 5 ||
          price <= 0
        ) {
          continue;
        }

        if (
          !isOrdinaryStock(
            row
          )
        ) {
          continue;
        }

        const s0Item = {

          symbol,

          price,

          exchange:
            row?.meta
              ?.exchange ||
            row?.meta
              ?.exchangeName ||
            null,

          quote_type:
            row?.meta
              ?.quoteType ||
            row?.meta
              ?.instrumentType ||
            null
        };

        scanState.s0.push(
          s0Item
        );

        /*
        S1 metrics.
        */

        const metrics =
          calculateS1(
            row
          );

        if (
          metrics.ok
        ) {

          scanState.s1.push(
            metrics
          );

        } else {

          scanState.errors.push({
            type:
              "s1",

            symbol,

            message:
              metrics.reason
          });
        }
      }

      /*
      --------------------------------------------------------
      RANKING
      --------------------------------------------------------
      */

      const ranked =
        scanState.s1
          .slice()
          .sort(
            (a, b) => {

              const ar =
                a.rvol15m ??
                -Infinity;

              const br =
                b.rvol15m ??
                -Infinity;

              if (
                br !== ar
              ) {
                return br - ar;
              }

              const aa =
                a.accel5m ??
                -Infinity;

              const ba =
                b.accel5m ??
                -Infinity;

              if (
                ba !== aa
              ) {
                return ba - aa;
              }

              const av =
                a.price_vs_vwap ??
                -Infinity;

              const bv =
                b.price_vs_vwap ??
                -Infinity;

              return bv - av;
            }
          );

      const limit =
        s1_limit ??
        50;

      scanState.s1 =
        ranked.slice(
          0,
          limit
        );

      /*
      --------------------------------------------------------
      LOTS
      --------------------------------------------------------
      */

      scanState.lots =
        chunk(
          scanState.s1,
          20
        );

      scanState.asof =
        new Date()
          .toISOString();

      scanState.elapsed_ms =
        Date.now() -
        started;

      scanState.universe_loaded_at =
        universeLoadedAt;

      scanState.stage =
        "S0_S1";

      return {
        content: [{
          type:
            "text",

          text:
            JSON.stringify({

              ok: true,

              stage:
                "S0_S1",

              version:
                APP_VERSION,

              asof:
                scanState.asof,

              elapsed_ms:
                scanState.elapsed_ms,

              universe_requested:
                list.length,

              s0_count:
                scanState.s0.length,

              s0:
                scanState.s0,

              s1_count:
                scanState.s1.length,

              s1:
                scanState.s1,

              lots_count:
                scanState.lots.length,

              lots:
                scanState.lots,

              errors_count:
                scanState.errors.length,

              errors:
                scanState.errors

            }, null, 2)
        }]
      };

    } catch (err) {

      scanState.stage =
        "error";

      scanState.elapsed_ms =
        Date.now() -
        started;

      scanState.errors =
        [{
          type:
            "fatal",

          message:
            err?.message ||
            String(err)
        }];

      return {
        content: [{
          type:
            "text",

          text:
            JSON.stringify({

              ok: false,

              version:
                APP_VERSION,

              stage:
                "S0_S1",

              elapsed_ms:
                scanState.elapsed_ms,

              errors:
                scanState.errors

            }, null, 2)
        }]
      };
    }
  }
);

/*
============================================================
GET SCAN STATE
============================================================
*/

server.tool(
  "get_scan_state",

  "Return latest S0/S1 scan state",

  {
    s1_limit:
      z.number()
        .int()
        .min(1)
        .max(200)
        .optional()
  },

  async ({
    s1_limit
  }) => {

    const limit =
      s1_limit ??
      scanState.s1.length;

    return {
      content: [{
        type:
          "text",

        text:
          JSON.stringify({

            ok: true,

            version:
              APP_VERSION,

            stage:
              scanState.stage,

            asof:
              scanState.asof,

            elapsed_ms:
              scanState.elapsed_ms,

            symbols_requested:
              scanState.symbols_requested,

            source:
              scanState.source,

            s0_count:
              scanState.s0.length,

            s0:
              scanState.s0,

            s1_count:
              Math.min(
                scanState.s1.length,
                limit
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
              scanState.errors

          }, null, 2)
      }]
    };
  }
);

/*
============================================================
MCP STREAMABLE HTTP TRANSPORT
============================================================
*/

/*
Railway needs a real HTTP MCP endpoint.

We intentionally use a stateful Streamable HTTP transport
here because the existing server/tool registrations are
created once and connected once.

A session ID is generated for each MCP client session.
*/

const mcpTransport =
  new StreamableHTTPServerTransport({
    sessionIdGenerator:
      () => randomUUID(),

    enableJsonResponse:
      true
  });

/*
Transport-level error logging.
*/

mcpTransport.onerror =
  error => {

    log(
      "MCP transport error:",
      error
    );
  };

/*
============================================================
HTTP SERVER
============================================================
*/

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
          req.url === "/" ||
          req.url === "/health"
        ) {

          res.writeHead(
            200,
            {
              "Content-Type":
                "application/json; charset=utf-8"
            }
          );

          res.end(
            JSON.stringify({

              ok: true,

              service:
                "yahoo-scan-mcp",

              version:
                APP_VERSION

            })
          );

          return;
        }

        /*
        ------------------------------------------------------
        MCP
        ------------------------------------------------------
        */

        if (
          req.url === "/mcp"
        ) {

          await mcpTransport.handleRequest(
            req,
            res
          );

          return;
        }

        /*
        ------------------------------------------------------
        404
        ------------------------------------------------------
        */

        res.writeHead(
          404,
          {
            "Content-Type":
              "application/json; charset=utf-8"
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
                "application/json; charset=utf-8"
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

        } else {

          try {
            res.end();
          } catch {}

        }
      }
    }
  );

/*
============================================================
START
============================================================
*/

async function main() {

  /*
  Connect the MCP server to the Streamable HTTP
  transport exactly once.
  */

  await server.connect(
    mcpTransport
  );

  /*
  Start Railway HTTP listener.
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
}

main().catch(
  error => {

    log(
      "Fatal error:",
      error
    );

    process.exit(1);
  }
);
