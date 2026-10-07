/*
========================================================
 Yahoo Scan MCP
 Version 1.6.2

 PRINCIPES
 - Railway démarre même si universe_s0.txt manque.
 - universe_s0.txt est chargé uniquement à la demande.
 - Streamable HTTP stateless.
 - Tous les tools utilisent registerTool().
 - Yahoo WS conservé.
 - Yahoo Spark diagnostiquable directement depuis Railway.
 - CORRECTION 1.6.2 :
   Yahoo Spark retourne result[].response.timestamp
   et result[].response.indicators.
========================================================
*/

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { z } from "zod";

import http from "node:http";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { YahooWS } from "./src/yahoo-ws.js";


/*
========================================================
 CONFIG
========================================================
*/

const PORT =
  Number(process.env.PORT || 8080);

const APP_VERSION =
  "1.6.2";

const __filename =
  fileURLToPath(import.meta.url);

const APP_DIR =
  path.dirname(__filename);

const CWD =
  process.cwd();

const BATCH_SIZE =
  20;

const CONCURRENCY =
  4;

const RETRIES =
  3;

const REQUEST_TIMEOUT_MS =
  15000;

const RETRY_DELAY_MS =
  600;

const UNIVERSE_FILENAME =
  "universe_s0.txt";

const S0_CACHE_FILENAME =
  "s0_materialized.json";

const S1_CACHE_FILENAME =
  "s1_materialized.json";


/*
========================================================
 TOOL REGISTRY
========================================================
*/

const TOOL_NAMES = [
  "ping",
  "get_status",
  "diagnose_filesystem",
  "get_universe",
  "yahoo_ws_test",
  "yahoo_spark_test",
  "yahoo_s0_prepare",
  "yahoo_s1_scan",
  "yahoo_sf_scan",
  "get_scan_state",
  "yahoo_s0_s1_scan"
];


/*
========================================================
 STATE
========================================================
*/

const scanState = {
  ok: true,
  version: APP_VERSION,
  stage: "IDLE",
  asof: null,
  elapsed_ms: 0,
  symbols_requested: 0,
  source: null,
  s0: [],
  s1: [],
  lots: [],
  errors: [],
  universe_file: null,
  started_at: null,
  finished_at: null
};


/*
========================================================
 CACHE
========================================================
*/

let universeCache = null;
let s0Cache = null;
let s1Cache = null;


/*
========================================================
 BASIC HELPERS
========================================================
*/

function sleep(ms) {
  return new Promise(
    resolve => setTimeout(resolve, ms)
  );
}


function nowIso() {
  return new Date().toISOString();
}


function uniqueUppercase(list) {
  return [
    ...new Set(
      list
        .map(
          x =>
            String(x || "")
              .trim()
              .toUpperCase()
        )
        .filter(Boolean)
    )
  ];
}


function chunk(array, size) {
  const result = [];

  for (
    let i = 0;
    i < array.length;
    i += size
  ) {
    result.push(
      array.slice(i, i + size)
    );
  }

  return result;
}


function isFiniteNumber(value) {
  return Number.isFinite(
    Number(value)
  );
}


/*
========================================================
 FILESYSTEM
========================================================
*/

function universeCandidates() {

  const candidates = [
    path.join(
      APP_DIR,
      UNIVERSE_FILENAME
    ),

    path.join(
      CWD,
      UNIVERSE_FILENAME
    ),

    path.join(
      "/app",
      UNIVERSE_FILENAME
    ),

    path.join(
      APP_DIR,
      "data",
      UNIVERSE_FILENAME
    ),

    path.join(
      CWD,
      "data",
      UNIVERSE_FILENAME
    ),

    path.join(
      "/app/data",
      UNIVERSE_FILENAME
    )
  ];

  return [
    ...new Set(candidates)
  ];
}


async function fileExists(filename) {

  try {

    await fs.access(
      filename
    );

    return true;

  } catch {

    return false;
  }
}


async function resolveUniverseFile() {

  const candidates =
    universeCandidates();

  for (
    const candidate of candidates
  ) {

    if (
      await fileExists(candidate)
    ) {
      return candidate;
    }
  }

  return null;
}


async function filesystemDiagnostic() {

  const result = {

    cwd:
      CWD,

    app_dir:
      APP_DIR,

    candidates:
      universeCandidates(),

    found:
      null,

    app_dir_files:
      [],

    cwd_files:
      []
  };

  result.found =
    await resolveUniverseFile();

  try {

    result.app_dir_files =
      await fs.readdir(
        APP_DIR
      );

  } catch (err) {

    result.app_dir_files = [
      `ERROR: ${err.message}`
    ];
  }

  if (
    CWD !== APP_DIR
  ) {

    try {

      result.cwd_files =
        await fs.readdir(
          CWD
        );

    } catch (err) {

      result.cwd_files = [
        `ERROR: ${err.message}`
      ];
    }
  }

  return result;
}


/*
========================================================
 UNIVERSE
========================================================
*/

async function loadUniverse() {

  if (
    universeCache
  ) {
    return universeCache;
  }

  const filename =
    await resolveUniverseFile();

  if (!filename) {

    const diagnostic =
      await filesystemDiagnostic();

    throw new Error(
      "universe_s0.txt introuvable dans le filesystem Railway.\n" +
      JSON.stringify(
        diagnostic,
        null,
        2
      )
    );
  }

  const raw =
    await fs.readFile(
      filename,
      "utf8"
    );

  const symbols =
    uniqueUppercase(
      raw
        .split(/\r?\n/)
        .map(
          line =>
            line.trim()
        )
        .filter(
          line =>
            line &&
            !line.startsWith("#")
        )
        .map(
          line =>
            line.split(
              /[\s,;]+/
            )[0]
        )
    );

  universeCache =
    symbols;

  scanState.universe_file =
    filename;

  return symbols;
}


/*
========================================================
 CACHE FILES
========================================================
*/

function cachePath(filename) {

  return path.join(
    APP_DIR,
    filename
  );
}


async function saveJson(
  filename,
  data
) {

  const filenamePath =
    cachePath(
      filename
    );

  await fs.writeFile(
    filenamePath,
    JSON.stringify(
      data,
      null,
      2
    ),
    "utf8"
  );

  return filenamePath;
}


async function readJsonIfExists(
  filename
) {

  const filenamePath =
    cachePath(
      filename
    );

  try {

    const raw =
      await fs.readFile(
        filenamePath,
        "utf8"
      );

    return JSON.parse(
      raw
    );

  } catch {

    return null;
  }
}


async function restoreCaches() {

  s0Cache =
    await readJsonIfExists(
      S0_CACHE_FILENAME
    );

  s1Cache =
    await readJsonIfExists(
      S1_CACHE_FILENAME
    );

  if (
    s0Cache?.s0
  ) {

    scanState.s0 =
      s0Cache.s0;

    scanState.source =
      s0Cache.source ||
      "Yahoo Spark";

    scanState.universe_file =
      s0Cache.universe_file ||
      null;
  }

  if (
    s1Cache?.s1
  ) {

    scanState.s1 =
      s1Cache.s1;

    scanState.lots =
      s1Cache.lots ||
      makeLots(
        s1Cache.s1,
        20
      );
  }
}


/*
========================================================
 HTTP FETCH
========================================================
*/

async function fetchWithTimeout(
  url,
  timeoutMs = REQUEST_TIMEOUT_MS
) {

  const controller =
    new AbortController();

  const timer =
    setTimeout(
      () => controller.abort(),
      timeoutMs
    );

  try {

    const response =
      await fetch(
        url,
        {
          method:
            "GET",

          headers: {

            "User-Agent":
              "Mozilla/5.0 (Windows NT 10.0; Win64; x64) " +
              "AppleWebKit/537.36 Chrome/140 Safari/537.36",

            "Accept":
              "application/json"
          },

          signal:
            controller.signal
        }
      );

    const text =
      await response.text();

    let data =
      null;

    try {

      data =
        JSON.parse(text);

    } catch {

      data =
        null;
    }

    if (
      !response.ok
    ) {

      const error =
        new Error(
          `Yahoo HTTP ${response.status}`
        );

      error.status =
        response.status;

      error.contentType =
        response.headers.get(
          "content-type"
        );

      error.bodyPreview =
        text.slice(
          0,
          500
        );

      throw error;
    }

    if (!data) {

      const error =
        new Error(
          "Yahoo returned non-JSON response"
        );

      error.status =
        response.status;

      error.contentType =
        response.headers.get(
          "content-type"
        );

      error.bodyPreview =
        text.slice(
          0,
          500
        );

      throw error;
    }

    return data;

  } finally {

    clearTimeout(
      timer
    );
  }
}


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
    chunk(
      symbols,
      BATCH_SIZE
    );

  const results =
    {};

  let cursor =
    0;

  async function worker() {

    while (true) {

      const index =
        cursor++;

      if (
        index >=
        batches.length
      ) {
        return;
      }

      const batch =
        batches[index];

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

      let data =
        null;

      let lastError =
        null;

      for (
        let attempt = 1;
        attempt <= RETRIES;
        attempt++
      ) {

        try {

          data =
            await fetchWithTimeout(
              url
            );

          break;

        } catch (err) {

          lastError =
            err;

          if (
            attempt < RETRIES
          ) {

            await sleep(
              RETRY_DELAY_MS *
              attempt
            );
          }
        }
      }

      if (!data) {

        for (
          const symbol of batch
        ) {

          results[symbol] = {

            symbol,

            error:
              lastError?.message ||
              "Yahoo request failed",

            http_status:
              lastError?.status ||
              null,

            content_type:
              lastError?.contentType ||
              null,

            body_preview:
              lastError?.bodyPreview ||
              null
          };
        }

        continue;
      }

      const spark =
        data?.spark?.result;

      if (
        !Array.isArray(
          spark
        )
      ) {

        for (
          const symbol of batch
        ) {

          results[symbol] = {

            symbol,

            error:
              "Yahoo response missing spark.result",

            response_keys:
              Object.keys(
                data || {}
              ),

            spark_keys:
              data?.spark
                ? Object.keys(
                    data.spark
                  )
                : []
          };
        }

        continue;
      }

      for (
        const item of spark
      ) {

        if (
          !item?.symbol
        ) {
          continue;
        }

        results[
          item.symbol
        ] =
          item;
      }

      const returned =
        new Set(
          spark
            .map(
              item =>
                item?.symbol
            )
            .filter(Boolean)
        );

      for (
        const symbol of batch
      ) {

        if (
          !returned.has(
            symbol
          )
        ) {

          results[symbol] = {

            symbol,

            error:
              "Yahoo returned no result for symbol"
          };
        }
      }
    }
  }

  const workers =
    [];

  for (
    let i = 0;
    i <
      Math.min(
        CONCURRENCY,
        batches.length
      );
    i++
  ) {

    workers.push(
      worker()
    );
  }

  await Promise.all(
    workers
  );

  return results;
}


/*
========================================================
 BAR EXTRACTION — CORRECTED 1.6.2
========================================================

 Yahoo Spark actuel :

 result[] = {
   symbol: "AAPL",
   response: {
     timestamp: [...],
     indicators: {
       quote: [...]
     }
   }
 }

 Certaines réponses peuvent toutefois fournir directement
 timestamp/indicators. Les deux formats sont acceptés.
========================================================
*/

function extractBars(item) {

  if (!item) {
    return [];
  }

  const response =
  Array.isArray(item.response)
    ? item.response[0]
    : (
        item.response ||
        item
      );

  const timestamps =
    Array.isArray(
      response.timestamp
    )
      ? response.timestamp
      : [];

  const quote =
    response.indicators
      ?.quote?.[0] ||
    {};

  const closes =
    Array.isArray(
      quote.close
    )
      ? quote.close
      : (
          Array.isArray(
            response.indicators
              ?.close?.[0]?.close
          )
            ? response.indicators
                .close[0].close
            : []
        );

  const opens =
    Array.isArray(
      quote.open
    )
      ? quote.open
      : [];

  const highs =
    Array.isArray(
      quote.high
    )
      ? quote.high
      : [];

  const lows =
    Array.isArray(
      quote.low
    )
      ? quote.low
      : [];

  const volumes =
    Array.isArray(
      quote.volume
    )
      ? quote.volume
      : [];

  const bars =
    [];

  for (
    let i = 0;
    i < timestamps.length;
    i++
  ) {

    const ts =
      Number(
        timestamps[i]
      ) * 1000;

    if (
      !Number.isFinite(ts)
    ) {
      continue;
    }

    const close =
      Number(
        closes[i]
      );

    const open =
      Number(
        opens[i]
      );

    const high =
      Number(
        highs[i]
      );

    const low =
      Number(
        lows[i]
      );

    const volume =
      Number(
        volumes[i]
      );

    bars.push({

      ts,

      open:
        Number.isFinite(open)
          ? open
          : null,

      high:
        Number.isFinite(high)
          ? high
          : null,

      low:
        Number.isFinite(low)
          ? low
          : null,

      close:
        Number.isFinite(close)
          ? close
          : null,

      volume:
        Number.isFinite(volume)
          ? volume
          : 0
    });
  }

  return bars;
}


/*
========================================================
 YAHOO SPARK DIAGNOSTIC — CORRECTED
========================================================
*/

async function runYahooSparkTest(
  symbol = "AAPL",
  range = "5d",
  interval = "5m"
) {

  const started =
    Date.now();

  const requestedSymbol =
    String(
      symbol || "AAPL"
    )
      .trim()
      .toUpperCase();

  const url =
    "https://query1.finance.yahoo.com/v7/finance/spark" +
    `?symbols=${encodeURIComponent(
      requestedSymbol
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

  const controller =
    new AbortController();

  const timer =
    setTimeout(
      () => controller.abort(),
      REQUEST_TIMEOUT_MS
    );

  try {

    const response =
      await fetch(
        url,
        {
          method:
            "GET",

          headers: {

            "User-Agent":
              "Mozilla/5.0 (Windows NT 10.0; Win64; x64) " +
              "AppleWebKit/537.36 Chrome/140 Safari/537.36",

            "Accept":
              "application/json"
          },

          signal:
            controller.signal
        }
      );

    const contentType =
      response.headers.get(
        "content-type"
      );

    const body =
      await response.text();

    let parsed =
      null;

    let jsonError =
      null;

    try {

      parsed =
        JSON.parse(
          body
        );

    } catch (err) {

      jsonError =
        err.message;
    }

    const spark =
      parsed?.spark ||
      null;

    const result =
      spark?.result;

    const first =
      Array.isArray(result) &&
      result.length
        ? result[0]
        : null;

    /*
     CORRECTION :
     Yahoo place les données dans first.response.
    */

    const firstResponse =
      first?.response ||
      first ||
      null;

    const bars =
      first
        ? extractBars(first)
        : [];

    const quote =
      firstResponse
        ?.indicators
        ?.quote
        ?.[0] ||
      null;

    return {

      ok:
        response.ok,

      version:
        APP_VERSION,

      symbol:
        requestedSymbol,

      range,

      interval,

      elapsed_ms:
        Date.now() -
        started,

      http_status:
        response.status,

      content_type:
        contentType,

      body_length:
        body.length,

      json_parse_ok:
        Boolean(parsed),

      json_error:
        jsonError,

      top_level_keys:
        parsed
          ? Object.keys(parsed)
          : [],

      spark_present:
        Boolean(spark),

      spark_keys:
        spark
          ? Object.keys(spark)
          : [],

      result_is_array:
        Array.isArray(result),

      result_count:
        Array.isArray(result)
          ? result.length
          : 0,

      first_result_keys:
        first
          ? Object.keys(first)
          : [],

      first_result_symbol:
        first?.symbol ||
        null,

      response_present:
        Boolean(
          first?.response
        ),

      response_keys:
        firstResponse
          ? Object.keys(
              firstResponse
            )
          : [],

      timestamp_count:
        Array.isArray(
          firstResponse?.timestamp
        )
          ? firstResponse.timestamp.length
          : 0,

      extracted_bar_count:
        bars.length,

      quote_keys:
        quote
          ? Object.keys(quote)
          : [],

      error:
        response.ok
          ? null
          : `Yahoo HTTP ${response.status}`,

      body_preview:
        response.ok
          ? null
          : body.slice(
              0,
              500
            )
    };

  } catch (err) {

    return {

      ok:
        false,

      version:
        APP_VERSION,

      symbol:
        requestedSymbol,

      range,

      interval,

      elapsed_ms:
        Date.now() -
        started,

      error:
        err.message,

      error_name:
        err.name,

      http_status:
        err.status ||
        null,

      content_type:
        err.contentType ||
        null,

      body_preview:
        err.bodyPreview ||
        null
    };

  } finally {

    clearTimeout(
      timer
    );
  }
}


/*
========================================================
 NEW YORK TIME
========================================================
*/

function nyDateKey(ts) {

  return new Intl.DateTimeFormat(
    "en-CA",
    {
      timeZone:
        "America/New_York",

      year:
        "numeric",

      month:
        "2-digit",

      day:
        "2-digit"
    }
  ).format(
    new Date(ts)
  );
}


function nyTimeParts(ts) {

  const parts =
    new Intl.DateTimeFormat(
      "en-US",
      {
        timeZone:
          "America/New_York",

        hour:
          "2-digit",

        minute:
          "2-digit",

        hour12:
          false
      }
    ).formatToParts(
      new Date(ts)
    );

  const map =
    {};

  for (
    const part of parts
  ) {

    if (
      part.type !==
      "literal"
    ) {

      map[part.type] =
        part.value;
    }
  }

  return {

    hour:
      Number(
        map.hour
      ),

    minute:
      Number(
        map.minute
      )
  };
}


function nyTimeKey(ts) {

  const {
    hour,
    minute
  } =
    nyTimeParts(ts);

  if (
    !Number.isFinite(hour) ||
    !Number.isFinite(minute)
  ) {
    return null;
  }

  const slot =
    Math.floor(
      minute / 15
    ) * 15;

  return (
    String(hour)
      .padStart(2, "0") +
    ":" +
    String(slot)
      .padStart(2, "0")
  );
}


/*
========================================================
 SESSION
========================================================
*/

function getSessionBars(
  bars
) {

  const groups =
    new Map();

  for (
    const bar of bars
  ) {

    const key =
      nyDateKey(
        bar.ts
      );

    if (
      !groups.has(key)
    ) {

      groups.set(
        key,
        []
      );
    }

    groups
      .get(key)
      .push(bar);
  }

  for (
    const list of
      groups.values()
  ) {

    list.sort(
      (a, b) =>
        a.ts - b.ts
    );
  }

  return groups;
}


function getCurrentSessionBars(
  bars
) {

  if (
    !bars.length
  ) {
    return [];
  }

  const sessions =
    getSessionBars(
      bars
    );

  const dates =
    [
      ...sessions.keys()
    ].sort();

  const currentDate =
    dates[
      dates.length - 1
    ];

  return (
    sessions.get(
      currentDate
    ) || []
  );
}


/*
========================================================
 VWAP
========================================================
*/

function calculateVWAP(
  bars
) {

  let pv =
    0;

  let volume =
    0;

  for (
    const bar of bars
  ) {

    if (
      !isFiniteNumber(
        bar.close
      ) ||
      !isFiniteNumber(
        bar.volume
      )
    ) {
      continue;
    }

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

  if (
    volume <= 0
  ) {
    return null;
  }

  return (
    pv / volume
  );
}


/*
========================================================
 SESSION PERFORMANCE
========================================================
*/

function sessionPerformance(
  session
) {

  if (
    !session?.length
  ) {
    return null;
  }

  const first =
    session.find(
      b =>
        isFiniteNumber(
          b.close
        )
    );

  const last =
    [...session]
      .reverse()
      .find(
        b =>
          isFiniteNumber(
            b.close
          )
      );

  if (
    !first ||
    !last ||
    first.close <= 0
  ) {
    return null;
  }

  return (
    (
      last.close /
      first.close -
      1
    ) * 100
  );
}


/*
========================================================
 RVOL BASELINE
========================================================
*/

function buildRvol15mBaseline(completedSessions) {
  const slotValues = new Map();

  for (const session of completedSessions) {
    if (!Array.isArray(session)) continue;

    const slotVolumes = new Map();

    for (const bar of session) {
      if (!bar || !Number.isFinite(bar.ts)) continue;

      const key = nyTimeKey(bar.ts);
      if (!key) continue;

      const volume = Number(bar.volume);

      if (!Number.isFinite(volume) || volume <= 0) {
        continue;
      }

      const previous = slotVolumes.get(key) || 0;
      slotVolumes.set(key, previous + volume);
    }

    for (const [key, volume] of slotVolumes) {
      if (!Number.isFinite(volume) || volume <= 0) {
        continue;
      }

      if (!slotValues.has(key)) {
        slotValues.set(key, []);
      }

      slotValues.get(key).push(volume);
    }
  }

  const baseline = {};

  for (const [key, values] of slotValues) {
    if (!Array.isArray(values) || values.length === 0) {
      continue;
    }

    const valid = values.filter(
      value => Number.isFinite(value) && value > 0
    );

    if (valid.length === 0) {
      continue;
    }

    const sum = valid.reduce(
      (total, value) => total + value,
      0
    );

    const average = sum / valid.length;

    if (Number.isFinite(average) && average > 0) {
      baseline[key] = average;
    }
  }

  return baseline;
}


/*
========================================================
 ORDINARY STOCK FILTER
========================================================
*/

function isOrdinaryStock(
  symbol,
  item
) {

  if (
    !symbol
  ) {
    return false;
  }

  const upper =
    symbol.toUpperCase();

  if (
    upper.endsWith("W") ||
    upper.endsWith("WS") ||
    upper.endsWith("WT") ||
    upper.endsWith("U")
  ) {
    return false;
  }

  return true;
}


/*
========================================================
 S0
========================================================
*/

function calculateS0(
  symbol,
  item
) {

  if (
    !isOrdinaryStock(
      symbol,
      item
    )
  ) {
    return null;
  }

  const bars =
    extractBars(
      item
    );

  if (
    bars.length < 20
  ) {
    return null;
  }

  const sessions =
    getSessionBars(
      bars
    );

  const dates =
    [
      ...sessions.keys()
    ].sort();

  if (
    dates.length < 3
  ) {
    return null;
  }

  const currentDate =
    dates[
      dates.length - 1
    ];

  const currentSession =
    sessions.get(
      currentDate
    ) || [];

  const completedSessions =
    dates
      .slice(0, -1)
      .map(
        d =>
          sessions.get(d)
      )
      .filter(Boolean);

  const previous =
    completedSessions[
      completedSessions.length - 1
    ];

  const previous2 =
    completedSessions[
      completedSessions.length - 2
    ];

  if (
    !previous ||
    !previous2
  ) {
    return null;
  }

  const currentLast =
    [...currentSession]
      .reverse()
      .find(
        b =>
          isFiniteNumber(
            b.close
          )
      );

  if (
    !currentLast
  ) {
    return null;
  }

  const price =
    Number(
      currentLast.close
    );

  if (
    !(
      price > 0 &&
      price < 5
    )
  ) {
    return null;
  }

  const j1 =
    sessionPerformance(
      previous
    );

  const j2 =
    sessionPerformance(
      previous2
    );

  const rvol15mBaseline =
    buildRvol15mBaseline(
      completedSessions
    );

  return {

    symbol,

    price,

    j1_pct:
      j1,

    j2_pct:
      j2,

    historical_sessions:
      completedSessions.length,

    current_session_bars:
      currentSession.length,

    rvol15m_baseline:
      rvol15mBaseline,

    rvol_baseline_sessions:
      completedSessions.length,

    prepared_at:
      nowIso()
  };
}


/*
========================================================
 CURRENT VOL15M
========================================================
*/

function calculateCurrentVol15M(
  completed
) {

  if (
    completed.length < 3
  ) {

    return {

      vol15m:
        null,

      slot:
        null
    };
  }

  const last3 =
    completed.slice(
      -3
    );

  const vol15m =
    last3.reduce(
      (
        sum,
        bar
      ) =>
        sum +
        Number(
          bar.volume || 0
        ),
      0
    );

  const slot =
    nyTimeKey(
      last3[0].ts
    );

  return {

    vol15m,

    slot
  };
}


/*
========================================================
 S1
========================================================
*/

function calculateS1(
  symbol,
  item,
  s0Record
) {

  if (
    !s0Record
  ) {
    return null;
  }

  const bars =
    extractBars(
      item
    );

  const currentSession =
    getCurrentSessionBars(
      bars
    );

  if (
    currentSession.length < 4
  ) {
    return null;
  }

  const now =
    Date.now();

  const completed =
    currentSession.filter(
      bar =>
        bar.ts +
        5 * 60 * 1000 <=
        now + 1000
    );

  if (
    completed.length < 4
  ) {
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

  const price =
    Number(
      current.close
    );

  if (
    !(
      price > 0 &&
      price < 5
    )
  ) {
    return null;
  }

  const {
    vol15m,
    slot:
      rvolSlot
  } =
    calculateCurrentVol15M(
      completed
    );

  const baseline =
    s0Record
      ?.rvol15m_baseline
      ?.[rvolSlot];

  const rvol15m =
    (
      Number.isFinite(
        vol15m
      ) &&
      Number.isFinite(
        Number(
          baseline
        )
      ) &&
      Number(
        baseline
      ) > 0
    )
      ? vol15m /
        Number(
          baseline
        )
      : null;

  const baselineVolumes =
    completed
      .slice(-4, -1)
      .map(
        b =>
          Number(
            b.volume || 0
          )
      )
      .filter(
        v =>
          v > 0
      );

  const meanBaseline =
    baselineVolumes.length
      ? baselineVolumes.reduce(
          (
            a,
            b
          ) =>
            a + b,
          0
        ) /
        baselineVolumes.length
      : null;

  const accel5m =
    meanBaseline > 0
      ? Number(
          current.volume || 0
        ) /
        meanBaseline
      : null;

  const vwap =
    calculateVWAP(
      completed
    );

  const priceVsVWAP =
    vwap > 0
      ? (
          price /
          vwap -
          1
        ) * 100
      : null;

  const highs =
    completed
      .map(
        b =>
          Number(
            b.high
          )
      )
      .filter(
        Number.isFinite
      );

  const hod =
    highs.length
      ? Math.max(
          ...highs
        )
      : null;

  const hodDistance =
    hod > 0
      ? (
          price /
          hod -
          1
        ) * 100
      : null;

  const change5m =
    previous.close > 0
      ? (
          price /
          previous.close -
          1
        ) * 100
      : null;

  const change10m =
    previous2.close > 0
      ? (
          price /
          previous2.close -
          1
        ) * 100
      : null;

  return {

    symbol,

    price,

    vol15m,

    rvol15m,

    rvol15m_slot:
      rvolSlot,

    rvol15m_baseline:
      Number.isFinite(
        Number(
          baseline
        )
      )
        ? Number(
            baseline
          )
        : null,

    accel5m,

    vwap,

    priceVsVWAP,

    hod,

    hodDistance,

    change5m,

    change10m,

    j1_pct:
      s0Record.j1_pct,

    j2_pct:
      s0Record.j2_pct,

    bars_completed:
      completed.length,

    asof:
      new Date(
        current.ts
      ).toISOString()
  };
}


/*
========================================================
 RANKING
========================================================
*/

function rankS1(
  records
) {

  return [
    ...records
  ].sort(
    (
      a,
      b
    ) => {

      const rvA =
        Number.isFinite(
          a.rvol15m
        )
          ? a.rvol15m
          : -Infinity;

      const rvB =
        Number.isFinite(
          b.rvol15m
        )
          ? b.rvol15m
          : -Infinity;

      if (
        rvB !== rvA
      ) {
        return (
          rvB -
          rvA
        );
      }

      const acA =
        Number.isFinite(
          a.accel5m
        )
          ? a.accel5m
          : -Infinity;

      const acB =
        Number.isFinite(
          b.accel5m
        )
          ? b.accel5m
          : -Infinity;

      if (
        acB !== acA
      ) {
        return (
          acB -
          acA
        );
      }

      const vwA =
        Number.isFinite(
          a.priceVsVWAP
        )
          ? a.priceVsVWAP
          : -Infinity;

      const vwB =
        Number.isFinite(
          b.priceVsVWAP
        )
          ? b.priceVsVWAP
          : -Infinity;

      if (
        vwB !== vwA
      ) {
        return (
          vwB -
          vwA
        );
      }

      const hodA =
        Number.isFinite(
          a.hodDistance
        )
          ? a.hodDistance
          : -Infinity;

      const hodB =
        Number.isFinite(
          b.hodDistance
        )
          ? b.hodDistance
          : -Infinity;

      return (
        hodB -
        hodA
      );
    }
  );
}


/*
========================================================
 LOTS
========================================================
*/

function makeLots(
  records,
  size = 20
) {

  return chunk(
    records,
    size
  ).map(
    (
      lot,
      index
    ) => ({

      lot:
        index + 1,

      size:
        lot.length,

      symbols:
        lot.map(
          x =>
            x.symbol
        ),

      records:
        lot
    })
  );
}


/*
========================================================
 WINNER GATE
========================================================
*/

function passesWinnerGate(
  record,
  options = {}
) {

  if (
    !record
  ) {
    return false;
  }

  const minPriceVsVWAP =
    Number.isFinite(
      options.minPriceVsVWAP
    )
      ? options.minPriceVsVWAP
      : null;

  const minAccel5M =
    Number.isFinite(
      options.minAccel5M
    )
      ? options.minAccel5M
      : null;

  const minVol15M =
    Number.isFinite(
      options.minVol15M
    )
      ? options.minVol15M
      : null;

  if (
    minPriceVsVWAP !== null &&
    (
      !Number.isFinite(
        record.priceVsVWAP
      ) ||
      record.priceVsVWAP <
        minPriceVsVWAP
    )
  ) {
    return false;
  }

  if (
    minAccel5M !== null &&
    (
      !Number.isFinite(
        record.accel5m
      ) ||
      record.accel5m <
        minAccel5M
    )
  ) {
    return false;
  }

  if (
    minVol15M !== null &&
    (
      !Number.isFinite(
        record.vol15m
      ) ||
      record.vol15m <
        minVol15M
    )
  ) {
    return false;
  }

  return true;
}


/*
========================================================
 S0 PREPARE
========================================================
*/

async function runS0Prepare(
  symbols = null
) {

  const started =
    Date.now();

  scanState.stage =
    "S0_PREPARE";

  scanState.started_at =
    nowIso();

  scanState.errors =
    [];

  let list;

  if (
    symbols?.length
  ) {

    list =
      uniqueUppercase(
        symbols
      );

  } else {

    list =
      await loadUniverse();
  }

  scanState.symbols_requested =
    list.length;

  const yahooData =
    await yahooSpark(
      list,
      "5d",
      "5m"
    );

  const s0 =
    [];

  let yahooErrors =
    0;

  for (
    const symbol of list
  ) {

    const item =
      yahooData[
        symbol
      ];

    if (
      item?.error
    ) {

      yahooErrors++;

      continue;
    }

    const record =
      calculateS0(
        symbol,
        item
      );

    if (
      record
    ) {
      s0.push(
        record
      );
    }
  }

  if (
    yahooErrors > 0
  ) {

    scanState.errors.push(
      `Yahoo Spark: ${yahooErrors} symboles/batches sans données exploitables.`
    );
  }

  s0Cache = {

    version:
      APP_VERSION,

    created_at:
      nowIso(),

    source:
      "Yahoo Spark",

    universe_file:
      scanState.universe_file,

    yahoo_errors:
      yahooErrors,

    s0
  };

  const cacheFile =
    await saveJson(
      S0_CACHE_FILENAME,
      s0Cache
    );

  scanState.s0 =
    s0;

  scanState.source =
    "Yahoo Spark";

  scanState.asof =
    nowIso();

  scanState.elapsed_ms =
    Date.now() -
    started;

  scanState.stage =
    "S0_READY";

  scanState.finished_at =
    nowIso();

  return {

    ok:
      true,

    stage:
      "S0_READY",

    version:
      APP_VERSION,

    elapsed_ms:
      scanState.elapsed_ms,

    universe_file:
      scanState.universe_file,

    universe_count:
      list.length,

    s0_count:
      s0.length,

    yahoo_errors:
      yahooErrors,

    cache_file:
      cacheFile,

    s0
  };
}


/*
========================================================
 S1 SCAN
========================================================
*/

async function runS1Scan(
  options = {}
) {

  const started =
    Date.now();

  scanState.stage =
    "S1";

  scanState.started_at =
    nowIso();

  scanState.errors =
    [];

  let s0 =
    scanState.s0?.length
      ? scanState.s0
      : s0Cache?.s0 ||
        [];

  if (
    !s0.length
  ) {

    throw new Error(
      "S0 non préparé. Exécute d'abord yahoo_s0_prepare."
    );
  }

  if (
    options.limit
  ) {

    s0 =
      s0.slice(
        0,
        Number(
          options.limit
        )
      );
  }

  const symbols =
    s0.map(
      x =>
        x.symbol
    );

  const yahooData =
    await yahooSpark(
      symbols,
      "1d",
      "5m"
    );

  const s1 =
    [];

  for (
    const s0Record of s0
  ) {

    const item =
      yahooData[
        s0Record.symbol
      ];

    if (
      item?.error
    ) {
      continue;
    }

    const record =
      calculateS1(
        s0Record.symbol,
        item,
        s0Record
      );

    if (
      record
    ) {
      s1.push(
        record
      );
    }
  }

  const ranked =
    rankS1(
      s1
    );

  const limit =
    Number(
      options.s1_limit ||
      100
    );

  const selected =
    ranked.slice(
      0,
      limit
    );

  const lots =
    makeLots(
      selected,
      20
    );

  s1Cache = {

    version:
      APP_VERSION,

    created_at:
      nowIso(),

    source:
      "Yahoo Spark",

    s1:
      selected,

    lots
  };

  const cacheFile =
    await saveJson(
      S1_CACHE_FILENAME,
      s1Cache
    );

  scanState.s1 =
    selected;

  scanState.lots =
    lots;

  scanState.asof =
    nowIso();

  scanState.elapsed_ms =
    Date.now() -
    started;

  scanState.stage =
    "S1_READY";

  scanState.finished_at =
    nowIso();

  return {

    ok:
      true,

    stage:
      "S1_READY",

    version:
      APP_VERSION,

    elapsed_ms:
      scanState.elapsed_ms,

    s0_count:
      s0.length,

    s1_count:
      selected.length,

    lots_count:
      lots.length,

    cache_file:
      cacheFile,

    s1:
      selected,

    lots:
      lots.map(
        lot => ({

          lot:
            lot.lot,

          size:
            lot.size,

          symbols:
            lot.symbols
        })
      )
  };
}


/*
========================================================
 SF SCAN
========================================================
*/

async function runSFScan(
  options = {}
) {

  const started =
    Date.now();

  scanState.stage =
    "SF";

  const source =
    s1Cache?.lots?.length
      ? s1Cache.lots
      : scanState.lots;

  if (
    !source?.length
  ) {

    throw new Error(
      "Aucun lot S1 disponible. Exécute d'abord yahoo_s1_scan."
    );
  }

  const startLot =
    Math.max(
      1,
      Number(
        options.start_lot ||
        1
      )
    );

  const maxLots =
    Number(
      options.max_lots ||
      source.length
    );

  const checked =
    [];

  let winner =
    null;

  for (
    const lot of source
      .filter(
        x =>
          x.lot >=
          startLot
      )
      .slice(
        0,
        maxLots
      )
  ) {

    const symbols =
      lot.symbols;

    const yahooData =
      await yahooSpark(
        symbols,
        "1d",
        "5m"
      );

    const refreshed =
      [];

    for (
      const oldRecord of
        lot.records
    ) {

      const item =
        yahooData[
          oldRecord.symbol
        ];

      if (
        !item ||
        item.error
      ) {
        continue;
      }

      const current =
        calculateS1(
          oldRecord.symbol,
          item,
          oldRecord
        );

      if (
        !current
      ) {
        continue;
      }

      refreshed.push(
        current
      );

      if (
        !winner &&
        passesWinnerGate(
          current,
          options
        )
      ) {

        winner =
          current;
      }
    }

    checked.push({

      lot:
        lot.lot,

      checked_count:
        refreshed.length,

      records:
        refreshed
    });

    if (
      winner
    ) {
      break;
    }
  }

  scanState.stage =
    winner
      ? "WINNER_FOUND"
      : "SF_COMPLETE";

  scanState.elapsed_ms =
    Date.now() -
    started;

  scanState.asof =
    nowIso();

  return {

    ok:
      true,

    stage:
      scanState.stage,

    version:
      APP_VERSION,

    elapsed_ms:
      scanState.elapsed_ms,

    winner,

    lots_checked:
      checked.length,

    checked
  };
}


/*
========================================================
 WEBSOCKET TEST
========================================================
*/

async function runYahooWSTest() {

  const ws =
    new YahooWS();

  const started =
    Date.now();

  try {

    await ws.connect();

    await ws.subscribe([
      "AAPL",
      "MSFT",
      "NVDA"
    ]);

    await sleep(
      10000
    );

    const status =
      typeof ws.getStatus ===
      "function"

        ? ws.getStatus()

        : {
            connected:
              true
          };

    return {

      ok:
        true,

      elapsed_ms:
        Date.now() -
        started,

      ...status
    };

  } finally {

    try {

      await ws.close();

    } catch {}
  }
}


/*
========================================================
 MCP TOOLS
========================================================
*/

function registerTools(
  server
) {

  server.registerTool(
    "ping",

    {
      description:
        "Health check du Yahoo Scan MCP.",

      inputSchema:
        z.object({})
    },

    async () => ({

      content: [{

        type:
          "text",

        text:
          JSON.stringify({

            ok:
              true,

            service:
              "yahoo-scan-mcp",

            version:
              APP_VERSION,

            timestamp:
              nowIso()
          })
      }]
    })
  );


  server.registerTool(
    "get_status",

    {
      description:
        "Retourne l'état du MCP et du scanner.",

      inputSchema:
        z.object({})
    },

    async () => ({

      content: [{

        type:
          "text",

        text:
          JSON.stringify({

            ok:
              true,

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

            s0_count:
              scanState.s0?.length ||
              0,

            s1_count:
              scanState.s1?.length ||
              0,

            lots_count:
              scanState.lots?.length ||
              0,

            universe_file:
              scanState.universe_file,

            tool_count:
              TOOL_NAMES.length,

            registered_tools:
              TOOL_NAMES,

            transport:
              "StreamableHTTP stateless",

            errors:
              scanState.errors
          })
      }]
    })
  );


  server.registerTool(
    "diagnose_filesystem",

    {
      description:
        "Diagnostic du filesystem Railway.",

      inputSchema:
        z.object({})
    },

    async () => {

      const diagnostic =
        await filesystemDiagnostic();

      return {

        content: [{

          type:
            "text",

          text:
            JSON.stringify(
              {
                ok:
                  true,

                version:
                  APP_VERSION,

                ...diagnostic
              },
              null,
              2
            )
        }]
      };
    }
  );


  server.registerTool(
    "get_universe",

    {
      description:
        "Charge et retourne universe_s0.txt.",

      inputSchema:
        z.object({})
    },

    async () => {

      try {

        const universe =
          await loadUniverse();

        return {

          content: [{

            type:
              "text",

            text:
              JSON.stringify({

                ok:
                  true,

                count:
                  universe.length,

                file:
                  scanState.universe_file,

                symbols:
                  universe
              })
          }]
        };

      } catch (err) {

        return {

          content: [{

            type:
              "text",

            text:
              JSON.stringify({

                ok:
                  false,

                error:
                  err.message
              })
          }]
        };
      }
    }
  );


  server.registerTool(
    "yahoo_ws_test",

    {
      description:
        "Teste la connexion Yahoo WebSocket.",

      inputSchema:
        z.object({})
    },

    async () => {

      try {

        const result =
          await runYahooWSTest();

        return {

          content: [{

            type:
              "text",

            text:
              JSON.stringify(
                result
              )
          }]
        };

      } catch (err) {

        return {

          content: [{

            type:
              "text",

            text:
              JSON.stringify({

                ok:
                  false,

                error:
                  err.message
              })
          }]
        };
      }
    }
  );


  server.registerTool(
    "yahoo_spark_test",

    {
      description:
        "Diagnostic direct de Yahoo Spark depuis Railway.",

      inputSchema:
        z.object({

          symbol:
            z.string()
              .optional(),

          range:
            z.string()
              .optional(),

          interval:
            z.string()
              .optional()
        })
    },

    async ({
      symbol,
      range,
      interval
    }) => {

      try {

        const result =
          await runYahooSparkTest(
            symbol ||
              "AAPL",

            range ||
              "5d",

            interval ||
              "5m"
          );

        return {

          content: [{

            type:
              "text",

            text:
              JSON.stringify(
                result,
                null,
                2
              )
          }]
        };

      } catch (err) {

        return {

          content: [{

            type:
              "text",

            text:
              JSON.stringify({

                ok:
                  false,

                error:
                  err.message
              })
          }]
        };
      }
    }
  );


  server.registerTool(
    "yahoo_s0_prepare",

    {
      description:
        "Prépare l'univers S0 et matérialise les données historiques.",

      inputSchema:
        z.object({

          symbols:
            z.array(
              z.string()
            )
            .optional()
        })
    },

    async ({
      symbols
    }) => {

      try {

        const result =
          await runS0Prepare(
            symbols
          );

        return {

          content: [{

            type:
              "text",

            text:
              JSON.stringify(
                result
              )
          }]
        };

      } catch (err) {

        scanState.stage =
          "S0_ERROR";

        scanState.errors.push(
          err.message
        );

        return {

          content: [{

            type:
              "text",

            text:
              JSON.stringify({

                ok:
                  false,

                stage:
                  "S0_ERROR",

                version:
                  APP_VERSION,

                error:
                  err.message
              })
          }]
        };
      }
    }
  );


  server.registerTool(
    "yahoo_s1_scan",

    {
      description:
        "Exécute S1 sur l'univers S0 matérialisé.",

      inputSchema:
        z.object({

          limit:
            z.number()
              .optional(),

          s1_limit:
            z.number()
              .optional()
        })
    },

    async ({
      limit,
      s1_limit
    }) => {

      try {

        const result =
          await runS1Scan({

            limit,

            s1_limit
          });

        return {

          content: [{

            type:
              "text",

            text:
              JSON.stringify(
                result
              )
          }]
        };

      } catch (err) {

        scanState.stage =
          "S1_ERROR";

        scanState.errors.push(
          err.message
        );

        return {

          content: [{

            type:
              "text",

            text:
              JSON.stringify({

                ok:
                  false,

                stage:
                  "S1_ERROR",

                version:
                  APP_VERSION,

                error:
                  err.message
              })
          }]
        };
      }
    }
  );


  server.registerTool(
    "yahoo_sf_scan",

    {
      description:
        "Rafraîchit les lots S1 et s'arrête au premier Winner Gate.",

      inputSchema:
        z.object({

          start_lot:
            z.number()
              .optional(),

          max_lots:
            z.number()
              .optional(),

          minPriceVsVWAP:
            z.number()
              .optional(),

          minAccel5M:
            z.number()
              .optional(),

          minVol15M:
            z.number()
              .optional()
        })
    },

    async ({
      start_lot,
      max_lots,
      minPriceVsVWAP,
      minAccel5M,
      minVol15M
    }) => {

      try {

        const result =
          await runSFScan({

            start_lot,

            max_lots,

            minPriceVsVWAP,

            minAccel5M,

            minVol15M
          });

        return {

          content: [{

            type:
              "text",

            text:
              JSON.stringify(
                result
              )
          }]
        };

      } catch (err) {

        scanState.stage =
          "SF_ERROR";

        scanState.errors.push(
          err.message
        );

        return {

          content: [{

            type:
              "text",

            text:
              JSON.stringify({

                ok:
                  false,

                stage:
                  "SF_ERROR",

                version:
                  APP_VERSION,

                error:
                  err.message
              })
          }]
        };
      }
    }
  );


  server.registerTool(
    "get_scan_state",

    {
      description:
        "Retourne l'état complet du scan.",

      inputSchema:
        z.object({})
    },

    async () => ({

      content: [{

        type:
          "text",

        text:
          JSON.stringify(
            scanState,
            null,
            2
          )
      }]
    })
  );


  server.registerTool(
    "yahoo_s0_s1_scan",

    {
      description:
        "Compatibilité : exécute S0 puis S1.",

      inputSchema:
        z.object({

          symbols:
            z.array(
              z.string()
            )
            .optional(),

          s1_limit:
            z.number()
              .optional()
        })
    },

    async ({
      symbols,
      s1_limit
    }) => {

      try {

        const s0 =
          await runS0Prepare(
            symbols
          );

        const s1 =
          await runS1Scan({
            s1_limit
          });

        return {

          content: [{

            type:
              "text",

            text:
              JSON.stringify({

                ok:
                  true,

                stage:
                  "S0_S1",

                s0,

                s1
              })
          }]
        };

      } catch (err) {

        return {

          content: [{

            type:
              "text",

            text:
              JSON.stringify({

                ok:
                  false,

                stage:
                  "S0_S1_ERROR",

                error:
                  err.message
              })
          }]
        };
      }
    }
  );
}


/*
========================================================
 MCP SERVER FACTORY
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
 HTTP SERVER
========================================================
*/

const httpServer =
  http.createServer(
    async (
      req,
      res
    ) => {

      /*
       ROOT
      */

      if (
        req.method === "GET" &&
        req.url === "/"
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

            ok:
              true,

            service:
              "yahoo-scan-mcp",

            version:
              APP_VERSION,

            transport:
              "StreamableHTTP stateless",

            tool_count:
              TOOL_NAMES.length,

            registered_tools:
              TOOL_NAMES
          })
        );

        return;
      }


      /*
       HEALTH
      */

      if (
        req.method === "GET" &&
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

            ok:
              true,

            service:
              "yahoo-scan-mcp",

            version:
              APP_VERSION,

            stage:
              scanState.stage,

            tool_count:
              TOOL_NAMES.length,

            registered_tools:
              TOOL_NAMES
          })
        );

        return;
      }


      /*
       DEBUG TOOLS
      */

      if (
        req.method === "GET" &&
        req.url === "/debug-tools"
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

            ok:
              true,

            version:
              APP_VERSION,

            tool_count:
              TOOL_NAMES.length,

            registered_tools:
              TOOL_NAMES
          })
        );

        return;
      }


      /*
       MCP
      */

      if (
        req.url === "/mcp" &&
        req.method === "POST"
      ) {

        try {

          const bodyChunks =
            [];

          for await (
            const chunk of req
          ) {

            bodyChunks.push(
              chunk
            );
          }

          const body =
            Buffer
              .concat(
                bodyChunks
              )
              .toString(
                "utf8"
              );

          const parsed =
            body
              ? JSON.parse(
                  body
                )
              : undefined;

          /*
           Stateless :
           nouveau serveur + transport
           pour chaque requête.
          */

          const server =
            createMcpServer();

          const transport =
            new StreamableHTTPServerTransport({

              sessionIdGenerator:
                undefined,

              enableJsonResponse:
                true
            });

          await server.connect(
            transport
          );

          await transport.handleRequest(
            req,
            res,
            parsed
          );

        } catch (err) {

          console.error(
            "[MCP ERROR]",
            err
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

                ok:
                  false,

                error:
                  err.message
              })
            );
          }
        }

        return;
      }


      /*
       404
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

          ok:
            false,

          error:
            "Not found"
        })
      );
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

    console.log(
      `[yahoo-scan-mcp] HTTP server listening on ${PORT}`
    );

    console.log(
      `[yahoo-scan-mcp] version ${APP_VERSION}`
    );

    console.log(
      `[yahoo-scan-mcp] cwd=${CWD}`
    );

    console.log(
      `[yahoo-scan-mcp] app_dir=${APP_DIR}`
    );

    console.log(
      `[yahoo-scan-mcp] tools=${TOOL_NAMES.length}`
    );

    console.log(
      `[yahoo-scan-mcp] registered_tools=${TOOL_NAMES.join(",")}`
    );

    console.log(
      "[yahoo-scan-mcp] universe loading deferred until yahoo_s0_prepare"
    );

    restoreCaches()
      .then(() => {

        console.log(
          "[yahoo-scan-mcp] caches restored"
        );

      })
      .catch(err => {

        console.error(
          "[yahoo-scan-mcp] cache restore warning:",
          err.message
        );
      });
  }
);
