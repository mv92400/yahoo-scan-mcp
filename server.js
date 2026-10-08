/*
============================================================
 Yahoo Scan MCP
 Version 1.9.1
============================================================

Architecture :

S0
- Universe NASDAQ / ordinary stocks
- Price < $5
- Historique 15m
- J-1 / J-2 FILTER EXCLUSIVELY HERE
- RVOL15M historical baseline
- Explicit DATE / ASOF
- Anti-lookahead historical cutoff
- Materialized S0

S1
- Intraday 5m
- Explicit DATE / ASOF
- Historical Yahoo Chart via period1 / period2
- Dynamic as-of
- VWAP
- RVOL15M
- Accel5M
- HOD / HOD distance
- STRICT BLOCKING FILTER
- NO J-1 / J-2 FILTER
- Anti-leak validation
- Explicit rejection reasons
- Materialized S1

SF
- Works ONLY from validated S1
- Explicit DATE / ASOF
- Default 15:59 NY
- Revalidates configured intraday thresholds
- Strict blocking filter
- Winner Gate

Transport
- Streamable HTTP
- Stateless
- 11 MCP tools

IMPORTANT
- All market timestamps are interpreted in America/New_York.
- Historical replay uses explicit date/asof.
- No stage may use data after its own asof.
- Historical S1 NEVER uses range=1d.
- Historical S1 uses period1/period2 + 5m.
============================================================
*/

import express from "express";
import fs from "fs";
import path from "path";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { z } from "zod";


/* =========================================================
   CONFIG
========================================================= */

const APP_VERSION = "1.9.1";

const PORT =
  Number(process.env.PORT || 8080);

const DATA_CONCURRENCY = 8;
const RETRIES = 3;
const REQUEST_TIMEOUT_MS = 15000;
const RETRY_DELAY_MS = 600;

const S0_RANGE = "1mo";
const S0_INTERVAL = "15m";

const S1_INTERVAL = "5m";

const MARKET_TIMEZONE =
  "America/New_York";


/* =========================================================
   STRATEGY THRESHOLDS
========================================================= */

const MAX_J1_J2_PCT = 5;

const S1_MIN_PRICE_VS_VWAP_PCT = -1;
const S1_MIN_RVOL15M = 1.30;
const S1_MIN_ACCEL5M = 20;

const MIN_S0_HISTORICAL_SESSIONS = 5;
const MIN_S0_VALID_VOLUME_BARS = 20;
const MIN_RVOL_BASELINE_SESSIONS = 5;
const MIN_S1_COMPLETED_BARS = 4;


/* =========================================================
   CACHE
========================================================= */

const CACHE_DIR =
  process.env.DATA_DIR ||
  process.cwd();

const UNIVERSE_FILE =
  path.join(
    CACHE_DIR,
    "universe_s0.txt"
  );

const S0_CACHE_FILE =
  path.join(
    CACHE_DIR,
    "s0_materialized.json"
  );

const S1_CACHE_FILE =
  path.join(
    CACHE_DIR,
    "s1_materialized.json"
  );


/* =========================================================
   GLOBAL STATE
========================================================= */

const scanState = {

  version:
    APP_VERSION,

  asof:
    null,

  date:
    null,

  s0_asof:
    null,

  s1_asof:
    null,

  sf_asof:
    null,

  elapsed_ms:
    0,

  running:
    false,

  stage:
    null,

  universe_requested:
    0,

  s0_count:
    0,

  s1_count:
    0,

  sf_count:
    0,

  s0:
    [],

  s1:
    [],

  sf:
    [],

  winner:
    null,

  yahoo_errors:
    0,

  last_error:
    null

};


/* =========================================================
   UTILS
========================================================= */

function nowIso() {

  return new Date()
    .toISOString();

}


function sleep(ms) {

  return new Promise(
    resolve =>
      setTimeout(
        resolve,
        ms
      )
  );

}


function isFiniteNumber(value) {

  return (
    typeof value === "number" &&
    Number.isFinite(value)
  );

}


function median(values) {

  const arr =
    values
      .filter(
        value =>
          Number.isFinite(value)
      )
      .sort(
        (a, b) =>
          a - b
      );

  if (!arr.length) {
    return null;
  }

  const middle =
    Math.floor(
      arr.length / 2
    );

  return (
    arr.length % 2
      ? arr[middle]
      : (
          arr[middle - 1] +
          arr[middle]
        ) / 2
  );

}


function normalizeSymbols(symbols) {

  return [
    ...new Set(
      (
        Array.isArray(symbols)
          ? symbols
          : []
      )
        .map(
          s =>
            String(s)
              .trim()
              .toUpperCase()
        )
        .filter(Boolean)
        .filter(
          s =>
            /^[A-Z0-9.\-]+$/
              .test(s)
        )
    )
  ];

}


/* =========================================================
   ASOF / NEW YORK TIME
========================================================= */

/*
------------------------------------------------------------
nyParts

Return date/time components in America/New_York.
------------------------------------------------------------
*/

function nyParts(ts) {

  const date =
    new Date(ts);

  if (
    !Number.isFinite(
      date.getTime()
    )
  ) {
    return null;
  }

  const parts =
    new Intl.DateTimeFormat(
      "en-US",
      {
        timeZone:
          MARKET_TIMEZONE,

        year:
          "numeric",

        month:
          "2-digit",

        day:
          "2-digit",

        hour:
          "2-digit",

        minute:
          "2-digit",

        second:
          "2-digit",

        hourCycle:
          "h23"
      }
    )
      .formatToParts(date);

  const result = {};

  for (const part of parts) {

    if (
      part.type !==
      "literal"
    ) {

      result[part.type] =
        part.value;

    }

  }

  return result;

}


/*
------------------------------------------------------------
Timezone offset at a UTC timestamp.

This is DST-aware.
------------------------------------------------------------
*/

function nyOffsetMs(ts) {

  const p =
    nyParts(ts);

  if (!p) {
    return null;
  }

  const reconstructedUTC =
    Date.UTC(
      Number(p.year),
      Number(p.month) - 1,
      Number(p.day),
      Number(p.hour),
      Number(p.minute),
      Number(p.second)
    );

  return (
    reconstructedUTC -
    new Date(ts).getTime()
  );

}


/*
------------------------------------------------------------
Convert a LOCAL New York datetime into UTC.

Example:

2026-09-18T15:59:00
        =>
2026-09-18T19:59:00Z

DST is determined automatically.
------------------------------------------------------------
*/

function nyLocalDateTimeToMs(
  localDateTime
) {

  const match =
    String(localDateTime)
      .trim()
      .match(
        /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,3}))?)?$/
      );

  if (!match) {
    return NaN;
  }

  const year =
    Number(match[1]);

  const month =
    Number(match[2]);

  const day =
    Number(match[3]);

  const hour =
    Number(match[4]);

  const minute =
    Number(match[5]);

  const second =
    Number(match[6] || 0);

  const millisecond =
    Number(
      (
        String(match[7] || "0") +
        "00"
      ).slice(0, 3)
    );

  const baseUTC =
    Date.UTC(
      year,
      month - 1,
      day,
      hour,
      minute,
      second,
      millisecond
    );

  let guess =
    baseUTC;

  /*
  ----------------------------------------------------------
  Iterate because the timezone offset depends on the date.
  ----------------------------------------------------------
  */

  for (
    let i = 0;
    i < 4;
    i++
  ) {

    const offset =
      nyOffsetMs(
        guess
      );

    if (
      !Number.isFinite(offset)
    ) {
      return NaN;
    }

    guess =
      baseUTC -
      offset;

  }

  return guess;

}


/*
------------------------------------------------------------
Next calendar date.

Pure calendar operation, independent of timezone.
------------------------------------------------------------
*/

function nextCalendarDate(
  date
) {

  const match =
    String(date)
      .match(
        /^(\d{4})-(\d{2})-(\d{2})$/
      );

  if (!match) {
    return null;
  }

  const value =
    new Date(
      Date.UTC(
        Number(match[1]),
        Number(match[2]) - 1,
        Number(match[3]) + 1
      )
    );

  return value
    .toISOString()
    .slice(0, 10);

}


/*
------------------------------------------------------------
parseAsOf

Accepts:
- ISO timestamp with timezone
- ISO timestamp ending Z
- ISO timestamp WITHOUT timezone
- Date-only YYYY-MM-DD
- null => current time

IMPORTANT:
A timestamp WITHOUT timezone is explicitly interpreted as
America/New_York.

Examples:

2026-09-18
2026-09-18T15:55:00
2026-09-18T15:55:00-04:00
2026-09-18T19:55:00Z
------------------------------------------------------------
*/

function parseAsOf(
  value,
  defaultHour = 15,
  defaultMinute = 55
) {

  if (
    value === null ||
    value === undefined ||
    value === ""
  ) {

    const currentMs =
      Date.now();

    return {

      date:
        nyDateKey(
          currentMs
        ),

      iso:
        new Date(
          currentMs
        ).toISOString(),

      ms:
        currentMs

    };

  }

  const raw =
    String(value)
      .trim();

  /*
  ----------------------------------------------------------
  DATE ONLY
  ----------------------------------------------------------
  */

  if (
    /^\d{4}-\d{2}-\d{2}$/
      .test(raw)
  ) {

    const local =
      `${raw}T` +
      `${String(defaultHour).padStart(2, "0")}:` +
      `${String(defaultMinute).padStart(2, "0")}:00`;

    const ms =
      nyLocalDateTimeToMs(
        local
      );

    if (
      !Number.isFinite(ms)
    ) {

      throw new Error(
        `Invalid New York date: ${raw}`
      );

    }

    return {

      date:
        raw,

      iso:
        new Date(ms)
          .toISOString(),

      ms

    };

  }

  /*
  ----------------------------------------------------------
  TIMESTAMP WITHOUT EXPLICIT TIMEZONE
  ----------------------------------------------------------
  */

  const hasExplicitTimezone =
    /(?:Z|[+\-]\d{2}:\d{2})$/i
      .test(raw);

  let ms;

  if (
    !hasExplicitTimezone
  ) {

    /*
    --------------------------------------------------------
    IMPORTANT FIX:
    JavaScript Date.parse() would interpret a timezone-less
    ISO timestamp using the server timezone.

    Railway may be UTC.

    Therefore we explicitly interpret it as NY time.
    --------------------------------------------------------
    */

    ms =
      nyLocalDateTimeToMs(
        raw
      );

  } else {

    ms =
      Date.parse(
        raw
      );

  }

  if (
    !Number.isFinite(ms)
  ) {

    throw new Error(
      `Invalid asof: ${raw}`
    );

  }

  const date =
    nyDateKey(
      ms
    );

  if (!date) {

    throw new Error(
      `Unable to determine NY market date for asof: ${raw}`
    );

  }

  return {

    date,

    iso:
      new Date(ms)
        .toISOString(),

    ms

  };

}


/*
------------------------------------------------------------
Resolve stage ASOF.
------------------------------------------------------------
*/

function resolveStageAsOf(
  asof,
  date,
  defaultHour,
  defaultMinute
) {

  if (
    asof !== undefined &&
    asof !== null &&
    String(asof).trim() !== ""
  ) {

    return parseAsOf(
      asof,
      defaultHour,
      defaultMinute
    );

  }

  if (
    date !== undefined &&
    date !== null &&
    String(date).trim() !== ""
  ) {

    return parseAsOf(
      String(date).trim(),
      defaultHour,
      defaultMinute
    );

  }

  return parseAsOf(
    null,
    defaultHour,
    defaultMinute
  );

}


function nyDateKey(ts) {

  const p =
    nyParts(ts);

  return p
    ? `${p.year}-${p.month}-${p.day}`
    : null;

}


function nyTimeKey(ts) {

  const p =
    nyParts(ts);

  if (!p) {
    return null;
  }

  const hour =
    Number(p.hour);

  const minute =
    Number(p.minute);

  if (
    hour < 9 ||
    (
      hour === 9 &&
      minute < 30
    )
  ) {
    return null;
  }

  if (
    hour > 16 ||
    (
      hour === 16 &&
      minute > 0
    )
  ) {
    return null;
  }

  return (
    `${p.hour}:${p.minute}`
  );

}


function nyMinutes(ts) {

  const p =
    nyParts(ts);

  if (!p) {
    return null;
  }

  return (
    Number(p.hour) * 60 +
    Number(p.minute)
  );

}


function isRegularSessionBar(ts) {

  const minutes =
    nyMinutes(ts);

  return (
    minutes !== null &&
    minutes >= 570 &&
    minutes <= 960
  );

}


/* =========================================================
   ORDINARY STOCK FILTER
========================================================= */

function isOrdinaryStock(
  symbol,
  item
) {

  if (
    !symbol ||
    typeof symbol !== "string"
  ) {
    return false;
  }

  const meta =
    item?.response?.[0]?.meta ||
    item?.meta ||
    {};

  const quoteType =
    String(
      meta.quoteType || ""
    )
      .toUpperCase();

  const exchange =
    String(
      meta.exchangeName || ""
    )
      .toUpperCase();

  const fullExchange =
    String(
      meta.fullExchangeName || ""
    )
      .toUpperCase();

  if (
    quoteType &&
    quoteType !== "EQUITY"
  ) {
    return false;
  }

  const nasdaq =
    exchange === "NMS" ||
    exchange === "NGM" ||
    exchange === "NCM" ||
    fullExchange.includes(
      "NASDAQ"
    );

  return nasdaq;

}


/* =========================================================
   YAHOO CHART
========================================================= */

async function fetchWithTimeout(
  url
) {

  const controller =
    new AbortController();

  const timer =
    setTimeout(
      () =>
        controller.abort(),
      REQUEST_TIMEOUT_MS
    );

  try {

    return await fetch(
      url,
      {
        signal:
          controller.signal,

        headers: {
          "User-Agent":
            "Mozilla/5.0 Yahoo-Scan-MCP"
        }
      }
    );

  } finally {

    clearTimeout(timer);

  }

}


/*
------------------------------------------------------------
Yahoo Chart using RANGE.

Used by:
- S0
- yahoo_spark_test

NOT used by historical S1.
------------------------------------------------------------
*/

async function yahooChart(
  symbol,
  range,
  interval
) {

  const url =
    "https://query1.finance.yahoo.com/v8/finance/chart/" +
    encodeURIComponent(symbol) +
    `?range=${encodeURIComponent(range)}` +
    `&interval=${encodeURIComponent(interval)}` +
    "&includePrePost=false" +
    "&events=div%2Csplits";

  let lastError =
    null;

  for (
    let attempt = 1;
    attempt <= RETRIES;
    attempt++
  ) {

    try {

      const response =
        await fetchWithTimeout(
          url
        );

      if (!response.ok) {

        throw new Error(
          `Yahoo HTTP ${response.status}`
        );

      }

      const json =
        await response.json();

      if (
        !json?.chart?.result?.length
      ) {

        throw new Error(
          "Yahoo Chart result absent"
        );

      }

      return {

        symbol,

        response:
          json.chart.result

      };

    } catch (error) {

      lastError =
        error;

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

  throw (
    lastError ||
    new Error(
      "Yahoo Chart failed"
    )
  );

}


/*
------------------------------------------------------------
Yahoo Chart HISTORICAL PERIOD.

CRITICAL S1 FIX.

Historical S1 MUST use:
period1 + period2 + interval=5m

and MUST NOT use:
range=1d

period1 / period2 are Unix seconds.
------------------------------------------------------------
*/

async function yahooChartPeriod(
  symbol,
  period1Ms,
  period2Ms,
  interval
) {

  const period1 =
    Math.floor(
      period1Ms / 1000
    );

  const period2 =
    Math.floor(
      period2Ms / 1000
    );

  const url =
    "https://query1.finance.yahoo.com/v8/finance/chart/" +
    encodeURIComponent(symbol) +
    `?period1=${period1}` +
    `&period2=${period2}` +
    `&interval=${encodeURIComponent(interval)}` +
    "&includePrePost=false" +
    "&events=div%2Csplits";

  let lastError =
    null;

  for (
    let attempt = 1;
    attempt <= RETRIES;
    attempt++
  ) {

    try {

      const response =
        await fetchWithTimeout(
          url
        );

      if (!response.ok) {

        throw new Error(
          `Yahoo HTTP ${response.status}`
        );

      }

      const json =
        await response.json();

      if (
        !json?.chart?.result?.length
      ) {

        throw new Error(
          "Yahoo Chart historical result absent"
        );

      }

      return {

        symbol,

        response:
          json.chart.result,

        period1,

        period2

      };

    } catch (error) {

      lastError =
        error;

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

  throw (
    lastError ||
    new Error(
      "Yahoo historical Chart failed"
    )
  );

}


/*
------------------------------------------------------------
Historical session window.

Example:

2026-09-18
period1 = 2026-09-18 00:00 NY
period2 = 2026-09-19 00:00 NY

This automatically respects DST.
------------------------------------------------------------
*/

function getNySessionWindow(
  marketDate
) {

  const nextDate =
    nextCalendarDate(
      marketDate
    );

  if (!nextDate) {

    throw new Error(
      `Invalid market date: ${marketDate}`
    );

  }

  const period1Ms =
    nyLocalDateTimeToMs(
      `${marketDate}T00:00:00`
    );

  const period2Ms =
    nyLocalDateTimeToMs(
      `${nextDate}T00:00:00`
    );

  if (
    !Number.isFinite(period1Ms) ||
    !Number.isFinite(period2Ms) ||
    period2Ms <= period1Ms
  ) {

    throw new Error(
      `Unable to create NY session window for ${marketDate}`
    );

  }

  return {

    period1Ms,

    period2Ms,

    period1:
      Math.floor(
        period1Ms / 1000
      ),

    period2:
      Math.floor(
        period2Ms / 1000
      )

  };

}


/* =========================================================
   BATCH DATA LOADER
========================================================= */

async function yahooChartBatch(
  symbols,
  range,
  interval,
  options = {}
) {

  const results = {};

  let errors = 0;

  let cursor = 0;

  const historical =
    options.historical === true;

  const period1Ms =
    options.period1Ms;

  const period2Ms =
    options.period2Ms;

  async function worker() {

    while (true) {

      const index =
        cursor++;

      if (
        index >=
        symbols.length
      ) {
        return;
      }

      const symbol =
        symbols[index];

      try {

        if (
          historical
        ) {

          results[symbol] =
            await yahooChartPeriod(
              symbol,
              period1Ms,
              period2Ms,
              interval
            );

        } else {

          results[symbol] =
            await yahooChart(
              symbol,
              range,
              interval
            );

        }

      } catch (error) {

        errors++;

        results[symbol] = {

          symbol,

          error:
            String(
              error?.message ||
              error
            ),

          response:
            []

        };

      }

    }

  }

  const workers =
    Math.min(
      DATA_CONCURRENCY,
      symbols.length
    );

  if (
    workers > 0
  ) {

    await Promise.all(
      Array.from(
        {
          length:
            workers
        },
        () =>
          worker()
      )
    );

  }

  return {
    results,
    errors
  };

}


/* =========================================================
   BAR EXTRACTION
========================================================= */

function extractBars(item) {

  const response =
    Array.isArray(
      item?.response
    )
      ? item.response[0]
      : (
          item?.response ||
          item
        );

  if (!response) {
    return [];
  }

  const timestamps =
    Array.isArray(
      response.timestamp
    )
      ? response.timestamp
      : [];

  const quote =
    response
      ?.indicators
      ?.quote?.[0] ||
    {};

  const closes =
    Array.isArray(
      quote.close
    )
      ? quote.close
      : [];

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

  const bars = [];

  for (
    let i = 0;
    i < timestamps.length;
    i++
  ) {

    const timestamp =
      Number(
        timestamps[i]
      );

    if (
      !Number.isFinite(
        timestamp
      )
    ) {
      continue;
    }

    const close =
      Number(
        closes[i]
      );

    if (
      !Number.isFinite(
        close
      ) ||
      close <= 0
    ) {
      continue;
    }

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

      ts:
        timestamp * 1000,

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

      close,

      volume:
        Number.isFinite(volume) &&
        volume >= 0
          ? volume
          : null

    });

  }

  return bars.sort(
    (a, b) =>
      a.ts - b.ts
  );

}


/* =========================================================
   ASOF BAR FILTER
========================================================= */

function filterBarsAsOf(
  bars,
  asofMs
) {

  return bars.filter(
    bar =>
      Number.isFinite(bar.ts) &&
      bar.ts <= asofMs + 1000
  );

}


/*
------------------------------------------------------------
Yahoo intraday timestamps represent candle START.

A 5m candle is completed only if:

start + 5min <= ASOF
------------------------------------------------------------
*/

function filterCompletedBarsAsOf(
  bars,
  asofMs,
  intervalMinutes
) {

  const durationMs =
    intervalMinutes *
    60 *
    1000;

  return bars.filter(
    bar =>
      Number.isFinite(bar.ts) &&
      bar.ts + durationMs <=
        asofMs + 1000
  );

}


/* =========================================================
   SESSION HELPERS
========================================================= */

function getSessionBars(
  bars
) {

  const sessions =
    new Map();

  for (const bar of bars) {

    if (
      !isRegularSessionBar(
        bar.ts
      )
    ) {
      continue;
    }

    const date =
      nyDateKey(
        bar.ts
      );

    if (!date) {
      continue;
    }

    if (
      !sessions.has(date)
    ) {

      sessions.set(
        date,
        []
      );

    }

    sessions
      .get(date)
      .push(bar);

  }

  return sessions;

}


function getCurrentSessionBars(
  bars,
  asofMs = Date.now()
) {

  const sessions =
    getSessionBars(
      filterBarsAsOf(
        bars,
        asofMs
      )
    );

  const dates =
    [
      ...sessions.keys()
    ]
      .sort();

  return dates.length
    ? (
        sessions.get(
          dates[
            dates.length - 1
          ]
        ) || []
      )
    : [];

}


function sessionPerformance(
  bars
) {

  if (
    !bars ||
    bars.length < 2
  ) {
    return null;
  }

  const first =
    bars.find(
      b =>
        isFiniteNumber(
          b.open
        ) &&
        b.open > 0
    );

  const last =
    [...bars]
      .reverse()
      .find(
        b =>
          isFiniteNumber(
            b.close
          )
      );

  if (
    !first ||
    !last
  ) {
    return null;
  }

  return (
    (
      last.close /
      first.open -
      1
    ) * 100
  );

}


/* =========================================================
   RVOL BASELINE
========================================================= */

function buildRvol15mBaseline(
  completedSessions
) {

  const slotValues =
    new Map();

  for (
    const session of
    completedSessions
  ) {

    const slotVolumes =
      new Map();

    for (
      const bar of
      session
    ) {

      const key =
        nyTimeKey(
          bar.ts
        );

      const volume =
        Number(
          bar.volume
        );

      if (
        !key ||
        !Number.isFinite(
          volume
        ) ||
        volume <= 0
      ) {
        continue;
      }

      slotVolumes.set(
        key,
        (
          slotVolumes.get(key) ||
          0
        ) + volume
      );

    }

    for (
      const [
        key,
        volume
      ] of slotVolumes
    ) {

      if (
        !slotValues.has(key)
      ) {

        slotValues.set(
          key,
          []
        );

      }

      slotValues
        .get(key)
        .push(volume);

    }

  }

  const baseline = {};

  for (
    const [
      key,
      values
    ] of slotValues
  ) {

    const valid =
      values.filter(
        value =>
          Number.isFinite(
            value
          ) &&
          value > 0
      );

    if (
      valid.length >=
      MIN_RVOL_BASELINE_SESSIONS
    ) {

      baseline[key] =
        median(valid);

    }

  }

  return baseline;

}


/* =========================================================
   S0 J-1 / J-2 FILTER
========================================================= */

function passesS0HistoricalFilter(
  j1Pct,
  j2Pct
) {

  if (
    !Number.isFinite(j1Pct) ||
    !Number.isFinite(j2Pct)
  ) {
    return false;
  }

  if (
    Math.abs(j1Pct) >
    MAX_J1_J2_PCT
  ) {
    return false;
  }

  if (
    Math.abs(j2Pct) >
    MAX_J1_J2_PCT
  ) {
    return false;
  }

  return true;

}


/* =========================================================
   S0
========================================================= */

function calculateS0(
  symbol,
  item,
  asofMs
) {

  if (
    !isOrdinaryStock(
      symbol,
      item
    )
  ) {
    return null;
  }

  const rawBars =
    extractBars(
      item
    );

  const bars =
    filterBarsAsOf(
      rawBars,
      asofMs
    );

  if (
    bars.length <
    MIN_S0_VALID_VOLUME_BARS
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
    ]
      .sort();

  if (
    dates.length <
    MIN_S0_HISTORICAL_SESSIONS + 1
  ) {
    return null;
  }

  const currentDate =
    nyDateKey(
      asofMs
    );

  if (
    !currentDate ||
    !sessions.has(
      currentDate
    )
  ) {
    return null;
  }

  const currentSession =
    sessions.get(
      currentDate
    ) || [];

  const completedDates =
    dates.filter(
      date =>
        date <
        currentDate
    );

  if (
    completedDates.length <
    MIN_S0_HISTORICAL_SESSIONS
  ) {
    return null;
  }

  const completedSessions =
    completedDates
      .map(
        date =>
          sessions.get(date)
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
        bar =>
          isFiniteNumber(
            bar.close
          )
      );

  if (!currentLast) {
    return null;
  }

  const price =
    currentLast.close;

  if (
    !(price > 0 && price < 5)
  ) {
    return null;
  }

  const validVolumeBars =
    bars.filter(
      bar =>
        Number.isFinite(
          bar.volume
        ) &&
        bar.volume > 0
    );

  if (
    validVolumeBars.length <
    MIN_S0_VALID_VOLUME_BARS
  ) {
    return null;
  }

  const j1Pct =
    sessionPerformance(
      previous
    );

  const j2Pct =
    sessionPerformance(
      previous2
    );

  if (
    !passesS0HistoricalFilter(
      j1Pct,
      j2Pct
    )
  ) {
    return null;
  }

  const rvol15mBaseline =
    buildRvol15mBaseline(
      completedSessions
    );

  if (
    Object.keys(
      rvol15mBaseline
    ).length < 10
  ) {
    return null;
  }

  return {

    symbol,

    price,

    j1_pct:
      j1Pct,

    j2_pct:
      j2Pct,

    historical_sessions:
      completedSessions.length,

    current_session_bars:
      currentSession.length,

    valid_volume_bars:
      validVolumeBars.length,

    rvol15m_baseline:
      rvol15mBaseline,

    rvol_baseline_sessions:
      completedSessions.length,

    asof:
      new Date(
        asofMs
      ).toISOString(),

    market_date:
      currentDate,

    prepared_at:
      nowIso()

  };

}


/* =========================================================
   S1 METRICS
========================================================= */

function calculateVWAP(
  bars
) {

  let pv = 0;
  let volume = 0;

  for (
    const bar of bars
  ) {

    if (
      !Number.isFinite(
        bar.volume
      ) ||
      bar.volume <= 0
    ) {
      continue;
    }

    const typical =
      (
        (
          Number.isFinite(
            bar.high
          )
            ? bar.high
            : bar.close
        ) +
        (
          Number.isFinite(
            bar.low
          )
            ? bar.low
            : bar.close
        ) +
        bar.close
      ) / 3;

    pv +=
      typical *
      bar.volume;

    volume +=
      bar.volume;

  }

  return volume > 0
    ? pv / volume
    : null;

}


/*
------------------------------------------------------------
calculateS1

Returns metrics or null.
------------------------------------------------------------
*/

function calculateS1(
  symbol,
  item,
  s0Record,
  asofMs
) {

  if (!s0Record) {
    return null;
  }

  const bars =
    extractBars(
      item
    );

  const eligibleBars =
    filterBarsAsOf(
      bars,
      asofMs
    );

  const completedEligible =
    filterCompletedBarsAsOf(
      eligibleBars,
      asofMs,
      5
    );

  const currentSession =
    getCurrentSessionBars(
      completedEligible,
      asofMs
    );

  const requestedDate =
    nyDateKey(
      asofMs
    );

  if (
    !requestedDate
  ) {
    return null;
  }

  const sessionDate =
    currentSession.length
      ? nyDateKey(
          currentSession[
            currentSession.length - 1
          ].ts
        )
      : null;

  if (
    sessionDate !==
    requestedDate
  ) {
    return null;
  }

  if (
    currentSession.length <
    MIN_S1_COMPLETED_BARS
  ) {
    return null;
  }

  const valid =
    currentSession.filter(
      bar =>
        Number.isFinite(
          bar.volume
        ) &&
        bar.volume > 0
    );

  if (
    valid.length <
    MIN_S1_COMPLETED_BARS
  ) {
    return null;
  }

  const last =
    valid[
      valid.length - 1
    ];

  const last5 =
    valid.slice(-1);

  const last10 =
    valid.slice(-2);

  const last15 =
    valid.slice(-3);

  const volume5m =
    last5.reduce(
      (sum, bar) =>
        sum + bar.volume,
      0
    );

  const volume10m =
    last10.reduce(
      (sum, bar) =>
        sum + bar.volume,
      0
    );

  const volume15m =
    last15.reduce(
      (sum, bar) =>
        sum + bar.volume,
      0
    );

  const slotKey =
    nyTimeKey(
      last.ts
    );

  const baseline =
    s0Record
      .rvol15m_baseline
      ?.[slotKey];

  const rvol15m =
    Number.isFinite(
      baseline
    ) &&
    baseline > 0
      ? volume15m / baseline
      : null;

  const previous5 =
    valid.length >= 2
      ? valid[
          valid.length - 2
        ].volume
      : null;

  const accel5m =
    Number.isFinite(
      previous5
    ) &&
    previous5 > 0
      ? (
          last.volume /
          previous5 -
          1
        ) * 100
      : null;

  const vwap =
    calculateVWAP(
      valid
    );

  const price =
    last.close;

  const priceVsVWAP =
    Number.isFinite(
      vwap
    ) &&
    vwap > 0
      ? (
          price /
          vwap -
          1
        ) * 100
      : null;

  const hod =
    Math.max(
      ...valid.map(
        bar =>
          Number.isFinite(
            bar.high
          )
            ? bar.high
            : bar.close
      )
    );

  const hodDistance =
    Number.isFinite(
      hod
    ) &&
    hod > 0
      ? (
          price /
          hod -
          1
        ) * 100
      : null;

  const previousClose =
    valid.length >= 2
      ? valid[
          valid.length - 2
        ].close
      : null;

  const change5m =
    Number.isFinite(
      previousClose
    ) &&
    previousClose > 0
      ? (
          price /
          previousClose -
          1
        ) * 100
      : null;

  const tenAgo =
    valid.length >= 3
      ? valid[
          valid.length - 3
        ].close
      : null;

  const change10m =
    Number.isFinite(
      tenAgo
    ) &&
    tenAgo > 0
      ? (
          price /
          tenAgo -
          1
        ) * 100
      : null;

  return {

    symbol,

    price,

    ts:
      last.ts,

    asof:
      new Date(
        asofMs
      ).toISOString(),

    market_date:
      requestedDate,

    session_date:
      nyDateKey(
        last.ts
      ),

    last_bar_ny:
      nyTimeKey(
        last.ts
      ),

    volume5m,

    volume10m,

    volume15m,

    rvol15m,

    rvol15m_baseline:
      baseline,

    accel5m,

    vwap,

    price_vs_vwap_pct:
      priceVsVWAP,

    hod,

    hod_distance_pct:
      hodDistance,

    change5m_pct:
      change5m,

    change10m_pct:
      change10m

  };

}


/* =========================================================
   S1 REJECTION REASON
========================================================= */

function getS1RejectionReason(
  symbol,
  item,
  s0Record,
  asofMs
) {

  if (!s0Record) {

    return {
      symbol,
      reason:
        "not_in_s0"
    };

  }

  const bars =
    extractBars(
      item
    );

  if (!bars.length) {

    return {
      symbol,
      reason:
        "no_bars"
    };

  }

  const eligibleBars =
    filterBarsAsOf(
      bars,
      asofMs
    );

  if (!eligibleBars.length) {

    return {
      symbol,
      reason:
        "no_bars_before_asof"
    };

  }

  const completed =
    filterCompletedBarsAsOf(
      eligibleBars,
      asofMs,
      5
    );

  if (!completed.length) {

    return {
      symbol,
      reason:
        "no_completed_bars"
    };

  }

  const session =
    getCurrentSessionBars(
      completed,
      asofMs
    );

  const requestedDate =
    nyDateKey(
      asofMs
    );

  if (!session.length) {

    return {
      symbol,
      reason:
        "no_current_session"
    };

  }

  const sessionDate =
    nyDateKey(
      session[
        session.length - 1
      ].ts
    );

  if (
    sessionDate !==
    requestedDate
  ) {

    return {
      symbol,
      reason:
        "wrong_session_date",

      requested_date:
        requestedDate,

      received_session_date:
        sessionDate

    };

  }

  if (
    session.length <
    MIN_S1_COMPLETED_BARS
  ) {

    return {
      symbol,
      reason:
        "insufficient_completed_bars",

      completed_bars:
        session.length,

      required:
        MIN_S1_COMPLETED_BARS

    };

  }

  const valid =
    session.filter(
      bar =>
        Number.isFinite(
          bar.volume
        ) &&
        bar.volume > 0
    );

  if (
    valid.length <
    MIN_S1_COMPLETED_BARS
  ) {

    return {
      symbol,
      reason:
        "insufficient_valid_volume_bars",

      valid_volume_bars:
        valid.length,

      required:
        MIN_S1_COMPLETED_BARS

    };

  }

  const record =
    calculateS1(
      symbol,
      item,
      s0Record,
      asofMs
    );

  if (!record) {

    return {
      symbol,
      reason:
        "metric_calculation_failed"
    };

  }

  if (
    !Number.isFinite(
      record.price_vs_vwap_pct
    )
  ) {

    return {
      symbol,
      reason:
        "invalid_vwap"
    };

  }

  if (
    record.price_vs_vwap_pct <
    S1_MIN_PRICE_VS_VWAP_PCT
  ) {

    return {
      symbol,
      reason:
        "below_vwap",

      price_vs_vwap_pct:
        record.price_vs_vwap_pct,

      minimum:
        S1_MIN_PRICE_VS_VWAP_PCT

    };

  }

  if (
    !Number.isFinite(
      record.rvol15m
    )
  ) {

    return {
      symbol,
      reason:
        "missing_rvol_baseline",

      slot:
        record.last_bar_ny

    };

  }

  if (
    record.rvol15m <
    S1_MIN_RVOL15M
  ) {

    return {
      symbol,
      reason:
        "below_rvol",

      rvol15m:
        record.rvol15m,

      minimum:
        S1_MIN_RVOL15M

    };

  }

  if (
    !Number.isFinite(
      record.accel5m
    )
  ) {

    return {
      symbol,
      reason:
        "invalid_accel5m"
    };

  }

  if (
    record.accel5m <
    S1_MIN_ACCEL5M
  ) {

    return {
      symbol,
      reason:
        "below_accel5m",

      accel5m:
        record.accel5m,

      minimum:
        S1_MIN_ACCEL5M

    };

  }

  return {
    symbol,
    reason:
      "unknown"
  };

}


/* =========================================================
   S1 STRICT FILTER
========================================================= */

function passesS1Filters(
  record
) {

  if (!record) {
    return false;
  }

  if (
    !Number.isFinite(
      record.price_vs_vwap_pct
    )
  ) {
    return false;
  }

  if (
    record.price_vs_vwap_pct <
    S1_MIN_PRICE_VS_VWAP_PCT
  ) {
    return false;
  }

  if (
    !Number.isFinite(
      record.rvol15m
    )
  ) {
    return false;
  }

  if (
    record.rvol15m <
    S1_MIN_RVOL15M
  ) {
    return false;
  }

  if (
    !Number.isFinite(
      record.accel5m
    )
  ) {
    return false;
  }

  if (
    record.accel5m <
    S1_MIN_ACCEL5M
  ) {
    return false;
  }

  return true;

}


/* =========================================================
   S1 ANTI-LEAK VALIDATION
========================================================= */

function validateS1NoLeak(
  records,
  asofMs = null
) {

  for (
    const record of records
  ) {

    if (
      !passesS1Filters(
        record
      )
    ) {

      throw new Error(
        `S1 FILTER LEAK DETECTED: ${record.symbol}`
      );

    }

    if (
      Number.isFinite(asofMs) &&
      Number.isFinite(record.ts) &&
      record.ts + 5 * 60 * 1000 >
      asofMs + 1000
    ) {

      throw new Error(
        `S1 COMPLETION/ASOF LEAK DETECTED: ${record.symbol}`
      );

    }

    if (
      Number.isFinite(asofMs) &&
      record.asof
    ) {

      const recordAsOfMs =
        Date.parse(
          record.asof
        );

      if (
        Number.isFinite(recordAsOfMs) &&
        Math.abs(
          recordAsOfMs -
          asofMs
        ) > 1000
      ) {

        throw new Error(
          `S1 ASOF MISMATCH: ${record.symbol}`
        );

      }

    }

    if (
      Number.isFinite(asofMs) &&
      record.market_date !==
        nyDateKey(asofMs)
    ) {

      throw new Error(
        `S1 MARKET DATE LEAK: ${record.symbol}`
      );

    }

  }

  return true;

}


/* =========================================================
   RANKING
========================================================= */

function rankS1(
  records
) {

  return [
    ...records
  ]
    .sort(
      (a, b) => {

        const ar =
          Number.isFinite(
            a.rvol15m
          )
            ? a.rvol15m
            : -Infinity;

        const br =
          Number.isFinite(
            b.rvol15m
          )
            ? b.rvol15m
            : -Infinity;

        if (
          br !== ar
        ) {
          return br - ar;
        }

        const aa =
          Number.isFinite(
            a.accel5m
          )
            ? a.accel5m
            : -Infinity;

        const ba =
          Number.isFinite(
            b.accel5m
          )
            ? b.accel5m
            : -Infinity;

        if (
          ba !== aa
        ) {
          return ba - aa;
        }

        const av =
          Number.isFinite(
            a.price_vs_vwap_pct
          )
            ? a.price_vs_vwap_pct
            : -Infinity;

        const bv =
          Number.isFinite(
            b.price_vs_vwap_pct
          )
            ? b.price_vs_vwap_pct
            : -Infinity;

        if (
          bv !== av
        ) {
          return bv - av;
        }

        const ah =
          Number.isFinite(
            a.hod_distance_pct
          )
            ? a.hod_distance_pct
            : -Infinity;

        const bh =
          Number.isFinite(
            b.hod_distance_pct
          )
            ? b.hod_distance_pct
            : -Infinity;

        return bh - ah;

      }
    );

}


/* =========================================================
   WINNER GATE
========================================================= */

function winnerGate(
  records,
  options = {}
) {

  const minPriceVsVWAP =
    Number.isFinite(
      options.minPriceVsVWAP
    )
      ? options.minPriceVsVWAP
      : S1_MIN_PRICE_VS_VWAP_PCT;

  const minAccel5M =
    Number.isFinite(
      options.minAccel5M
    )
      ? options.minAccel5M
      : S1_MIN_ACCEL5M;

  const minRvol15M =
    Number.isFinite(
      options.minVol15M
    )
      ? options.minVol15M
      : S1_MIN_RVOL15M;

  for (
    const record of records
  ) {

    if (
      !Number.isFinite(
        record.price_vs_vwap_pct
      ) ||
      record.price_vs_vwap_pct <
      minPriceVsVWAP
    ) {
      continue;
    }

    if (
      !Number.isFinite(
        record.accel5m
      ) ||
      record.accel5m <
      minAccel5M
    ) {
      continue;
    }

    if (
      !Number.isFinite(
        record.rvol15m
      ) ||
      record.rvol15m <
      minRvol15M
    ) {
      continue;
    }

    return record;

  }

  return null;

}


/* =========================================================
   CACHE
========================================================= */

function writeJson(
  file,
  data
) {

  fs.writeFileSync(
    file,
    JSON.stringify(
      data,
      null,
      2
    ),
    "utf8"
  );

}


function readJson(
  file
) {

  if (
    !fs.existsSync(
      file
    )
  ) {
    return null;
  }

  try {

    return JSON.parse(
      fs.readFileSync(
        file,
        "utf8"
      )
    );

  } catch {

    return null;

  }

}


function validCache(
  cache,
  expectedStage = null,
  expectedDate = null,
  expectedAsOf = null
) {

  if (
    !cache ||
    cache.version !==
      APP_VERSION
  ) {
    return false;
  }

  if (
    expectedStage &&
    cache.stage !==
      expectedStage
  ) {
    return false;
  }

  if (
    expectedDate &&
    cache.market_date !==
      expectedDate
  ) {
    return false;
  }

  if (
    expectedAsOf &&
    cache.asof !==
      expectedAsOf
  ) {
    return false;
  }

  if (
    !cache.market_date
  ) {
    return false;
  }

  if (
    !cache.asof
  ) {
    return false;
  }

  if (
    !Number.isFinite(
      Number(cache.asof_ms)
    )
  ) {
    return false;
  }

  return true;

}


/* =========================================================
   UNIVERSE
========================================================= */

function readUniverse(
  limit = null
) {

  if (
    !fs.existsSync(
      UNIVERSE_FILE
    )
  ) {

    throw new Error(
      `Missing ${UNIVERSE_FILE}`
    );

  }

  let symbols =
    fs.readFileSync(
      UNIVERSE_FILE,
      "utf8"
    )
      .split(/\r?\n/)
      .map(
        line =>
          line
            .trim()
            .toUpperCase()
      )
      .filter(Boolean)
      .filter(
        symbol =>
          /^[A-Z0-9.\-]+$/
            .test(symbol)
      );

  symbols =
    [
      ...new Set(symbols)
    ];

  if (
    Number.isInteger(limit) &&
    limit > 0
  ) {

    symbols =
      symbols.slice(
        0,
        limit
      );

  }

  return symbols;

}


/* =========================================================
   SCAN S0
========================================================= */

async function runS0(
  symbols,
  options = {}
) {

  if (
    scanState.running
  ) {

    throw new Error(
      "Scan already running"
    );

  }

  const universe =
    normalizeSymbols(
      symbols
    );

  const resolved =
    resolveStageAsOf(
      options.asof,
      options.date,
      15,
      55
    );

  const s0AsOfMs =
    resolved.ms;

  scanState.running =
    true;

  scanState.stage =
    "S0";

  scanState.asof =
    resolved.iso;

  scanState.date =
    resolved.date;

  scanState.s0_asof =
    resolved.iso;

  scanState.last_error =
    null;

  const started =
    Date.now();

  try {

    const {
      results,
      errors
    } =
      await yahooChartBatch(
        universe,
        S0_RANGE,
        S0_INTERVAL
      );

    const s0 = [];

    for (
      const symbol of universe
    ) {

      const record =
        calculateS0(
          symbol,
          results[symbol],
          s0AsOfMs
        );

      if (record) {
        s0.push(record);
      }

    }

    scanState.universe_requested =
      universe.length;

    scanState.s0 =
      s0;

    scanState.s0_count =
      s0.length;

    scanState.yahoo_errors =
      errors;

    scanState.elapsed_ms =
      Date.now() -
      started;

    writeJson(
      S0_CACHE_FILE,
      {

        version:
          APP_VERSION,

        stage:
          "S0",

        market_date:
          resolved.date,

        source:
          "Yahoo Chart OHLCV",

        created_at:
          nowIso(),

        asof:
          resolved.iso,

        asof_ms:
          s0AsOfMs,

        universe_requested:
          universe.length,

        s0_count:
          s0.length,

        s0

      }
    );

    return {

      ok:
        true,

      stage:
        "S0",

      date:
        resolved.date,

      asof:
        resolved.iso,

      asof_ms:
        s0AsOfMs,

      elapsed_ms:
        scanState.elapsed_ms,

      universe_requested:
        universe.length,

      s0_count:
        s0.length,

      s0,

      yahoo_errors:
        errors

    };

  } catch (error) {

    scanState.last_error =
      String(
        error?.message ||
        error
      );

    throw error;

  } finally {

    scanState.running =
      false;

    scanState.stage =
      null;

  }

}


/* =========================================================
   LOAD S0 FOR DATE / ASOF
========================================================= */

function loadS0ForDate(
  date,
  asof = null
) {

  const cache =
    readJson(
      S0_CACHE_FILE
    );

  if (
    !validCache(
      cache,
      "S0",
      date,
      asof
    )
  ) {

    throw new Error(
      asof
        ? `Valid S0 cache required for ${date} / ${asof}`
        : `Valid S0 cache required for ${date}`
    );

  }

  return cache;

}


/* =========================================================
   SCAN S1
========================================================= */

async function runS1(
  symbols,
  options = {}
) {

  const requested =
    normalizeSymbols(
      symbols
    );

  let resolved;

  if (
    options.asof !== undefined &&
    options.asof !== null &&
    String(options.asof).trim() !== ""
  ) {

    resolved =
      resolveStageAsOf(
        options.asof,
        null,
        15,
        55
      );

  } else if (
    options.date !== undefined &&
    options.date !== null &&
    String(options.date).trim() !== ""
  ) {

    resolved =
      resolveStageAsOf(
        null,
        options.date,
        15,
        55
      );

  } else {

    const existingS0 =
      readJson(
        S0_CACHE_FILE
      );

    if (
      existingS0?.market_date &&
      existingS0?.asof
    ) {

      resolved =
        resolveStageAsOf(
          null,
          existingS0.market_date,
          15,
          55
        );

    } else {

      resolved =
        resolveStageAsOf(
          null,
          null,
          15,
          55
        );

    }

  }

  const s1AsOfMs =
    resolved.ms;

  /*
  ----------------------------------------------------------
  S0 ASOF must be exactly the same historical snapshot.

  We deliberately do NOT accept an S0 generated at another
  ASOF for historical S1.
  ----------------------------------------------------------
  */

  const s0Cache =
    loadS0ForDate(
      resolved.date,
      resolved.iso
    );

  const s0Map =
    new Map(
      (
        s0Cache.s0 ||
        []
      )
        .map(
          record =>
            [
              record.symbol,
              record
            ]
        )
    );

  let filtered;

  if (
    requested.length
  ) {

    filtered =
      requested.filter(
        symbol =>
          s0Map.has(
            symbol
          )
      );

  } else {

    filtered =
      (
        s0Cache.s0 ||
        []
      )
        .map(
          record =>
            record.symbol
        );

  }

  if (
    scanState.running
  ) {

    throw new Error(
      "Scan already running"
    );

  }

  scanState.running =
    true;

  scanState.stage =
    "S1";

  scanState.asof =
    resolved.iso;

  scanState.date =
    resolved.date;

  scanState.s1_asof =
    resolved.iso;

  scanState.last_error =
    null;

  const started =
    Date.now();

  try {

    /*
    --------------------------------------------------------
    CRITICAL HISTORICAL S1 FIX
    --------------------------------------------------------

    Fetch ONLY the requested NY calendar day.

    Never use:
      range=1d

    because range=1d means the CURRENT Yahoo day.

    --------------------------------------------------------
    */

    const window =
      getNySessionWindow(
        resolved.date
      );

    const {
      results,
      errors
    } =
      await yahooChartBatch(
        filtered,
        null,
        S1_INTERVAL,
        {

          historical:
            true,

          period1Ms:
            window.period1Ms,

          period2Ms:
            window.period2Ms

        }
      );

    const s1 = [];

    const rejectionReasons = [];

    let rejected =
      0;

    for (
      const symbol of filtered
    ) {

      const record =
        calculateS1(
          symbol,
          results[symbol],
          s0Map.get(symbol),
          s1AsOfMs
        );

      if (
        record &&
        passesS1Filters(
          record
        )
      ) {

        s1.push(record);

      } else {

        rejected++;

        rejectionReasons.push(
          getS1RejectionReason(
            symbol,
            results[symbol],
            s0Map.get(symbol),
            s1AsOfMs
          )
        );

      }

    }

    const ranked =
      rankS1(
        s1
      );

    validateS1NoLeak(
      ranked,
      s1AsOfMs
    );

    scanState.s1 =
      ranked;

    scanState.s1_count =
      ranked.length;

    scanState.yahoo_errors =
      errors;

    scanState.elapsed_ms =
      Date.now() -
      started;

    writeJson(
      S1_CACHE_FILE,
      {

        version:
          APP_VERSION,

        stage:
          "S1",

        market_date:
          resolved.date,

        source:
          "Yahoo Chart OHLCV",

        interval:
          S1_INTERVAL,

        historical:
          true,

        period1:
          window.period1,

        period2:
          window.period2,

        created_at:
          nowIso(),

        asof:
          resolved.iso,

        asof_ms:
          s1AsOfMs,

        s0_asof:
          s0Cache.asof,

        input_count:
          filtered.length,

        rejected_count:
          rejected,

        rejection_reasons:
          rejectionReasons,

        s1:
          ranked

      }
    );

    return {

      ok:
        true,

      stage:
        "S1",

      date:
        resolved.date,

      asof:
        resolved.iso,

      asof_ms:
        s1AsOfMs,

      elapsed_ms:
        scanState.elapsed_ms,

      universe_requested:
        filtered.length,

      s1_input_count:
        filtered.length,

      s1_rejected_count:
        rejected,

      s1_count:
        ranked.length,

      s1:
        ranked,

      rejection_reasons:
        rejectionReasons,

      yahoo_errors:
        errors

    };

  } catch (error) {

    scanState.last_error =
      String(
        error?.message ||
        error
      );

    throw error;

  } finally {

    scanState.running =
      false;

    scanState.stage =
      null;

  }

}


/* =========================================================
   SF
========================================================= */

async function runSF(
  symbols = null,
  options = {}
) {

  const cache =
    readJson(
      S1_CACHE_FILE
    );

  if (
    !validCache(
      cache,
      "S1"
    )
  ) {

    throw new Error(
      "Valid S1 cache required"
    );

  }

  const cacheDate =
    cache.market_date;

  const resolved =
    resolveStageAsOf(
      options.asof,
      options.date ||
        cacheDate,
      15,
      59
    );

  if (
    resolved.date !==
    cacheDate
  ) {

    throw new Error(
      `SF date mismatch: S1 cache=${cacheDate}, SF=${resolved.date}`
    );

  }

  const sfAsOfMs =
    resolved.ms;

  let records =
    Array.isArray(
      cache.s1
    )
      ? cache.s1
      : [];

  validateS1NoLeak(
    records,
    Number(cache.asof_ms)
  );

  if (
    Array.isArray(symbols) &&
    symbols.length
  ) {

    const wanted =
      new Set(
        normalizeSymbols(
          symbols
        )
      );

    records =
      records.filter(
        record =>
          wanted.has(
            record.symbol
          )
      );

  }

  const minPriceVsVWAP =
    Number.isFinite(
      options.minPriceVsVWAP
    )
      ? options.minPriceVsVWAP
      : S1_MIN_PRICE_VS_VWAP_PCT;

  const minAccel5M =
    Number.isFinite(
      options.minAccel5M
    )
      ? options.minAccel5M
      : S1_MIN_ACCEL5M;

  const minRvol15M =
    Number.isFinite(
      options.minVol15M
    )
      ? options.minVol15M
      : S1_MIN_RVOL15M;

  const sfFiltered =
    records.filter(
      record => {

        if (
          !Number.isFinite(
            record.price_vs_vwap_pct
          ) ||
          record.price_vs_vwap_pct <
          minPriceVsVWAP
        ) {
          return false;
        }

        if (
          !Number.isFinite(
            record.accel5m
          ) ||
          record.accel5m <
          minAccel5M
        ) {
          return false;
        }

        if (
          !Number.isFinite(
            record.rvol15m
          ) ||
          record.rvol15m <
          minRvol15M
        ) {
          return false;
        }

        return true;

      }
    );

  validateS1NoLeak(
    sfFiltered,
    Number(cache.asof_ms)
  );

  const ranked =
    rankS1(
      sfFiltered
    );

  const winner =
    winnerGate(
      ranked,
      {
        minPriceVsVWAP,

        minAccel5M,

        minVol15M:
          minRvol15M
      }
    );

  scanState.sf =
    ranked;

  scanState.sf_count =
    ranked.length;

  scanState.winner =
    winner;

  scanState.stage =
    "SF";

  scanState.asof =
    resolved.iso;

  scanState.date =
    resolved.date;

  scanState.sf_asof =
    resolved.iso;

  return {

    ok:
      true,

    stage:
      "SF",

    date:
      resolved.date,

    asof:
      resolved.iso,

    asof_ms:
      sfAsOfMs,

    s1_asof:
      cache.asof,

    input_count:
      records.length,

    sf_count:
      ranked.length,

    sf:
      ranked,

    winner,

    filters: {

      minPriceVsVWAP,

      minAccel5M,

      minRVOL15M:
        minRvol15M

    }

  };

}


/* =========================================================
   S0 + S1
========================================================= */

async function runS0S1(
  symbols,
  options = {}
) {

  const universe =
    normalizeSymbols(
      symbols
    );

  const resolved =
    resolveStageAsOf(
      options.asof,
      options.date,
      15,
      55
    );

  const s0Result =
    await runS0(
      universe,
      {
        asof:
          resolved.iso
      }
    );

  const s0Symbols =
    s0Result.s0
      .map(
        record =>
          record.symbol
      );

  if (
    !s0Symbols.length
  ) {

    return {

      ok:
        true,

      stage:
        "S0_S1",

      date:
        resolved.date,

      s0_asof:
        s0Result.asof,

      s1_asof:
        resolved.iso,

      asof:
        resolved.iso,

      elapsed_ms:
        s0Result.elapsed_ms,

      universe_requested:
        universe.length,

      s0_count:
        0,

      s1_count:
        0,

      s0:
        [],

      s1:
        [],

      yahoo_errors:
        s0Result.yahoo_errors

    };

  }

  const s1Result =
    await runS1(
      s0Symbols,
      {
        asof:
          resolved.iso
      }
    );

  return {

    ok:
      true,

    stage:
      "S0_S1",

    date:
      resolved.date,

    s0_asof:
      s0Result.asof,

    s1_asof:
      s1Result.asof,

    asof:
      s1Result.asof,

    elapsed_ms:
      s0Result.elapsed_ms +
      s1Result.elapsed_ms,

    universe_requested:
      universe.length,

    s0_count:
      s0Result.s0_count,

    s1_count:
      s1Result.s1_count,

    s0:
      s0Result.s0,

    s1:
      s1Result.s1,

    rejection_reasons:
      s1Result.rejection_reasons,

    yahoo_errors:
      (
        s0Result.yahoo_errors ||
        0
      ) +
      (
        s1Result.yahoo_errors ||
        0
      )

  };

}


/* =========================================================
   MCP RESULT HELPER
========================================================= */

function textResult(
  payload
) {

  return {

    content: [

      {

        type:
          "text",

        text:
          JSON.stringify(
            payload,
            null,
            2
          )

      }

    ]

  };

}


/* =========================================================
   MCP SERVER
========================================================= */

function createMcpServer() {

  const server =
    new McpServer(
      {
        name:
          "yahoo-scan-mcp",

        version:
          APP_VERSION
      }
    );


  /* -------------------------------------------------------
     1. ping
  ------------------------------------------------------- */

  server.tool(
    "ping",
    {},
    async () =>
      textResult(
        {

          ok:
            true,

          pong:
            true,

          version:
            APP_VERSION,

          time:
            nowIso()

        }
      )
  );


  /* -------------------------------------------------------
     2. get_status
  ------------------------------------------------------- */

  server.tool(
    "get_status",
    {},
    async () =>
      textResult(
        {

          ok:
            true,

          version:
            APP_VERSION,

          state:
            scanState

        }
      )
  );


  /* -------------------------------------------------------
     3. diagnose_filesystem
  ------------------------------------------------------- */

  server.tool(
    "diagnose_filesystem",
    {},
    async () => {

      const files = [

        UNIVERSE_FILE,

        S0_CACHE_FILE,

        S1_CACHE_FILE

      ];

      const output =
        files.map(
          file => ({

            file,

            exists:
              fs.existsSync(
                file
              ),

            size:
              fs.existsSync(
                file
              )
                ? fs.statSync(
                    file
                  ).size
                : 0

          })
        );

      return textResult(
        {

          ok:
            true,

          cwd:
            process.cwd(),

          files:
            output

        }
      );

    }
  );


  /* -------------------------------------------------------
     4. get_universe
  ------------------------------------------------------- */

  server.tool(
    "get_universe",
    {

      limit:
        z.number()
          .int()
          .positive()
          .optional()

    },

    async ({
      limit
    }) => {

      const symbols =
        readUniverse(
          limit
        );

      return textResult(
        {

          ok:
            true,

          count:
            symbols.length,

          symbols

        }
      );

    }
  );


  /* -------------------------------------------------------
     5. yahoo_spark_test
  ------------------------------------------------------- */

  server.tool(
    "yahoo_spark_test",
    {

      symbol:
        z.string(),

      range:
        z.string()
          .optional(),

      interval:
        z.string()
          .optional()

    },

    async ({
      symbol,
      range = "5d",
      interval = "5m"
    }) => {

      const normalized =
        symbol
          .trim()
          .toUpperCase();

      const started =
        Date.now();

      try {

        const result =
          await yahooChart(
            normalized,
            range,
            interval
          );

        const bars =
          extractBars(
            result
          );

        const raw =
          result
            ?.response?.[0] ||
          {};

        const quote =
          raw
            ?.indicators
            ?.quote?.[0] ||
          {};

        return textResult(
          {

            ok:
              true,

            source:
              "Yahoo Chart OHLCV",

            symbol:
              normalized,

            range,

            interval,

            elapsed_ms:
              Date.now() -
              started,

            result_count:
              Array.isArray(
                result.response
              )
                ? result.response.length
                : 0,

            response_keys:
              Object.keys(
                raw
              ),

            quote_keys:
              Object.keys(
                quote
              ),

            timestamp_count:
              Array.isArray(
                raw.timestamp
              )
                ? raw.timestamp.length
                : 0,

            extracted_bar_count:
              bars.length,

            valid_volume_count:
              bars.filter(
                b =>
                  Number.isFinite(
                    b.volume
                  ) &&
                  b.volume > 0
              ).length,

            sample:
              bars.slice(-5),

            yahoo_errors:
              0

          }
        );

      } catch (error) {

        return textResult(
          {

            ok:
              false,

            source:
              "Yahoo Chart OHLCV",

            symbol:
              normalized,

            error:
              String(
                error?.message ||
                error
              ),

            elapsed_ms:
              Date.now() -
              started

          }
        );

      }

    }
  );


  /* -------------------------------------------------------
     6. yahoo_ws_test

     IMPORTANT:
     No import of yahoo-ws.js.
  ------------------------------------------------------- */

  server.tool(
    "yahoo_ws_test",
    {

      symbols:
        z.array(
          z.string()
        )

    },

    async ({
      symbols
    }) => {

      const normalized =
        normalizeSymbols(
          symbols
        );

      return textResult(
        {

          ok:
            true,

          status:
            "isolated",

          symbols:
            normalized,

          note:
            "Yahoo Chart OHLCV is used by S0/S1. WebSocket is not required for server startup."

        }
      );

    }
  );


  /* -------------------------------------------------------
     7. yahoo_s0_prepare
  ------------------------------------------------------- */

  server.tool(
    "yahoo_s0_prepare",
    {

      symbols:
        z.array(
          z.string()
        )
        .optional(),

      limit:
        z.number()
          .int()
          .positive()
          .optional(),

      date:
        z.string()
          .regex(
            /^\d{4}-\d{2}-\d{2}$/,
            "date must be YYYY-MM-DD"
          )
          .optional(),

      asof:
        z.string()
          .optional()

    },

    async ({
      symbols,
      limit,
      date,
      asof
    }) => {

      const universe =
        Array.isArray(
          symbols
        )
          ? normalizeSymbols(
              symbols
            )
          : readUniverse(
              limit
            );

      return textResult(
        await runS0(
          universe,
          {
            date,
            asof
          }
        )
      );

    }
  );


  /* -------------------------------------------------------
     8. yahoo_s1_scan
  ------------------------------------------------------- */

  server.tool(
    "yahoo_s1_scan",
    {

      symbols:
        z.array(
          z.string()
        )
        .optional(),

      limit:
        z.number()
          .int()
          .positive()
          .optional(),

      date:
        z.string()
          .regex(
            /^\d{4}-\d{2}-\d{2}$/,
            "date must be YYYY-MM-DD"
          )
          .optional(),

      asof:
        z.string()
          .optional()

    },

    async ({
      symbols,
      limit,
      date,
      asof
    }) => {

      let universe;

      if (
        Array.isArray(
          symbols
        )
      ) {

        universe =
          normalizeSymbols(
            symbols
          );

      } else {

        const s0Cache =
          readJson(
            S0_CACHE_FILE
          );

        if (
          !s0Cache?.s0
        ) {

          throw new Error(
            "Valid S0 cache required"
          );

        }

        universe =
          (
            s0Cache.s0 ||
            []
          )
            .map(
              record =>
                record.symbol
            );

        if (
          Number.isInteger(
            limit
          ) &&
          limit > 0
        ) {

          universe =
            universe.slice(
              0,
              limit
            );

        }

      }

      return textResult(
        await runS1(
          universe,
          {
            date,
            asof
          }
        )
      );

    }
  );


  /* -------------------------------------------------------
     9. yahoo_s0_s1_scan
  ------------------------------------------------------- */

  server.tool(
    "yahoo_s0_s1_scan",
    {

      symbols:
        z.array(
          z.string()
        )
        .optional(),

      limit:
        z.number()
          .int()
          .positive()
          .optional(),

      date:
        z.string()
          .regex(
            /^\d{4}-\d{2}-\d{2}$/,
            "date must be YYYY-MM-DD"
          )
          .optional(),

      asof:
        z.string()
          .optional()

    },

    async ({
      symbols,
      limit,
      date,
      asof
    }) => {

      const universe =
        Array.isArray(
          symbols
        )
          ? normalizeSymbols(
              symbols
            )
          : readUniverse(
              limit
            );

      return textResult(
        await runS0S1(
          universe,
          {
            date,
            asof
          }
        )
      );

    }
  );


  /* -------------------------------------------------------
     10. yahoo_sf_scan
  ------------------------------------------------------- */

  server.tool(
    "yahoo_sf_scan",
    {

      symbols:
        z.array(
          z.string()
        )
        .optional(),

      date:
        z.string()
          .regex(
            /^\d{4}-\d{2}-\d{2}$/,
            "date must be YYYY-MM-DD"
          )
          .optional(),

      asof:
        z.string()
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

    },

    async ({
      symbols,
      date,
      asof,
      minPriceVsVWAP,
      minAccel5M,
      minVol15M
    }) => {

      return textResult(
        await runSF(
          symbols,
          {

            date,

            asof,

            minPriceVsVWAP,

            minAccel5M,

            minVol15M

          }
        )
      );

    }
  );


  /* -------------------------------------------------------
     11. get_scan_state
  ------------------------------------------------------- */

  server.tool(
    "get_scan_state",
    {},
    async () =>
      textResult(
        {

          ok:
            true,

          state:
            scanState

        }
      )
  );


  return server;

}


/* =========================================================
   EXPRESS
========================================================= */

const app =
  express();

app.disable(
  "x-powered-by"
);

app.use(
  express.json(
    {
      limit:
        "10mb"
    }
  )
);


/* =========================================================
   ROOT
========================================================= */

app.get(
  "/",
  (_req, res) => {

    res.json(
      {

        ok:
          true,

        service:
          "yahoo-scan-mcp",

        version:
          APP_VERSION,

        transport:
          "streamable-http",

        endpoint:
          "/mcp",

        time:
          nowIso()

      }

    );

  }
);


/* =========================================================
   HEALTH
========================================================= */

app.get(
  "/health",
  (_req, res) => {

    res.json(
      {

        ok:
          true,

        service:
          "yahoo-scan-mcp",

        version:
          APP_VERSION,

        transport:
          "streamable-http",

        time:
          nowIso()

      }

    );

  }
);


/* =========================================================
   MCP POST
========================================================= */

app.post(
  "/mcp",

  async (
    req,
    res
  ) => {

    let server =
      null;

    let transport =
      null;

    try {

      server =
        createMcpServer();

      transport =
        new StreamableHTTPServerTransport(
          {

            sessionIdGenerator:
              undefined,

            enableJsonResponse:
              true

          }
        );

      await server.connect(
        transport
      );

      await transport.handleRequest(
        req,
        res,
        req.body
      );

    } catch (error) {

      console.error(
        "[MCP ERROR]",
        error
      );

      if (
        !res.headersSent
      ) {

        res
          .status(500)
          .json(
            {

              ok:
                false,

              error:
                String(
                  error?.message ||
                  error
                )

            }
          );

      }

    } finally {

      try {

        if (
          transport &&
          typeof transport.close ===
            "function"
        ) {

          await transport.close();

        }

      } catch {}

      try {

        if (
          server &&
          typeof server.close ===
            "function"
        ) {

          await server.close();

        }

      } catch {}

    }

  }
);


/* =========================================================
   MCP GET
========================================================= */

app.get(
  "/mcp",
  (_req, res) => {

    res
      .status(405)
      .json(
        {

          ok:
            false,

          error:
            "GET /mcp is not supported in stateless mode. Use POST."

        }
      );

  }
);


/* =========================================================
   MCP DELETE
========================================================= */

app.delete(
  "/mcp",
  (_req, res) => {

    res
      .status(405)
      .json(
        {

          ok:
            false,

          error:
            "DELETE /mcp is not supported in stateless mode."

        }
      );

  }
);


/* =========================================================
   404
========================================================= */

app.use(
  (_req, res) => {

    res
      .status(404)
      .json(
        {

          ok:
            false,

          error:
            "Not found"

        }

      );

  }
);


/* =========================================================
   START
========================================================= */

app.listen(
  PORT,
  "0.0.0.0",
  () => {

    console.log(
      `[yahoo-scan-mcp] v${APP_VERSION} started`
    );

    console.log(
      `[yahoo-scan-mcp] HTTP server listening on ${PORT}`
    );

    console.log(
      `[yahoo-scan-mcp] MCP endpoint: /mcp`
    );

    console.log(
      `[yahoo-scan-mcp] Data source: Yahoo Chart OHLCV`
    );

    console.log(
      `[yahoo-scan-mcp] Universe: ${UNIVERSE_FILE}`
    );

    console.log(
      `[yahoo-scan-mcp] Market timezone: ${MARKET_TIMEZONE}`
    );

  }
);
