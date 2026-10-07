/*
============================================================
 Yahoo Scan MCP
 Version 1.7.1
============================================================
- Yahoo Chart OHLCV
- S0 historique 15m
- S1 intraday 5m
- RVOL15M
- VWAP
- Accel5M
- HOD / HOD distance
- Streamable HTTP stateless
- 11 MCP tools
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

const APP_VERSION = "1.7.1";

const PORT =
  Number(process.env.PORT || 8080);

const DATA_CONCURRENCY = 8;
const RETRIES = 3;
const REQUEST_TIMEOUT_MS = 15000;
const RETRY_DELAY_MS = 600;

const S0_RANGE = "1mo";
const S0_INTERVAL = "15m";

const S1_RANGE = "1d";
const S1_INTERVAL = "5m";

const MIN_S0_HISTORICAL_SESSIONS = 5;
const MIN_S0_VALID_VOLUME_BARS = 20;
const MIN_RVOL_BASELINE_SESSIONS = 5;
const MIN_S1_COMPLETED_BARS = 4;

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
      .filter(Number.isFinite)
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
   NEW YORK TIME
========================================================= */

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
          "America/New_York",

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


/* =========================================================
   BATCH DATA LOADER
========================================================= */

async function yahooChartBatch(
  symbols,
  range,
  interval
) {

  const results = {};

  let errors = 0;

  let cursor = 0;

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

        results[symbol] =
          await yahooChart(
            symbol,
            range,
            interval
          );

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
  bars
) {

  const sessions =
    getSessionBars(
      bars
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
   S0
========================================================= */

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
        date =>
          sessions.get(date)
      )
      .filter(Boolean);

  if (
    completedSessions.length <
    MIN_S0_HISTORICAL_SESSIONS
  ) {
    return null;
  }

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
      sessionPerformance(
        previous
      ),

    j2_pct:
      sessionPerformance(
        previous2
      ),

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


function calculateS1(
  symbol,
  item,
  s0Record
) {

  if (!s0Record) {
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

  const now =
    Date.now();

  const completed =
    currentSession.filter(
      bar =>
        bar.ts +
        5 * 60 * 1000
        <= now + 1000
    );

  if (
    completed.length <
    MIN_S1_COMPLETED_BARS
  ) {
    return null;
  }

  const valid =
    completed.filter(
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

    session_date:
      nyDateKey(
        last.ts
      ),

    volume5m,

    volume10m,

    volume15m,

    rvol15m,

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
      change10m,

    j1_pct:
      s0Record.j1_pct,

    j2_pct:
      s0Record.j2_pct

  };

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
      : -1;

  const minAccel5M =
    Number.isFinite(
      options.minAccel5M
    )
      ? options.minAccel5M
      : 1;

  const minVol15M =
    Number.isFinite(
      options.minVol15M
    )
      ? options.minVol15M
      : 0;

  for (
    const record of records
  ) {

    if (
      !Number.isFinite(
        record.price_vs_vwap_pct
      )
    ) {
      continue;
    }

    if (
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
        record.volume15m
      ) ||
      record.volume15m <
      minVol15M
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


function currentNYDate() {

  const parts =
    new Intl.DateTimeFormat(
      "en-US",
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
    )
      .formatToParts(
        new Date()
      );

  const obj = {};

  for (
    const p of parts
  ) {

    if (
      p.type !==
      "literal"
    ) {

      obj[p.type] =
        p.value;

    }

  }

  return (
    `${obj.year}-${obj.month}-${obj.day}`
  );

}


function validCache(
  cache
) {

  return Boolean(
    cache &&
    cache.version ===
      APP_VERSION &&
    cache.market_date ===
      currentNYDate()
  );

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
  symbols
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

  scanState.running =
    true;

  scanState.stage =
    "S0";

  scanState.asof =
    nowIso();

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
          results[symbol]
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

        market_date:
          currentNYDate(),

        source:
          "Yahoo Chart OHLCV",

        created_at:
          nowIso(),

        s0
      }
    );

    return {

      ok:
        true,

      stage:
        "S0",

      asof:
        scanState.asof,

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
   SCAN S1
========================================================= */

async function runS1(
  symbols
) {

  if (
    scanState.running
  ) {

    throw new Error(
      "Scan already running"
    );

  }

  const s0Cache =
    readJson(
      S0_CACHE_FILE
    );

  if (
    !validCache(
      s0Cache
    )
  ) {

    throw new Error(
      "Valid S0 cache required"
    );

  }

  const requested =
    normalizeSymbols(
      symbols
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

  const filtered =
    requested.filter(
      symbol =>
        s0Map.has(
          symbol
        )
    );

  scanState.running =
    true;

  scanState.stage =
    "S1";

  scanState.asof =
    nowIso();

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
        filtered,
        S1_RANGE,
        S1_INTERVAL
      );

    const s1 = [];

    for (
      const symbol of filtered
    ) {

      const record =
        calculateS1(
          symbol,
          results[symbol],
          s0Map.get(symbol)
        );

      if (record) {
        s1.push(record);
      }

    }

    const ranked =
      rankS1(
        s1
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

        market_date:
          currentNYDate(),

        source:
          "Yahoo Chart OHLCV",

        created_at:
          nowIso(),

        s1:
          ranked
      }
    );

    return {

      ok:
        true,

      stage:
        "S1",

      asof:
        scanState.asof,

      elapsed_ms:
        scanState.elapsed_ms,

      universe_requested:
        filtered.length,

      s1_count:
        ranked.length,

      s1:
        ranked,

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
      cache
    )
  ) {

    throw new Error(
      "Valid S1 cache required"
    );

  }

  let records =
    Array.isArray(
      cache.s1
    )
      ? cache.s1
      : [];

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

  const ranked =
    rankS1(
      records
    );

  const winner =
    winnerGate(
      ranked,
      options
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
    nowIso();

  return {

    ok:
      true,

    stage:
      "SF",

    asof:
      scanState.asof,

    sf_count:
      ranked.length,

    sf:
      ranked,

    winner

  };

}


/* =========================================================
   S0 + S1
========================================================= */

async function runS0S1(
  symbols
) {

  const universe =
    normalizeSymbols(
      symbols
    );

  const s0Result =
    await runS0(
      universe
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

      asof:
        nowIso(),

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
      s0Symbols
    );

  return {

    ok:
      true,

    stage:
      "S0_S1",

    asof:
      nowIso(),

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
     Prevents the previous default-export error.
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
          .optional()

    },

    async ({
      symbols,
      limit
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
          universe
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
          .optional()

    },

    async ({
      symbols,
      limit
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
          !validCache(
            s0Cache
          )
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
          universe
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
          .optional()

    },

    async ({
      symbols,
      limit
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
          universe
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
      minPriceVsVWAP,
      minAccel5M,
      minVol15M
    }) => {

      return textResult(
        await runSF(
          symbols,
          {

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


  /* =======================================================
     IMPORTANT:
     createMcpServer MUST return the MCP server.
  ======================================================= */

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

  }
);
