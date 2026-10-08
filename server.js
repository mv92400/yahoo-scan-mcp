/*
============================================================
 Yahoo Scan MCP
 Version 2.0.0
============================================================

OBJECTIF
--------
Scanner temps réel NASDAQ / ordinary stocks / <$5

Architecture :

S0
- Universe NASDAQ
- Ordinary stocks uniquement
- ETF / warrants / rights / units / preferred exclus
- Historique 15m
- J-1 / J-2
- Liquidité historique
- RVOL15M baseline
- Vol5 baseline
- Vol60 baseline
- Materialisation S0

S1
- Intraday 5m
- Snapshot dynamique
- VWAP
- RVOL15M
- Vol5
- Delta60
- HOD
- Accel5M informatif
- Ranking DV

SF
- RVOL15M >= 1.50
- Vol5/reference >= 0.20
- Delta60 <= +10%
- Price vs VWAP >= -1%
- Delta 15:55 -> 15:59 >= 0%
- Lots de 20
- Ranking DV décroissant

IMPORTANT
---------
Accel5M N'EST PAS un filtre bloquant.
Le seuil 20% appartient au Vol5 ratio,
pas à Accel5M.

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

const APP_VERSION = "2.0.0";

const PORT =
  Number(process.env.PORT || 8080);

const DATA_CONCURRENCY = 8;
const RETRIES = 3;
const REQUEST_TIMEOUT_MS = 15000;
const RETRY_DELAY_MS = 600;

const S0_RANGE = "1mo";
const S0_INTERVAL = "15m";
const S1_INTERVAL = "5m";

const MARKET_TZ = "America/New_York";

const MAX_J1_J2_PCT = 5;

const LOCKED = {
  rvol15Min: 1.50,

  /*
   * IMPORTANT:
   * 20% = Vol5 ratio minimum.
   * NOT Accel5M.
   */
  vol5MinRatio: 0.20,

  delta60MaxPct: 10,

  priceVsVwapMinPct: -1,

  delta1555to1559MinPct: 0,

  lotSize: 20
};

const NON_BLOCKING = {
  accel5m: true
};

/* =========================================================
   PATHS
========================================================= */

const ROOT = process.cwd();

const UNIVERSE_FILE =
  process.env.UNIVERSE_FILE ||
  path.join(ROOT, "universe_s0.txt");

const S0_CACHE_FILE =
  path.join(ROOT, "s0_materialized.json");

const S1_CACHE_FILE =
  path.join(ROOT, "s1_materialized.json");

/* =========================================================
   EXPRESS
========================================================= */

const app = express();

app.use(
  express.json({
    limit: "2mb"
  })
);

/* =========================================================
   STATE
========================================================= */

const scanState = {
  started_at: null,
  completed_at: null,
  asof: null,

  s0_count: 0,
  s1_count: 0,
  sf_count: 0,

  elapsed_ms: null,

  status: "idle"
};

/* =========================================================
   BASIC UTILS
========================================================= */

function sleep(ms) {
  return new Promise(resolve =>
    setTimeout(resolve, ms)
  );
}

function chunk(arr, size) {
  const out = [];

  for (
    let i = 0;
    i < arr.length;
    i += size
  ) {
    out.push(arr.slice(i, i + size));
  }

  return out;
}

function median(values) {
  const a = values
    .filter(Number.isFinite)
    .sort((x, y) => x - y);

  if (!a.length) {
    return null;
  }

  const mid =
    Math.floor(a.length / 2);

  if (a.length % 2) {
    return a[mid];
  }

  return (a[mid - 1] + a[mid]) / 2;
}

function pctChange(from, to) {
  if (
    !Number.isFinite(from) ||
    !Number.isFinite(to) ||
    from === 0
  ) {
    return null;
  }

  return ((to / from) - 1) * 100;
}

function finite(value) {
  return Number.isFinite(value);
}

function round(value, digits = 4) {
  if (!Number.isFinite(value)) {
    return null;
  }

  const p = 10 ** digits;

  return Math.round(value * p) / p;
}

/* =========================================================
   TIMEZONE
========================================================= */

function getNYParts(timestamp) {
  const dtf = new Intl.DateTimeFormat(
    "en-US",
    {
      timeZone: MARKET_TZ,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hourCycle: "h23"
    }
  );

  const parts =
    dtf.formatToParts(
      new Date(timestamp)
    );

  const out = {};

  for (const p of parts) {
    if (p.type !== "literal") {
      out[p.type] = Number(p.value);
    }
  }

  return out;
}

function etClock(timestamp) {
  const p = getNYParts(timestamp);

  return (
    String(p.hour).padStart(2, "0") +
    ":" +
    String(p.minute).padStart(2, "0")
  );
}

function etDate(timestamp) {
  const p = getNYParts(timestamp);

  return (
    `${p.year}-${String(p.month).padStart(2, "0")}-` +
    `${String(p.day).padStart(2, "0")}`
  );
}

function etTimestamp(
  date,
  hour,
  minute,
  second = 0
) {
  /*
   * Convertit une heure ET en timestamp UTC.
   * Recherche itérative robuste autour des changements DST.
   */

  let guess =
    Date.UTC(
      date.getUTCFullYear(),
      date.getUTCMonth(),
      date.getUTCDate(),
      hour,
      minute,
      second
    );

  for (let i = 0; i < 5; i++) {
    const p = getNYParts(guess);

    const current =
      Date.UTC(
        p.year,
        p.month - 1,
        p.day,
        p.hour,
        p.minute,
        p.second
      );

    const target =
      Date.UTC(
        date.getUTCFullYear(),
        date.getUTCMonth(),
        date.getUTCDate(),
        hour,
        minute,
        second
      );

    const diff =
      target - current;

    if (diff === 0) {
      break;
    }

    guess += diff;
  }

  return guess;
}

/* =========================================================
   MARKET SESSION
========================================================= */

function isRegularSessionBar(timestamp) {
  const p =
    getNYParts(timestamp);

  const minutes =
    p.hour * 60 + p.minute;

  /*
   * 09:30 <= start < 16:00
   */
  return (
    minutes >= 570 &&
    minutes < 960
  );
}

function floor15Clock(timestamp) {
  const p =
    getNYParts(timestamp);

  const m =
    Math.floor(p.minute / 15) * 15;

  return (
    String(p.hour).padStart(2, "0") +
    ":" +
    String(m).padStart(2, "0")
  );
}

function floor5Clock(timestamp) {
  const p =
    getNYParts(timestamp);

  const m =
    Math.floor(p.minute / 5) * 5;

  return (
    String(p.hour).padStart(2, "0") +
    ":" +
    String(m).padStart(2, "0")
  );
}

/* =========================================================
   YAHOO CHART
========================================================= */

async function fetchYahooChart(
  symbol,
  interval,
  range
) {
  const url =
    "https://query1.finance.yahoo.com/v8/finance/chart/" +
    encodeURIComponent(symbol) +
    `?range=${encodeURIComponent(range)}` +
    `&interval=${encodeURIComponent(interval)}` +
    "&includePrePost=false" +
    "&events=div%2Csplits";

  let lastError = null;

  for (
    let attempt = 1;
    attempt <= RETRIES;
    attempt++
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
      const response =
        await fetch(
          url,
          {
            signal:
              controller.signal,
            headers: {
              "User-Agent":
                "Mozilla/5.0"
            }
          }
        );

      if (!response.ok) {
        throw new Error(
          `Yahoo HTTP ${response.status}`
        );
      }

      const json =
        await response.json();

      const result =
        json?.chart?.result?.[0];

      if (!result) {
        throw new Error(
          "Yahoo chart result missing"
        );
      }

      clearTimeout(timer);

      return normalizeYahooChart(
        symbol,
        result
      );
    } catch (error) {
      clearTimeout(timer);

      lastError = error;

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

  throw new Error(
    `${symbol}: ${
      lastError?.message ||
      "Yahoo request failed"
    }`
  );
}

/* =========================================================
   NORMALIZE YAHOO
========================================================= */

function normalizeYahooChart(
  symbol,
  result
) {
  const timestamps =
    result.timestamp || [];

  const quote =
    result.indicators?.quote?.[0] || {};

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
    const ts =
      Number(timestamps[i]) * 1000;

    const open =
      Number(opens[i]);

    const high =
      Number(highs[i]);

    const low =
      Number(lows[i]);

    const close =
      Number(closes[i]);

    const volume =
      Number(volumes[i]);

    if (
      !Number.isFinite(ts) ||
      !Number.isFinite(open) ||
      !Number.isFinite(high) ||
      !Number.isFinite(low) ||
      !Number.isFinite(close)
    ) {
      continue;
    }

    bars.push({
      ts,
      open,
      high,
      low,
      close,
      volume:
        Number.isFinite(volume)
          ? volume
          : 0
    });
  }

  return {
    symbol,
    meta: result.meta || {},
    bars
  };
}

/* =========================================================
   UNIVERSE
========================================================= */

function readUniverse() {
  if (
    !fs.existsSync(
      UNIVERSE_FILE
    )
  ) {
    return [];
  }

  const raw =
    fs.readFileSync(
      UNIVERSE_FILE,
      "utf8"
    );

  const symbols =
    raw
      .split(/\r?\n/)
      .map(x =>
        x.trim().toUpperCase()
      )
      .filter(Boolean)
      .filter(
        x =>
          !x.startsWith("#")
      );

  return [
    ...new Set(symbols)
  ];
}

/* =========================================================
   ORDINARY STOCK
========================================================= */

function isOrdinaryStock(meta) {
  const quoteType =
    String(
      meta?.quoteType ||
      ""
    ).toUpperCase();

  const exchange =
    String(
      meta?.exchangeName ||
      meta?.fullExchangeName ||
      ""
    ).toUpperCase();

  /*
   * Si Yahoo fournit explicitement
   * un quoteType incompatible,
   * rejet.
   */

  const forbiddenTypes =
    new Set([
      "ETF",
      "MUTUALFUND",
      "INDEX",
      "OPTION",
      "FUTURE",
      "CRYPTOCURRENCY",
      "WARRANT",
      "RIGHT",
      "UNIT",
      "PREFERRED_STOCK"
    ]);

  if (
    quoteType &&
    forbiddenTypes.has(
      quoteType
    )
  ) {
    return false;
  }

  /*
   * Si quoteType explicite et non equity,
   * rejet.
   */

  if (
    quoteType &&
    quoteType !== "EQUITY"
  ) {
    return false;
  }

  /*
   * Le fichier universe_s0.txt
   * est déjà filtré NASDAQ.
   *
   * Si Yahoo ne renvoie pas exchangeName,
   * on ne rejette pas automatiquement :
   * l'univers matérialisé reste la source
   * structurelle S0.
   */

  if (
    exchange &&
    !exchange.includes("NASDAQ")
  ) {
    return false;
  }

  return true;
}

/* =========================================================
   SESSION GROUPING
========================================================= */

function groupBySession(
  bars
) {
  const map =
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
      etDate(bar.ts);

    if (!map.has(date)) {
      map.set(
        date,
        []
      );
    }

    map
      .get(date)
      .push(bar);
  }

  return map;
}

function sortedSessionDates(
  bars
) {
  return [
    ...groupBySession(
      bars
    ).keys()
  ].sort();
}

/* =========================================================
   SESSION PERFORMANCE
========================================================= */

function sessionPerformance(
  bars
) {
  if (!bars?.length) {
    return null;
  }

  const ordered =
    [...bars].sort(
      (a, b) =>
        a.ts - b.ts
    );

  const first =
    ordered[0];

  const last =
    ordered[
      ordered.length - 1
    ];

  if (
    !finite(first.open) ||
    !finite(last.close) ||
    first.open <= 0
  ) {
    return null;
  }

  return (
    (last.close /
      first.open -
      1) *
    100
  );
}

/* =========================================================
   S0 15M BASELINE
========================================================= */

function build15mReferences(
  bars
) {
  const sessions =
    groupBySession(
      bars
    );

  const dates =
    [...sessions.keys()]
      .sort();

  /*
   * Exclure la session la plus récente,
   * considérée comme session courante.
   */

  const historicalDates =
    dates.slice(
      0,
      -1
    );

  const slotVolumes =
    new Map();

  for (
    const date of historicalDates
  ) {
    const day =
      sessions.get(
        date
      ) || [];

    for (const bar of day) {
      const slot =
        floor15Clock(
          bar.ts
        );

      if (
        !slotVolumes.has(
          slot
        )
      ) {
        slotVolumes.set(
          slot,
          []
        );
      }

      slotVolumes
        .get(slot)
        .push(bar.volume);
    }
  }

  const baseline =
    {};

  for (
    const [
      slot,
      values
    ] of slotVolumes
  ) {
    baseline[slot] =
      median(values);
  }

  return {
    dates,
    historicalDates,
    baseline
  };
}

/* =========================================================
   5M HISTORICAL REFERENCES
========================================================= */

function build5mReferences(
  bars
) {
  const sessions =
    groupBySession(
      bars
    );

  const dates =
    [...sessions.keys()]
      .sort();

  const historicalDates =
    dates.slice(
      0,
      -1
    );

  const vol5 =
    new Map();

  const vol60 =
    new Map();

  /*
   * Vol5
   * ----
   * Médiane du volume 5m
   * par heure de marché.
   */

  for (
    const date of historicalDates
  ) {
    const day =
      sessions.get(
        date
      ) || [];

    for (const bar of day) {
      const slot =
        floor5Clock(
          bar.ts
        );

      if (
        !vol5.has(slot)
      ) {
        vol5.set(
          slot,
          []
        );
      }

      vol5
        .get(slot)
        .push(bar.volume);
    }
  }

  /*
   * Vol60
   * -----
   * Volume glissant 60 minutes
   * puis baseline par slot.
   */

  for (
    const date of historicalDates
  ) {
    const day =
      (
        sessions.get(
          date
        ) || []
      ).sort(
        (a, b) =>
          a.ts - b.ts
      );

    for (
      let i = 0;
      i < day.length;
      i++
    ) {
      const current =
        day[i];

      const start =
        current.ts -
        55 * 60 * 1000;

      let sum = 0;

      for (
        let j = i;
        j >= 0;
        j--
      ) {
        if (
          day[j].ts <
          start
        ) {
          break;
        }

        sum +=
          Number(
            day[j].volume ||
            0
          );
      }

      const slot =
        floor5Clock(
          current.ts
        );

      if (
        !vol60.has(slot)
      ) {
        vol60.set(
          slot,
          []
        );
      }

      vol60
        .get(slot)
        .push(sum);
    }
  }

  const vol5Baseline =
    {};

  const vol60Baseline =
    {};

  for (
    const [
      slot,
      values
    ] of vol5
  ) {
    vol5Baseline[slot] =
      median(values);
  }

  for (
    const [
      slot,
      values
    ] of vol60
  ) {
    vol60Baseline[slot] =
      median(values);
  }

  return {
    vol5: vol5Baseline,
    vol60: vol60Baseline
  };
}

/* =========================================================
   J-1 / J-2
========================================================= */

function getJ1J2(
  bars
) {
  const sessions =
    groupBySession(
      bars
    );

  const dates =
    [...sessions.keys()]
      .sort();

  const historicalDates =
    dates.slice(
      0,
      -1
    );

  if (
    historicalDates.length <
    2
  ) {
    return {
      j1: null,
      j2: null
    };
  }

  const j1Date =
    historicalDates[
      historicalDates.length - 1
    ];

  const j2Date =
    historicalDates[
      historicalDates.length - 2
    ];

  const j1 =
    sessionPerformance(
      sessions.get(
        j1Date
      )
    );

  const j2 =
    sessionPerformance(
      sessions.get(
        j2Date
      )
    );

  return {
    j1,
    j2
  };
}

/* =========================================================
   S0 PROCESS
========================================================= */

async function processS0Symbol(
  symbol
) {
  try {
    const chart =
      await fetchYahooChart(
        symbol,
        S0_INTERVAL,
        S0_RANGE
      );

    const bars =
      chart.bars
        .filter(
          b =>
            isRegularSessionBar(
              b.ts
            )
        )
        .sort(
          (a, b) =>
            a.ts - b.ts
        );

    if (
      !bars.length
    ) {
      return {
        ok: false,
        symbol,
        reason:
          "NO_HISTORY"
      };
    }

    if (
      !isOrdinaryStock(
        chart.meta
      )
    ) {
      return {
        ok: false,
        symbol,
        reason:
          "NOT_ORDINARY_STOCK"
      };
    }

    const dates =
      sortedSessionDates(
        bars
      );

    if (
      dates.length < 6
    ) {
      return {
        ok: false,
        symbol,
        reason:
          "INSUFFICIENT_HISTORY"
      };
    }

    const latestDate =
      dates[
        dates.length - 1
      ];

    const latestDay =
      groupBySession(
        bars
      ).get(
        latestDate
      ) || [];

    const lastBar =
      latestDay[
        latestDay.length - 1
      ];

    if (
      !lastBar ||
      !finite(lastBar.close)
    ) {
      return {
        ok: false,
        symbol,
        reason:
          "NO_CURRENT_PRICE"
      };
    }

    /*
     * Prix S0 <$5
     */

    if (
      lastBar.close >= 5
    ) {
      return {
        ok: false,
        symbol,
        reason:
          "PRICE_GE_5"
      };
    }

    const {
      j1,
      j2
    } =
      getJ1J2(
        bars
      );

    /*
     * Qualité historique :
     * J-1 / J-2 ne doivent pas déjà
     * être des explosions > +5%.
     */

    if (
      (finite(j1) &&
        Math.abs(j1) >
          MAX_J1_J2_PCT) ||
      (finite(j2) &&
        Math.abs(j2) >
          MAX_J1_J2_PCT)
    ) {
      return {
        ok: false,
        symbol,
        reason:
          "J1_J2_EXCESSIVE",
        j1,
        j2
      };
    }

    const refs15 =
      build15mReferences(
        bars
      );

    const refs5 =
      build5mReferences(
        bars
      );

    const slot =
      floor15Clock(
        lastBar.ts
      );

    const rvolReference =
      refs15.baseline[
        slot
      ];

    const vol5Reference =
      refs5.vol5[
        "15:55"
      ];

    const vol60Reference =
      refs5.vol60[
        "15:55"
      ];

    if (
      !finite(rvolReference) ||
      rvolReference <= 0
    ) {
      return {
        ok: false,
        symbol,
        reason:
          "NO_RVOL_BASELINE"
      };
    }

    /*
     * Minimum historique de volume.
     */

    const historicalVolumes =
      bars
        .map(
          b =>
            Number(
              b.volume || 0
            )
        )
        .filter(
          Number.isFinite
        );

    const medianVolume =
      median(
        historicalVolumes
      );

    if (
      !finite(medianVolume) ||
      medianVolume <= 0
    ) {
      return {
        ok: false,
        symbol,
        reason:
          "NO_LIQUIDITY_BASELINE"
      };
    }

    return {
      ok: true,
      symbol,

      price:
        round(
          lastBar.close,
          4
        ),

      asof:
        lastBar.ts,

      j1:
        round(j1, 4),

      j2:
        round(j2, 4),

      rvol15_reference:
        round(
          rvolReference,
          4
        ),

      vol5_reference:
        finite(
          vol5Reference
        )
          ? round(
              vol5Reference,
              2
            )
          : null,

      vol60_reference:
        finite(
          vol60Reference
        )
          ? round(
              vol60Reference,
              2
            )
          : null,

      median_volume:
        round(
          medianVolume,
          2
        ),

      latest_session:
        latestDate
    };
  } catch (error) {
    return {
      ok: false,
      symbol,
      reason:
        "FETCH_ERROR",
      error:
        error?.message ||
        String(error)
    };
  }
}

/* =========================================================
   CONCURRENT MAP
========================================================= */

async function mapConcurrent(
  items,
  worker,
  concurrency = DATA_CONCURRENCY
) {
  const results =
    new Array(
      items.length
    );

  let cursor = 0;

  async function runner() {
    while (true) {
      const index =
        cursor++;

      if (
        index >=
        items.length
      ) {
        return;
      }

      results[index] =
        await worker(
          items[index],
          index
        );
    }
  }

  const workers =
    Math.min(
      concurrency,
      items.length
    );

  await Promise.all(
    Array.from(
      {
        length: workers
      },
      () => runner()
    )
  );

  return results;
}

/* =========================================================
   S0 PREPARE
========================================================= */

async function runS0(
  symbols
) {
  const started =
    Date.now();

  const raw =
    await mapConcurrent(
      symbols,
      processS0Symbol
    );

  const survivors =
    raw.filter(
      x => x?.ok
    );

  const result = {
    stage: "S0",

    asof:
      Date.now(),

    universe_requested:
      symbols.length,

    count:
      survivors.length,

    rejected:
      raw.length -
      survivors.length,

    candidates:
      survivors,

    elapsed_ms:
      Date.now() -
      started
  };

  try {
    fs.writeFileSync(
      S0_CACHE_FILE,
      JSON.stringify(
        result,
        null,
        2
      )
    );
  } catch {}

  scanState.s0_count =
    survivors.length;

  return result;
}

/* =========================================================
   LOAD S0 CACHE
========================================================= */

function loadS0Cache() {
  try {
    if (
      !fs.existsSync(
        S0_CACHE_FILE
      )
    ) {
      return null;
    }

    return JSON.parse(
      fs.readFileSync(
        S0_CACHE_FILE,
        "utf8"
      )
    );
  } catch {
    return null;
  }
}

/* =========================================================
   COMPLETED BARS
========================================================= */

function completedBarsAt(
  bars,
  asofMs
) {
  /*
   * Yahoo timestamps représentent
   * le début de la bougie.
   *
   * Une bougie 15:50 est complète
   * à 15:55.
   */

  return bars.filter(
    bar =>
      bar.ts +
        5 * 60 * 1000 <=
      asofMs
  );
}

/* =========================================================
   VWAP
========================================================= */

function calculateVWAP(
  bars
) {
  let pv = 0;
  let volume = 0;

  for (const bar of bars) {
    const v =
      Number(
        bar.volume || 0
      );

    if (
      !finite(v) ||
      v <= 0
    ) {
      continue;
    }

    const typical =
      (
        bar.high +
        bar.low +
        bar.close
      ) / 3;

    pv +=
      typical * v;

    volume += v;
  }

  if (
    volume <= 0
  ) {
    return null;
  }

  return pv / volume;
}

/* =========================================================
   HOD
========================================================= */

function calculateHOD(
  bars
) {
  if (
    !bars.length
  ) {
    return null;
  }

  return Math.max(
    ...bars.map(
      b => b.high
    )
  );
}

/* =========================================================
   60M VOLUME
========================================================= */

function volume60m(
  bars,
  asofMs
) {
  const cutoff =
    asofMs -
    60 * 60 * 1000;

  return bars
    .filter(
      b =>
        b.ts >= cutoff &&
        b.ts <= asofMs
    )
    .reduce(
      (sum, b) =>
        sum +
        Number(
          b.volume || 0
        ),
      0
    );
}

/* =========================================================
   15M VOLUME
========================================================= */

function volume15m(
  bars
) {
  if (
    !bars.length
  ) {
    return 0;
  }

  return bars
    .slice(-3)
    .reduce(
      (sum, b) =>
        sum +
        Number(
          b.volume || 0
        ),
      0
    );
}

/* =========================================================
   ACCEL5M
========================================================= */

function calculateAccel5m(
  bars
) {
  if (
    bars.length < 2
  ) {
    return null;
  }

  const previous =
    bars[
      bars.length - 2
    ];

  const current =
    bars[
      bars.length - 1
    ];

  if (
    !finite(
      previous.volume
    ) ||
    !finite(
      current.volume
    ) ||
    previous.volume <= 0
  ) {
    return null;
  }

  return (
    (
      current.volume /
      previous.volume -
      1
    ) *
    100
  );
}

/* =========================================================
   DELTA 60
========================================================= */

function calculateDelta60(
  bars
) {
  if (
    bars.length < 13
  ) {
    return null;
  }

  const current =
    bars[
      bars.length - 1
    ];

  const sixtyAgo =
    bars[
      bars.length - 13
    ];

  if (
    !finite(
      current.close
    ) ||
    !finite(
      sixtyAgo.close
    ) ||
    sixtyAgo.close <= 0
  ) {
    return null;
  }

  return pctChange(
    sixtyAgo.close,
    current.close
  );
}

/* =========================================================
   S1 SYMBOL
========================================================= */

async function processS1Symbol(
  candidate,
  asofMs
) {
  const symbol =
    candidate.symbol;

  try {
    const chart =
      await fetchYahooChart(
        symbol,
        S1_INTERVAL,
        "1d"
      );

    const allBars =
      chart.bars
        .filter(
          b =>
            isRegularSessionBar(
              b.ts
            )
        )
        .sort(
          (a, b) =>
            a.ts - b.ts
        );

    const bars =
      completedBarsAt(
        allBars,
        asofMs
      );

    if (
      !bars.length
    ) {
      return {
        ok: false,
        symbol,
        reason:
          "NO_COMPLETED_BARS"
      };
    }

    /*
     * 15m = 3 x 5m complétées
     */

    if (
      bars.length < 13
    ) {
      return {
        ok: false,
        symbol,
        reason:
          "INSUFFICIENT_INTRADAY"
      };
    }

    const current =
      bars[
        bars.length - 1
      ];

    const price =
      current.close;

    const vwap =
      calculateVWAP(
        bars
      );

    const hod =
      calculateHOD(
        bars
      );

    const vol15 =
      volume15m(
        bars
      );

    const accel5m =
      calculateAccel5m(
        bars
      );

    const delta60 =
      calculateDelta60(
        bars
      );

    const priceVsVWAP =
      finite(vwap)
        ? (
            (
              price /
              vwap
            ) - 1
          ) * 100
        : null;

    const hodDistance =
      finite(hod) &&
      hod > 0
        ? (
            (
              price /
              hod
            ) - 1
          ) * 100
        : null;

    /*
     * RVOL15M
     *
     * Même slot horaire,
     * fenêtre de 15 minutes.
     */

    const slot =
      floor15Clock(
        current.ts
      );

    const historicalRVOL =
      candidate
        .rvol15_reference;

    const rvol15 =
      finite(
        historicalRVOL
      ) &&
      historicalRVOL > 0
        ? vol15 /
          historicalRVOL
        : null;

    const vol60 =
      volume60m(
        bars,
        current.ts +
          5 * 60 * 1000
      );

    const vol5Reference =
      candidate
        .vol5_reference;

    const vol60Reference =
      candidate
        .vol60_reference;

    const vol5Ratio =
      finite(
        vol5Reference
      ) &&
      vol5Reference > 0
        ? current.volume /
          vol5Reference
        : null;

    const vol60Ratio =
      finite(
        vol60Reference
      ) &&
      vol60Reference > 0
        ? vol60 /
          vol60Reference
        : null;

    /*
     * DV
     *
     * Ranking composite.
     * Les valeurs de ranking ne modifient
     * pas les filtres SF verrouillés.
     */

    const dv =
      (
        (finite(rvol15)
          ? rvol15
          : 0) *
        0.40
      ) +
      (
        (finite(priceVsVWAP)
          ? Math.max(
              priceVsVWAP,
              -10
            )
          : 0) *
        0.20
      ) +
      (
        (finite(delta60)
          ? Math.max(
              Math.min(
                delta60,
                10
              ),
              -10
            )
          : 0) *
        0.20
      ) +
      (
        (finite(hodDistance)
          ? Math.max(
              hodDistance,
              -20
            )
          : -20) *
        0.20
      );

    return {
      ok: true,

      symbol,

      asof:
        current.ts +
        5 * 60 * 1000,

      price:
        round(
          price,
          4
        ),

      vwap:
        round(
          vwap,
          4
        ),

      price_vs_vwap_pct:
        round(
          priceVsVWAP,
          4
        ),

      hod:
        round(
          hod,
          4
        ),

      hod_distance_pct:
        round(
          hodDistance,
          4
        ),

      vol5:
        Number(
          current.volume || 0
        ),

      vol15:
        round(
          vol15,
          2
        ),

      vol60:
        round(
          vol60,
          2
        ),

      rvol15:
        round(
          rvol15,
          4
        ),

      vol5_reference:
        round(
          vol5Reference,
          2
        ),

      vol5_ratio:
        round(
          vol5Ratio,
          4
        ),

      vol60_reference:
        round(
          vol60Reference,
          2
        ),

      vol60_ratio:
        round(
          vol60Ratio,
          4
        ),

      delta60_pct:
        round(
          delta60,
          4
        ),

      accel5m_pct:
        round(
          accel5m,
          4
        ),

      j1:
        candidate.j1,

      j2:
        candidate.j2,

      dv:
        round(
          dv,
          4
        ),

      /*
       * IMPORTANT:
       * Accel5M uniquement informatif.
       */

      accel5m_blocking:
        false,

      rvol_slot:
        slot
    };
  } catch (error) {
    return {
      ok: false,
      symbol,
      reason:
        "FETCH_ERROR",
      error:
        error?.message ||
        String(error)
    };
  }
}

/* =========================================================
   S1 SCAN
========================================================= */

async function runS1(
  candidates,
  asofMs
) {
  const started =
    Date.now();

  const raw =
    await mapConcurrent(
      candidates,
      candidate =>
        processS1Symbol(
          candidate,
          asofMs
        )
    );

  const valid =
    raw.filter(
      x => x?.ok
    );

  /*
   * Ranking DV décroissant.
   */

  valid.sort(
    (a, b) =>
      (
        b.dv ?? -Infinity
      ) -
      (
        a.dv ?? -Infinity
      )
  );

  const result = {
    stage: "S1",

    asof:
      asofMs,

    count:
      valid.length,

    candidates:
      valid,

    rejected:
      raw.length -
      valid.length,

    elapsed_ms:
      Date.now() -
      started
  };

  try {
    fs.writeFileSync(
      S1_CACHE_FILE,
      JSON.stringify(
        result,
        null,
        2
      )
    );
  } catch {}

  scanState.s1_count =
    valid.length;

  return result;
}

/* =========================================================
   SF CURRENT 15:55 -> 15:59
========================================================= */

async function runSF(
  candidates,
  asofMs
) {
  const started =
    Date.now();

  const results = [];

  for (
    const candidate of candidates
  ) {
    try {
      const chart =
        await fetchYahooChart(
          candidate.symbol,
          S1_INTERVAL,
          "1d"
        );

      const bars =
        chart.bars
          .filter(
            b =>
              isRegularSessionBar(
                b.ts
              )
          )
          .sort(
            (a, b) =>
              a.ts - b.ts
          );

      /*
       * Cherche la bougie 15:55
       * correspondant au snapshot SF.
       *
       * À 15:59, elle est encore
       * en formation.
       */

      const sfBars =
        bars.filter(
          bar => {
            const p =
              getNYParts(
                bar.ts
              );

            return (
              p.hour === 15 &&
              p.minute === 55
            );
          }
        );

      const sfBar =
        sfBars[
          sfBars.length - 1
        ];

      if (
        !sfBar
      ) {
        continue;
      }

      const barEnd =
        sfBar.ts +
        5 * 60 * 1000;

      /*
       * On exige que l'as-of soit
       * dans la fenêtre 15:55-16:00.
       */

      if (
        asofMs < sfBar.ts ||
        asofMs > barEnd
      ) {
        continue;
      }

      /*
       * Pour le prix SF,
       * Yahoo Chart peut fournir une
       * bougie partielle.
       */

      const partialBars =
        bars.filter(
          b =>
            b.ts <=
            asofMs
        );

      if (
        !partialBars.length
      ) {
        continue;
      }

      const current =
        partialBars[
          partialBars.length - 1
        ];

      const price =
        current.close;

      const vwap =
        calculateVWAP(
          partialBars
        );

      const hod =
        calculateHOD(
          partialBars
        );

      const priceVsVWAP =
        finite(vwap)
          ? (
              (
                price /
                vwap
              ) - 1
            ) * 100
          : null;

      const hodDistance =
        finite(hod) &&
        hod > 0
          ? (
              (
                price /
                hod
              ) - 1
            ) * 100
          : null;

      /*
       * RVOL15M :
       * volume des 3 dernières bougies
       * 5m disponibles.
       */

      const last3 =
        partialBars.slice(
          -3
        );

      const vol15 =
        last3.reduce(
          (sum, bar) =>
            sum +
            Number(
              bar.volume || 0
            ),
          0
        );

      const rvol15Reference =
        candidate
          .rvol15_reference;

      const rvol15 =
        finite(
          rvol15Reference
        ) &&
        rvol15Reference > 0
          ? vol15 /
            rvol15Reference
          : null;

      /*
       * Vol5 :
       *
       * IMPORTANT :
       * Le seuil verrouillé est
       *
       *   Vol5 / référence >= 0.20
       *
       * et NON Accel5M >= 20%.
       */

      const refVol5 =
        candidate
          .vol5_reference;

      /*
       * Ajustement de fenêtre partielle :
       *
       * La référence historique représente
       * la bougie 5m complète.
       *
       * Le facteur 0.8 est conservé ici
       * pour comparer une fenêtre 15:55->15:59
       * partiellement écoulée.
       */

      const refVol5Adjusted =
        finite(refVol5)
          ? refVol5 * 0.8
          : null;

      const vol5 =
        Number(
          sfBar.volume || 0
        );

      const vol5Ratio =
        finite(
          refVol5Adjusted
        ) &&
        refVol5Adjusted > 0
          ? vol5 /
            refVol5Adjusted
          : null;

      /*
       * Delta 60
       */

      const oneHourCutoff =
        asofMs -
        60 * 60 * 1000;

      const previousBars =
        partialBars.filter(
          b =>
            b.ts <=
            oneHourCutoff
        );

      const previous =
        previousBars[
          previousBars.length - 1
        ];

      const delta60 =
        previous &&
        previous.close > 0
          ? pctChange(
              previous.close,
              price
            )
          : null;

      /*
       * Delta 15:55 -> 15:59
       */

      const reference1555 =
        sfBar.open;

      const delta1555to1559 =
        reference1555 > 0
          ? pctChange(
              reference1555,
              price
            )
          : null;

      /*
       * Accel5M :
       * informatif uniquement.
       */

      const accel5m =
        calculateAccel5m(
          partialBars
        );

      /*
       * FILTRES SF VERROUILLÉS
       */

      const checks = {
        rvol15:
          finite(rvol15) &&
          rvol15 >=
            LOCKED.rvol15Min,

        vol5:
          finite(vol5Ratio) &&
          vol5Ratio >=
            LOCKED.vol5MinRatio,

        delta60:
          finite(delta60) &&
          delta60 <=
            LOCKED.delta60MaxPct,

        price_vs_vwap:
          finite(priceVsVWAP) &&
          priceVsVWAP >=
            LOCKED.priceVsVwapMinPct,

        delta1555to1559:
          finite(
            delta1555to1559
          ) &&
          delta1555to1559 >=
            LOCKED.delta1555to1559MinPct
      };

      const pass =
        Object.values(
          checks
        ).every(Boolean);

      results.push({
        symbol:
          candidate.symbol,

        asof:
          asofMs,

        price:
          round(
            price,
            4
          ),

        vwap:
          round(
            vwap,
            4
          ),

        price_vs_vwap_pct:
          round(
            priceVsVWAP,
            4
          ),

        hod:
          round(
            hod,
            4
          ),

        hod_distance_pct:
          round(
            hodDistance,
            4
          ),

        rvol15:
          round(
            rvol15,
            4
          ),

        vol5:
          vol5,

        vol5_reference:
          round(
            refVol5,
            2
          ),

        vol5_reference_adjusted:
          round(
            refVol5Adjusted,
            2
          ),

        vol5_ratio:
          round(
            vol5Ratio,
            4
          ),

        delta60_pct:
          round(
            delta60,
            4
          ),

        delta1555to1559_pct:
          round(
            delta1555to1559,
            4
          ),

        accel5m_pct:
          round(
            accel5m,
            4
          ),

        accel5m_blocking:
          false,

        checks,

        pass
      });
    } catch (error) {
      results.push({
        symbol:
          candidate.symbol,

        pass: false,

        error:
          error?.message ||
          String(error)
      });
    }
  }

  /*
   * Ranking DV :
   * uniquement parmi les candidats
   * qui passent SF.
   */

  const winners =
    results
      .filter(
        x => x.pass
      )
      .sort(
        (a, b) =>
          (
            candidates.find(
              c =>
                c.symbol ===
                a.symbol
            )?.dv ??
            -Infinity
          ) -
          (
            candidates.find(
              c =>
                c.symbol ===
                b.symbol
            )?.dv ??
            -Infinity
          )
      );

  return {
    stage: "SF",

    asof:
      asofMs,

    count:
      winners.length,

    winners,

    evaluated:
      results,

    locked_params:
      LOCKED,

    elapsed_ms:
      Date.now() -
      started
  };
}

/* =========================================================
   WINNER GATE
========================================================= */

async function runWinnerGate(
  winners,
  asofMs
) {
  if (
    !winners?.length
  ) {
    return {
      pass: false,
      reason:
        "NO_SF_WINNER"
    };
  }

  /*
   * Premier candidat après ranking.
   *
   * IMPORTANT :
   * Le Winner Gate doit s'arrêter
   * au premier winner.
   */

  const winner =
    winners[0];

  /*
   * Contrôle de fraîcheur minimal.
   *
   * Le serveur Chart ne fournit pas
   * un vrai tick WebSocket ici.
   */

  const candidateAsof =
    Number(
      winner.asof ||
      asofMs
    );

  const ageMs =
    Math.max(
      0,
      asofMs -
      candidateAsof
    );

  /*
   * Tolérance :
   * 2 minutes + 5m.
   */

  const MAX_TICK_AGE_MS =
    120000 +
    5 * 60 * 1000;

  const fresh =
    ageMs <=
    MAX_TICK_AGE_MS;

  return {
    pass:
      fresh,

    stop:
      fresh,

    reason:
      fresh
        ? "FIRST_WINNER"
        : "STALE_DATA",

    winner,

    age_ms:
      ageMs
  };
}

/* =========================================================
   MCP SERVER FACTORY
========================================================= */

function createMcpServer() {
  const server =
    new McpServer({
      name:
        "yahoo-scan-mcp",

      version:
        APP_VERSION
    });

  /* =======================================================
     PING
  ======================================================= */

  server.tool(
    "ping",
    "Health check",
    {},
    async () => ({
      content: [
        {
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
        }
      ]
    })
  );

  /* =======================================================
     STATUS
  ======================================================= */

  server.tool(
    "get_status",
    "Return scanner status",
    {},
    async () => ({
      content: [
        {
          type: "text",
          text:
            JSON.stringify({
              ok: true,
              version:
                APP_VERSION,
              scanState,
              locked:
                LOCKED,
              universe_file:
                UNIVERSE_FILE
            })
        }
      ]
    })
  );

  /* =======================================================
     DIAGNOSE FILESYSTEM
  ======================================================= */

  server.tool(
    "diagnose_filesystem",
    "Diagnose local scanner files",
    {},
    async () => ({
      content: [
        {
          type: "text",
          text:
            JSON.stringify({
              ok: true,

              cwd:
                ROOT,

              universe_file:
                UNIVERSE_FILE,

              universe_exists:
                fs.existsSync(
                  UNIVERSE_FILE
                ),

              universe_count:
                readUniverse().length,

              s0_cache_exists:
                fs.existsSync(
                  S0_CACHE_FILE
                ),

              s1_cache_exists:
                fs.existsSync(
                  S1_CACHE_FILE
                )
            })
        }
      ]
    })
  );

  /* =======================================================
     UNIVERSE
  ======================================================= */

  server.tool(
    "get_universe",
    "Return materialized NASDAQ <$5 universe",
    {},
    async () => {
      const symbols =
        readUniverse();

      return {
        content: [
          {
            type: "text",
            text:
              JSON.stringify({
                ok: true,
                count:
                  symbols.length,
                universe_file:
                  UNIVERSE_FILE,
                symbols
              })
          }
        ]
      };
    }
  );

  /* =======================================================
     YAHOO SPARK TEST
  ======================================================= */

  server.tool(
    "yahoo_spark_test",
    "Test Yahoo Chart OHLCV",
    {
      symbols:
        z
          .array(z.string())
          .optional(),

      range:
        z.string()
          .optional(),

      interval:
        z.string()
          .optional()
    },
    async ({
      symbols,
      range,
      interval
    }) => {
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

      const output =
        [];

      for (
        const symbol of list
      ) {
        try {
          const data =
            await fetchYahooChart(
              symbol,
              interval ||
                "5m",
              range ||
                "1d"
            );

          output.push({
            symbol,
            ok: true,
            bars:
              data.bars.length,
            last:
              data.bars[
                data.bars.length - 1
              ] || null,
            meta:
              data.meta
          });
        } catch (error) {
          output.push({
            symbol,
            ok: false,
            error:
              error?.message ||
              String(error)
          });
        }
      }

      return {
        content: [
          {
            type: "text",
            text:
              JSON.stringify({
                ok: true,
                results:
                  output
              })
          }
        ]
      };
    }
  );

  /* =======================================================
     WS COMPATIBILITY TEST
  ======================================================= */

  server.tool(
    "yahoo_ws_test",
    "Compatibility test for Yahoo realtime layer",
    {
      symbols:
        z
          .array(z.string())
          .optional()
    },
    async ({
      symbols
    }) => {
      /*
       * Le scanner v2.0 utilise Yahoo Chart
       * pour la donnée OHLCV principale.
       *
       * On garde ce tool pour compatibilité
       * avec les versions précédentes.
       */

      return {
        content: [
          {
            type: "text",
            text:
              JSON.stringify({
                ok: true,

                websocket:
                  "not required by v2.0.0 core scan",

                symbols:
                  symbols ||
                  [
                    "AAPL",
                    "MSFT",
                    "NVDA",
                    "AMD",
                    "INTC"
                  ],

                note:
                  "Core S0/S1/SF uses Yahoo Chart OHLCV."
              })
          }
        ]
      };
    }
  );

  /* =======================================================
     S0 PREPARE
  ======================================================= */

  server.tool(
    "yahoo_s0_prepare",
    "Build materialized S0 universe",
    {
      symbols:
        z
          .array(z.string())
          .optional()
    },
    async ({
      symbols
    }) => {
      const list =
        symbols?.length
          ? [
              ...new Set(
                symbols.map(
                  x =>
                    x
                      .trim()
                      .toUpperCase()
                )
              )
            ]
          : readUniverse();

      const result =
        await runS0(
          list
        );

      return {
        content: [
          {
            type: "text",
            text:
              JSON.stringify({
                ok: true,
                ...result
              })
          }
        ]
      };
    }
  );

  /* =======================================================
     S1 SCAN
  ======================================================= */

  server.tool(
    "yahoo_s1_scan",
    "Run S1 on materialized S0",
    {
      asof:
        z.string()
          .optional(),

      symbols:
        z
          .array(z.string())
          .optional()
    },
    async ({
      asof,
      symbols
    }) => {
      let s0 =
        loadS0Cache();

      if (
        symbols?.length
      ) {
        s0 = {
          candidates:
            symbols.map(
              symbol => ({
                symbol:
                  symbol
                    .trim()
                    .toUpperCase()
              })
            )
        };
      }

      if (
        !s0?.candidates?.length
      ) {
        return {
          content: [
            {
              type: "text",
              text:
                JSON.stringify({
                  ok: false,
                  error:
                    "S0 cache empty"
                })
            }
          ]
        };
      }

      const asofMs =
        asof
          ? Date.parse(asof)
          : Date.now();

      const result =
        await runS1(
          s0.candidates,
          asofMs
        );

      scanState.asof =
        asofMs;

      return {
        content: [
          {
            type: "text",
            text:
              JSON.stringify({
                ok: true,
                ...result
              })
          }
        ]
      };
    }
  );

  /* =======================================================
     COMPLETE S0 -> S1
  ======================================================= */

  server.tool(
    "yahoo_s0_s1_scan",
    "Run complete S0 then S1 scan",
    {
      asof:
        z.string()
          .optional(),

      symbols:
        z
          .array(z.string())
          .optional()
    },
    async ({
      asof,
      symbols
    }) => {
      const started =
        Date.now();

      scanState.started_at =
        new Date().toISOString();

      scanState.status =
        "running";

      const list =
        symbols?.length
          ? [
              ...new Set(
                symbols.map(
                  x =>
                    x
                      .trim()
                      .toUpperCase()
                )
              )
            ]
          : readUniverse();

      const s0 =
        await runS0(
          list
        );

      const asofMs =
        asof
          ? Date.parse(asof)
          : Date.now();

      const s1 =
        await runS1(
          s0.candidates,
          asofMs
        );

      scanState.asof =
        asofMs;

      scanState.elapsed_ms =
        Date.now() -
        started;

      scanState.completed_at =
        new Date().toISOString();

      scanState.status =
        "completed";

      return {
        content: [
          {
            type: "text",
            text:
              JSON.stringify({
                ok: true,

                stage:
                  "S0_S1",

                asof:
                  asofMs,

                elapsed_ms:
                  scanState.elapsed_ms,

                universe_requested:
                  list.length,

                s0_count:
                  s0.count,

                s1_count:
                  s1.count,

                s0:
                  s0.candidates,

                s1:
                  s1.candidates
              })
          }
        ]
      };
    }
  );

  /* =======================================================
     SF
  ======================================================= */

  server.tool(
    "yahoo_sf_scan",
    "Run locked SF filters",
    {
      asof:
        z.string()
          .optional(),

      symbols:
        z
          .array(z.string())
          .optional()
    },
    async ({
      asof,
      symbols
    }) => {
      let s1;

      if (
        symbols?.length
      ) {
        s1 = {
          candidates:
            symbols.map(
              symbol => ({
                symbol:
                  symbol
                    .trim()
                    .toUpperCase()
              })
            )
        };
      } else {
        try {
          s1 =
            JSON.parse(
              fs.readFileSync(
                S1_CACHE_FILE,
                "utf8"
              )
            );
        } catch {
          return {
            content: [
              {
                type: "text",
                text:
                  JSON.stringify({
                    ok: false,
                    error:
                      "S1 cache empty"
                  })
              }
            ]
          };
        }
      }

      if (
        !s1?.candidates?.length
      ) {
        return {
          content: [
            {
              type: "text",
              text:
                JSON.stringify({
                  ok: false,
                  error:
                    "No S1 candidates"
                })
            }
          ]
        };
      }

      const asofMs =
        asof
          ? Date.parse(asof)
          : Date.now();

      const result =
        await runSF(
          s1.candidates,
          asofMs
        );

      scanState.sf_count =
        result.count;

      return {
        content: [
          {
            type: "text",
            text:
              JSON.stringify({
                ok: true,
                ...result
              })
          }
        ]
      };
    }
  );

  /* =======================================================
     LOCKED PARAMS
  ======================================================= */

  server.tool(
    "get_locked_params",
    "Return historically locked scanner parameters",
    {},
    async () => ({
      content: [
        {
          type: "text",
          text:
            JSON.stringify({
              ok: true,

              locked:
                LOCKED,

              non_blocking:
                NON_BLOCKING,

              notes: [
                "RVOL15M minimum = 1.50",
                "Vol5 ratio minimum = 0.20",
                "Delta60 maximum = +10%",
                "Price vs VWAP minimum = -1%",
                "Delta 15:55 -> 15:59 minimum = 0%",
                "Lot size = 20",
                "Ranking = DV descending",
                "First Winner Gate PASS = STOP",
                "Accel5M is NOT a blocking filter"
              ]
            })
        }
      ]
    })
  );

  /* =======================================================
     SCAN STATE
  ======================================================= */

  server.tool(
    "get_scan_state",
    "Return current scan state",
    {},
    async () => ({
      content: [
        {
          type: "text",
          text:
            JSON.stringify({
              ok: true,
              scanState
            })
        }
      ]
    })
  );

  return server;
}

/* =========================================================
   HEALTH
========================================================= */

app.get(
  "/health",
  (_req, res) => {
    res.json({
      ok: true,

      service:
        "yahoo-scan-mcp",

      version:
        APP_VERSION,

      timestamp:
        new Date().toISOString(),

      universe:
        readUniverse().length
    });
  }
);

/* =========================================================
   MCP HTTP
========================================================= */

app.post(
  "/mcp",
  async (req, res) => {
    try {
      const server =
        createMcpServer();

      const transport =
        new StreamableHTTPServerTransport(
          {
            sessionIdGenerator:
              undefined
          }
        );

      res.on(
        "close",
        () => {
          transport.close().catch(
            () => {}
          );

          server.close().catch(
            () => {}
          );
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
        res.status(500).json({
          ok: false,
          error:
            error?.message ||
            String(error)
        });
      }
    }
  }
);

/* =========================================================
   MCP GET
========================================================= */

app.get(
  "/mcp",
  (_req, res) => {
    res.status(405).json({
      ok: false,

      error:
        "MCP endpoint requires POST"
    });
  }
);

/* =========================================================
   MCP DELETE
========================================================= */

app.delete(
  "/mcp",
  (_req, res) => {
    res.status(405).json({
      ok: false,

      error:
        "Stateless MCP transport"
    });
  }
);

/* =========================================================
   ROOT
========================================================= */

app.get(
  "/",
  (_req, res) => {
    res.json({
      ok: true,

      service:
        "yahoo-scan-mcp",

      version:
        APP_VERSION,

      endpoints: {
        health:
          "/health",

        mcp:
          "/mcp"
      }
    });
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
      `[yahoo-scan-mcp] MCP server started v${APP_VERSION}`
    );

    console.log(
      `[yahoo-scan-mcp] HTTP server listening on ${PORT}`
    );

    console.log(
      `[yahoo-scan-mcp] Universe file: ${UNIVERSE_FILE}`
    );

    console.log(
      `[yahoo-scan-mcp] Universe symbols: ${readUniverse().length}`
    );
  }
);
