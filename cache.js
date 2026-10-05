// Live cache: latest tick + rolling ticks per symbol, volume increments, VWAP.

const ET_FMT = new Intl.DateTimeFormat("en-US", {
  timeZone: "America/New_York", hour12: false,
  year: "numeric", month: "2-digit", day: "2-digit",
  hour: "2-digit", minute: "2-digit", second: "2-digit",
});

function etParts(ms) {
  const p = Object.fromEntries(ET_FMT.formatToParts(new Date(ms)).map(x => [x.type, x.value]));
  return { y: +p.year, mo: +p.month, d: +p.day, h: +p.hour % 24, mi: +p.minute, s: +p.second };
}

/** Epoch ms of today's HH:MM:SS in America/New_York (relative to refMs). */
export function etToday(h, m, s = 0, refMs = Date.now()) {
  const { y, mo, d } = etParts(refMs);
  const guess = Date.UTC(y, mo - 1, d, h, m, s);
  const g = etParts(guess);
  const asUtc = Date.UTC(g.y, g.mo - 1, g.d, g.h, g.mi, g.s);
  return guess - (asUtc - guess); // corrects for the ET offset (DST-safe)
}

export function etDateString(ms = Date.now()) {
  const { y, mo, d } = etParts(ms);
  return `${y}-${String(mo).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
}

export class LiveCache {
  constructor(retentionMs = 70 * 60 * 1000) {
    this.retentionMs = retentionMs;
    this.sym = new Map(); // symbol -> state
    this.startedAt = Date.now();
    this.ticksReceived = 0;
    this.decodeErrors = 0;
  }

  resetCoverage() { this.startedAt = Date.now(); }

  update(d) {
    const s = d.id;
    if (!s || !(d.price > 0)) return;
    const t = Number(d.time) || Date.now();
    const v = Number(d.dayVolume) || 0;
    let st = this.sym.get(s);
    if (!st) {
      st = { ring: [], last: null, sumPV: 0, sumV: 0, firstSeen: Date.now() };
      this.sym.set(s, st);
    }
    // VWAP accumulation from day-volume increments (see README: accurate only if cache started at open)
    const prev = st.last;
    if (prev && v > prev.v) {
      const dv = v - prev.v;
      st.sumPV += d.price * dv;
      st.sumV += dv;
    } else if (!prev && v > 0) {
      st.sumPV += d.price * (Number(d.lastSize) || 0);
      st.sumV += Number(d.lastSize) || 0;
    }
    st.last = {
      t, p: d.price, v, recvAt: Date.now(),
      high: d.dayHigh, low: d.dayLow, open: d.openPrice, prevClose: d.previousClose,
      change: d.change, changePct: d.changePercent, bid: d.bid, ask: d.ask,
      bidSize: Number(d.bidSize), askSize: Number(d.askSize), lastSize: Number(d.lastSize),
      exchange: d.exchange, currency: d.currency, quoteType: d.quoteType,
    };
    st.ring.push({ t, p: d.price, v });
    const cutoff = Date.now() - this.retentionMs;
    while (st.ring.length && st.ring[0].t < cutoff) st.ring.shift();
    this.ticksReceived++;
  }

  get(s) { return this.sym.get(s); }

  /** Last tick at or before t -> {p, v} or null. */
  at(s, t) {
    const st = this.sym.get(s);
    if (!st) return null;
    let found = null;
    for (const x of st.ring) { if (x.t <= t) found = x; else break; }
    return found;
  }

  /**
   * Volume traded in (t0, t1]. Returns null if coverage is insufficient.
   * No tick before t0 while the cache was already running at t0 => baseline = first tick's volume minus its last size.
   */
  volume(s, t0, t1) {
    const st = this.sym.get(s);
    if (!st || !st.ring.length) return null;
    const end = this.at(s, t1);
    if (!end) return null;
    let start = this.at(s, t0);
    let startV;
    if (start) startV = start.v;
    else if (this.startedAt <= t0) {
      const f = st.ring[0];
      startV = Math.max(0, f.v - (Number(st.last?.lastSize) || 0));
      if (f.t > t1) return 0;
    } else return null;
    return Math.max(0, end.v - startV);
  }

  vwap(s) {
    const st = this.sym.get(s);
    return st && st.sumV > 0 ? st.sumPV / st.sumV : null;
  }

  freshnessMs(s, now = Date.now()) {
    const st = this.sym.get(s);
    return st?.last ? now - st.last.recvAt : null;
  }

  stats() {
    return {
      symbols_with_ticks: this.sym.size,
      ticks_received: this.ticksReceived,
      decode_errors: this.decodeErrors,
      cache_started_at: new Date(this.startedAt).toISOString(),
      retention_min: this.retentionMs / 60000,
    };
  }
                                                                      }
                                                                      
