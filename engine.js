// S1 engine, Ranking DV, Lot engine, SF engine, Winner Gate.
// Price < $5 is fixed at S0 (universe file) and is NOT re-checked in S1/SF/Winner Gate.
import { etToday } from "./cache.js";

const MIN = 60 * 1000;

export const LOCKED = {
  rvol15Min: 1.5,
  vol5MinPct: 0.2,
  delta60MaxPct: 0.1,
  priceVsVwapMinPct: -1,
  delta1555to1559MinPct: 0,
  lotSize: 20,
};

// Unlocked parameters: kept configurable, defaults deliberately permissive.
export const UNLOCKED = {
  minDollarVolumeS1: 0,        // S1 liquidity threshold (not locked)
  maxTickAgeMs: 120 * 1000,    // freshness tolerance (not locked)
  vol5RefScale: 0.8,           // SF 15:59 only sees 15:55->15:59 (4 of 5 minutes): ref.vol5 scaled by 4/5
};

/**
 * universe: Map<symbol, {type:'CS', exchange:'NASDAQ'}>
 * refs:     Map<symbol, {vol15, vol5, vol60}>  historical reference volumes for the same window (~2 months)
 */
export class Scanner {
  constructor(cache, universe, refs, opts = {}) {
    this.cache = cache;
    this.universe = universe;
    this.refs = refs;
    this.p = { ...UNLOCKED, ...opts };
    this.s1Result = null;
    this.lots = [];
  }

  /** S1 at 15:55 ET (asOfMs lets you replay a given instant). */
  runS1(asOfMs = etToday(15, 55)) {
    const t0 = Date.now();
    const rows = [];
    const counters = { universe: this.universe.size, with_ticks: 0, no_refs: 0, no_volume_data: 0, low_liquidity: 0 };

    for (const sym of this.universe.keys()) {
      const st = this.cache.get(sym);
      if (!st?.last) continue;
      counters.with_ticks++;
      const ref = this.refs.get(sym);
      if (!ref || !(ref.vol15 > 0)) { counters.no_refs++; continue; }

      const px = this.cache.at(sym, asOfMs);
      const vol15 = this.cache.volume(sym, asOfMs - 15 * MIN, asOfMs);
      const vol60 = this.cache.volume(sym, asOfMs - 60 * MIN, asOfMs);
      const vol5 = this.cache.volume(sym, asOfMs - 5 * MIN, asOfMs);
      const volPrev5 = this.cache.volume(sym, asOfMs - 10 * MIN, asOfMs - 5 * MIN);
      if (!px || vol15 == null) { counters.no_volume_data++; continue; }

      const dayVolume = px.v;
      const dv = px.p * dayVolume;
      if (dv < this.p.minDollarVolumeS1) { counters.low_liquidity++; continue; }

      const vwap = this.cache.vwap(sym);
      rows.push({
        symbol: sym, price: px.p, dayVolume, dv,
        vol15, rvol15: vol15 / ref.vol15,
        vol60, delta60: vol60 != null && ref.vol60 > 0 ? vol60 / ref.vol60 - 1 : null,
        vwap, priceVsVwapPct: vwap ? (px.p / vwap - 1) * 100 : null,
        accel5m: vol5 != null && volPrev5 > 0 ? vol5 / volPrev5 : null, // bonus, not a filter
        price1555: px.p,
      });
    }

    // Ranking DV (descending) then lots of 20, pre-formed before SF
    rows.sort((a, b) => b.dv - a.dv);
    rows.forEach((r, i) => (r.rank = i + 1));
    this.lots = [];
    for (let i = 0; i < rows.length; i += LOCKED.lotSize) this.lots.push(rows.slice(i, i + LOCKED.lotSize));

    this.s1Result = {
      as_of: new Date(asOfMs).toISOString(),
      started_at: new Date(t0).toISOString(),
      ended_at: new Date().toISOString(),
      duration_ms: Date.now() - t0,
      counters: { ...counters, shortlist: rows.length, lots: this.lots.length },
      shortlist: rows,
    };
    return this.s1Result;
  }

  /** SF at 15:59 ET: lots in DV order, first candidate passing SF + Winner Gate wins, then STOP. */
  runSF(asOfMs = etToday(15, 59)) {
    const t0 = Date.now();
    const t1555 = etToday(15, 55, 0, asOfMs);
    const examined = [];
    let winner = null;
    let lotReached = 0;

    outer: for (let li = 0; li < this.lots.length; li++) {
      lotReached = li + 1;
      for (const c of this.lots[li]) {
        const m = this.sfMetrics(c.symbol, asOfMs, t1555);
        const checks = this.sfChecks(m);
        const pass = Object.values(checks).every(Boolean);
        const rec = { rank: c.rank, symbol: c.symbol, dv: c.dv, metrics: m, checks, pass };
        if (pass) {
          const gate = this.winnerGate(c.symbol, asOfMs);
          rec.winner_gate = gate;
          examined.push(rec);
          if (gate.pass) {
            winner = { ...rec, signal_time: new Date(asOfMs).toISOString(), lot: li + 1 };
            break outer; // STOP: first Winner Gate PASS
          }
        } else examined.push(rec);
      }
    }

    return {
      as_of: new Date(asOfMs).toISOString(),
      duration_ms: Date.now() - t0,
      lot_reached: lotReached,
      candidates_examined: examined.length,
      winner,
      examined,
    };
  }

  sfMetrics(sym, asOfMs, t1555) {
    const ref = this.refs.get(sym) || {};
    const px = this.cache.at(sym, asOfMs);
    const px1555 = this.cache.at(sym, t1555);
    const vol15 = this.cache.volume(sym, asOfMs - 15 * MIN, asOfMs);
    const vol5 = this.cache.volume(sym, t1555, asOfMs); // point-in-time: 15:55 -> 15:59
    const vol60 = this.cache.volume(sym, asOfMs - 60 * MIN, asOfMs);
    const vwap = this.cache.vwap(sym);
    const vol5Ref = ref.vol5 > 0 ? ref.vol5 * this.p.vol5RefScale : null;
    const st = this.cache.get(sym);
    return {
      price: px?.p ?? null,
      rvol15: vol15 != null && ref.vol15 > 0 ? vol15 / ref.vol15 : null,
      vol5_ratio: vol5 != null && vol5Ref ? vol5 / vol5Ref : null,
      delta60_pct: vol60 != null && ref.vol60 > 0 ? (vol60 / ref.vol60 - 1) * 100 : null,
      price_vs_vwap_pct: px && vwap ? (px.p / vwap - 1) * 100 : null,
      delta1555_1559_pct: px && px1555 ? (px.p / px1555.p - 1) * 100 : null,
      hod: st?.last?.high ?? null,
      distance_hod_pct: px && st?.last?.high ? (px.p / st.last.high - 1) * 100 : null,
    };
  }

  sfChecks(m) {
    const ok = (v, f) => v != null && f(v);
    return {
      rvol15: ok(m.rvol15, v => v >= LOCKED.rvol15Min),
      vol5: ok(m.vol5_ratio, v => v >= LOCKED.vol5MinPct),
      delta60: ok(m.delta60_pct, v => v <= LOCKED.delta60MaxPct * 100),
      vwap: ok(m.price_vs_vwap_pct, v => v >= LOCKED.priceVsVwapMinPct),
      momentum: ok(m.delta1555_1559_pct, v => v >= LOCKED.delta1555to1559MinPct),
    };
  }

  /** Identity, freshness, complete data. (No price check: fixed at S0.) */
  winnerGate(sym, asOfMs) {
    const u = this.universe.get(sym);
    const st = this.cache.get(sym);
    const age = st?.last ? asOfMs - st.last.t : null;
    const checks = {
      identity: !!u && u.type === "CS" && u.exchange === "NASDAQ",
      freshness: age != null && age <= this.p.maxTickAgeMs,
      data_complete: !!(st?.last && this.refs.get(sym) && this.cache.vwap(sym) != null),
    };
    return { pass: Object.values(checks).every(Boolean), checks, last_tick_age_ms: age };
  }
  }
      
