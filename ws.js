// Yahoo Finance streamer: connect, subscribe, unsubscribe, reconnect, decode.
import WebSocket from "ws";
import protobuf from "protobufjs";

const URL = "wss://streamer.finance.yahoo.com/?version=2";

const PROTO = `
syntax = "proto3";
message PricingData {
  string id = 1;
  float price = 2;
  sint64 time = 3;
  string currency = 4;
  string exchange = 5;
  int32 quoteType = 6;
  int32 marketHours = 7;
  float changePercent = 8;
  sint64 dayVolume = 9;
  float dayHigh = 10;
  float dayLow = 11;
  float change = 12;
  string shortName = 13;
  sint64 expireDate = 14;
  float openPrice = 15;
  float previousClose = 16;
  float strikePrice = 17;
  string underlyingSymbol = 18;
  sint64 openInterest = 19;
  sint64 optionsType = 20;
  sint64 miniOption = 21;
  sint64 lastSize = 22;
  float bid = 23;
  sint64 bidSize = 24;
  float ask = 25;
  sint64 askSize = 26;
  sint64 priceHint = 27;
  sint64 vol24hr = 28;
  sint64 volAllCurrencies = 29;
  string fromCurrency = 30;
  string lastMarket = 31;
  double circulatingSupply = 32;
  double marketcap = 33;
}`;
const PricingData = protobuf.parse(PROTO).root.lookupType("PricingData");

export class YahooWS {
  constructor(cache, { chunk = 500, chunkDelayMs = 50, log = () => {} } = {}) {
    this.cache = cache;
    this.chunk = chunk;
    this.chunkDelayMs = chunkDelayMs;
    this.log = log;
    this.ws = null;
    this.subscribed = new Set();
    this.connectedAt = null;
    this.reconnects = 0;
    this.closedByUser = false;
    this.retry = 0;
    this.errors = [];
  }

  connect() {
    return new Promise((resolve, reject) => {
      this.closedByUser = false;
      const ws = new WebSocket(URL, { headers: { Origin: "https://finance.yahoo.com" } });
      this.ws = ws;
      let settled = false;
      ws.on("open", () => {
        this.connectedAt = Date.now();
        this.retry = 0;
        if (this.subscribed.size) this._sendSubscribe([...this.subscribed]); // resubscribe after reconnect
        settled = true; resolve();
      });
      ws.on("message", raw => this._onMessage(raw));
      ws.on("error", e => {
        this._err(`ws error: ${e.message}`);
        if (!settled) { settled = true; reject(e); }
      });
      ws.on("close", () => {
        if (this.closedByUser) return;
        const delay = Math.min(30000, 1000 * 2 ** this.retry++);
        this.reconnects++;
        this._err(`ws closed, reconnect in ${delay} ms`);
        setTimeout(() => this.connect().catch(() => {}), delay);
      });
    });
  }

  _onMessage(raw) {
    try {
      const msg = JSON.parse(raw.toString());
      if (msg.type !== "pricing" || !msg.message) return;
      const d = PricingData.toObject(PricingData.decode(Buffer.from(msg.message, "base64")), { longs: Number });
      this.cache.update(d);
    } catch (e) {
      this.cache.decodeErrors++;
    }
  }

  async subscribe(symbols) {
    const fresh = symbols.filter(s => !this.subscribed.has(s));
    fresh.forEach(s => this.subscribed.add(s));
    await this._sendSubscribe(fresh);
  }

  async _sendSubscribe(list) {
    for (let i = 0; i < list.length; i += this.chunk) {
      if (this.ws?.readyState !== WebSocket.OPEN) return;
      this.ws.send(JSON.stringify({ subscribe: list.slice(i, i + this.chunk) }));
      if (this.chunkDelayMs) await new Promise(r => setTimeout(r, this.chunkDelayMs));
    }
  }

  unsubscribe(symbols) {
    symbols.forEach(s => this.subscribed.delete(s));
    if (this.ws?.readyState === WebSocket.OPEN) this.ws.send(JSON.stringify({ unsubscribe: symbols }));
  }

  close() {
    this.closedByUser = true;
    this.ws?.close();
  }

  _err(m) { this.errors.push({ at: new Date().toISOString(), m }); if (this.errors.length > 50) this.errors.shift(); this.log(m); }

  status() {
    return {
      open: this.ws?.readyState === WebSocket.OPEN,
      connected_at: this.connectedAt ? new Date(this.connectedAt).toISOString() : null,
      subscribed: this.subscribed.size,
      reconnects: this.reconnects,
      recent_errors: this.errors.slice(-5),
    };
  }
                       }
                   
