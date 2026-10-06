import WebSocket from "ws";

const YAHOO_WS_URL = "wss://streamer.finance.yahoo.com/?version=2";

function readVarint(buf, offset) {
  let result = 0;
  let shift = 0;

  while (offset < buf.length) {
    const byte = buf[offset++];

    result += (byte & 0x7f) * 2 ** shift;

    if (!(byte & 0x80)) {
      return { value: result, offset };
    }

    shift += 7;

    if (shift > 63) {
      throw new Error("varint too long");
    }
  }

  throw new Error("unexpected end of varint");
}

function readFloat(buf, offset) {
  if (offset + 4 > buf.length) {
    throw new Error("unexpected end of float");
  }

  return {
    value: buf.readFloatLE(offset),
    offset: offset + 4
  };
}

function readLengthDelimited(buf, offset) {
  const len = readVarint(buf, offset);

  const end = len.offset + Number(len.value);

  if (end > buf.length) {
    throw new Error("unexpected end of length-delimited field");
  }

  return {
    value: buf.subarray(len.offset, end),
    offset: end
  };
}

function readString(buf, offset) {
  const r = readLengthDelimited(buf, offset);

  return {
    value: r.value.toString("utf8"),
    offset: r.offset
  };
}

function skipField(buf, offset, wireType) {
  switch (wireType) {
    case 0: {
      return readVarint(buf, offset).offset;
    }

    case 1:
      if (offset + 8 > buf.length) {
        throw new Error("unexpected end of fixed64");
      }
      return offset + 8;

    case 2: {
      return readLengthDelimited(buf, offset).offset;
    }

    case 5:
      if (offset + 4 > buf.length) {
        throw new Error("unexpected end of fixed32");
      }
      return offset + 4;

    default:
      throw new Error(`unsupported wire type ${wireType}`);
  }
}

function decodePricingData(buffer) {
  const buf = Buffer.from(buffer);

  const tick = {
    id: null,
    price: null,
    time: null,
    currency: null,
    exchange: null,
    quoteType: null,
    marketHours: null,
    changePercent: null,
    dayVolume: null,
    dayHigh: null,
    dayLow: null,
    change: null,
    shortName: null,
    expireDate: null,
    openPrice: null,
    previousClose: null,
    strikePrice: null,
    underlyingSymbol: null,
    openInterest: null,
    optionsType: null,
    miniOption: null,
    lastSize: null,
    bid: null,
    bidSize: null,
    ask: null,
    askSize: null,
    priceHint: null
  };

  let offset = 0;

  while (offset < buf.length) {
    const key = readVarint(buf, offset);
    offset = key.offset;

    const fieldNumber = Math.floor(key.value / 8);
    const wireType = key.value % 8;

    switch (fieldNumber) {
      // string
      case 1: {
        if (wireType !== 2) {
          offset = skipField(buf, offset, wireType);
          break;
        }

        const r = readString(buf, offset);
        tick.id = r.value;
        offset = r.offset;
        break;
      }

      // float
      case 2: {
        if (wireType !== 5) {
          offset = skipField(buf, offset, wireType);
          break;
        }

        const r = readFloat(buf, offset);
        tick.price = r.value;
        offset = r.offset;
        break;
      }

      // uint64
      case 3: {
        if (wireType !== 0) {
          offset = skipField(buf, offset, wireType);
          break;
        }

        const r = readVarint(buf, offset);
        tick.time = Number(r.value);
        offset = r.offset;
        break;
      }

      // string
      case 4: {
        if (wireType !== 2) {
          offset = skipField(buf, offset, wireType);
          break;
        }

        const r = readString(buf, offset);
        tick.currency = r.value;
        offset = r.offset;
        break;
      }

      // string
      case 5: {
        if (wireType !== 2) {
          offset = skipField(buf, offset, wireType);
          break;
        }

        const r = readString(buf, offset);
        tick.exchange = r.value;
        offset = r.offset;
        break;
      }

      // uint32
      case 6: {
        if (wireType !== 0) {
          offset = skipField(buf, offset, wireType);
          break;
        }

        const r = readVarint(buf, offset);
        tick.quoteType = Number(r.value);
        offset = r.offset;
        break;
      }

      // uint32
      case 7: {
        if (wireType !== 0) {
          offset = skipField(buf, offset, wireType);
          break;
        }

        const r = readVarint(buf, offset);
        tick.marketHours = Number(r.value);
        offset = r.offset;
        break;
      }

      // float
      case 8: {
        if (wireType !== 5) {
          offset = skipField(buf, offset, wireType);
          break;
        }

        const r = readFloat(buf, offset);
        tick.changePercent = r.value;
        offset = r.offset;
        break;
      }

      // uint64
      case 9: {
        if (wireType !== 0) {
          offset = skipField(buf, offset, wireType);
          break;
        }

        const r = readVarint(buf, offset);
        tick.dayVolume = Number(r.value);
        offset = r.offset;
        break;
      }

      // float
      case 10: {
        if (wireType !== 5) {
          offset = skipField(buf, offset, wireType);
          break;
        }

        const r = readFloat(buf, offset);
        tick.dayHigh = r.value;
        offset = r.offset;
        break;
      }

      // float
      case 11: {
        if (wireType !== 5) {
          offset = skipField(buf, offset, wireType);
          break;
        }

        const r = readFloat(buf, offset);
        tick.dayLow = r.value;
        offset = r.offset;
        break;
      }

      // float
      case 12: {
        if (wireType !== 5) {
          offset = skipField(buf, offset, wireType);
          break;
        }

        const r = readFloat(buf, offset);
        tick.change = r.value;
        offset = r.offset;
        break;
      }

      // string
      case 13: {
        if (wireType !== 2) {
          offset = skipField(buf, offset, wireType);
          break;
        }

        const r = readString(buf, offset);
        tick.shortName = r.value;
        offset = r.offset;
        break;
      }

      // uint64
      case 14: {
        if (wireType !== 0) {
          offset = skipField(buf, offset, wireType);
          break;
        }

        const r = readVarint(buf, offset);
        tick.expireDate = Number(r.value);
        offset = r.offset;
        break;
      }

      // float
      case 15: {
        if (wireType !== 5) {
          offset = skipField(buf, offset, wireType);
          break;
        }

        const r = readFloat(buf, offset);
        tick.openPrice = r.value;
        offset = r.offset;
        break;
      }

      // float
      case 16: {
        if (wireType !== 5) {
          offset = skipField(buf, offset, wireType);
          break;
        }

        const r = readFloat(buf, offset);
        tick.previousClose = r.value;
        offset = r.offset;
        break;
      }

      // float
      case 17: {
        if (wireType !== 5) {
          offset = skipField(buf, offset, wireType);
          break;
        }

        const r = readFloat(buf, offset);
        tick.strikePrice = r.value;
        offset = r.offset;
        break;
      }

      // string
      case 18: {
        if (wireType !== 2) {
          offset = skipField(buf, offset, wireType);
          break;
        }

        const r = readString(buf, offset);
        tick.underlyingSymbol = r.value;
        offset = r.offset;
        break;
      }

      // uint64
      case 19: {
        if (wireType !== 0) {
          offset = skipField(buf, offset, wireType);
          break;
        }

        const r = readVarint(buf, offset);
        tick.openInterest = Number(r.value);
        offset = r.offset;
        break;
      }

      // uint32
      case 20: {
        if (wireType !== 0) {
          offset = skipField(buf, offset, wireType);
          break;
        }

        const r = readVarint(buf, offset);
        tick.optionsType = Number(r.value);
        offset = r.offset;
        break;
      }

      // uint32 / bool
      case 21: {
        if (wireType !== 0) {
          offset = skipField(buf, offset, wireType);
          break;
        }

        const r = readVarint(buf, offset);
        tick.miniOption = Boolean(r.value);
        offset = r.offset;
        break;
      }

      // uint64
      case 22: {
        if (wireType !== 0) {
          offset = skipField(buf, offset, wireType);
          break;
        }

        const r = readVarint(buf, offset);
        tick.lastSize = Number(r.value);
        offset = r.offset;
        break;
      }

      // float
      case 23: {
        if (wireType !== 5) {
          offset = skipField(buf, offset, wireType);
          break;
        }

        const r = readFloat(buf, offset);
        tick.bid = r.value;
        offset = r.offset;
        break;
      }

      // uint64
      case 24: {
        if (wireType !== 0) {
          offset = skipField(buf, offset, wireType);
          break;
        }

        const r = readVarint(buf, offset);
        tick.bidSize = Number(r.value);
        offset = r.offset;
        break;
      }

      // float
      case 25: {
        if (wireType !== 5) {
          offset = skipField(buf, offset, wireType);
          break;
        }

        const r = readFloat(buf, offset);
        tick.ask = r.value;
        offset = r.offset;
        break;
      }

      // uint64
      case 26: {
        if (wireType !== 0) {
          offset = skipField(buf, offset, wireType);
          break;
        }

        const r = readVarint(buf, offset);
        tick.askSize = Number(r.value);
        offset = r.offset;
        break;
      }

      // uint32
      case 27: {
        if (wireType !== 0) {
          offset = skipField(buf, offset, wireType);
          break;
        }

        const r = readVarint(buf, offset);
        tick.priceHint = Number(r.value);
        offset = r.offset;
        break;
      }

      default:
        offset = skipField(buf, offset, wireType);
    }
  }

  return tick;
}

function decodeYahooMessage(data) {
  const outer = JSON.parse(data.toString());

  if (!outer.message) {
    return null;
  }

  const payload = Buffer.from(outer.message, "base64");

  return decodePricingData(payload);
}

export class YahooWS {
  constructor({ log = console.error } = {}) {
    this.log = log;
    this.ws = null;

    this.connected = false;
    this.subscribed = false;

    this.symbols = [];
    this.messages = 0;
    this.ticks = 0;
    this.errors = 0;
    this.decodeErrors = 0;

    this.connectedAt = null;
    this.firstMessageAt = null;

    this.lastMessages = [];
    this.recentTicks = [];
    this.latest = new Map();

    this.heartbeat = null;
  }

  async connect() {
    if (this.connected && this.ws) {
      return;
    }

    await new Promise((resolve, reject) => {
      const ws = new WebSocket(YAHOO_WS_URL);

      this.ws = ws;

      const timeout = setTimeout(() => {
        try {
          ws.close();
        } catch {}

        reject(new Error("Yahoo WebSocket connection timeout"));
      }, 15000);

      ws.on("open", () => {
        clearTimeout(timeout);

        this.connected = true;
        this.connectedAt = Date.now();

        this.log("[yahoo-ws] connected");

        this.heartbeat = setInterval(() => {
          if (
            this.connected &&
            this.ws &&
            this.symbols.length
          ) {
            try {
              this.ws.send(
                JSON.stringify({
                  subscribe: this.symbols
                })
              );
            } catch {}
          }
        }, 15000);

        resolve();
      });

      ws.on("message", (data) => {
        this.messages++;

        if (!this.firstMessageAt) {
          this.firstMessageAt = Date.now();
        }

        this.lastMessages.push({
          receivedAt: new Date().toISOString(),
          bytes: data.length
        });

        if (this.lastMessages.length > 10) {
          this.lastMessages.shift();
        }

        try {
          const tick = decodeYahooMessage(data);

          if (
            !tick ||
            !tick.id ||
            tick.price == null ||
            !Number.isFinite(tick.price)
          ) {
            return;
          }

          const normalized = {
            symbol: tick.id,
            price: tick.price,

            time: tick.time,

            timestamp:
              tick.time != null
                ? new Date(Number(tick.time)).toISOString()
                : null,

            currency: tick.currency,
            exchange: tick.exchange,
            quote_type: tick.quoteType,
            market_hours: tick.marketHours,

            change: tick.change,
            change_percent: tick.changePercent,

            day_volume: tick.dayVolume,
            day_high: tick.dayHigh,
            day_low: tick.dayLow,

            open_price: tick.openPrice,
            previous_close: tick.previousClose,

            last_size: tick.lastSize,

            bid: tick.bid,
            bid_size: tick.bidSize,

            ask: tick.ask,
            ask_size: tick.askSize,

            price_hint: tick.priceHint,

            received_at: new Date().toISOString()
          };

          this.latest.set(normalized.symbol, normalized);

          this.recentTicks.push(normalized);

          if (this.recentTicks.length > 20) {
            this.recentTicks.shift();
          }

          this.ticks++;
        } catch (err) {
          this.decodeErrors++;

          this.log(
            "[yahoo-ws] decode error:",
            err?.message || String(err)
          );
        }
      });

      ws.on("error", (err) => {
        this.errors++;

        this.log(
          "[yahoo-ws] error:",
          err.message
        );

        if (!this.connected) {
          clearTimeout(timeout);
          reject(err);
        }
      });

      ws.on("close", () => {
        this.connected = false;
        this.subscribed = false;

        if (this.heartbeat) {
          clearInterval(this.heartbeat);
          this.heartbeat = null;
        }

        this.log("[yahoo-ws] closed");
      });
    });
  }

  subscribe(symbols) {
    if (!this.ws || !this.connected) {
      throw new Error(
        "Yahoo WebSocket is not connected"
      );
    }

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

    if (!list.length) {
      throw new Error("No symbols supplied");
    }

    this.symbols = list;

    this.ws.send(
      JSON.stringify({
        subscribe: list
      })
    );

    this.subscribed = true;

    this.log(
      `[yahoo-ws] subscribed to ${list.length} symbols`
    );

    return list.length;
  }

  status() {
    return {
      connected: this.connected,
      subscribed: this.subscribed,

      symbols: this.symbols.length,
      subscribed_symbols: this.symbols,

      messages: this.messages,
      ticks: this.ticks,

      errors: this.errors,
      decode_errors: this.decodeErrors,

      connected_at: this.connectedAt
        ? new Date(this.connectedAt).toISOString()
        : null,

      first_message_at: this.firstMessageAt
        ? new Date(this.firstMessageAt).toISOString()
        : null,

      latest: Object.fromEntries(this.latest),

      recent_ticks: this.recentTicks,

      last_messages: this.lastMessages
    };
  }

  close() {
    if (this.heartbeat) {
      clearInterval(this.heartbeat);
      this.heartbeat = null;
    }

    if (this.ws) {
      try {
        this.ws.close();
      } catch {}
    }

    this.ws = null;

    this.connected = false;
    this.subscribed = false;
  }
}
