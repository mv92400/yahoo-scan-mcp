import WebSocket from "ws";

const YAHOO_WS_URL = "wss://streamer.finance.yahoo.com/?version=2";

/* =========================
   Protobuf helpers
   ========================= */

function readVarint(buf, offset) {
  let result = 0;
  let shift = 0;

  while (offset < buf.length) {
    const byte = buf[offset++];

    result += (byte & 0x7f) * 2 ** shift;

    if (!(byte & 0x80)) {
      return {
        value: result,
        offset
      };
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

  const length = Number(len.value);
  const end = len.offset + length;

  if (!Number.isSafeInteger(length)) {
    throw new Error("invalid length-delimited size");
  }

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

    case 1: {
      if (offset + 8 > buf.length) {
        throw new Error("unexpected end of fixed64");
      }

      return offset + 8;
    }

    case 2: {
      return readLengthDelimited(buf, offset).offset;
    }

    case 5: {
      if (offset + 4 > buf.length) {
        throw new Error("unexpected end of fixed32");
      }

      return offset + 4;
    }

    default:
      throw new Error(`unsupported wire type ${wireType}`);
  }
}

/* =========================
   Yahoo PricingData decoder
   ========================= */

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

    /*
     * Protobuf valid wire types:
     * 0 = varint
     * 1 = fixed64
     * 2 = length-delimited
     * 5 = fixed32
     *
     * 3/4 are deprecated group types.
     */
    if (
      wireType !== 0 &&
      wireType !== 1 &&
      wireType !== 2 &&
      wireType !== 5
    ) {
      throw new Error(
        `invalid protobuf wire type ${wireType} at offset ${offset}`
      );
    }

    switch (fieldNumber) {
      /* =========================
         1 - id / symbol
         ========================= */

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

      /* =========================
         2 - price
         ========================= */

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

      /* =========================
         3 - time
         ========================= */

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

      /* =========================
         4 - currency
         ========================= */

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

      /* =========================
         5 - exchange
         ========================= */

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

      /* =========================
         6 - quoteType
         ========================= */

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

      /* =========================
         7 - marketHours
         ========================= */

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

      /* =========================
         8 - changePercent
         ========================= */

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

      /* =========================
         9 - dayVolume
         ========================= */

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

      /* =========================
         10 - dayHigh
         ========================= */

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

      /* =========================
         11 - dayLow
         ========================= */

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

      /* =========================
         12 - change
         ========================= */

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

      /* =========================
         13 - shortName
         ========================= */

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

      /* =========================
         14 - expireDate
         ========================= */

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

      /* =========================
         15 - openPrice
         ========================= */

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

      /* =========================
         16 - previousClose
         ========================= */

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

      /* =========================
         17 - strikePrice
         ========================= */

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

      /* =========================
         18 - underlyingSymbol
         ========================= */

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

      /* =========================
         19 - openInterest
         ========================= */

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

      /* =========================
         20 - optionsType
         ========================= */

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

      /* =========================
         21 - miniOption
         ========================= */

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

      /* =========================
         22 - lastSize
         ========================= */

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

      /* =========================
         23 - bid
         ========================= */

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

      /* =========================
         24 - bidSize
         ========================= */

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

      /* =========================
         25 - ask
         ========================= */

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

      /* =========================
         26 - askSize
         ========================= */

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

      /* =========================
         27 - priceHint
         ========================= */

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

      /* =========================
         Unknown field
         ========================= */

      default: {
        offset = skipField(buf, offset, wireType);
        break;
      }
    }
  }

  return tick;
}

/* =========================
   Yahoo JSON wrapper
   ========================= */

function decodeYahooMessage(data) {
  const outer = JSON.parse(data.toString());

  if (!outer.message) {
    return null;
  }

  const payload = Buffer.from(
    outer.message,
    "base64"
  );

  return decodePricingData(payload);
}

/* =========================
   Yahoo WebSocket client
   ========================= */

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

        reject(
          new Error(
            "Yahoo WebSocket connection timeout"
          )
        );
      }, 15000);

      ws.on("open", () => {
        clearTimeout(timeout);

        this.connected = true;
        this.connectedAt = Date.now();

        this.log(
          "[yahoo-ws] connected"
        );

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
          receivedAt:
            new Date().toISOString(),

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

          let timestamp = null;

          if (
            tick.time != null &&
            Number.isFinite(Number(tick.time))
          ) {
            const d = new Date(
              Number(tick.time)
            );

            if (!Number.isNaN(d.getTime())) {
              timestamp = d.toISOString();
            }
          }

          const normalized = {
            symbol: tick.id,

            price: tick.price,

            time: tick.time,

            timestamp,

            currency: tick.currency,

            exchange: tick.exchange,

            quote_type: tick.quoteType,

            market_hours: tick.marketHours,

            change: tick.change,

            change_percent:
              tick.changePercent,

            day_volume: tick.dayVolume,

            day_high: tick.dayHigh,

            day_low: tick.dayLow,

            open_price: tick.openPrice,

            previous_close:
              tick.previousClose,

            last_size: tick.lastSize,

            bid: tick.bid,

            bid_size: tick.bidSize,

            ask: tick.ask,

            ask_size: tick.askSize,

            price_hint: tick.priceHint,

            received_at:
              new Date().toISOString()
          };

          this.latest.set(
            normalized.symbol,
            normalized
          );

          this.recentTicks.push(
            normalized
          );

          if (this.recentTicks.length > 20) {
            this.recentTicks.shift();
          }

          this.ticks++;
        } catch (err) {
          this.decodeErrors++;

          this.log(
            "[yahoo-ws] decode error:",
            err?.message ||
              String(err)
          );
        }
      });

      ws.on("error", (err) => {
        this.errors++;

        this.log(
          "[yahoo-ws] error:",
          err?.message ||
            String(err)
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
          clearInterval(
            this.heartbeat
          );

          this.heartbeat = null;
        }

        this.log(
          "[yahoo-ws] closed"
        );
      });
    });
  }

  subscribe(symbols) {
    if (
      !this.ws ||
      !this.connected
    ) {
      throw new Error(
        "Yahoo WebSocket is not connected"
      );
    }

    const list = [
      ...new Set(
        symbols
          .map((s) =>
            String(s)
              .trim()
              .toUpperCase()
          )
          .filter(Boolean)
      )
    ];

    if (!list.length) {
      throw new Error(
        "No symbols supplied"
      );
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

      subscribed_symbols:
        this.symbols,

      messages: this.messages,

      ticks: this.ticks,

      errors: this.errors,

      decode_errors:
        this.decodeErrors,

      connected_at:
        this.connectedAt
          ? new Date(
              this.connectedAt
            ).toISOString()
          : null,

      first_message_at:
        this.firstMessageAt
          ? new Date(
              this.firstMessageAt
            ).toISOString()
          : null,

      latest:
        Object.fromEntries(
          this.latest
        ),

      recent_ticks:
        this.recentTicks,

      last_messages:
        this.lastMessages
    };
  }

  close() {
    if (this.heartbeat) {
      clearInterval(
        this.heartbeat
      );

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
