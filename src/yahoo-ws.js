import WebSocket from "ws";

const YAHOO_WS_URL =
  "wss://streamer.finance.yahoo.com/?version=2";

// ------------------------------------------------------------
// PROTOBUF HELPERS
// ------------------------------------------------------------

function readVarint(buffer, offset) {
  let value = 0n;
  let shift = 0n;
  let pos = offset;

  while (pos < buffer.length) {
    const byte = buffer[pos++];

    value |= BigInt(byte & 0x7f) << shift;

    if ((byte & 0x80) === 0) {
      return {
        value,
        offset: pos
      };
    }

    shift += 7n;

    if (shift > 70n) {
      throw new Error("Invalid protobuf varint");
    }
  }

  throw new Error("Unexpected end of protobuf varint");
}

function zigzagDecode(value) {
  return (value >> 1n) ^ -(value & 1n);
}

function uint64ToNumber(value) {
  const number = Number(value);

  if (!Number.isSafeInteger(number)) {
    return Number(value);
  }

  return number;
}

function readFloat(buffer, offset) {
  if (offset + 4 > buffer.length) {
    throw new Error("Unexpected end of protobuf float");
  }

  return {
    value: buffer.readFloatLE(offset),
    offset: offset + 4
  };
}

function readDouble(buffer, offset) {
  if (offset + 8 > buffer.length) {
    throw new Error("Unexpected end of protobuf double");
  }

  return {
    value: buffer.readDoubleLE(offset),
    offset: offset + 8
  };
}

function readString(buffer, offset) {
  const lengthResult = readVarint(buffer, offset);

  const length = uint64ToNumber(lengthResult.value);

  const start = lengthResult.offset;
  const end = start + length;

  if (end > buffer.length) {
    throw new Error("Unexpected end of protobuf string");
  }

  return {
    value: buffer.toString("utf8", start, end),
    offset: end
  };
}

function skipField(buffer, offset, wireType) {
  switch (wireType) {

    // varint
    case 0: {
      return readVarint(buffer, offset).offset;
    }

    // 64-bit
    case 1: {
      const next = offset + 8;

      if (next > buffer.length) {
        throw new Error("Unexpected end of protobuf 64-bit field");
      }

      return next;
    }

    // length-delimited
    case 2: {
      const lengthResult = readVarint(buffer, offset);
      const length = uint64ToNumber(lengthResult.value);
      const next = lengthResult.offset + length;

      if (next > buffer.length) {
        throw new Error(
          "Unexpected end of protobuf length-delimited field"
        );
      }

      return next;
    }

    // 32-bit
    case 5: {
      const next = offset + 4;

      if (next > buffer.length) {
        throw new Error("Unexpected end of protobuf 32-bit field");
      }

      return next;
    }

    default:
      throw new Error(
        `Unsupported protobuf wire type: ${wireType}`
      );
  }
}

// ------------------------------------------------------------
// YAHOO PRICINGDATA DECODER
// ------------------------------------------------------------
//
// Yahoo PricingData schema:
//
// 1  id
// 2  price
// 3  time
// 4  currency
// 5  exchange
// 6  quote_type
// 7  market_hours
// 8  change_percent
// 9  day_volume
// 10 day_high
// 11 day_low
// 12 change
// 13 short_name
// 14 expire_date
// 15 open_price
// 16 previous_close
// 17 strike_price
// 18 underlying_symbol
// 19 open_interest
// 20 options_type
// 21 mini_option
// 22 last_size
// 23 bid
// 24 bid_size
// 25 ask
// 26 ask_size
// 27 price_hint
//
// ------------------------------------------------------------

function decodePricingData(buffer) {

  if (!Buffer.isBuffer(buffer)) {
    buffer = Buffer.from(buffer);
  }

  let offset = 0;

  const result = {};

  while (offset < buffer.length) {

    const tagResult = readVarint(buffer, offset);

    const tag = uint64ToNumber(tagResult.value);

    offset = tagResult.offset;

    const fieldNumber = tag >>> 3;
    const wireType = tag & 7;

    switch (fieldNumber) {

      // ------------------------------------------------------
      // id
      // ------------------------------------------------------

      case 1: {
        if (wireType !== 2) {
          offset = skipField(buffer, offset, wireType);
          break;
        }

        const value = readString(buffer, offset);

        result.id = value.value;
        offset = value.offset;
        break;
      }

      // ------------------------------------------------------
      // price
      // ------------------------------------------------------

      case 2: {
        if (wireType !== 5) {
          offset = skipField(buffer, offset, wireType);
          break;
        }

        const value = readFloat(buffer, offset);

        result.price = value.value;
        offset = value.offset;
        break;
      }

      // ------------------------------------------------------
      // time - sint64
      // ------------------------------------------------------

      case 3: {
        if (wireType !== 0) {
          offset = skipField(buffer, offset, wireType);
          break;
        }

        const value = readVarint(buffer, offset);

        result.time = uint64ToNumber(
          zigzagDecode(value.value)
        );

        offset = value.offset;
        break;
      }

      // ------------------------------------------------------
      // currency
      // ------------------------------------------------------

      case 4: {
        if (wireType !== 2) {
          offset = skipField(buffer, offset, wireType);
          break;
        }

        const value = readString(buffer, offset);

        result.currency = value.value;
        offset = value.offset;
        break;
      }

      // ------------------------------------------------------
      // exchange
      // ------------------------------------------------------

      case 5: {
        if (wireType !== 2) {
          offset = skipField(buffer, offset, wireType);
          break;
        }

        const value = readString(buffer, offset);

        result.exchange = value.value;
        offset = value.offset;
        break;
      }

      // ------------------------------------------------------
      // quote_type
      // ------------------------------------------------------

      case 6: {
        if (wireType !== 0) {
          offset = skipField(buffer, offset, wireType);
          break;
        }

        const value = readVarint(buffer, offset);

        result.quote_type =
          uint64ToNumber(value.value);

        offset = value.offset;
        break;
      }

      // ------------------------------------------------------
      // market_hours
      // ------------------------------------------------------

      case 7: {
        if (wireType !== 0) {
          offset = skipField(buffer, offset, wireType);
          break;
        }

        const value = readVarint(buffer, offset);

        result.market_hours =
          uint64ToNumber(value.value);

        offset = value.offset;
        break;
      }

      // ------------------------------------------------------
      // change_percent
      // ------------------------------------------------------

      case 8: {
        if (wireType !== 5) {
          offset = skipField(buffer, offset, wireType);
          break;
        }

        const value = readFloat(buffer, offset);

        result.change_percent = value.value;
        offset = value.offset;
        break;
      }

      // ------------------------------------------------------
      // day_volume
      // ------------------------------------------------------

      case 9: {
        if (wireType !== 0) {
          offset = skipField(buffer, offset, wireType);
          break;
        }

        const value = readVarint(buffer, offset);

        result.day_volume =
          uint64ToNumber(value.value);

        offset = value.offset;
        break;
      }

      // ------------------------------------------------------
      // day_high
      // ------------------------------------------------------

      case 10: {
        if (wireType !== 5) {
          offset = skipField(buffer, offset, wireType);
          break;
        }

        const value = readFloat(buffer, offset);

        result.day_high = value.value;
        offset = value.offset;
        break;
      }

      // ------------------------------------------------------
      // day_low
      // ------------------------------------------------------

      case 11: {
        if (wireType !== 5) {
          offset = skipField(buffer, offset, wireType);
          break;
        }

        const value = readFloat(buffer, offset);

        result.day_low = value.value;
        offset = value.offset;
        break;
      }

      // ------------------------------------------------------
      // change
      // ------------------------------------------------------

      case 12: {
        if (wireType !== 5) {
          offset = skipField(buffer, offset, wireType);
          break;
        }

        const value = readFloat(buffer, offset);

        result.change = value.value;
        offset = value.offset;
        break;
      }

      // ------------------------------------------------------
      // short_name
      // ------------------------------------------------------

      case 13: {
        if (wireType !== 2) {
          offset = skipField(buffer, offset, wireType);
          break;
        }

        const value = readString(buffer, offset);

        result.short_name = value.value;
        offset = value.offset;
        break;
      }

      // ------------------------------------------------------
      // expire_date
      // ------------------------------------------------------

      case 14: {
        if (wireType !== 0) {
          offset = skipField(buffer, offset, wireType);
          break;
        }

        const value = readVarint(buffer, offset);

        result.expire_date =
          uint64ToNumber(value.value);

        offset = value.offset;
        break;
      }

      // ------------------------------------------------------
      // open_price
      // ------------------------------------------------------

      case 15: {
        if (wireType !== 5) {
          offset = skipField(buffer, offset, wireType);
          break;
        }

        const value = readFloat(buffer, offset);

        result.open_price = value.value;
        offset = value.offset;
        break;
      }

      // ------------------------------------------------------
      // previous_close
      // ------------------------------------------------------

      case 16: {
        if (wireType !== 5) {
          offset = skipField(buffer, offset, wireType);
          break;
        }

        const value = readFloat(buffer, offset);

        result.previous_close = value.value;
        offset = value.offset;
        break;
      }

      // ------------------------------------------------------
      // strike_price
      // ------------------------------------------------------

      case 17: {
        if (wireType !== 5) {
          offset = skipField(buffer, offset, wireType);
          break;
        }

        const value = readFloat(buffer, offset);

        result.strike_price = value.value;
        offset = value.offset;
        break;
      }

      // ------------------------------------------------------
      // underlying_symbol
      // ------------------------------------------------------

      case 18: {
        if (wireType !== 2) {
          offset = skipField(buffer, offset, wireType);
          break;
        }

        const value = readString(buffer, offset);

        result.underlying_symbol = value.value;
        offset = value.offset;
        break;
      }

      // ------------------------------------------------------
      // open_interest
      // ------------------------------------------------------

      case 19: {
        if (wireType !== 0) {
          offset = skipField(buffer, offset, wireType);
          break;
        }

        const value = readVarint(buffer, offset);

        result.open_interest =
          uint64ToNumber(value.value);

        offset = value.offset;
        break;
      }

      // ------------------------------------------------------
      // options_type
      // ------------------------------------------------------

      case 20: {
        if (wireType !== 0) {
          offset = skipField(buffer, offset, wireType);
          break;
        }

        const value = readVarint(buffer, offset);

        result.options_type =
          uint64ToNumber(value.value);

        offset = value.offset;
        break;
      }

      // ------------------------------------------------------
      // mini_option
      // ------------------------------------------------------

      case 21: {
        if (wireType !== 0) {
          offset = skipField(buffer, offset, wireType);
          break;
        }

        const value = readVarint(buffer, offset);

        result.mini_option =
          uint64ToNumber(value.value);

        offset = value.offset;
        break;
      }

      // ------------------------------------------------------
      // last_size
      // ------------------------------------------------------

      case 22: {
        if (wireType !== 0) {
          offset = skipField(buffer, offset, wireType);
          break;
        }

        const value = readVarint(buffer, offset);

        result.last_size =
          uint64ToNumber(value.value);

        offset = value.offset;
        break;
      }

      // ------------------------------------------------------
      // bid
      // ------------------------------------------------------

      case 23: {
        if (wireType !== 5) {
          offset = skipField(buffer, offset, wireType);
          break;
        }

        const value = readFloat(buffer, offset);

        result.bid = value.value;
        offset = value.offset;
        break;
      }

      // ------------------------------------------------------
      // bid_size
      // ------------------------------------------------------

      case 24: {
        if (wireType !== 0) {
          offset = skipField(buffer, offset, wireType);
          break;
        }

        const value = readVarint(buffer, offset);

        result.bid_size =
          uint64ToNumber(value.value);

        offset = value.offset;
        break;
      }

      // ------------------------------------------------------
      // ask
      // ------------------------------------------------------

      case 25: {
        if (wireType !== 5) {
          offset = skipField(buffer, offset, wireType);
          break;
        }

        const value = readFloat(buffer, offset);

        result.ask = value.value;
        offset = value.offset;
        break;
      }

      // ------------------------------------------------------
      // ask_size
      // ------------------------------------------------------

      case 26: {
        if (wireType !== 0) {
          offset = skipField(buffer, offset, wireType);
          break;
        }

        const value = readVarint(buffer, offset);

        result.ask_size =
          uint64ToNumber(value.value);

        offset = value.offset;
        break;
      }

      // ------------------------------------------------------
      // price_hint
      // ------------------------------------------------------

      case 27: {
        if (wireType !== 0) {
          offset = skipField(buffer, offset, wireType);
          break;
        }

        const value = readVarint(buffer, offset);

        result.price_hint =
          uint64ToNumber(value.value);

        offset = value.offset;
        break;
      }

      // ------------------------------------------------------
      // Unknown field
      // ------------------------------------------------------

      default: {
        offset = skipField(
          buffer,
          offset,
          wireType
        );

        break;
      }
    }
  }

  return result;
}

// ------------------------------------------------------------
// BASE64 + JSON DECODER
// ------------------------------------------------------------

function decodeYahooMessage(data) {

  const raw =
    Buffer.isBuffer(data)
      ? data.toString("utf8")
      : String(data);

  const json = JSON.parse(raw);

  if (!json.message) {
    throw new Error(
      "Yahoo message does not contain 'message'"
    );
  }

  const protobufBuffer =
    Buffer.from(json.message, "base64");

  return decodePricingData(protobufBuffer);
}

// ------------------------------------------------------------
// YAHOO WEBSOCKET
// ------------------------------------------------------------

export class YahooWS {

  constructor({ log = console.error } = {}) {

    this.log = log;
    this.ws = null;

    this.connected = false;
    this.subscribed = false;

    this.symbols = [];

    this.messages = 0;
    this.ticks = 0;
    this.decodeErrors = 0;

    this.connectedAt = null;
    this.firstMessageAt = null;

    this.lastMessages = [];

    // Dernier tick valide par ticker
    this.latest = new Map();

    // Petit historique récent global
    this.recentTicks = [];

    this.heartbeat = null;
  }

  // ----------------------------------------------------------
  // CONNECT
  // ----------------------------------------------------------

  async connect() {

    if (this.connected && this.ws) {
      return;
    }

    await new Promise((resolve, reject) => {

      const ws =
        new WebSocket(YAHOO_WS_URL);

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

        resolve();
      });

      // ------------------------------------------------------
      // MESSAGE
      // ------------------------------------------------------

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

          const tick =
            decodeYahooMessage(data);

          // Validation minimale :
          // un vrai tick doit au minimum avoir
          // un ticker et un prix numérique.

          if (
            !tick.id ||
            typeof tick.price !== "number" ||
            !Number.isFinite(tick.price)
          ) {
            throw new Error(
              "Decoded Yahoo message is missing id or valid price"
            );
          }

          const normalized = {

            symbol:
              String(tick.id).toUpperCase(),

            price:
              tick.price,

            time:
              tick.time ?? null,

            timestamp:
              tick.time
                ? new Date(
                    Number(tick.time) * 1000
                  ).toISOString()
                : null,

            currency:
              tick.currency ?? null,

            exchange:
              tick.exchange ?? null,

            quote_type:
              tick.quote_type ?? null,

            market_hours:
              tick.market_hours ?? null,

            change:
              tick.change ?? null,

            change_percent:
              tick.change_percent ?? null,

            day_volume:
              tick.day_volume ?? null,

            day_high:
              tick.day_high ?? null,

            day_low:
              tick.day_low ?? null,

            open_price:
              tick.open_price ?? null,

            previous_close:
              tick.previous_close ?? null,

            last_size:
              tick.last_size ?? null,

            bid:
              tick.bid ?? null,

            bid_size:
              tick.bid_size ?? null,

            ask:
              tick.ask ?? null,

            ask_size:
              tick.ask_size ?? null,

            price_hint:
              tick.price_hint ?? null,

            received_at:
              new Date().toISOString()
          };

          // --------------------------------------------------
          // STORE LAST TICK PER SYMBOL
          // --------------------------------------------------

          this.latest.set(
            normalized.symbol,
            normalized
          );

          // --------------------------------------------------
          // RECENT TICKS
          // --------------------------------------------------

          this.recentTicks.push(normalized);

          if (this.recentTicks.length > 50) {
            this.recentTicks.shift();
          }

          // --------------------------------------------------
          // REAL DECODED TICK COUNT
          // --------------------------------------------------

          this.ticks++;

        } catch (err) {

          this.decodeErrors++;

          this.log(
            "[yahoo-ws] decode error:",
            err?.message || String(err)
          );
        }
      });

      // ------------------------------------------------------
      // ERROR
      // ------------------------------------------------------

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

      // ------------------------------------------------------
      // CLOSE
      // ------------------------------------------------------

      ws.on("close", () => {

        this.connected = false;
        this.subscribed = false;

        this.stopHeartbeat();

        this.log(
          "[yahoo-ws] closed"
        );
      });
    });
  }

  // ----------------------------------------------------------
  // SUBSCRIBE
  // ----------------------------------------------------------

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

    this.startHeartbeat();

    this.log(
      `[yahoo-ws] subscribed to ${list.length} symbols`
    );

    return list.length;
  }

  // ----------------------------------------------------------
  // HEARTBEAT
  // ----------------------------------------------------------
  //
  // yfinance renvoie périodiquement l'abonnement.
  // Cela permet de maintenir le flux vivant.
  // ----------------------------------------------------------

  startHeartbeat() {

    this.stopHeartbeat();

    this.heartbeat = setInterval(() => {

      if (
        !this.ws ||
        !this.connected ||
        !this.subscribed ||
        !this.symbols.length
      ) {
        return;
      }

      try {

        this.ws.send(
          JSON.stringify({
            subscribe: this.symbols
          })
        );

      } catch (err) {

        this.log(
          "[yahoo-ws] heartbeat error:",
          err?.message || String(err)
        );
      }

    }, 15000);
  }

  stopHeartbeat() {

    if (this.heartbeat) {

      clearInterval(this.heartbeat);

      this.heartbeat = null;
    }
  }

  // ----------------------------------------------------------
  // STATUS
  // ----------------------------------------------------------

  status() {

    return {

      connected:
        this.connected,

      subscribed:
        this.subscribed,

      symbols:
        this.symbols.length,

      subscribed_symbols:
        this.symbols,

      messages:
        this.messages,

      ticks:
        this.ticks,

      decode_errors:
        this.decodeErrors,

      errors:
        this.errors,

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
        this.recentTicks.slice(-10),

      last_messages:
        this.lastMessages
    };
  }

  // ----------------------------------------------------------
  // CLOSE
  // ----------------------------------------------------------

  close() {

    this.stopHeartbeat();

    if (!this.ws) {
      return;
    }

    try {
      this.ws.close();
    } catch {}

    this.ws = null;

    this.connected = false;
    this.subscribed = false;
  }
}
