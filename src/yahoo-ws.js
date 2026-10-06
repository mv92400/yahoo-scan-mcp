import WebSocket from "ws";

const YAHOO_WS_URL = "wss://streamer.finance.yahoo.com/?version=2";

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

    this.connectedAt = null;
    this.firstMessageAt = null;

    this.lastMessages = [];
  }

  async connect() {
    if (this.connected && this.ws) return;

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

        // Pour l'instant on ne décode pas encore le protobuf.
        // Cette couche sert uniquement à valider la connexion
        // et le flux brut Yahoo.
        this.ticks++;
      });

      ws.on("error", (err) => {
        this.errors++;
        this.log("[yahoo-ws] error:", err.message);

        if (!this.connected) {
          clearTimeout(timeout);
          reject(err);
        }
      });

      ws.on("close", () => {
        this.connected = false;
        this.subscribed = false;

        this.log("[yahoo-ws] closed");
      });
    });
  }

  subscribe(symbols) {
    if (!this.ws || !this.connected) {
      throw new Error("Yahoo WebSocket is not connected");
    }

    const list = [...new Set(
      symbols
        .map(s => String(s).trim().toUpperCase())
        .filter(Boolean)
    )];

    if (!list.length) {
      throw new Error("No symbols supplied");
    }

    this.symbols = list;

    this.ws.send(JSON.stringify({
      subscribe: list
    }));

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
      messages: this.messages,
      ticks: this.ticks,
      errors: this.errors,
      connected_at: this.connectedAt
        ? new Date(this.connectedAt).toISOString()
        : null,
      first_message_at: this.firstMessageAt
        ? new Date(this.firstMessageAt).toISOString()
        : null,
      last_messages: this.lastMessages
    };
  }

  close() {
    if (!this.ws) return;

    try {
      this.ws.close();
    } catch {}

    this.ws = null;
    this.connected = false;
    this.subscribed = false;
  }
}
