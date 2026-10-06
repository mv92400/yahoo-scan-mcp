#!/usr/bin/env node

import http from "node:http";

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { z } from "zod";

import { YahooWS } from "./src/yahoo-ws.js";

const PORT = Number(process.env.PORT || 8080);

function log(...args) {
  console.error("[yahoo-scan-mcp]", ...args);
}

function json(res, status, body) {
  const data = JSON.stringify(body);

  res.writeHead(status, {
    "Content-Type": "application/json; charset=utf-8",
    "Content-Length": Buffer.byteLength(data),
    "Cache-Control": "no-store"
  });

  res.end(data);
}

// ------------------------------------------------------------
// YAHOO WEBSOCKET
// ------------------------------------------------------------

const yahoo = new YahooWS({
  log
});

// ------------------------------------------------------------
// MCP SERVER
// ------------------------------------------------------------

function buildServer() {
  const server = new McpServer({
    name: "yahoo-scan-mcp",
    version: "1.0.0"
  });

  // ----------------------------------------------------------
  // PING
  // ----------------------------------------------------------

  server.tool(
    "ping",
    "Basic health test for the Yahoo Small Cap scanner MCP.",
    {},
    async () => ({
      content: [
        {
          type: "text",
          text: JSON.stringify({
            ok: true,
            service: "yahoo-scan-mcp",
            version: "1.0.0",
            timestamp: new Date().toISOString()
          })
        }
      ]
    })
  );

  // ----------------------------------------------------------
  // STATUS
  // ----------------------------------------------------------

  server.tool(
    "get_status",
    "Returns the current status of the MCP server and Yahoo WebSocket.",
    {},
    async () => ({
      content: [
        {
          type: "text",
          text: JSON.stringify({
            ok: true,
            service: "yahoo-scan-mcp",
            stage: "yahoo_websocket_test",
            yahoo_websocket: yahoo.status(),
            universe_loaded: false,
            timestamp: new Date().toISOString()
          }, null, 2)
        }
      ]
    })
  );

  // ----------------------------------------------------------
  // YAHOO WEBSOCKET TEST
  // ----------------------------------------------------------

  server.tool(
    "yahoo_ws_test",
    "Connects to Yahoo Finance WebSocket, subscribes to symbols and tests live market data reception.",
    {
      symbols: z.array(z.string()).optional(),
      seconds: z.number().int().min(5).max(120).optional()
    },
    async ({ symbols, seconds }) => {

      const list = symbols?.length
        ? symbols
        : [
            "AAPL",
            "MSFT",
            "NVDA",
            "AMD",
            "INTC"
          ];

      const duration = seconds ?? 15;

      const started = Date.now();

      try {

        // Connect to Yahoo
        await yahoo.connect();

        // Subscribe
        const subscribed = yahoo.subscribe(list);

        // Wait for incoming messages
        await new Promise(resolve =>
          setTimeout(resolve, duration * 1000)
        );

        const status = yahoo.status();

        return {
          content: [
            {
              type: "text",
              text: JSON.stringify({
                ok: true,
                requested_symbols: list,
                requested_count: list.length,
                subscribed_count: subscribed,
                duration_seconds: duration,
                elapsed_ms: Date.now() - started,
                yahoo: status,

                note:
                  "This is currently a raw WebSocket connectivity test. " +
                  "Yahoo payload decoding into validated ticker ticks will be implemented next."
              }, null, 2)
            }
          ]
        };

      } catch (err) {

        return {
          content: [
            {
              type: "text",
              text: JSON.stringify({
                ok: false,
                error: err?.message || String(err),
                yahoo: yahoo.status()
              }, null, 2)
            }
          ]
        };
      }
    }
  );

  return server;
}

// ------------------------------------------------------------
// READ REQUEST BODY
// ------------------------------------------------------------

async function readBody(req) {
  const chunks = [];
  let size = 0;

  for await (const chunk of req) {

    chunks.push(chunk);
    size += chunk.length;

    if (size > 4 * 1024 * 1024) {
      throw new Error("Request body too large");
    }
  }

  if (chunks.length === 0) {
    return undefined;
  }

  return JSON.parse(
    Buffer.concat(chunks).toString("utf8")
  );
}

// ------------------------------------------------------------
// HTTP SERVER
// ------------------------------------------------------------

const httpServer = http.createServer(
  async (req, res) => {

    try {

      // ------------------------------------------------------
      // HEALTH
      // ------------------------------------------------------

      if (
        req.method === "GET" &&
        req.url === "/health"
      ) {
        return json(res, 200, {
          ok: true,
          service: "yahoo-scan-mcp",
          timestamp: new Date().toISOString()
        });
      }

      // ------------------------------------------------------
      // MCP
      // ------------------------------------------------------

      if (req.url === "/mcp") {

        if (
          !["POST", "GET", "DELETE"].includes(req.method)
        ) {
          res.writeHead(405, {
            Allow: "GET, POST, DELETE"
          });

          return res.end();
        }

        const mcpServer = buildServer();

        const transport =
          new StreamableHTTPServerTransport({
            sessionIdGenerator: undefined
          });

        res.on("close", () => {
          transport.close().catch(() => {});
          mcpServer.close().catch(() => {});
        });

        await mcpServer.connect(transport);

        let body;

        if (req.method === "POST") {
          body = await readBody(req);
        }

        await transport.handleRequest(
          req,
          res,
          body
        );

        return;
      }

      // ------------------------------------------------------
      // ROOT
      // ------------------------------------------------------

      if (
        req.method === "GET" &&
        req.url === "/"
      ) {
        return json(res, 200, {
          ok: true,
          service: "yahoo-scan-mcp",
          version: "1.0.0",
          authentication: "none",
          endpoints: {
            health: "/health",
            mcp: "/mcp"
          }
        });
      }

      // ------------------------------------------------------
      // 404
      // ------------------------------------------------------

      return json(res, 404, {
        error: "Not found"
      });

    } catch (err) {

      log("HTTP error:", err);

      if (!res.headersSent) {
        return json(res, 500, {
          error: "Internal server error"
        });
      }

      res.end();
    }
  }
);

// ------------------------------------------------------------
// SERVER ERROR
// ------------------------------------------------------------

httpServer.on("error", (err) => {
  log("HTTP server error:", err);
  process.exit(1);
});

// ------------------------------------------------------------
// START
// ------------------------------------------------------------

httpServer.listen(
  PORT,
  "0.0.0.0",
  () => {
    log(
      `Listening on 0.0.0.0:${PORT}`
    );

    log(
      `Health: /health`
    );

    log(
      `MCP: /mcp`
    );
  }
);
