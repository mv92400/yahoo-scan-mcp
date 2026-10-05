#!/usr/bin/env node

import http from "node:http";
import crypto from "node:crypto";

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { z } from "zod";

const PORT = Number(process.env.PORT || 8080);
const MCP_TOKEN = process.env.MCP_TOKEN || "";

if (!MCP_TOKEN) {
  console.error("[yahoo-scan-mcp] ERROR: MCP_TOKEN is not set");
  process.exit(1);
}

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

function authorized(req) {
  const auth = req.headers.authorization || "";
  return auth === `Bearer ${MCP_TOKEN}`;
}

function buildServer() {
  const server = new McpServer({
    name: "yahoo-scan-mcp",
    version: "1.0.0"
  });

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

  server.tool(
    "get_status",
    "Returns the current status of the MCP server.",
    {},
    async () => ({
      content: [
        {
          type: "text",
          text: JSON.stringify({
            ok: true,
            service: "yahoo-scan-mcp",
            stage: "bootstrap",
            yahoo_websocket: false,
            universe_loaded: false,
            timestamp: new Date().toISOString()
          })
        }
      ]
    })
  );

  return server;
}

async function readBody(req) {
  const chunks = [];

  for await (const chunk of req) {
    chunks.push(chunk);

    const size = chunks.reduce((n, c) => n + c.length, 0);

    if (size > 4 * 1024 * 1024) {
      throw new Error("Request body too large");
    }
  }

  if (chunks.length === 0) return undefined;

  return JSON.parse(Buffer.concat(chunks).toString("utf8"));
}

const httpServer = http.createServer(async (req, res) => {
  try {
    // ------------------------------------------------------------
    // Health endpoint — no authentication
    // ------------------------------------------------------------
    if (req.method === "GET" && req.url === "/health") {
      return json(res, 200, {
        ok: true,
        service: "yahoo-scan-mcp",
        timestamp: new Date().toISOString()
      });
    }

    // ------------------------------------------------------------
    // MCP endpoint
    // ------------------------------------------------------------
    if (req.url === "/mcp") {
      if (!authorized(req)) {
        return json(res, 401, {
          error: "Unauthorized"
        });
      }

      if (!["POST", "GET", "DELETE"].includes(req.method)) {
        res.writeHead(405, {
          Allow: "GET, POST, DELETE"
        });
        return res.end();
      }

      const mcpServer = buildServer();

      const transport = new StreamableHTTPServerTransport({
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

      await transport.handleRequest(req, res, body);

      return;
    }

    // ------------------------------------------------------------
    // Root
    // ------------------------------------------------------------
    if (req.method === "GET" && req.url === "/") {
      return json(res, 200, {
        ok: true,
        service: "yahoo-scan-mcp",
        endpoints: {
          health: "/health",
          mcp: "/mcp"
        }
      });
    }

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
});

httpServer.on("error", (err) => {
  log("HTTP server error:", err);
  process.exit(1);
});

httpServer.listen(PORT, "0.0.0.0", () => {
  log(`Listening on 0.0.0.0:${PORT}`);
  log(`Health: /health`);
  log(`MCP: /mcp`);
});
