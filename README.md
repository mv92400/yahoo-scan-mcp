# yahoo-scan-mcp

Small Cap EOD Scanner (S0 → S1 15:55 → Ranking DV → lots × 20 → SF 15:59 → Winner Gate → STOP) as an MCP server over the Yahoo WebSocket.

## Install / run
```
npm install
node server.js        # stdio MCP server
```
Claude Desktop config: `{"mcpServers":{"yahoo-scan":{"command":"node","args":["/path/to/yahoo-scan-mcp/server.js"]}}}`

## Tools
| Tool | Role |
|---|---|
| `load_universe` | S0 universe (CSV with `symbol` column, one ticker per line, or JSON) + optional refs JSON `{SYM:{vol15,vol5,vol60}}` |
| `yahoo_ws_test` | Connect, subscribe N tickers, measure coverage / latency (5 → 100 → 500 → 1000 → 2000 → 3227) |
| `start_stream` | Subscribe the whole universe |
| `run_s1` / `run_sf` | Run a layer manually (optional `as_of` ISO timestamp) |
| `arm_day` | Timers: S1 at 15:55:00 ET, SF at 15:59:00 ET |
| `ws_status`, `get_locked_params` | Diagnostics |

Results are written to `logs/YYYY-MM-DD_s1.json` / `_sf.json`.

## Decisions applied
- **Price < $5 is fixed at S0** (the universe file must already be filtered). It is not re-checked in S1, SF or the Winner Gate.
- Locked thresholds: RVOL15M ≥ 1.50, Vol5 ≥ 20 %, Δ60 ≤ +10 %, Price vs VWAP ≥ −1 %, Δ15:55→15:59 ≥ 0 %, DV desc, lots of 20, STOP at first Winner Gate PASS.
- Unlocked parameters stay configurable and permissive (`UNLOCKED` in `src/engine.js`): S1 liquidity (default 0), tick freshness (120 s), Accel5M is a bonus metric only.

## Assumptions to confirm
1. **RVOL15M / Vol5 / Δ60 need reference volumes** (`refs`) built from ~2 months of history. The WebSocket cannot provide them.
2. **Vol5 ≥ 20 %** is implemented as `Vol(15:55→15:59) / (ref.vol5 × 0.8)` ≥ 0.20 (point-in-time window = 4 of 5 minutes). Adjust `vol5RefScale` if your metric differs.
3. **Δ60** = `Vol60 / ref.vol60 − 1` (Vol60 = trailing 60 min).
4. **VWAP** is computed from ticks received since the stream started. It is only exact if the stream runs from the open; otherwise seed it or start earlier.
5. Cache retention is 70 min (needed for Vol60), not 30.
6. Yahoo's WebSocket is unofficial: no guarantee of delivery or rate limits. Run `yahoo_ws_test` at increasing sizes during market hours.
