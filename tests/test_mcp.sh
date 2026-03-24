#!/bin/bash
# ═══════════════════════════════════════════════════════════════════════
# Memento MCP Bridge Test Suite
# Tests the MCP Stdio transport by sending JSON-RPC messages to the
# memento-mcp binary and validating responses.
#
# Requires: jq, memento daemon running, memento-mcp binary built
# ═══════════════════════════════════════════════════════════════════════

set -euo pipefail

MEMENTO_MCP="${MEMENTO_MCP:-./target/release/memento-mcp}"
SOCK="/tmp/memento.sock"
PASS=0
FAIL=0
TOTAL=0

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

# ─── Pre-flight ──────────────────────────────────────────────────────

if ! command -v jq &>/dev/null; then
    echo -e "${RED}❌ jq not found${NC}"
    exit 1
fi

if [ ! -S "$SOCK" ]; then
    echo -e "${RED}❌ Memento daemon not running ($SOCK not found)${NC}"
    exit 1
fi

if [ ! -x "$MEMENTO_MCP" ]; then
    # Try debug build
    MEMENTO_MCP="./target/debug/memento-mcp"
    if [ ! -x "$MEMENTO_MCP" ]; then
        echo -e "${RED}❌ memento-mcp binary not found. Build with: cargo build --bin memento-mcp${NC}"
        exit 1
    fi
fi

# ─── MCP JSON-RPC Helper ────────────────────────────────────────────
# Sends an initialize + tool call to the MCP binary via stdin/stdout

send_mcp() {
    local method="$1"
    local params="$2"
    local id="$3"

    local init_msg='{"jsonrpc":"2.0","id":0,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}'
    local initialized_msg='{"jsonrpc":"2.0","method":"notifications/initialized"}'
    local call_msg="{\"jsonrpc\":\"2.0\",\"id\":${id},\"method\":\"${method}\",\"params\":${params}}"

    # Send all messages separated by newlines, capture all output
    printf '%s\n%s\n%s\n' "$init_msg" "$initialized_msg" "$call_msg" \
        | timeout 10 "$MEMENTO_MCP" 2>/dev/null \
        | grep -v '"method":"notifications' \
        | tail -1
}

test_mcp() {
    local name="$1"
    local method="$2"
    local params="$3"
    local jq_check="$4"
    local id="$((TOTAL + 1))"
    ((TOTAL++))

    local result
    result=$(send_mcp "$method" "$params" "$id" 2>&1) || true

    if [ -z "$result" ]; then
        echo -e "  ${RED}❌ $name — no response${NC}"
        ((FAIL++))
        return
    fi

    if echo "$result" | jq -e "$jq_check" >/dev/null 2>&1; then
        echo -e "  ${GREEN}✅ $name${NC}"
        ((PASS++))
    else
        echo -e "  ${RED}❌ $name${NC}"
        echo -e "     ${YELLOW}Got: $(echo "$result" | head -c 200)${NC}"
        ((FAIL++))
    fi
}

# ─── Tests ───────────────────────────────────────────────────────────

echo -e "${CYAN}═══════════════════════════════════════════════════${NC}"
echo -e "${CYAN}  🧠 Memento MCP Bridge Test Suite${NC}"
echo -e "${CYAN}═══════════════════════════════════════════════════${NC}"
echo ""

echo -e "${CYAN}── Tool Discovery ─────────────────────────────────${NC}"

test_mcp "initialize: returns server info" \
    "initialize" \
    '{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}' \
    '.result.capabilities.tools'

echo ""
echo -e "${CYAN}── Tool Calls ─────────────────────────────────────${NC}"

test_mcp "store_memory: create test entry via MCP" \
    "tools/call" \
    '{"name":"store_memory","arguments":{"key":"_mcp_test_entry","content":"MCP bridge test content","tags":"mcp,test"}}' \
    '.result'

test_mcp "list_all_memories: returns index" \
    "tools/call" \
    '{"name":"list_all_memories","arguments":{}}' \
    '.result'

test_mcp "retrieve_memory: get test entry" \
    "tools/call" \
    '{"name":"retrieve_memory","arguments":{"key":"_mcp_test_entry"}}' \
    '.result'

test_mcp "search_memories: find by keyword" \
    "tools/call" \
    '{"name":"search_memories","arguments":{"query":"mcp"}}' \
    '.result'

test_mcp "delete_memory: cleanup test entry" \
    "tools/call" \
    '{"name":"delete_memory","arguments":{"key":"_mcp_test_entry"}}' \
    '.result'

echo ""

# ─── Results ─────────────────────────────────────────────────────────

echo -e "${CYAN}═══════════════════════════════════════════════════${NC}"
if [ "$FAIL" -eq 0 ]; then
    echo -e "  ${GREEN}🎉 All $TOTAL MCP tests passed!${NC}"
else
    echo -e "  ${GREEN}✅ $PASS passed${NC}  ${RED}❌ $FAIL failed${NC}  (total: $TOTAL)"
fi
echo -e "${CYAN}═══════════════════════════════════════════════════${NC}"

exit "$FAIL"
