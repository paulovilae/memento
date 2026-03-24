#!/bin/bash
# ═══════════════════════════════════════════════════════════════════════
# Memento IPC Test Suite
# Tests every IPC action against a live Memento daemon via socat.
# Requires: socat, jq, a running Memento daemon on /tmp/memento.sock
# ═══════════════════════════════════════════════════════════════════════

set -euo pipefail

SOCK="/tmp/memento.sock"
PASS=0
FAIL=0
TOTAL=0

# ─── Colors ──────────────────────────────────────────────────────────
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# ─── Helpers ─────────────────────────────────────────────────────────

check_deps() {
    for cmd in socat jq; do
        if ! command -v "$cmd" &>/dev/null; then
            echo -e "${RED}❌ Required tool '$cmd' not found. Install it first.${NC}"
            exit 1
        fi
    done
}

check_daemon() {
    if [ ! -S "$SOCK" ]; then
        echo -e "${RED}❌ Memento daemon not running (socket not found: $SOCK)${NC}"
        echo -e "${YELLOW}   Start it with: cargo run --bin memento${NC}"
        exit 1
    fi
}

send_ipc() {
    echo "$1" | socat -t5 - UNIX-CONNECT:"$SOCK" 2>/dev/null
}

test_ipc() {
    local name="$1"
    local payload="$2"
    local jq_check="$3"
    ((TOTAL++))

    local result
    result=$(send_ipc "$payload" 2>&1) || true

    if [ -z "$result" ]; then
        echo -e "  ${RED}❌ $name — empty response (daemon crashed?)${NC}"
        ((FAIL++))
        return
    fi

    if echo "$result" | jq -e "$jq_check" >/dev/null 2>&1; then
        echo -e "  ${GREEN}✅ $name${NC}"
        ((PASS++))
    else
        echo -e "  ${RED}❌ $name${NC}"
        echo -e "     ${YELLOW}Expected: $jq_check${NC}"
        echo -e "     ${YELLOW}Got: $(echo "$result" | head -c 200)${NC}"
        ((FAIL++))
    fi
}

# ─── Pre-flight ──────────────────────────────────────────────────────

check_deps
check_daemon

echo -e "${CYAN}═══════════════════════════════════════════════════${NC}"
echo -e "${CYAN}  🧠 Memento IPC Test Suite${NC}"
echo -e "${CYAN}═══════════════════════════════════════════════════${NC}"
echo ""

# ─── 1. Chat Memory Tests ───────────────────────────────────────────

echo -e "${CYAN}── Chat Memory ────────────────────────────────────${NC}"

test_ipc "save_memory: store a chat message" \
    '{"action":"save_memory","payload":{"chat_id":"test-ipc-001","role":"user","content":"Hello from test suite"}}' \
    '.status == "success"'

test_ipc "save_memory: store assistant reply" \
    '{"action":"save_memory","payload":{"chat_id":"test-ipc-001","role":"assistant","content":"Hi! I am Memento."}}' \
    '.status == "success"'

test_ipc "get_context: retrieve chat history" \
    '{"action":"get_context","payload":{"chat_id":"test-ipc-001","limit":10}}' \
    '.status == "success" and (.messages | length) >= 2'

test_ipc "get_context: empty chat returns empty array" \
    '{"action":"get_context","payload":{"chat_id":"nonexistent-chat-999","limit":5}}' \
    '.status == "success" and (.messages | length) == 0'

echo ""

# ─── 2. Knowledge Store Tests ───────────────────────────────────────

echo -e "${CYAN}── Knowledge Store (CRUD) ──────────────────────────${NC}"

test_ipc "store_knowledge: create entry" \
    '{"action":"store_knowledge","payload":{"key":"_test_memento_suite","content":"Integration test content for Memento IPC","tags":"test,integration,ci"}}' \
    '.status == "success" and .action == "stored"'

test_ipc "get_knowledge: retrieve by key" \
    '{"action":"get_knowledge","payload":{"key":"_test_memento_suite"}}' \
    '.status == "success" and .content == "Integration test content for Memento IPC"'

test_ipc "store_knowledge: upsert updates existing" \
    '{"action":"store_knowledge","payload":{"key":"_test_memento_suite","content":"Updated content via upsert","tags":"test,updated"}}' \
    '.status == "success"'

test_ipc "get_knowledge: verify upsert worked" \
    '{"action":"get_knowledge","payload":{"key":"_test_memento_suite"}}' \
    '.content == "Updated content via upsert" and .tags == "test,updated"'

test_ipc "list_knowledge: returns index" \
    '{"action":"list_knowledge","payload":{}}' \
    '.status == "success" and .total >= 1'

test_ipc "search_knowledge: find by tag" \
    '{"action":"search_knowledge","payload":{"query":"updated"}}' \
    '.status == "success" and .results >= 1'

test_ipc "search_knowledge: no results for gibberish" \
    '{"action":"search_knowledge","payload":{"query":"xyzzy_nonexistent_9999"}}' \
    '.status == "success" and .results == 0'

test_ipc "delete_knowledge: remove test entry" \
    '{"action":"delete_knowledge","payload":{"key":"_test_memento_suite"}}' \
    '.status == "success" and .action == "deleted"'

test_ipc "get_knowledge: verify deletion" \
    '{"action":"get_knowledge","payload":{"key":"_test_memento_suite"}}' \
    '.status == "not_found"'

test_ipc "delete_knowledge: idempotent on missing key" \
    '{"action":"delete_knowledge","payload":{"key":"_test_memento_suite"}}' \
    '.status == "not_found"'

echo ""

# ─── 3. App Registry Tests ──────────────────────────────────────────

echo -e "${CYAN}── App Registry ───────────────────────────────────${NC}"

test_ipc "list_apps: returns registered apps" \
    '{"action":"list_apps","payload":{}}' \
    '.status == "success"'

test_ipc "query_app: rejects non-SELECT" \
    '{"action":"query_app","payload":{"app":"movilo","query":"DROP TABLE users"}}' \
    '.error'

test_ipc "query_app: unknown app returns error" \
    '{"action":"query_app","payload":{"app":"nonexistent_app","query":"SELECT 1"}}' \
    '.error'

echo ""

# ─── 4. Error Handling Tests ─────────────────────────────────────────

echo -e "${CYAN}── Error Handling ─────────────────────────────────${NC}"

test_ipc "unknown action: returns error" \
    '{"action":"totally_bogus_action","payload":{}}' \
    '.error'

test_ipc "store_knowledge: missing key returns error" \
    '{"action":"store_knowledge","payload":{"key":"","content":"no key","tags":""}}' \
    '.error'

test_ipc "search_knowledge: empty query returns error" \
    '{"action":"search_knowledge","payload":{"query":""}}' \
    '.error'

echo ""

# ─── Results ─────────────────────────────────────────────────────────

echo -e "${CYAN}═══════════════════════════════════════════════════${NC}"
if [ "$FAIL" -eq 0 ]; then
    echo -e "  ${GREEN}🎉 All $TOTAL tests passed!${NC}"
else
    echo -e "  ${GREEN}✅ $PASS passed${NC}  ${RED}❌ $FAIL failed${NC}  (total: $TOTAL)"
fi
echo -e "${CYAN}═══════════════════════════════════════════════════${NC}"

exit "$FAIL"
