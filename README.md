# 🧠 Memento — Sovereign Memory Agent

**Role:** Persistent Long-Term Memory, Knowledge Store & Bayesian Interaction Tracker  
**Stack:** Pure Rust  
**Transport:** Unix Domain Socket (`/tmp/memento.sock`) — Zero HTTP overhead  
**MCP Bridge:** Stdio transport via `memento-mcp` binary  

---

## Architecture

```
┌───────────────────────────────────────────────────┐
│  Memento Daemon (headless)                        │
│                                                   │
│  ┌──────────┐ ┌──────────────┐ ┌───────────────┐  │
│  │  Chat    │ │  Knowledge   │ │  Bayesian     │  │
│  │  Memory  │ │  Store       │ │  Tracker      │  │
│  └────┬─────┘ └──────┬───────┘ └──────┬────────┘  │
│       │              │               │             │
│  ┌────┴──────────────┴───────────────┴───────┐    │
│  │          SQLite (memory.db)               │    │
│  └───────────────────────────────────────────┘    │
│                                                   │
│  ┌──────────┐ ┌──────────────┐ ┌───────────────┐  │
│  │  App     │ │  Folder      │ │  Hardware     │  │
│  │  Registry│ │  Watcher     │ │  Discovery    │  │
│  └────┬─────┘ └──────────────┘ └───────────────┘  │
│       │                                           │
│  ┌────┴──────────────────────────────────────┐    │
│  │  App Postgres Pools (Movilo, Vetra, etc.) │    │
│  └───────────────────────────────────────────┘    │
│                                                   │
│  ──── UDS: /tmp/memento.sock ────────────────     │
└───────────────────────────────────────────────────┘
         ▲              ▲              ▲
    Hera (AI)     Imaginclaw       MCP Agents
                 (Telegram)    (Antigravity, etc.)
```

---

## IPC Protocol

All communication uses JSON over Unix Domain Socket at `/tmp/memento.sock`.

**Request format:**
```json
{
  "action": "<action_name>",
  "payload": { ... }
}
```

**Response format:**
```json
{
  "status": "success",
  ...
}
```

---

## IPC Actions Reference

### 1. Chat Memory

#### `save_memory`
Store a chat message for context retrieval.

```json
// Request
{
  "action": "save_memory",
  "payload": {
    "chat_id": "telegram-12345",
    "role": "user",
    "content": "What time is my flight?"
  }
}

// Response
{ "status": "success" }
```

#### `get_context`
Retrieve recent messages for a conversation.

```json
// Request
{
  "action": "get_context",
  "payload": {
    "chat_id": "telegram-12345",
    "limit": 20
  }
}

// Response
{
  "status": "success",
  "messages": [
    { "role": "user", "content": "What time is my flight?" },
    { "role": "assistant", "content": "Your flight departs at 3:00 PM." }
  ]
}
```

---

### 2. Knowledge Store (Tagged Key-Value Memory)

#### `store_knowledge`
Upsert a persistent memory entry. Existing keys are updated.

```json
// Request
{
  "action": "store_knowledge",
  "payload": {
    "key": "server_specs",
    "content": "RTX 3090, 64GB RAM, Ryzen 9 5900X, Ubuntu 24.04",
    "tags": "hardware,setup,gpu"
  }
}

// Response
{ "status": "success", "key": "server_specs", "action": "stored" }
```

#### `get_knowledge`
Retrieve a single entry by exact key.

```json
// Request
{ "action": "get_knowledge", "payload": { "key": "server_specs" } }

// Response
{
  "status": "success",
  "key": "server_specs",
  "content": "RTX 3090, 64GB RAM, Ryzen 9 5900X, Ubuntu 24.04",
  "tags": "hardware,setup,gpu",
  "char_count": 47,
  "created_at": "2026-03-19 22:00:00",
  "updated_at": "2026-03-19 22:00:00"
}
```

#### `list_knowledge`
Compact index of all entries.

```json
// Request
{ "action": "list_knowledge", "payload": {} }

// Response
{
  "status": "success",
  "total": 3,
  "memories": [
    {
      "key": "server_specs",
      "title": "RTX 3090, 64GB RAM, Ryzen 9 5900X, Ubuntu 24.04",
      "tags": "hardware,setup,gpu",
      "char_count": 47,
      "updated_at": "2026-03-19 22:00:00"
    }
  ]
}
```

#### `search_knowledge`
Keyword search across keys, content, and tags (SQLite LIKE).

```json
// Request
{ "action": "search_knowledge", "payload": { "query": "gpu" } }

// Response
{
  "status": "success",
  "query": "gpu",
  "results": 1,
  "memories": [
    {
      "key": "server_specs",
      "snippet": "RTX 3090, 64GB RAM, Ryzen 9 5900X, Ubuntu 24.04",
      "tags": "hardware,setup,gpu",
      "char_count": 47,
      "updated_at": "2026-03-19 22:00:00"
    }
  ]
}
```

#### `delete_knowledge`
Remove an entry by exact key.

```json
// Request
{ "action": "delete_knowledge", "payload": { "key": "server_specs" } }

// Response
{ "status": "success", "key": "server_specs", "action": "deleted" }
```

---

### 3. App Registry (Cross-App Database Access)

#### `list_apps`
List all ImagineOS apps registered in `etc/apps.toml`.

```json
// Request
{ "action": "list_apps", "payload": {} }

// Response
{
  "status": "success",
  "apps": [
    {
      "slug": "movilo",
      "name": "Movilo",
      "description": "Healthcare marketplace",
      "key_tables": ["providers", "patients", "appointments"]
    }
  ]
}
```

#### `query_app`
Run read-only SQL against any registered app's Postgres database.
Only `SELECT` and `WITH` queries are allowed. Results are auto-limited.

```json
// Request
{
  "action": "query_app",
  "payload": {
    "app": "movilo",
    "query": "SELECT name, specialty FROM providers LIMIT 5",
    "limit": 50
  }
}

// Response
{
  "status": "success",
  "app": "movilo",
  "count": 5,
  "rows": [
    { "name": "Dr. Garcia", "specialty": "Cardiología" }
  ]
}
```

---

### 4. Bayesian Interaction Tracking *(Planned — Phase 1)*

#### `log_interaction`
Log a user choice for Bayesian preference learning.

#### `get_user_prior`
Retrieve persisted prior distribution for a user + domain.

#### `save_user_prior`
Persist the posterior as the new prior for the next session.

---

### 5. Hybrid Document Retrieval

`Memento` now supports a native document-index backend for structured long-form sources.

Use `page_tree` indexes for:
- policies
- contracts
- manuals
- reports
- compliance packs

#### `upsert_document_index`
Store or replace a hierarchical document index.

```json
{
  "action": "upsert_document_index",
  "payload": {
    "document_id": "vetra-policy-001",
    "tenant_id": "tenant-main",
    "app_id": "vetra",
    "owner_scope": "workspace",
    "title": "Remote Work Policy",
    "summary": "Policy covering remote work expectations and approvals.",
    "index_type": "page_tree",
    "source_type": "policy",
    "root_node_id": "root",
    "status": "active",
    "nodes": [
      {
        "node_id": "root",
        "title": "Remote Work Policy",
        "summary": "Top-level summary",
        "level": 0,
        "node_type": "document",
        "page_from": 1,
        "page_to": 8
      }
    ]
  }
}
```

#### `get_document_index`
Fetch the full stored document index and all nodes by `document_id`.

#### `list_document_indexes`
List indexed documents filtered by `app_id`, `tenant_id`, or `index_type`.

#### `query_document_index`
Query `page_tree` indexes using symbolic node scoring over titles, summaries, and tags.

See also:
- [hybrid_retrieval.md](docs/hybrid_retrieval.md)

---

## MCP Bridge

The `memento-mcp` binary exposes 5 tools over MCP Stdio transport:

| MCP Tool | Maps to IPC Action |
|---|---|
| `store_memory` | `store_knowledge` |
| `retrieve_memory` | `get_knowledge` |
| `list_all_memories` | `list_knowledge` |
| `search_memories` | `search_knowledge` |
| `delete_memory` | `delete_knowledge` |

**Setup (add to your MCP config):**
```json
{
  "mcpServers": {
    "memento": {
      "command": "/home/paulo/Programs/apps/OS/Memento/target/release/memento-mcp"
    }
  }
}
```

---

## Semantic Memory Roadmap

The next planned expansion is `Memento v3`, which adds:

- research projects and sessions
- concept graph memory
- claims and evidence with provenance
- consolidation across many research sessions
- retention, compression, archive, and deletion policy

Design reference:

- [Apps/OS-v3/docs/MEMENTO_V3_SEMANTIC_MEMORY_PLAN.md](/home/paulo/Programs/apps/OS/Apps/OS-v3/docs/MEMENTO_V3_SEMANTIC_MEMORY_PLAN.md)

---

## Folder Ingestion

Memento watches configured directories and extracts text from:
- **PDF** — via `pdf-extract`
- **DOCX** — XML extraction from zip archive
- **XLSX** — via `calamine` spreadsheet reader
- **TXT / MD** — direct read

Configure in `~/.config/memento/config.json`:
```json
{
  "watched_folders": [
    { "path": "/home/paulo/Documents", "sanitize_pii": true },
    { "path": "/home/paulo/Projects/notes", "sanitize_pii": false }
  ]
}
```

---

## Hardware Discovery

On startup, Memento auto-detects compute capabilities:

| Detection | Strategy |
|---|---|
| `/dev/nvidia0` or CUDA | VRAM Fast Path (FastEmbed + SLM) |
| Metal.framework | Metal Unified Memory |
| Neither | CPU Fallback (ONNX Quantized) |

---

## Running

### Development
```bash
cargo run --bin memento
```

### Production (PM2)
```bash
cargo build --release
pm2 start ecosystem.config.cjs
```

---

## Testing

### Rust Tests (payload validation)
```bash
cargo test
```

### Live IPC Tests (requires running daemon)
```bash
bash tests/test_memento.sh
```

### MCP Protocol Tests
```bash
bash tests/test_mcp.sh
```

### Prompt-based Tests (for AI agents)
See `tests/PROMPTS.md` — copy-paste scenarios to validate through natural language.
