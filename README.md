# 🧠 Memento — Sovereign Memory Agent

**Role:** Persistent Long-Term Memory, Knowledge Store & Bayesian Interaction Tracker  
**Stack:** Pure Rust  
**Transport:** Unix Domain Socket (`/tmp/memento.sock`) — Zero HTTP overhead  
**MCP Bridge:** Stdio transport via `memento-mcp` binary  

## Bundle Position

`Memento` is not the full Ava assistant by itself.

It is one component inside the canonical Ava bundle:

- `Argus`
- `Sentinel`
- `Imaginclaw`
- `Hera/hera-core`
- `Memento`

Before diagnosing assistant capability or adding features, read:

- [Ava Bundle Capabilities Matrix](/home/paulo/Programs/apps/OS/docs/AVA_BUNDLE_CAPABILITIES_MATRIX.md)

Mandatory rule:

- Do not treat missing orchestration, approvals, scheduling, or channel behavior as missing `Memento` capability without checking the full bundle first.
- Do not duplicate assistant control-plane logic in `Memento` when the correct owner is `Imaginclaw`, `Sentinel`, or `Argus`.

---

## What makes Memento different — the Knowledge Graph (relational memory)

> **This is the edge.** Most "memory" layers for LLM apps are a vector index over
> text chunks: you embed slices of text, cosine-match them, and paste the top-k
> into a prompt. That is flat retrieval — it has no idea that two chunks talk about
> the *same person*, that a fact *contradicts* an earlier one, or that a clause
> *applies to* another. **Memento adds a sovereign knowledge graph on top of its
> memory tiers**, so recall is *relational*, not just *similar*.

**One graph per scope, fed by two sources** (`src/kg_store.rs`, migration 12):

- **RAG documents** — entities + relations extracted from `rag_chunk`.
- **Durable memory** — facts / decisions / summaries promoted in `scoped_memory`.
- **NOT** raw chat turns (too noisy — the graph is fed from the *durable* layer).

Both feeders resolve into the same `kg_entity` / `kg_relation` tables. So a fact
stated in a chat, an entity in a CV, and a clause in a contract about the *same*
company collapse into **one node**, and a question can *hop* across them.

**Entity resolution is built in:** entities are deduped by normalized name + type
within a scope (`"Paulo Vila"`, `"PAULO  vila."`, `"paulo vila"` → one node, with a
running `mention_count`). This is what turns "N text chunks" into "the ideas and
how they relate".

**Sovereign + cheap.** The expensive part of graph-RAG (GraphRAG/LightRAG-style) is
the LLM that extracts `(entity, relation, entity)` triples on every chunk. On a
cloud API that is a real bill; on our stack the extractor is **Hera, local on
genesis (2× RTX 3090)** — GPU time, ~0 cloud tokens. Same "caller supplies the
embedding" contract as the RAG store. No Python framework is vendored: we copied
the *method* (LightRAG-style entity+relation graph, skipping the costly global
community-summarization layer), and implemented it natively in Rust + Postgres.

**KG IPC actions** (scope = `app_id` / `tenant_id` / `expert_id` / `collection`):

| Action | Purpose |
|---|---|
| `kg_upsert_triples` | Merge `entities` + `triples` (server-side resolution: canonical name + controlled type → variants collapse to one node) |
| `kg_graph` | Full scoped subgraph (entities w/ embeddings + edges) — feeds the `graph-kit` viewer |
| `kg_neighbors` | k-hop expansion from seed entities — graph retrieval |
| `kg_centrality` | PageRank over the scoped graph (top-N important entities) — pure Rust, no LLM |
| `kg_clear` | Wipe a scope's graph before a full re-extraction |

**Architecture — where the LLM lives (decided 2026-06-30).** Three layers, and
Memento is deliberately the LLM-free one:

```
PRESENTATION   graph-kit <os-graph>           — draws (Sigma WebGL, PageRank, Louvain)
PIPELINE (kit) os-rag-kit / os-knowledge-kit  — orchestrates Hera→canonicalize→store (LLM HERE)
STORE+COMPUTE  Memento (this)                 — kg_* + resolution + PageRank/communities, NO LLM
LLM RUNTIME    Hera                            — generate / embed
```

Memento **never calls the LLM**. Generating a graph from text (triple extraction,
abstracts, co-mention) needs Hera and lives in the *pipeline kit* that sits above and
writes into Memento — so the durable Postgres store is never coupled to the volatile
GPU service and keeps serving recall even when Hera is busy/down. What Memento **does**
own is everything computable without an LLM: entity **resolution/canonicalization**
(so every writer dedups identically), graph algorithms (**PageRank** live; Louvain /
shortest-path next), and the store itself. This keeps the knowledge graph a complete,
reusable Memento capability — the moat — with the LLM strictly outside.

> **Roadmap:** graph store + resolution + PageRank are live. Next: server-side Louvain
> communities + shortest-path, k-hop retrieval wired into recall, and temporal
> (bi-temporal, Graphiti-style) edges if memory needs time-travel.

### Open-source vs. commercial (positioning)

Memento's **protocol, the five memory tiers, and the basic stores** are the open,
sovereign *standard* (consistent with the platform open-core stance: the OS is an
open white-label standard; the differentiating layers are the product). The
**relational knowledge-graph layer is the moat** — it is what makes Memento more
than a vector cache. Treat it as the commercial / premium edge of the open core;
do not casually re-document it as "just another store". When this decision is
finalized, record it here and in the platform open-core note.

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

## Current Priorities

Recent hardening work focused on four areas:

- performance: added concrete Postgres indexes for hot memory and document lookup paths
- structure: reduced repeated `scoped_memory` filter/query logic in the IPC layer
- operations: `MEMENTO_DATABASE_URL` is now supported for explicit deployment configuration
- testing: document index integration now runs against an ephemeral Docker Postgres instead of staying ignored
- security: sensitive IPC actions now require `client.app`, optional per-app tokens, and restricted ACLs
- lifecycle: scoped memory now tracks `usage_count`, `last_used_at`, and `promoted_from`
- product: canonical retrieval now includes durable facts, recent events, and explicit memory promotion

This pushes `Memento` closer to a real platform service instead of a collection of useful features.

---

## IPC Protocol

All communication uses JSON over Unix Domain Socket at `/tmp/memento.sock`.

**Request format:**
```json
{
  "action": "<action_name>",
  "payload": { ... },
  "client": {
    "app": "os-v3",
    "token": "optional-shared-secret"
  }
}
```

**Response format:**
```json
{
  "status": "success",
  ...
}
```

Sensitive actions such as `query_app`, schema discovery, knowledge store, bio writes, and document index operations are now gated by local ACLs. Configure them with:

- `MEMENTO_SOCKET_MODE`
- `MEMENTO_CLIENT_TOKENS`
- `MEMENTO_PRIVILEGED_CLIENTS`
- `MEMENTO_APP_QUERY_CLIENTS`
- `MEMENTO_SCHEMA_CLIENTS`
- `MEMENTO_DOCUMENT_INDEX_CLIENTS`
- `MEMENTO_KNOWLEDGE_CLIENTS`
- `MEMENTO_BIO_CLIENTS`
- `MEMENTO_AUDIT_CLIENTS`
- `MEMENTO_RUNTIME_CLIENTS`

Additional canonical memory actions now available:

- `get_durable_facts`
- `get_recent_events`
- `memory_promote`
- `get_metrics`
- `get_runtime_preflight`
- `record_runtime_observation`
- `promote_runtime_hint`

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

### 3. Scoped Palace Memory

Inspired by the useful part of MemPalace, `Memento` can now persist raw memory records with stable structure instead of forcing everything through summary-only storage.

Each record can optionally live inside:

- `wing` — broad domain or app (`vetra`, `movilo`, `os`)
- `hall` — sub-domain (`contracts`, `onboarding`, `support`)
- `room` — concrete thread, case, or workflow (`msa-negotiation`, `tenant-issue-42`)

This keeps verbatim memory navigable without pretending the structure itself is "AI".

#### `save_memory_record`
Store a verbatim or derived record with optional palace metadata.

```json
{
  "action": "save_memory_record",
  "payload": {
    "user_id": "user-123",
    "tenant_id": "tenant-main",
    "app_id": "vetra",
    "scope": "workspace",
    "wing": "vetra",
    "hall": "contracts",
    "room": "msa-negotiation",
    "entry_title": "MSA redlines call",
    "memory_type": "event",
    "tags": ["msa", "redlines"],
    "content": "Counterparty rejected the uncapped indemnity language."
  }
}
```

#### `query_memory_records`
Filter stored records by identity, app, scope, or palace location.

#### `search_memory_records`
Search raw records using token overlap against title, content, memory type, and palace metadata.

This is designed for:

- conversation recall
- project memory
- case-room timelines
- high-fidelity debugging trails

#### `get_memory_timeline`
Return chronological verbatim entries for a scoped room, wing, or session.

#### `get_working_context`
Return purpose-shaped context for runtime use from the same scoped records:

- summaries
- decisions
- preferences
- open loops
- recent events

This is the preferred read path when a caller needs actionable context instead of raw storage primitives.

#### `get_preferences`
Return active, non-expired preference-like memories within the requested scope.

This favors records tagged or typed as preferences and avoids forcing callers to manually filter generic memory rows.

See also:
- [palace_memory.md](docs/palace_memory.md)

---

### 4. App Registry (Cross-App Database Access)

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

### 5. Bayesian Interaction Tracking *(Planned — Phase 1)*

#### `log_interaction`
Log a user choice for Bayesian preference learning.

#### `get_user_prior`
Retrieve persisted prior distribution for a user + domain.

#### `save_user_prior`
Persist the posterior as the new prior for the next session.

---

### 6. Hybrid Document Retrieval

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
