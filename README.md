# Memento

**Multi-tenant AI memory daemon in Rust — persistent context, semantic recall, knowledge graph, and scoped memory across applications.**

[![Rust](https://img.shields.io/badge/rust-2021--edition-orange)](https://www.rust-lang.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
[![Status: Production](https://img.shields.io/badge/status-production-green)]()

Memento is a daemon that listens on a Unix Domain Socket (`/tmp/memento.sock`) and provides persistent AI memory to any number of applications simultaneously. Each application gets its own isolated scope with per-client token ACLs. It integrates with [Hera](https://github.com/paulovilae/hera) as the memory backend for LLM conversations.

---

## Memory tiers

Five tiers, all implemented and production-deployed:

| Module | Tables | Purpose |
|---|---|---|
| `scoped_memory` | `scoped_memory` | **Recursive State Node** — session/room/project summaries, semantic recall, auto-derivation, durable facts |
| `chat_memory` | `memento_memory` | Per-chat turn log with adaptive recency/overlap scoring |
| `knowledge` | `knowledge_store` | Tagged key-value knowledge with full-text search |
| `kg_store` | `kg_entity`, `kg_relation` | Sovereign knowledge graph — entity resolution, triple upsert, graph traversal |
| `document_index` | `document_indexes`, `document_index_nodes` | Hierarchical document parsing and retrieval |
| `interaction_memory` | `bayesian_interactions`, `user_priors` | Bayesian priors/posteriors over option-choice domains |
| `runtime_memory` | runtime observations | Per-route preflight hints and latency telemetry |
| `audit` | `audit_log` | Signed, append-only audit chain with retention enforcement |

---

## Architecture

```
                    ┌─────────────────────────────────────────┐
                    │            Memento Daemon               │
                    │         /tmp/memento.sock               │
                    │                                         │
  Hera / App ─────► │  Action dispatcher                      │
  (JSON over UDS)   │  match req.action                       │
                    │                                         │
                    │  ┌──────────────────┐                   │
                    │  │  scoped_memory   │ ← Recursive       │
                    │  │  - save_record   │   State Node      │
                    │  │  - recall_ctx    │   (project/room/  │
                    │  │  - semantic_rcl  │   session tiers)  │
                    │  │  - compress_*    │                   │
                    │  │  - auto_derive   │                   │
                    │  └──────────────────┘                   │
                    │                                         │
                    │  ┌──────────────┐  ┌─────────────────┐ │
                    │  │  kg_store    │  │  chat_memory    │ │
                    │  │  entities    │  │  knowledge      │ │
                    │  │  relations   │  │  document_index │ │
                    │  │  graph query │  │  audit_log      │ │
                    │  └──────────────┘  └─────────────────┘ │
                    │                                         │
                    │  ┌──────────────────────────────────┐  │
                    │  │  security (per-client ACL tokens) │  │
                    │  │  app_registry (apps.toml)         │  │
                    │  │  query_cache (read invalidation)  │  │
                    │  └──────────────────────────────────┘  │
                    └─────────────────────────────────────────┘
                                    │
                              PostgreSQL
```

---

## The Recursive State Node (scoped_memory)

Memory is organized in a multi-axis hierarchy:

```
user_id × tenant_id × app_id × session_id
    └── wing × hall × room
            └── memory_type (event | summary | durable_fact | preference | decision | ...)
```

Key actions:

| Action | What it does |
|---|---|
| `save_scoped_memory` | Write a memory record with optional embedding |
| `recall_recursive_context` | One call → project + room + session summaries + durable_facts + recent_events |
| `semantic_recall` | Cosine rerank scope-filtered rows against a query embedding |
| `compress_session` / `compress_room` / `compress_project` | Build summary entries at each tier |
| `memory_promote` | Move a record up the hierarchy by heuristic criteria |
| `derive_memory` | Force derivation (auto-summarization) |

`auto_derive` triggers automatically when enough events accumulate — sessions summarize to rooms, rooms to projects, projects to long-term context.

---

## Semantic recall

Memento stores 384-dim embeddings on `scoped_memory` records. [Hera](https://github.com/paulovilae/hera) embeds the query locally using candle BERT (`paraphrase-multilingual-MiniLM-L12-v2`, CPU-only, multilingual) and passes the vector to `semantic_recall`. Memento cosine-reranks scope-filtered rows and returns the top-k most relevant memories. No external embedding API.

---

## Knowledge graph (kg_store)

A relational/graph RAG layer on Postgres:

- **`kg_upsert_triples`** — `(subject, predicate, object, confidence, source)` with entity resolution
- **`kg_graph`** — subgraph traversal from a starting entity
- **`kg_neighbors`** — direct neighbors of an entity

Entities and relations are first-class objects with provenance. Designed to be fed by structured extraction (GLiNER/GLiREL) rather than raw LLM outputs.

---

## Multi-tenancy and security

Every client gets a token with declarative ACLs:

```json
{
  "app_id": "myapp",
  "allowed_actions": ["save_scoped_memory", "recall_recursive_context", "query_app"],
  "db_scope": "myapp"
}
```

Cross-app reads are denied by default. Tokens live in `~/.config/imagineos/secrets/memento-client-tokens.json`.

---

## IPC protocol

All communication is JSON over Unix Domain Socket at `/tmp/memento.sock`.

**Save a memory:**
```json
{
  "action": "save_scoped_memory",
  "payload": {
    "app_id": "myapp",
    "user_id": "user123",
    "session_id": "sess456",
    "content": "User prefers concise answers",
    "memory_type": "preference",
    "scope": "user"
  }
}
```

**Recall context (one call, all tiers):**
```json
{
  "action": "recall_recursive_context",
  "payload": {
    "app_id": "myapp",
    "user_id": "user123",
    "session_id": "sess456"
  }
}
```

**Semantic recall:**
```json
{
  "action": "semantic_recall",
  "payload": {
    "app_id": "myapp",
    "user_id": "user123",
    "query_embedding": [0.12, -0.34, ...],
    "top_k": 5
  }
}
```

**SQL query against registered app DB:**
```json
{
  "action": "query_app",
  "payload": {
    "app": "myapp",
    "query": "SELECT * FROM items WHERE active = true LIMIT 10"
  }
}
```

---

## Build & run

```bash
git clone https://github.com/paulovilae/memento
cd memento

export MEMENTO_DATABASE_URL=postgresql://user:password@localhost:5432/memento_db
cargo run

# Or release build
cargo build --release && ./target/release/memento
```

Migrations run automatically on startup — idempotent, versioned via `schema_migrations` guard table.

---

## Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `MEMENTO_DATABASE_URL` | `postgresql://postgres:postgres@localhost:5432/memento_db` | Postgres connection |
| `DATABASE_URL` | — | Fallback if `MEMENTO_DATABASE_URL` not set |
| `MEMENTO_SOCKET_PATH` | `/tmp/memento.sock` | UDS socket path |
| `MEMENTO_CLIENT_TOKENS_FILE` | `~/.config/imagineos/secrets/memento-client-tokens.json` | Per-client ACL tokens |

---

## Part of the Vilaros OS stack

| Service | Role |
|---|---|
| [Hera](https://github.com/paulovilae/hera) | LLM orchestration — embeds queries, calls Memento for context |
| **Memento** (this repo) | Persistent memory, semantic recall, knowledge graph |
| Sentinel | Ingress, TLS, identity |
| Argus | Hardware detection, cluster placement |
| OS-v3 | Governance, app registry |

---

## License

MIT — see [LICENSE](LICENSE)

---

*Memento is the memory layer of [Vilaros OS](https://vilaros.ai) — a sovereign AI operating system built in Rust.*
