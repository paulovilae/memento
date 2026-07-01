# CLAUDE.md — Memento

This file provides guidance when working inside the `Memento/` submodule.

---

## Submodule Rules

Memento is a git submodule tracked by the parent OS repo. The rules are:

1. **Always commit inside Memento first.** Make changes, `git add`, `git commit` inside `Memento/`. Only then go to the OS root and update the parent pointer (`git add Memento && git commit -m "chore: update Memento pointer"`).
2. **Never modify Memento files and commit only from the OS root.** The parent only tracks a commit hash. If you commit from the OS root without committing inside Memento first, the submodule will show as dirty forever.
3. **`+` prefix in `git submodule status`** means the checked-out commit differs from what the parent records. Resolve by committing inside Memento, not by discarding changes.

Publication target: `paulovilae/memento`. Postgres-backed (Docker container `imagineos-postgres`). Apps connect via TCP.

---

## What Memento actually is (read this before reimplementing memory)

Memento is a **multi-tenant memory daemon**: a single process exposing a Unix Domain Socket (`/tmp/memento.sock`), serving many registered applications, each with its own database scope, gated by per-client tokens with declarative ACLs.

It already contains **far more than the docs imply**. Before adding "memory" features, audit which of these are already in place:

### Five memory tiers (all already implemented)

| Module                             | Tables / data                                      | Purpose                                                                     |
|------------------------------------|----------------------------------------------------|-----------------------------------------------------------------------------|
| `chat_memory`                      | `memento_memory` (chat_id-scoped turn log)         | Verbatim turns indexed by `chat_id`, with adaptive recency/overlap scoring  |
| `scoped_memory`                    | `scoped_memory` (multi-axis: user/tenant/app/session + wing/hall/room + memory_type) | **The Recursive State Node** — session/room/project summaries, auto-derivation, recall, promotion, durable facts |
| `runtime_memory`                   | runtime observations + learned hints               | Per-route preflight + latency/regression telemetry                          |
| `knowledge`                        | `knowledge_store` (key/value/tags + FTS)           | Persistent tagged knowledge                                                 |
| `interaction_memory` (Bayesian)    | `bayesian_interactions`, `user_priors`             | Priors and posteriors over option-choice domains                            |
| `document_index`                   | `document_indexes`, `document_index_nodes`         | Hierarchical document parsing + retrieval                                   |
| `bio`                              | `paulo_bio_*` (multilingual)                       | Paulo's personal bio for the website agents                                 |
| `audit`                            | `audit_log` (signed, retention-enforced)           | Append-only audit chain                                                     |

### scoped_memory — the Recursive State Node

This is the most important thing here, and the most easily missed. The patent text described a "Recursive State Node" — that is **already implemented** in `scoped_memory/`:

- `save_record` → `auto_derive` triggers `maybe_run_continuous_derivation` when enough events accumulate.
- `compress_session` / `compress_room` / `compress_project` build summary entries at three levels.
- `recall_recursive_context` returns project + room + session summaries + working_context + durable_facts + recent_events in one call.
- `memory_promote` moves a record up the memory hierarchy with heuristic criteria.
- `derive_memory` is the public surface to force derivation.
- **`semantic_recall` (since 2026-05-27, frame A)** cosine-reranks scope-filtered rows against a `query_embedding` supplied by the caller (Hera embeds via candle BERT MiniLM-L12 multilingual and passes the vector).

The schema is multi-axis on purpose: `user_id` + `tenant_id` + `app_id` + `expert_id` + `session_id` + `device_id` + `scope` + `wing` + `hall` + `room` + `memory_type` + tags. Cross-app reads are denied by default; the per-token ACL gates routing.

If you find yourself writing "a memory subsystem" — **stop, you're probably duplicating scoped_memory**.

---

## Architecture

```
Memento/
└── src/
    ├── main.rs                    — daemon startup, UDS listener at /tmp/memento.sock,
    │                                 action dispatcher (match req.action.as_str())
    ├── schema.rs                  — Postgres pool init
    ├── migrations.rs              — versioned migrations (run_migration with schema_migrations guard)
    ├── security.rs                — client tokens, ACL, app-scope enforcement
    ├── app_registry.rs            — discovers registered apps via OS/etc/apps.toml
    ├── chat_memory.rs             — turn-by-turn chat memory (adaptive scoring)
    ├── scoped_memory/             — recursive state node (THIS IS THE BIG ONE)
    │   ├── mod.rs                 — public actions (save_record, query_records, semantic_recall, ...)
    │   ├── helpers.rs             — filters, where-clause builders, SCOPED_MEMORY_SELECT_COLUMNS
    │   ├── parsing.rs             — SaveRecordInput, INSERT, embedding persistence
    │   └── derivation.rs          — recursive summary building, scoring, fetch_scoped_rows
    ├── runtime_memory.rs          — preflight + observations + learned-hint promotion
    ├── kg_store.rs                — SOVEREIGN KNOWLEDGE GRAPH (relational/graph RAG): kg_entity + kg_relation, entity resolution, kg_upsert_triples / kg_graph / kg_neighbors. The differentiator — see README "What makes Memento different". Fed by RAG + DURABLE memory, never raw turns.
    ├── knowledge.rs               — knowledge_store CRUD + search
    ├── interaction_memory.rs      — Bayesian priors/posteriors
    ├── document_index*.rs         — document parsing + indexed retrieval
    ├── bio.rs                     — Paulo bio (multilingual)
    ├── audit.rs                   — signed, retention-enforced audit log
    ├── query_cache.rs             — read-side cache (invalidated on writes)
    ├── recall_telemetry.rs        — recall-side observability (recent)
    ├── hardware.rs                — node hardware discovery
    ├── ingestion.rs               — folder watcher
    ├── metrics.rs                 — per-action counters
    └── bin/
        └── memento_mcp.rs         — Memento as an MCP server (rmcp)
```

Publication target: `paulovilae/memento`.

---

## IPC Socket — live actions

Memento listens on `/tmp/memento.sock`. Every request has the shape:

```json
{
  "action": "<name>",
  "payload": { ... },
  "client": { "app": "<caller>", "token": "<token>" }
}
```

**Live actions** (see `src/main.rs::process_uds_stream`):

| Group                  | Actions                                                                                                     |
|------------------------|-------------------------------------------------------------------------------------------------------------|
| Chat memory            | `save_memory`, `get_context`, `record_context_feedback`, `get_context_profile`, `clear_context`             |
| App registry           | `list_apps`, `query_app`, `describe_app`, `describe_all_apps`                                               |
| Knowledge              | `store_knowledge`, `get_knowledge`, `list_knowledge`, `search_knowledge`, `delete_knowledge`, `vector_search` |
| Bayesian               | `log_interaction`, `get_user_prior`, `save_user_prior`                                                       |
| **Scoped memory**      | `save_scoped_memory` (alias `save_memory_record`), `get_scoped_memory` / `query_memory_records`, `search_memory_records`, `get_memory_timeline`, `get_working_context`, `get_preferences`, `get_durable_facts`, `get_recent_events`, `memory_promote`, `derive_memory`, `compress_session`, `compress_room`, `compress_project`, `recall_recursive_context`, **`semantic_recall`** |
| Audit                  | `audit_log`                                                                                                 |
| Metrics                | `get_metrics`                                                                                               |
| Runtime                | `get_runtime_preflight`, `record_runtime_observation`, `promote_runtime_hint`, `save_agent_run_summary`     |
| Document index         | `upsert_document_index`, `get_document_index`, `list_document_indexes`, `query_document_index`              |
| Bio                    | `query_bio`, `seed_bio`, `delete_bio`                                                                       |

**Note**: action names that look like duplicates are real aliases (`save_scoped_memory` = `save_memory_record`; `get_scoped_memory` = `query_memory_records`). Don't add a third name.

---

## Security and per-client ACL

`security::SecurityConfig` reads token definitions from `MEMENTO_CLIENT_TOKENS_FILE` (`/home/paulo/.config/imagineos/secrets/memento-client-tokens.json`). `authorize(action, payload, client)` decides:

- A whitelist of actions allowed without a client (anonymous reads / introspection).
- For tokenized actions, it resolves the client token, gates the action by the token's allow-list, and (for app-scoped actions) enforces `payload.app_id == client.app` via `require_payload_app_match`.
- `semantic_recall` currently falls in the default `Ok(())` arm and is gated by the SQL filter (the caller must supply at least one scope field), not by ACL. That's intentional for read-only scope-filtered access.

Audit signing keys come from `MEMENTO_AUDIT_SIGNATURE_KEYS_FILE`. Audit retention is governed by `MEMENTO_AUDIT_RETENTION_DAYS` (default 365). The audit purger runs once every 24 h.

---

## Migrations

`src/migrations.rs::run_all` runs versioned migrations behind a `schema_migrations` table (`version, name, applied_at`). Each call to `run_migration(version, name, future)` **skips if `version` already exists** — so:

> **GOTCHA**: adding a new `ensure_pg_column(...)` inside an already-applied `migration_N_*` function does nothing in production. The migration is marked applied, the call gets skipped, and your column never exists. **Always add new schema in a new migration number.**

Live migrations:

1. `core_memory` — `memento_memory`
2. `adaptive_memory` — adaptive profile/feedback
3. `bayesian_memory` — interactions/priors
4. `scoped_memory` — main scoped_memory table + many `ensure_pg_column` extensions (status, wing/hall/room, entry_title, content_json, ...)
5. `audit_and_bio`
6. `document_index`
7. `audit_chain` — adds payload_json + prev_entry_hash + signature_verified to `audit_log`
8. **`scoped_embedding`** — adds `scoped_memory.embedding TEXT` (JSON-encoded f32 vector) **and** the performance index `idx_scoped_memory_scope_time ON scoped_memory (user_id, app_id, session_id, timestamp DESC)`. The index resolved an observed 6–14 s sequential-scan issue on filtered recall.

When adding a migration, also add `ensure_pg_index` / `ensure_pg_expression_index` helpers as needed (see top of `migrations.rs`).

---

## Run mode — Memento is `cargo run`, not a pre-deployed binary

This is unusual and tripped a session. Unlike Hera and OS-v3 (which ship a pre-deployed binary under `~/bin/`), Memento on the production nodes is run from source via `pm2`:

```
script args: -lc /home/paulo/Programs/apps/OS/scripts/pm2_env_wrapper.sh ./scripts/start_release.sh
```

`pm2 restart memento-node` causes `cargo run` to recompile if the source changed (often 1–2 minutes), then takes over the socket. While compiling, the UDS socket is down — callers will get "Connection refused" until compile finishes.

Implication: **deploying a Memento change is `pm2 restart`, not `scp` of a binary** — Syncthing already moved the source. Verify the restart settled by hitting `/tmp/memento.sock` with `get_metrics` (cheap action).

---

## Topology — single primary on genesis + hot standby on anchor (verified 2026-06-15)

There is **one** `memento-node` running, on **genesis** (`/tmp/memento.sock`, Postgres DB `acciona_db`). Its database is **streamed to anchor by physical Postgres replication** (slot `anchor_slot`, async, ~60 ms lag, RPO ≈ 0). anchor's Postgres is a **read-only standby** (`pg_is_in_recovery = true`) and currently runs **no** `memento-node` (no socket, no process).

Consequence (current reality):

- A turn saved by Hera on genesis is written to genesis's `acciona_db` **and replicated to anchor within ~60 ms** — anchor holds an identical, near-real-time copy.
- anchor does not accept Memento writes (standby), so it produces **no divergent data**; nothing unique can be lost if anchor goes down.
- If **genesis** dies, all Memento data survives on anchor's standby (RPO ≈ 0). **Failover is MANUAL**: promote anchor's standby to primary and start a `memento-node` there (`anchor.ecosystem.config.cjs` defines the block; it is not running while genesis is primary). Runbook: `docs/DB_FAILOVER_RUNBOOK.md`.
- Async replication means a sudden genesis crash can lose the last few ms of writes — fine for chat memory.

> ⚠️ **SUPERSEDED:** the earlier design ("each edge node runs its own independent `memento-node` with its own Postgres; no cross-node sync; nodes don't see each other's events") was replaced by the genesis→anchor streaming replication set up **2026-06-06**. Do not reason from the old model. `semantic_recall` on the live (genesis) node sees the full store; anchor is a replica of the same data, not a separate island.

---

## Performance gotcha

Before frame A's migration 8, scope-filtered queries (`WHERE user_id=... AND app_id=... AND session_id=... ORDER BY timestamp DESC`) were doing sequential scans → 6–14 s observed. Migration 8 added the composite index that brings it back to milliseconds.

If you see slow `scoped_memory` queries, the first thing to check is the index. The second is the candidate-cap inside `semantic_recall` (default 400, clamped 24–2000).

---

## Build

```bash
# in submodule
cd Memento && cargo build --release --bin memento     # production binary
cd Memento && cargo run                               # development (what pm2 does)
cargo test                                            # unit + integration (some require Docker postgres)
bash tests/test_memento.sh                            # requires running daemon
bash tests/test_mcp.sh                                # MCP bridge bin test
```

No GPU features. Memento is CPU-only Postgres + sqlx code.

---

## "Don't rebuild what's already here" rule

A recurring lesson from the May 2026 work: features that look missing from the patent text and from the heritage doc were **already shipped inside Memento**. Audit before writing new code:

1. `grep -nE "<action name>" src/main.rs` — is the action already registered?
2. `grep -rn "<concept>" src/scoped_memory/` — likely candidate for memory features.
3. Read the migrations file before adding a column or table — the migration may exist or the column may have been added via `ensure_pg_column` inside an already-applied migration (don't try to "add it back").

Examples that bit us:
- The Recursive State Node was already in `scoped_memory/` — we only needed to cable Hera to consume it (frame C1).
- `save_scoped_memory` with `memory_type=preference|decision|...` already exists — the "self-editing memory" feature (frame D) is just a tool JSON pointing at it.
- `compress_session/room/project` and `recall_recursive_context` were live before this session.

Treat any doc claim like "Not yet" / "Pending" / "TODO" as **a hypothesis to verify against the code**, not as fact.
