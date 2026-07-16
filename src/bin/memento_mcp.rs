/// Memento MCP Bridge — Exposes Memento's knowledge store as MCP tools.
///
/// This is a thin Stdio MCP server that translates MCP tool calls into
/// JSON messages over UDS (`/tmp/memento.sock`), making Memento accessible
/// to all MCP-compatible AI agents (Antigravity, Gemini CLI, Cursor, etc.)
///
/// Usage:
///   cargo run --bin memento-mcp
///
/// MCP config (add to your client's mcp_config.json):
/// {
///   "mcpServers": {
///     "memento": {
///       "command": "/path/to/memento-mcp"
///     }
///   }
/// }
use rmcp::{
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{ServerCapabilities, ServerInfo},
    schemars, tool, tool_handler, tool_router,
    transport::stdio,
    ServerHandler, ServiceExt,
};
use serde::Deserialize;
use std::sync::OnceLock;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

const MEMENTO_SOCKET: &str = "/tmp/memento.sock";

/// Per-process session identifier for direct-MCP usage telemetry. Stdio MCP
/// has no native session concept, so we synthesize one once at process
/// startup (pid + boot-time nanos) and reuse it for every call this process
/// makes — good enough to group "one Claude Code session's worth of Memento
/// MCP calls" without pulling in a uuid dependency this crate doesn't have.
static MCP_SESSION_ID: OnceLock<String> = OnceLock::new();

fn mcp_session_id() -> &'static str {
    MCP_SESSION_ID.get_or_init(|| {
        let pid = std::process::id();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        format!("mcp-{pid}-{nanos}")
    })
}

/// Classify a `send_ipc_raw` response into (success, error) using the same
/// transport-level error prefixes `send_ipc_raw` itself returns on failure
/// (socket connect/write/read failures) — not a JSON-body inspection of
/// Memento's actual response, which may have its own `{"ok": false, ...}`
/// shape that callers already handle individually.
fn classify_transport_result(response: &str) -> (bool, Option<String>) {
    if response.starts_with("Error writing to Memento socket")
        || response.starts_with("Error shutting down write")
        || response.starts_with("Error reading from Memento socket")
        || response.starts_with("Cannot connect to Memento daemon")
    {
        (false, Some(response.to_string()))
    } else {
        (true, None)
    }
}

fn request_envelope(action: &str, payload: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "action": action,
        "payload": payload,
        "client": {
            "app": "memento-mcp",
            "token": std::env::var("MEMENTO_CLIENT_TOKEN").ok()
        }
    })
}

// ─── MCP Tool Parameter Schemas ─────────────────────────────────────────

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct StoreMemoryParams {
    /// Unique key for this memory (e.g., "server_specs", "project_conventions")
    #[schemars(description = "Unique key identifier for the memory entry")]
    key: String,
    /// The full content to store
    #[schemars(description = "The full text content of the memory")]
    content: String,
    /// Comma-separated tags for categorization
    #[schemars(
        description = "Comma-separated tags for categorization (e.g., 'setup,specs,hardware')"
    )]
    #[serde(default)]
    tags: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct RetrieveMemoryParams {
    /// Exact key of the memory to retrieve
    #[schemars(description = "The exact key of the memory entry to retrieve")]
    key: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct SearchMemoriesParams {
    /// Search query — will match against key, content, and tags
    #[schemars(description = "Keyword to search across memory keys, content, and tags")]
    query: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct DeleteMemoryParams {
    /// Exact key of the memory to delete
    #[schemars(description = "The exact key of the memory entry to delete")]
    key: String,
}

// ─── Scoped Memory (Recursive State Node) Parameter Schemas ────────────
//
// store_memory/retrieve_memory/list_all_memories/search_memories above talk
// to `knowledge_store` (a separate, older key/value table). The tools that
// actually save_memory writes to (`scoped_memory` — the recursive state
// node) had NO read-side MCP tool before this, even though the IPC actions
// (query_memory_records / recall_recursive_context) were always live. These
// two close that gap.
//
// NOTE on app_id: this bridge's IPC client identity is hardcoded to
// app="memento-mcp" (see request_envelope). security.rs::require_payload_app_match
// rejects a payload `app_id` that doesn't match the caller's own app unless the
// caller is privileged (hera/os-v3) — so filtering by `app_id` will usually be
// refused for this bridge. Filter by expert_id/user_id/session_id/scope/
// memory_type/wing/hall/room instead — those aren't app_id-gated.

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct RecallScopedMemoryParams {
    #[schemars(description = "Filter by expert_id (e.g. 'ava'). Prefer this over app_id — app_id is usually rejected for this bridge, see tool description.")]
    #[serde(default)]
    expert_id: Option<String>,
    #[schemars(description = "Filter by user_id")]
    #[serde(default)]
    user_id: Option<String>,
    #[schemars(description = "Filter by app_id — WARNING: usually rejected by ACL for this bridge unless it equals 'memento-mcp'. Use expert_id/scope/memory_type instead.")]
    #[serde(default)]
    app_id: Option<String>,
    #[schemars(description = "Filter by session_id")]
    #[serde(default)]
    session_id: Option<String>,
    #[schemars(description = "Filter by scope, e.g. 'personal', 'app', 'durable'")]
    #[serde(default)]
    scope: Option<String>,
    #[schemars(description = "Filter by memory_type, e.g. 'note', 'decision', 'preference', 'task'")]
    #[serde(default)]
    memory_type: Option<String>,
    #[schemars(description = "Max rows to return (default 50)")]
    #[serde(default)]
    limit: Option<i64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct RecallRecursiveContextParams {
    #[schemars(description = "Filter by expert_id (e.g. 'ava'). Prefer this over app_id — see recall_scoped_memory note.")]
    #[serde(default)]
    expert_id: Option<String>,
    #[schemars(description = "Filter by user_id")]
    #[serde(default)]
    user_id: Option<String>,
    #[schemars(description = "Filter by app_id — usually rejected by ACL for this bridge, see recall_scoped_memory note")]
    #[serde(default)]
    app_id: Option<String>,
    #[schemars(description = "Filter by session_id")]
    #[serde(default)]
    session_id: Option<String>,
    #[schemars(description = "Filter by scope, e.g. 'personal', 'app', 'durable'")]
    #[serde(default)]
    scope: Option<String>,
    #[schemars(description = "Max durable facts to return (default 16). Use 5 for compact inline context.")]
    #[serde(default)]
    max_durable_facts: Option<i64>,
    #[schemars(description = "Max recent events to return (default 12). Use 5 for compact inline context.")]
    #[serde(default)]
    max_recent_events: Option<i64>,
    #[schemars(description = "Skip working_context section (saves ~40% tokens — it duplicates top-level durable_facts + recent_events). Defaults to false.")]
    #[serde(default)]
    skip_working_context: Option<bool>,
}

// ─── Knowledge Graph (kg_*) Parameter Schemas ───────────────────────────
//
// Read-only wrappers over Memento's sovereign knowledge graph (kg_entity/
// kg_relation). For codebase graphs, scope with collection="code_graph" and
// app_id=<crate-slug> (e.g. "code-graph-kit") — see code-graph-kit's indexer.

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct KgGraphParams {
    #[schemars(description = "Crate/app slug to scope the query, e.g. 'code-graph-kit'. Omit for all scopes.")]
    #[serde(default)]
    app_id: Option<String>,
    #[schemars(description = "Collection namespace — use 'code_graph' for codebase graphs")]
    #[serde(default)]
    collection: Option<String>,
    #[schemars(description = "Max entities to return (default 200, max 2000)")]
    #[serde(default)]
    max_entities: Option<i64>,
    #[schemars(description = "Max relations to return (default 400, max 5000)")]
    #[serde(default)]
    max_relations: Option<i64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct KgNeighborsParams {
    #[schemars(
        description = "REQUIRED, non-empty. Seed entity_ids to expand from — obtain these FIRST from a kg_centrality or kg_graph call (the `entity_id` field, e.g. \"e_34e06cda19fd9c01\"), never invent them. Omitting this errors with 'missing field seeds'."
    )]
    seeds: Vec<String>,
    #[schemars(description = "Crate/app slug to scope the query")]
    #[serde(default)]
    app_id: Option<String>,
    #[schemars(description = "Collection namespace — use 'code_graph' for codebase graphs")]
    #[serde(default)]
    collection: Option<String>,
    #[schemars(description = "Hops to expand (default 1, max 3)")]
    #[serde(default)]
    hops: Option<i64>,
    #[schemars(description = "Max entities to return (default 60, max 1000)")]
    #[serde(default)]
    max_entities: Option<i64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct KgCentralityParams {
    #[schemars(description = "Crate/app slug to scope the query — omit for the whole graph")]
    #[serde(default)]
    app_id: Option<String>,
    #[schemars(description = "Collection namespace — use 'code_graph' for codebase graphs")]
    #[serde(default)]
    collection: Option<String>,
    #[schemars(description = "How many top entities to return (default 20, max 500)")]
    #[serde(default)]
    top: Option<i64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct KgPathParams {
    #[schemars(description = "Source entity_id")]
    from: String,
    #[schemars(description = "Target entity_id")]
    to: String,
    #[schemars(description = "Crate/app slug to scope the query")]
    #[serde(default)]
    app_id: Option<String>,
    #[schemars(description = "Collection namespace — use 'code_graph' for codebase graphs")]
    #[serde(default)]
    collection: Option<String>,
    #[schemars(description = "Max hops to search (default 5, max 8)")]
    #[serde(default)]
    max_hops: Option<i64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct KgCommunitiesParams {
    #[schemars(description = "Crate/app slug to scope the query — omit for the whole graph")]
    #[serde(default)]
    app_id: Option<String>,
    #[schemars(description = "Collection namespace — use 'code_graph' for codebase graphs")]
    #[serde(default)]
    collection: Option<String>,
    #[schemars(description = "Minimum community size to include (default 2)")]
    #[serde(default)]
    min_size: Option<i64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct KgSemanticSearchParams {
    #[schemars(description = "Query vector already computed by the caller (384-dim, model paraphrase-multilingual-MiniLM-L12-v2) — this tool does NOT calculate embeddings itself, the caller must generate it first (e.g. Hera embed_text_local or the 'embed' IPC action) and pass the resulting float array here.")]
    query_embedding: Vec<f32>,
    #[schemars(description = "Crate/app slug to scope the query — omit for the whole graph")]
    #[serde(default)]
    app_id: Option<String>,
    #[schemars(description = "Collection namespace — use 'code_graph' for codebase graphs")]
    #[serde(default)]
    collection: Option<String>,
    #[schemars(description = "Max results to return (default 10, max 50)")]
    #[serde(default)]
    top: Option<usize>,
}

// ─── IPC Client Helper ──────────────────────────────────────────────────

/// Sends a JSON action to the Memento daemon over UDS and returns the response.
/// This is the raw transport call — no telemetry. Used directly (not via
/// `send_ipc`) for the fire-and-forget usage-log call itself, so logging a
/// call never recursively logs itself.
async fn send_ipc_raw(action: &str, payload: serde_json::Value) -> String {
    let msg = request_envelope(action, payload);

    match UnixStream::connect(MEMENTO_SOCKET).await {
        Ok(mut stream) => {
            let msg_bytes = msg.to_string();
            if let Err(e) = stream.write_all(msg_bytes.as_bytes()).await {
                return format!("Error writing to Memento socket: {}", e);
            }
            // Shutdown write side to signal we're done sending
            if let Err(e) = stream.shutdown().await {
                return format!("Error shutting down write: {}", e);
            }
            let mut response = String::new();
            if let Err(e) = stream.read_to_string(&mut response).await {
                return format!("Error reading from Memento socket: {}", e);
            }
            response
        }
        Err(e) => {
            format!(
                "Cannot connect to Memento daemon at {}. Is it running? Error: {}",
                MEMENTO_SOCKET, e
            )
        }
    }
}

/// Sends a JSON action to the Memento daemon over UDS and returns the
/// response — the single choke point every MCP tool handler in this bridge
/// goes through. Wraps `send_ipc_raw` with direct-MCP-usage telemetry: times
/// the call, classifies success/error, and fires a SECOND, non-blocking IPC
/// call (`mcp_log_usage`) to record it. The usage-log call is spawned on its
/// own task and its result is discarded — a logging failure (or a slow
/// Memento) must never delay or break the original tool response.
async fn send_ipc(action: &str, payload: serde_json::Value) -> String {
    let started = Instant::now();
    let response = send_ipc_raw(action, payload).await;
    let duration_ms = started.elapsed().as_millis() as i64;

    let (success, error) = classify_transport_result(&response);
    let action = action.to_string();
    let log_payload = serde_json::json!({
        "session_id": mcp_session_id(),
        "tool_name": action,
        "action": action,
        "app_id": "memento-mcp",
        "duration_ms": duration_ms,
        "success": success,
        "error": error,
    });

    tokio::spawn(async move {
        let _ = send_ipc_raw("mcp_log_usage", log_payload).await;
    });

    response
}

// ─── MCP Server Implementation ──────────────────────────────────────────

#[derive(Debug, Clone)]
struct MementoMcp {
    tool_router: ToolRouter<Self>,
}

#[tool_router]
impl MementoMcp {
    pub fn new() -> Self {
        Self {
            tool_router: Self::tool_router(),
        }
    }

    /// Store a memory entry. If the key already exists, it will be updated.
    #[tool(
        description = "Store or update a persistent memory entry with a unique key, content, and optional tags. Use this to save important information that should persist across conversations."
    )]
    async fn store_memory(&self, Parameters(params): Parameters<StoreMemoryParams>) -> String {
        send_ipc(
            "store_knowledge",
            serde_json::json!({
                "key": params.key,
                "content": params.content,
                "tags": params.tags
            }),
        )
        .await
    }

    /// Retrieve a memory entry by its exact key.
    #[tool(
        description = "Retrieve a specific memory entry by its exact key. Use list_all_memories() first to find the correct key."
    )]
    async fn retrieve_memory(
        &self,
        Parameters(params): Parameters<RetrieveMemoryParams>,
    ) -> String {
        send_ipc("get_knowledge", serde_json::json!({ "key": params.key })).await
    }

    /// List all stored memories. Returns a compact index with keys, titles,
    /// tags, char counts, and timestamps.
    #[tool(
        description = "List all stored memory entries. Returns a compact directory with key, title (first 80 chars), tags, character count, and last updated timestamp for every entry. Use this FIRST to find the exact key before calling retrieve_memory()."
    )]
    async fn list_all_memories(&self) -> String {
        send_ipc("list_knowledge", serde_json::json!({})).await
    }

    /// Search memories by keyword across keys, content, and tags.
    #[tool(
        description = "Search all memories by keyword. Matches against keys, content, and tags. Returns snippets of matching entries. Use this for content-based searches, NOT for key lookup (use list_all_memories for that)."
    )]
    async fn search_memories(
        &self,
        Parameters(params): Parameters<SearchMemoriesParams>,
    ) -> String {
        send_ipc(
            "search_knowledge",
            serde_json::json!({ "query": params.query }),
        )
        .await
    }

    /// Delete a memory entry by its exact key.
    #[tool(
        description = "Delete a specific memory entry by its exact key. This action is permanent."
    )]
    async fn delete_memory(&self, Parameters(params): Parameters<DeleteMemoryParams>) -> String {
        send_ipc("delete_knowledge", serde_json::json!({ "key": params.key })).await
    }

    /// Read back scoped_memory (the Recursive State Node) — what save_memory
    /// actually writes to. Distinct from list_all_memories/search_memories,
    /// which only see the older knowledge_store table.
    #[tool(
        description = "Read entries from scoped_memory (the Recursive State Node) — this is where the save_memory tool (used by Hera, Ava, and agent dual-writes) actually stores content. list_all_memories/search_memories do NOT see this table. Requires at least one filter: expert_id, user_id, session_id, scope, or memory_type. Prefer expert_id/scope/memory_type over app_id (app_id is usually rejected by ACL for this bridge)."
    )]
    async fn recall_scoped_memory(
        &self,
        Parameters(params): Parameters<RecallScopedMemoryParams>,
    ) -> String {
        send_ipc(
            "query_memory_records",
            serde_json::json!({
                "expert_id": params.expert_id,
                "user_id": params.user_id,
                "app_id": params.app_id,
                "session_id": params.session_id,
                "scope": params.scope,
                "memory_type": params.memory_type,
                "limit": params.limit,
            }),
        )
        .await
    }

    /// Full recursive recall: project/room/session summaries + working
    /// context + durable facts + recent events, in one call.
    #[tool(
        description = "Fetch the full recursive memory context for a scope: project/room/session summaries, working context, durable facts, and recent events, in one call. This is the Recursive State Node's flagship read — richer than recall_scoped_memory's flat row list. Requires at least one filter: expert_id, user_id, session_id, or scope. Prefer expert_id/scope over app_id (app_id is usually rejected by ACL for this bridge)."
    )]
    async fn recall_recursive_context(
        &self,
        Parameters(params): Parameters<RecallRecursiveContextParams>,
    ) -> String {
        send_ipc(
            "recall_recursive_context",
            serde_json::json!({
                "expert_id": params.expert_id,
                "user_id": params.user_id,
                "app_id": params.app_id,
                "session_id": params.session_id,
                "scope": params.scope,
                "max_durable_facts": params.max_durable_facts,
                "max_recent_events": params.max_recent_events,
                "skip_working_context": params.skip_working_context,
            }),
        )
        .await
    }

    /// Fetch the scoped knowledge graph (entities + relations). For codebases,
    /// use this BEFORE grepping the filesystem to see what's already known:
    /// which files/functions/modules exist and how they connect.
    #[tool(
        description = "Fetch the knowledge graph for a scope (entities + relations, ranked by mention count / weight). For codebase questions, call with collection='code_graph' and app_id=<crate-slug> BEFORE using Grep/Glob — this returns files/functions/modules and their calls/imports/inherits relations directly, no filesystem scan needed. Omit app_id to see all indexed crates."
    )]
    async fn kg_graph(&self, Parameters(params): Parameters<KgGraphParams>) -> String {
        send_ipc(
            "kg_graph",
            serde_json::json!({
                "app_id": params.app_id,
                "collection": params.collection,
                "max_entities": params.max_entities,
                "max_relations": params.max_relations,
            }),
        )
        .await
    }

    /// k-hop expansion from seed entities — "what connects to this?"
    #[tool(
        description = "Expand k hops from one or more seed entity_ids (from kg_graph/kg_centrality results) to find everything connected to them. Use this to answer 'what calls/imports/uses X?' without grepping — pass the entity_id of a function/file/module as seed."
    )]
    async fn kg_neighbors(&self, Parameters(params): Parameters<KgNeighborsParams>) -> String {
        send_ipc(
            "kg_neighbors",
            serde_json::json!({
                "seeds": params.seeds,
                "app_id": params.app_id,
                "collection": params.collection,
                "hops": params.hops,
                "max_entities": params.max_entities,
            }),
        )
        .await
    }

    /// PageRank over the scoped graph — the "god nodes" (most important/connected).
    #[tool(
        description = "Rank entities by PageRank centrality within a scope — the 'god nodes' (most-connected files/functions/modules). Use this to orient in an unfamiliar crate before reading files: call with collection='code_graph', app_id=<crate-slug> to see what actually matters structurally."
    )]
    async fn kg_centrality(&self, Parameters(params): Parameters<KgCentralityParams>) -> String {
        send_ipc(
            "kg_centrality",
            serde_json::json!({
                "app_id": params.app_id,
                "collection": params.collection,
                "top": params.top,
            }),
        )
        .await
    }

    /// Shortest path between two entities — "how does X relate to Y?"
    #[tool(
        description = "Find the shortest path between two entity_ids in the knowledge graph — answers 'how is X connected to Y?' (e.g. which modules sit between a controller and a DB table) without manually tracing imports."
    )]
    async fn kg_path(&self, Parameters(params): Parameters<KgPathParams>) -> String {
        send_ipc(
            "kg_path",
            serde_json::json!({
                "from": params.from,
                "to": params.to,
                "app_id": params.app_id,
                "collection": params.collection,
                "max_hops": params.max_hops,
            }),
        )
        .await
    }

    /// Label-propagation communities — clusters of related entities.
    #[tool(
        description = "Cluster entities into communities (label propagation) within a scope — groups of files/functions that are structurally related. Use this for 'what are the major subsystems in this crate?' before reading code."
    )]
    async fn kg_communities(&self, Parameters(params): Parameters<KgCommunitiesParams>) -> String {
        send_ipc(
            "kg_communities",
            serde_json::json!({
                "app_id": params.app_id,
                "collection": params.collection,
                "min_size": params.min_size,
            }),
        )
        .await
    }

    /// Cosine-rerank entities against a caller-supplied query embedding.
    #[tool(
        description = "Semantic search over the knowledge graph: cosine-ranks entities against a query embedding YOU already computed (this tool never calls an embedding model itself). Use this when you have a natural-language question and its embedding vector and want the closest-matching entities by meaning, not just by name/keyword like kg_graph. Scope with collection='code_graph' and app_id=<crate-slug> for codebase search."
    )]
    async fn kg_semantic_search(
        &self,
        Parameters(params): Parameters<KgSemanticSearchParams>,
    ) -> String {
        send_ipc(
            "kg_semantic_search",
            serde_json::json!({
                "query_embedding": params.query_embedding,
                "app_id": params.app_id,
                "collection": params.collection,
                "top": params.top,
            }),
        )
        .await
    }
}

#[tool_handler]
impl ServerHandler for MementoMcp {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build()).with_instructions(
            "Memento is the sovereign long-term memory for ImagineOS. \
                 Use the Memory Index Protocol: \
                 1) Call list_all_memories() to get the complete key directory. \
                 2) Call retrieve_memory(exact_key) to fetch specific content. \
                 Only use search_memories() for content-based searches. \
                 store_memory/retrieve_memory/list_all_memories/search_memories only see \
                 the older knowledge_store table. The save_memory tool (used by Hera/Ava/agent \
                 dual-writes) writes to a DIFFERENT table, scoped_memory (the Recursive State \
                 Node) — read it back with recall_scoped_memory (flat filtered rows) or \
                 recall_recursive_context (project/room/session summaries + working context + \
                 durable facts + recent events in one call). Both need at least one filter — \
                 prefer expert_id/scope/memory_type over app_id, which is usually ACL-rejected \
                 for this bridge. \
                 \
                 Memento also holds a knowledge graph of this codebase (collection='code_graph', \
                 app_id=<crate-slug>), pre-computed from AST + doc analysis across the monorepo. \
                 Before grepping or globbing an unfamiliar crate, prefer: kg_centrality (what \
                 matters structurally), kg_graph (files/functions + their calls/imports), \
                 kg_neighbors (what connects to a given entity), kg_path (how two entities relate), \
                 kg_communities (major subsystems). Falls back cleanly to Grep/Glob if the crate \
                 hasn't been indexed yet."
                .to_string(),
        )
    }
}

// ─── Entrypoint ─────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Log to stderr to keep stdout clean for MCP protocol
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();

    tracing::info!("🧠 Starting Memento MCP Bridge (Stdio transport)");

    let service = MementoMcp::new().serve(stdio()).await.inspect_err(|e| {
        tracing::error!("MCP serving error: {:?}", e);
    })?;

    service.waiting().await?;
    Ok(())
}
