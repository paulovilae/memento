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
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

const MEMENTO_SOCKET: &str = "/tmp/memento.sock";

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
    #[schemars(description = "Seed entity_ids to expand from (e.g. a function or file node id)")]
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

// ─── IPC Client Helper ──────────────────────────────────────────────────

/// Sends a JSON action to the Memento daemon over UDS and returns the response.
async fn send_ipc(action: &str, payload: serde_json::Value) -> String {
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
