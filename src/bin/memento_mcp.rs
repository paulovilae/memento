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

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ResearchProjectParams {
    #[schemars(description = "Stable research project identifier")]
    project_id: String,
    #[schemars(description = "Human-readable project title")]
    title: String,
    #[schemars(description = "Optional project goal", default)]
    goal: Option<String>,
    #[schemars(description = "Optional structured research questions", default)]
    questions_json: Option<serde_json::Value>,
    #[schemars(description = "Optional constraints object", default)]
    constraints_json: Option<serde_json::Value>,
    #[schemars(description = "Optional deliverable type", default)]
    deliverable_type: Option<String>,
    #[schemars(description = "Optional owner", default)]
    owner: Option<String>,
    #[schemars(description = "Optional user identity", default)]
    user_id: Option<String>,
    #[schemars(description = "Optional tenant identifier", default)]
    tenant_id: Option<String>,
    #[schemars(description = "Optional app context", default)]
    app_id: Option<String>,
    #[schemars(description = "Optional scope key", default)]
    scope_key: Option<String>,
    #[schemars(description = "Optional project status", default)]
    status: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ResearchProjectIdParams {
    #[schemars(description = "Exact research project identifier")]
    project_id: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ListResearchProjectsParams {
    #[schemars(description = "Optional app filter", default)]
    app_id: Option<String>,
    #[schemars(description = "Optional tenant filter", default)]
    tenant_id: Option<String>,
    #[schemars(description = "Optional project status filter", default)]
    status: Option<String>,
    #[schemars(description = "Maximum number of projects to return", default)]
    limit: Option<i64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ResearchSessionParams {
    #[schemars(description = "Stable research session identifier")]
    session_id: String,
    #[schemars(description = "Parent project identifier")]
    project_id: String,
    #[schemars(description = "Optional session title", default)]
    title: Option<String>,
    #[schemars(description = "Optional session brief", default)]
    brief: Option<String>,
    #[schemars(description = "Optional originating channel", default)]
    channel: Option<String>,
    #[schemars(description = "Optional tools summary object", default)]
    tools_json: Option<serde_json::Value>,
    #[schemars(description = "Optional agents summary object", default)]
    agents_json: Option<serde_json::Value>,
    #[schemars(description = "Optional summary", default)]
    summary: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ResearchSourceParams {
    #[schemars(description = "Stable research source identifier")]
    source_id: String,
    #[schemars(description = "Parent project identifier")]
    project_id: String,
    #[schemars(description = "Optional session identifier", default)]
    session_id: Option<String>,
    #[schemars(description = "Source kind such as web_page, pdf, or chat_reply")]
    source_kind: String,
    #[schemars(description = "Optional canonical URI or path", default)]
    source_uri: Option<String>,
    #[schemars(description = "Optional human-readable label", default)]
    source_label: Option<String>,
    #[schemars(description = "Optional title", default)]
    title: Option<String>,
    #[schemars(description = "Optional summary", default)]
    summary: Option<String>,
    #[schemars(description = "Optional content type", default)]
    content_type: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ResearchSourceIdParams {
    #[schemars(description = "Exact research source identifier")]
    source_id: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ListResearchSourcesParams {
    #[schemars(description = "Optional parent project filter", default)]
    project_id: Option<String>,
    #[schemars(description = "Optional session filter", default)]
    session_id: Option<String>,
    #[schemars(description = "Optional source kind filter", default)]
    source_kind: Option<String>,
    #[schemars(description = "Maximum number of sources to return", default)]
    limit: Option<i64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ConceptParams {
    #[schemars(description = "Stable concept identifier")]
    concept_id: String,
    #[schemars(description = "Canonical concept name")]
    canonical_name: String,
    #[schemars(description = "Optional concept summary", default)]
    summary: Option<String>,
    #[schemars(description = "Optional concept domain", default)]
    domain: Option<String>,
    #[schemars(description = "Optional aliases object", default)]
    aliases_json: Option<serde_json::Value>,
    #[schemars(description = "Optional confidence score", default)]
    confidence: Option<f64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ConceptIdParams {
    #[schemars(description = "Exact concept identifier")]
    concept_id: String,
    #[schemars(description = "Maximum linked rows to return", default)]
    limit: Option<i64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ClaimParams {
    #[schemars(description = "Stable claim identifier")]
    claim_id: String,
    #[schemars(description = "Claim text")]
    claim_text: String,
    #[schemars(description = "Primary concept identifier")]
    primary_concept_id: String,
    #[schemars(description = "Optional project identifier", default)]
    project_id: Option<String>,
    #[schemars(description = "Optional session identifier", default)]
    session_id: Option<String>,
    #[schemars(description = "Optional claim type", default)]
    claim_type: Option<String>,
    #[schemars(description = "Optional confidence score", default)]
    confidence: Option<f64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct EvidenceParams {
    #[schemars(description = "Stable evidence identifier")]
    evidence_id: String,
    #[schemars(description = "Parent claim identifier")]
    claim_id: String,
    #[schemars(description = "Supporting snippet")]
    snippet: String,
    #[schemars(description = "Optional source kind", default)]
    source_kind: Option<String>,
    #[schemars(description = "Optional source reference", default)]
    source_ref: Option<String>,
    #[schemars(description = "Optional locator", default)]
    locator: Option<String>,
    #[schemars(description = "Optional extraction method", default)]
    extraction_method: Option<String>,
    #[schemars(description = "Optional confidence score", default)]
    confidence: Option<f64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct RelationParams {
    #[schemars(description = "Stable relation identifier")]
    edge_id: String,
    #[schemars(description = "Source concept identifier")]
    from_concept_id: String,
    #[schemars(description = "Target concept identifier")]
    to_concept_id: String,
    #[schemars(description = "Relation type")]
    relation_type: String,
    #[schemars(description = "Optional relation weight", default)]
    weight: Option<f64>,
    #[schemars(description = "Optional confidence score", default)]
    confidence: Option<f64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ClaimIdParams {
    #[schemars(description = "Exact claim identifier")]
    claim_id: String,
}

// ─── IPC Client Helper ──────────────────────────────────────────────────

/// Sends a JSON action to the Memento daemon over UDS and returns the response.
async fn send_ipc(action: &str, payload: serde_json::Value) -> String {
    let msg = serde_json::json!({
        "action": action,
        "payload": payload
    });

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

    #[tool(description = "Create or update a semantic-memory research project in Memento.")]
    async fn create_research_project(
        &self,
        Parameters(params): Parameters<ResearchProjectParams>,
    ) -> String {
        send_ipc(
            "upsert_research_project",
            serde_json::json!({
                "project_id": params.project_id,
                "title": params.title,
                "goal": params.goal,
                "questions_json": params.questions_json,
                "constraints_json": params.constraints_json,
                "deliverable_type": params.deliverable_type,
                "owner": params.owner,
                "user_id": params.user_id,
                "tenant_id": params.tenant_id,
                "app_id": params.app_id,
                "scope_key": params.scope_key,
                "status": params.status
            }),
        )
        .await
    }

    #[tool(description = "Fetch a research project with its sessions and linked concepts.")]
    async fn get_research_project(
        &self,
        Parameters(params): Parameters<ResearchProjectIdParams>,
    ) -> String {
        send_ipc(
            "get_research_project",
            serde_json::json!({ "project_id": params.project_id }),
        )
        .await
    }

    #[tool(description = "List semantic-memory research projects in Memento.")]
    async fn list_research_projects(
        &self,
        Parameters(params): Parameters<ListResearchProjectsParams>,
    ) -> String {
        send_ipc(
            "list_research_projects",
            serde_json::json!({
                "app_id": params.app_id,
                "tenant_id": params.tenant_id,
                "status": params.status,
                "limit": params.limit.unwrap_or(25)
            }),
        )
        .await
    }

    #[tool(description = "Create or update a research session under an existing project.")]
    async fn create_research_session(
        &self,
        Parameters(params): Parameters<ResearchSessionParams>,
    ) -> String {
        send_ipc(
            "create_research_session",
            serde_json::json!({
                "session_id": params.session_id,
                "project_id": params.project_id,
                "title": params.title,
                "brief": params.brief,
                "channel": params.channel,
                "tools_json": params.tools_json,
                "agents_json": params.agents_json,
                "summary": params.summary
            }),
        )
        .await
    }

    #[tool(
        description = "Create or update a first-class research source in Memento semantic memory."
    )]
    async fn create_research_source(
        &self,
        Parameters(params): Parameters<ResearchSourceParams>,
    ) -> String {
        send_ipc(
            "upsert_research_source",
            serde_json::json!({
                "source_id": params.source_id,
                "project_id": params.project_id,
                "session_id": params.session_id,
                "source_kind": params.source_kind,
                "source_uri": params.source_uri,
                "source_label": params.source_label,
                "title": params.title,
                "summary": params.summary,
                "content_type": params.content_type
            }),
        )
        .await
    }

    #[tool(description = "Fetch a specific research source by exact source id.")]
    async fn get_research_source(
        &self,
        Parameters(params): Parameters<ResearchSourceIdParams>,
    ) -> String {
        send_ipc(
            "get_research_source",
            serde_json::json!({ "source_id": params.source_id }),
        )
        .await
    }

    #[tool(
        description = "List research sources with optional project/session/source-kind filters."
    )]
    async fn list_research_sources(
        &self,
        Parameters(params): Parameters<ListResearchSourcesParams>,
    ) -> String {
        send_ipc(
            "list_research_sources",
            serde_json::json!({
                "project_id": params.project_id,
                "session_id": params.session_id,
                "source_kind": params.source_kind,
                "limit": params.limit.unwrap_or(25)
            }),
        )
        .await
    }

    #[tool(description = "Create or update a semantic concept node in Memento.")]
    async fn upsert_concept_node(&self, Parameters(params): Parameters<ConceptParams>) -> String {
        send_ipc(
            "upsert_concept_node",
            serde_json::json!({
                "concept_id": params.concept_id,
                "canonical_name": params.canonical_name,
                "summary": params.summary,
                "domain": params.domain,
                "aliases_json": params.aliases_json,
                "confidence": params.confidence
            }),
        )
        .await
    }

    #[tool(description = "Append a grounded claim to Memento's semantic memory.")]
    async fn append_claim(&self, Parameters(params): Parameters<ClaimParams>) -> String {
        send_ipc(
            "append_claim_record",
            serde_json::json!({
                "claim_id": params.claim_id,
                "claim_text": params.claim_text,
                "primary_concept_id": params.primary_concept_id,
                "project_id": params.project_id,
                "session_id": params.session_id,
                "claim_type": params.claim_type,
                "confidence": params.confidence
            }),
        )
        .await
    }

    #[tool(description = "Append evidence supporting a grounded claim in Memento.")]
    async fn append_evidence(&self, Parameters(params): Parameters<EvidenceParams>) -> String {
        send_ipc(
            "append_evidence_record",
            serde_json::json!({
                "evidence_id": params.evidence_id,
                "claim_id": params.claim_id,
                "snippet": params.snippet,
                "source_kind": params.source_kind,
                "source_ref": params.source_ref,
                "locator": params.locator,
                "extraction_method": params.extraction_method,
                "confidence": params.confidence
            }),
        )
        .await
    }

    #[tool(description = "Create or update a semantic relation edge between two concepts.")]
    async fn link_concepts(&self, Parameters(params): Parameters<RelationParams>) -> String {
        send_ipc(
            "link_concepts",
            serde_json::json!({
                "edge_id": params.edge_id,
                "from_concept_id": params.from_concept_id,
                "to_concept_id": params.to_concept_id,
                "relation_type": params.relation_type,
                "weight": params.weight,
                "confidence": params.confidence
            }),
        )
        .await
    }

    #[tool(description = "Expand a concept into its claims and relations.")]
    async fn expand_concept(&self, Parameters(params): Parameters<ConceptIdParams>) -> String {
        send_ipc(
            "expand_concept",
            serde_json::json!({
                "concept_id": params.concept_id,
                "limit": params.limit.unwrap_or(20)
            }),
        )
        .await
    }

    #[tool(description = "Trace a claim back to its supporting evidence, session, and project.")]
    async fn trace_claim_provenance(
        &self,
        Parameters(params): Parameters<ClaimIdParams>,
    ) -> String {
        send_ipc(
            "trace_claim_provenance",
            serde_json::json!({ "claim_id": params.claim_id }),
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
                 Only use search_memories() for content-based searches."
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
