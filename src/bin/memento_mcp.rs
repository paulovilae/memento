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
    ServerHandler, ServiceExt,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{ServerCapabilities, ServerInfo},
    schemars, tool, tool_handler, tool_router,
    transport::stdio,
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
    #[schemars(description = "Comma-separated tags for categorization (e.g., 'setup,specs,hardware')")]
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
    #[tool(description = "Store or update a persistent memory entry with a unique key, content, and optional tags. Use this to save important information that should persist across conversations.")]
    async fn store_memory(
        &self,
        Parameters(params): Parameters<StoreMemoryParams>,
    ) -> String {
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
    #[tool(description = "Retrieve a specific memory entry by its exact key. Use list_all_memories() first to find the correct key.")]
    async fn retrieve_memory(
        &self,
        Parameters(params): Parameters<RetrieveMemoryParams>,
    ) -> String {
        send_ipc(
            "get_knowledge",
            serde_json::json!({ "key": params.key }),
        )
        .await
    }

    /// List all stored memories. Returns a compact index with keys, titles,
    /// tags, char counts, and timestamps.
    #[tool(description = "List all stored memory entries. Returns a compact directory with key, title (first 80 chars), tags, character count, and last updated timestamp for every entry. Use this FIRST to find the exact key before calling retrieve_memory().")]
    async fn list_all_memories(&self) -> String {
        send_ipc("list_knowledge", serde_json::json!({})).await
    }

    /// Search memories by keyword across keys, content, and tags.
    #[tool(description = "Search all memories by keyword. Matches against keys, content, and tags. Returns snippets of matching entries. Use this for content-based searches, NOT for key lookup (use list_all_memories for that).")]
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
    #[tool(description = "Delete a specific memory entry by its exact key. This action is permanent.")]
    async fn delete_memory(
        &self,
        Parameters(params): Parameters<DeleteMemoryParams>,
    ) -> String {
        send_ipc(
            "delete_knowledge",
            serde_json::json!({ "key": params.key }),
        )
        .await
    }
}

#[tool_handler]
impl ServerHandler for MementoMcp {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_instructions(
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

    let service = MementoMcp::new()
        .serve(stdio())
        .await
        .inspect_err(|e| {
            tracing::error!("MCP serving error: {:?}", e);
        })?;

    service.waiting().await?;
    Ok(())
}
