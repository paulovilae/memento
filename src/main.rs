use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tracing::{error, info, warn, Level};

mod app_registry;
mod audit;
mod bio;
mod chat_memory;
pub mod config;
mod doc_extract;
mod document_index;
mod document_index_ipc;
mod hardware;
mod ingestion;
mod interaction_memory;
mod kg_store;
mod knowledge;
mod metrics;
mod migrations;
mod query_cache;
mod rag_store;
pub mod recall_telemetry;
mod runtime_memory;
mod schema;
mod scoped_memory;
mod security;

#[derive(Serialize, Deserialize, Debug)]
struct IpcMessage {
    action: String,
    #[serde(default)]
    payload: serde_json::Value,
    #[serde(default)]
    client: Option<security::ClientIdentity>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 0. Load environment variables from .env
    dotenvy::dotenv().ok();

    // 1. Initialize Tracing
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();
    info!("🧠 Starting Memento Local Node (ImagineOS Mesh)...");

    // 2. Hardware Discovery (Liquid Compute)
    let profile = hardware::discover_hardware();
    info!("⚙️ Hardware Strategy: {}", profile.strategy);

    // 3. Start Local Folder Watcher (Asynchronous Privacy Queue Ingestion)
    let config = config::load_config();
    if config.watched_folders.is_empty() {
        info!("No folders configured yet. Add them via the dashboard!");
    } else if let Err(e) = ingestion::start_folder_watcher(config.watched_folders).await {
        eprintln!("Failed to start folder watcher: {}", e);
    }

    // 4. Initialize Memory Database
    let db_pool = schema::init_db()
        .await
        .expect("Failed to initialize Memento Postgres database");
    info!("💾 Memory database initialized");

    // 4b. Initialize Knowledge Store table
    knowledge::init_knowledge_table(&db_pool)
        .await
        .expect("Failed to initialize knowledge store");

    // 5. Discover App Databases (reads OS/etc/apps.toml)
    let os_root = std::env::var("IMAGINEOS_ROOT")
        .unwrap_or_else(|_| "/home/paulo/Programs/apps/OS".to_string());
    let app_connections = Arc::new(app_registry::discover_apps(&os_root).await);
    let security = Arc::new(security::SecurityConfig::from_env());

    let audit_pool = db_pool.clone();
    tokio::spawn(async move {
        loop {
            match audit::purge_expired_audit_entries(&audit_pool).await {
                Ok(deleted) if deleted > 0 => {
                    info!("Purged {} expired audit log rows", deleted);
                }
                Ok(_) => {}
                Err(error) => warn!(?error, "Failed to purge expired audit log rows"),
            }
            tokio::time::sleep(std::time::Duration::from_secs(60 * 60 * 24)).await;
        }
    });

    // 6. Setup Unix Domain Socket (UDS) for Zero-Copy IPC with Hera/Imaginclaw.
    // Default is `/tmp/memento.sock` (what every client hardcodes); tests/dev can
    // point to a private path via MEMENTO_SOCKET_PATH so they don't clobber a
    // running production daemon on the same machine.
    let socket_path_owned =
        std::env::var("MEMENTO_SOCKET_PATH").unwrap_or_else(|_| "/tmp/memento.sock".to_string());
    let socket_path = socket_path_owned.as_str();

    // Clean up old socket if it exists
    if Path::new(socket_path).exists() {
        std::fs::remove_file(socket_path)?;
    }

    let uds_listener = UnixListener::bind(socket_path)?;

    // Set permissions so everyone can read/write to the socket
    let mut perms = std::fs::metadata(socket_path)?.permissions();
    perms.set_mode(security.socket_mode);
    std::fs::set_permissions(socket_path, perms)?;

    info!("⚡ UDS zero-copy listener active on {}", socket_path);

    // 7. Block on IPC Listener natively
    handle_uds_connections(uds_listener, db_pool, app_connections, security).await;

    Ok(())
}

type AppConnections = Arc<HashMap<String, app_registry::AppConnection>>;
type SecurityConfig = Arc<security::SecurityConfig>;

async fn handle_uds_connections(
    listener: UnixListener,
    pool: sqlx::PgPool,
    apps: AppConnections,
    security: SecurityConfig,
) {
    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let pool = pool.clone();
                let apps = apps.clone();
                let security = security.clone();
                tokio::spawn(async move {
                    if let Err(e) = process_uds_stream(stream, pool, apps, security).await {
                        error!("Error processing UDS stream: {}", e);
                    }
                });
            }
            Err(e) => error!("Error accepting UDS connection: {}", e),
        }
    }
}

async fn process_uds_stream(
    mut stream: UnixStream,
    pool: sqlx::PgPool,
    apps: AppConnections,
    security: SecurityConfig,
) -> anyhow::Result<()> {
    // Acumula el mensaje COMPLETO antes de parsear: un request puede superar un solo chunk de 32 KB
    // (p.ej. `rag_ingest_document` de un documento grande con texto + embeddings). Leemos en bucle
    // hasta que el JSON sea parseable o hasta EOF. Los clientes hacen un request por conexión
    // (write + shutdown del lado de escritura), así que el cierre marca el fin del mensaje y el
    // try-parse permite responder en cuanto el JSON está completo aunque el cliente no cierre.
    let mut chunk = vec![0u8; 8192 * 4];
    const MAX_MSG: usize = 64 * 1024 * 1024; // backstop anti-OOM (64 MB)
    loop {
        let mut buffer: Vec<u8> = Vec::new();
        let req: IpcMessage = loop {
            let n = stream.read(&mut chunk).await?;
            if n == 0 {
                if buffer.is_empty() {
                    return Ok(()); // conexión cerrada limpiamente, sin mensaje pendiente
                }
                match serde_json::from_slice(&buffer) {
                    Ok(parsed) => break parsed,
                    Err(e) => {
                        let err_res =
                            serde_json::json!({ "error": format!("Invalid JSON: {}", e) });
                        let _ = stream.write_all(err_res.to_string().as_bytes()).await;
                        return Ok(());
                    }
                }
            }
            buffer.extend_from_slice(&chunk[..n]);
            if buffer.len() > MAX_MSG {
                let err_res = serde_json::json!({ "error": "request too large" });
                let _ = stream.write_all(err_res.to_string().as_bytes()).await;
                return Ok(());
            }
            // ¿JSON completo ya? Si aún no parsea (mensaje incompleto), seguimos leyendo.
            if let Ok(parsed) = serde_json::from_slice::<IpcMessage>(&buffer) {
                break parsed;
            }
        };

        if let Err(message) = security.authorize(&req.action, &req.payload, &req.client) {
            metrics::record_denied(&req.action);
            warn!(
                action = %req.action,
                client = req.client.as_ref().map(|client| client.app.as_str()).unwrap_or("<anonymous>"),
                "Denied IPC request"
            );
            let response = serde_json::json!({ "error": message });
            let _ = stream.write_all(response.to_string().as_bytes()).await;
            continue;
        }

        let started = Instant::now();
        let response = match req.action.as_str() {
            "save_memory" => chat_memory::save_memory(&pool, req.payload).await,
            "get_context" => chat_memory::get_context(&pool, req.payload).await,
            "record_context_feedback" => {
                chat_memory::record_context_feedback(&pool, req.payload).await
            }
            "get_context_profile" => chat_memory::get_context_profile(&pool, req.payload).await,
            "clear_context" => chat_memory::clear_context(&pool, req.payload).await,

            // ─── App Registry Actions ──────────────────────────────────
            "list_apps" => app_registry::list_apps_json(&apps),

            "query_app" => {
                let app_slug = req
                    .payload
                    .get("app")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let query = req
                    .payload
                    .get("query")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let limit = req
                    .payload
                    .get("limit")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(50);
                app_registry::query_app(&apps, app_slug, query, limit).await
            }

            // ─── Schema Auto-Discovery ──────────────────────────────
            "describe_app" => {
                let app_slug = req
                    .payload
                    .get("app")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                app_registry::describe_app(&apps, app_slug).await
            }

            "describe_all_apps" => app_registry::describe_all_apps(&apps).await,

            // ─── Knowledge Store Actions ─────────────────────────────
            "store_knowledge" => {
                let key = req
                    .payload
                    .get("key")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let content = req
                    .payload
                    .get("content")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let tags = req
                    .payload
                    .get("tags")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");

                if key.is_empty() || content.is_empty() {
                    serde_json::json!({ "error": "Missing 'key' or 'content' in payload" })
                } else {
                    knowledge::store(&pool, key, content, tags).await
                }
            }

            "get_knowledge" => {
                let key = req
                    .payload
                    .get("key")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if key.is_empty() {
                    serde_json::json!({ "error": "Missing 'key' in payload" })
                } else {
                    knowledge::get(&pool, key).await
                }
            }

            "list_knowledge" => knowledge::list(&pool).await,

            "search_knowledge" => {
                let query = req
                    .payload
                    .get("query")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if query.is_empty() {
                    serde_json::json!({ "error": "Missing 'query' in payload" })
                } else {
                    knowledge::search(&pool, query).await
                }
            }

            "delete_knowledge" => {
                let key = req
                    .payload
                    .get("key")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if key.is_empty() {
                    serde_json::json!({ "error": "Missing 'key' in payload" })
                } else {
                    knowledge::delete(&pool, key).await
                }
            }

            // ─── Bayesian Interaction Tracking ────────────────────────
            "log_interaction" => interaction_memory::log_interaction(&pool, req.payload).await,

            "get_user_prior" => interaction_memory::get_user_prior(&pool, req.payload).await,

            "save_user_prior" => interaction_memory::save_user_prior(&pool, req.payload).await,

            // ─── Virtual Office: Scoped Memory ────────────────────────
            "save_scoped_memory" | "save_memory_record" => {
                scoped_memory::save_record(&pool, req.payload).await
            }

            "get_scoped_memory" | "query_memory_records" => {
                scoped_memory::query_records(&pool, req.payload).await
            }

            "search_memory_records" => scoped_memory::search_records(&pool, req.payload).await,

            "get_memory_timeline" => scoped_memory::get_timeline(&pool, req.payload).await,

            "get_working_context" => scoped_memory::get_working_context(&pool, req.payload).await,

            "get_preferences" => scoped_memory::get_preferences(&pool, req.payload).await,
            "get_durable_facts" => scoped_memory::get_durable_facts(&pool, req.payload).await,
            "get_recent_events" => scoped_memory::get_recent_events(&pool, req.payload).await,
            "memory_promote" => scoped_memory::memory_promote(&pool, req.payload).await,
            "derive_memory" => scoped_memory::derive_memory(&pool, req.payload).await,
            "compress_session" => scoped_memory::compress_session(&pool, req.payload).await,
            "compress_room" => scoped_memory::compress_room(&pool, req.payload).await,
            "compress_project" => scoped_memory::compress_project(&pool, req.payload).await,
            "recall_recursive_context" => {
                scoped_memory::recall_recursive_context(&pool, req.payload).await
            }
            "semantic_recall" => scoped_memory::semantic_recall(&pool, req.payload).await,
            "recall_feedback" => recall_telemetry::recall_feedback(&pool, req.payload).await,

            "delete_scoped_memory" => {
                // Accept both { "ids": [1,2,3] } and { "id": 1 }; normalise to Vec<i32>.
                let ids: Vec<i32> =
                    if let Some(arr) = req.payload.get("ids").and_then(|v| v.as_array()) {
                        arr.iter()
                            .filter_map(|v| v.as_i64().map(|n| n as i32))
                            .collect()
                    } else if let Some(single) = req.payload.get("id").and_then(|v| v.as_i64()) {
                        vec![single as i32]
                    } else {
                        Vec::new()
                    };
                scoped_memory::delete_records(&pool, ids).await
            }

            "scoped_memory_app_stats" => scoped_memory::app_stats(&pool).await,

            // ─── Virtual Office: Audit Log ─────────────────────────────
            "audit_log" => audit::audit_log(&pool, req.payload).await,
            "get_metrics" => metrics::get_metrics(),
            "get_runtime_preflight" => {
                runtime_memory::get_runtime_preflight(&pool, req.payload).await
            }
            "record_runtime_observation" => {
                runtime_memory::record_runtime_observation(&pool, req.payload).await
            }
            "promote_runtime_hint" => {
                runtime_memory::promote_runtime_hint(&pool, req.payload).await
            }
            "save_agent_run_summary" => {
                runtime_memory::save_agent_run_summary(&pool, req.payload).await
            }
            "upsert_document_index" => document_index_ipc::upsert(&pool, req.payload).await,
            "get_document_index" => document_index_ipc::get(&pool, req.payload).await,
            "list_document_indexes" => document_index_ipc::list(&pool, req.payload).await,
            "query_document_index" => document_index_ipc::query(&pool, req.payload).await,

            // ─── Conversor de documentos (path → texto; ver doc_extract.rs) ──
            "extract_text" => doc_extract::extract_text(req.payload).await,

            // ─── RAG Document Store ───────────────────────────────────────
            "rag_ingest_document" => rag_store::ingest_document(&pool, req.payload).await,
            "rag_list_documents" => rag_store::list_documents(&pool, req.payload).await,
            "rag_get_document" => rag_store::get_document(&pool, req.payload).await,
            "rag_update_document" => rag_store::update_document(&pool, req.payload).await,
            "rag_reembed_document" => rag_store::reembed_document(&pool, req.payload).await,
            "rag_delete_document" => rag_store::delete_document(&pool, req.payload).await,
            "rag_search" => rag_store::search(&pool, req.payload).await,
            "rag_pinned" => rag_store::pinned(&pool, req.payload).await,
            "rag_chunk_vectors" => rag_store::chunk_vectors(&pool, req.payload).await,
            "kg_upsert_triples" => kg_store::upsert_triples(&pool, req.payload).await,
            "kg_graph" => kg_store::graph(&pool, req.payload).await,
            "kg_neighbors" => kg_store::neighbors(&pool, req.payload).await,

            // ─── Paulo Bio Data Actions ───────────────────────────────
            "query_bio" => bio::query_bio(&pool, req.payload).await,
            "seed_bio" => bio::seed_bio(&pool, req.payload).await,
            "delete_bio" => bio::delete_bio(&pool, req.payload).await,

            "vector_search" => {
                let query = req
                    .payload
                    .get("query")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");

                if query.is_empty() {
                    serde_json::json!({ "error": "Missing 'query' in payload" })
                } else {
                    // search returns a serde_json::Value representing the search results directly
                    let results_value = knowledge::search(&pool, query).await;
                    serde_json::json!({
                        "status": "success",
                        "results": results_value,
                    })
                }
            }

            _ => {
                serde_json::json!({ "error": format!("Unknown action: {}", req.action) })
            }
        };

        info!(
            action = %req.action,
            client = req.client.as_ref().map(|client| client.app.as_str()).unwrap_or("<anonymous>"),
            duration_ms = started.elapsed().as_millis(),
            success = response.get("error").is_none(),
            "Handled IPC request"
        );
        metrics::record_request(
            &req.action,
            started.elapsed().as_millis(),
            response.get("error").is_none(),
        );

        if let Err(e) = stream.write_all(response.to_string().as_bytes()).await {
            error!("Failed to send IPC response: {}", e);
        }
        // El bucle continúa: una conexión puede traer más requests (secuenciales). El cierre del
        // cliente (read → 0 con buffer vacío) sale de la función con Ok más arriba.
    }
}
