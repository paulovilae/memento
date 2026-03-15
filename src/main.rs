use std::os::unix::fs::PermissionsExt;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::net::{UnixListener, UnixStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{info, error, Level};
use serde::{Deserialize, Serialize};
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};
use sqlx::{Column, Row};

mod hardware;
mod ingestion;
mod app_registry;
pub mod config;

#[derive(Serialize, Deserialize, Debug)]
struct IpcMessage {
    action: String,
    #[serde(default)]
    payload: serde_json::Value,
}

#[derive(Serialize, Deserialize, Debug)]
struct MemoryEntry {
    chat_id: String,
    role: String,
    content: String,
}

async fn init_db() -> anyhow::Result<SqlitePool> {
    let mut path = dirs::config_dir().unwrap_or_else(|| PathBuf::from("."));
    path.push("memento");
    std::fs::create_dir_all(&path).unwrap_or_default();
    path.push("memory.db");
    
    let db_url = format!("sqlite://{}?mode=rwc", path.to_string_lossy());
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect(&db_url).await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS memento_memory (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id TEXT NOT NULL,
            role TEXT NOT NULL,
            content TEXT NOT NULL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        "#
    )
    .execute(&pool)
    .await?;

    Ok(pool)
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
    } else {
        if let Err(e) = ingestion::start_folder_watcher(config.watched_folders).await {
            eprintln!("Failed to start folder watcher: {}", e);
        }
    }

    // 4. Initialize Memory Database
    let db_pool = init_db().await.expect("Failed to initialize SQLite database");
    info!("💾 Memory database initialized");

    // 5. Discover App Databases (reads OS/etc/apps.toml)
    let os_root = std::env::var("IMAGINEOS_ROOT")
        .unwrap_or_else(|_| "/home/paulo/Programs/apps/OS".to_string());
    let app_connections = Arc::new(app_registry::discover_apps(&os_root).await);

    // 6. Setup Unix Domain Socket (UDS) for Zero-Copy IPC with Hera/Imaginclaw
    let socket_path = "/tmp/memento.sock";
    
    // Clean up old socket if it exists
    if Path::new(socket_path).exists() {
        std::fs::remove_file(socket_path)?;
    }

    let uds_listener = UnixListener::bind(socket_path)?;
    
    // Set permissions so everyone can read/write to the socket
    let mut perms = std::fs::metadata(socket_path)?.permissions();
    perms.set_mode(0o777);
    std::fs::set_permissions(socket_path, perms)?;

    info!("⚡ UDS zero-copy listener active on {}", socket_path);

    // 7. Block on IPC Listener natively
    handle_uds_connections(uds_listener, db_pool, app_connections).await;

    Ok(())
}

type AppConnections = Arc<HashMap<String, app_registry::AppConnection>>;

async fn handle_uds_connections(listener: UnixListener, pool: SqlitePool, apps: AppConnections) {
    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let pool = pool.clone();
                let apps = apps.clone();
                tokio::spawn(async move {
                    if let Err(e) = process_uds_stream(stream, pool, apps).await {
                        error!("Error processing UDS stream: {}", e);
                    }
                });
            }
            Err(e) => error!("Error accepting UDS connection: {}", e),
        }
    }
}

async fn process_uds_stream(mut stream: UnixStream, pool: SqlitePool, apps: AppConnections) -> anyhow::Result<()> {
    let mut buffer = vec![0; 8192 * 4];
    loop {
        let n = stream.read(&mut buffer).await?;
        if n == 0 {
            break;
        }

        let msg_str = std::str::from_utf8(&buffer[..n])?;
        
        let req: IpcMessage = match serde_json::from_str(msg_str) {
            Ok(r) => r,
            Err(e) => {
                let err_res = serde_json::json!({ "error": format!("Invalid JSON: {}", e) });
                let _ = stream.write_all(err_res.to_string().as_bytes()).await;
                continue;
            }
        };

        let response = match req.action.as_str() {
            "save_memory" => {
                if let Ok(entry) = serde_json::from_value::<MemoryEntry>(req.payload) {
                    let result = sqlx::query(
                        "INSERT INTO memento_memory (chat_id, role, content) VALUES (?, ?, ?)"
                    )
                    .bind(&entry.chat_id)
                    .bind(&entry.role)
                    .bind(&entry.content)
                    .execute(&pool)
                    .await;

                    match result {
                        Ok(_) => serde_json::json!({ "status": "success" }),
                        Err(e) => serde_json::json!({ "error": format!("DB error: {}", e) }),
                    }
                } else {
                    serde_json::json!({ "error": "Invalid payload for save_memory" })
                }
            }
            "get_context" => {
                let chat_id = req.payload.get("chat_id").and_then(|v| v.as_str()).unwrap_or("");
                let limit = req.payload.get("limit").and_then(|v| v.as_i64()).unwrap_or(20);
                
                let rows_result = sqlx::query(
                    "SELECT role, content FROM (SELECT * FROM memento_memory WHERE chat_id = ? ORDER BY timestamp DESC LIMIT ?) ORDER BY timestamp ASC"
                )
                .bind(chat_id)
                .bind(limit)
                .fetch_all(&pool)
                .await;

                match rows_result {
                    Ok(rows) => {
                        let mut messages = Vec::new();
                        for row in rows {
                            let r: String = row.get::<String, _>("role");
                            let c: String = row.get::<String, _>("content");
                            messages.push(serde_json::json!({ "role": r, "content": c }));
                        }
                        serde_json::json!({ "status": "success", "messages": messages })
                    }
                    Err(e) => serde_json::json!({ "error": format!("DB error: {}", e) }),
                }
            }

            // ─── App Registry Actions ──────────────────────────────────
            "list_apps" => {
                let app_list: Vec<serde_json::Value> = apps.iter().map(|(slug, conn)| {
                    serde_json::json!({
                        "slug": slug,
                        "name": conn.name,
                        "description": conn.description,
                        "key_tables": conn.key_tables,
                    })
                }).collect();
                serde_json::json!({ "status": "success", "apps": app_list })
            }

            "query_app" => {
                let app_slug = req.payload.get("app")
                    .and_then(|v| v.as_str()).unwrap_or("");
                let query = req.payload.get("query")
                    .and_then(|v| v.as_str()).unwrap_or("");
                let limit = req.payload.get("limit")
                    .and_then(|v| v.as_i64()).unwrap_or(50);

                if query.is_empty() {
                    serde_json::json!({ "error": "Missing 'query' in payload" })
                } else if let Some(app_conn) = apps.get(app_slug) {
                    // Safety: only allow SELECT queries (read-only)
                    let trimmed = query.trim().to_uppercase();
                    if !trimmed.starts_with("SELECT") {
                        serde_json::json!({ "error": "Only SELECT queries are allowed" })
                    } else {
                        // Enforce a LIMIT to prevent huge result sets
                        let safe_query = if trimmed.contains("LIMIT") {
                            query.to_string()
                        } else {
                            format!("{} LIMIT {}", query, limit)
                        };

                        match sqlx::query(&safe_query)
                            .fetch_all(&app_conn.pool)
                            .await
                        {
                            Ok(rows) => {
                                let results: Vec<serde_json::Value> = rows.iter().map(|row| {
                                    // Convert each row to a JSON object
                                    let columns = row.columns();
                                    let mut obj = serde_json::Map::new();
                                    for col in columns {
                                        let name = col.name();
                                        // Try string first, then integer, then null
                                        if let Ok(v) = row.try_get::<String, _>(name) {
                                            obj.insert(name.to_string(), serde_json::json!(v));
                                        } else if let Ok(v) = row.try_get::<i64, _>(name) {
                                            obj.insert(name.to_string(), serde_json::json!(v));
                                        } else if let Ok(v) = row.try_get::<i32, _>(name) {
                                            obj.insert(name.to_string(), serde_json::json!(v));
                                        } else if let Ok(v) = row.try_get::<bool, _>(name) {
                                            obj.insert(name.to_string(), serde_json::json!(v));
                                        } else {
                                            obj.insert(name.to_string(), serde_json::Value::Null);
                                        }
                                    }
                                    serde_json::Value::Object(obj)
                                }).collect();
                                serde_json::json!({
                                    "status": "success",
                                    "app": app_slug,
                                    "count": results.len(),
                                    "rows": results
                                })
                            }
                            Err(e) => serde_json::json!({ "error": format!("Query error: {}", e) }),
                        }
                    }
                } else {
                    let available: Vec<&String> = apps.keys().collect();
                    serde_json::json!({
                        "error": format!("App '{}' not found", app_slug),
                        "available_apps": available
                    })
                }
            }

            _ => {
                serde_json::json!({ "error": format!("Unknown action: {}", req.action) })
            }
        };

        if let Err(e) = stream.write_all(response.to_string().as_bytes()).await {
            error!("Failed to send IPC response: {}", e);
        }
    }
    
    Ok(())
}
