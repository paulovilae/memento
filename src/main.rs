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
mod knowledge;
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

    // Bayesian interaction tracking tables
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS bayesian_interactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            domain TEXT NOT NULL,
            round INTEGER NOT NULL,
            options_json TEXT NOT NULL,
            choice_index INTEGER NOT NULL,
            prior_json TEXT,
            posterior_json TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        "#
    )
    .execute(&pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS user_priors (
            user_id TEXT NOT NULL,
            domain TEXT NOT NULL,
            prior_json TEXT NOT NULL,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (user_id, domain)
        )
        "#
    )
    .execute(&pool)
    .await?;

    // ─── Virtual Office: Scoped Memory ────────────────────────────
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS scoped_memory (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            tenant_id TEXT NOT NULL DEFAULT 'default',
            app_id TEXT NOT NULL DEFAULT 'os',
            expert_id TEXT NOT NULL DEFAULT 'ava',
            session_id TEXT NOT NULL DEFAULT '',
            device_id TEXT NOT NULL DEFAULT 'server',
            scope TEXT NOT NULL DEFAULT 'personal',
            source TEXT NOT NULL DEFAULT 'chat',
            content TEXT NOT NULL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        "#
    )
    .execute(&pool)
    .await?;

    // ─── Virtual Office: Audit Log ───────────────────────────────
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            actor TEXT NOT NULL,
            expert_identity TEXT NOT NULL,
            capability_used TEXT NOT NULL,
            sensitive_action TEXT,
            target_app TEXT,
            target_page TEXT,
            mutation_description TEXT NOT NULL,
            tenant_id TEXT,
            session_id TEXT,
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

    // 4b. Initialize Knowledge Store table
    knowledge::init_knowledge_table(&db_pool).await
        .expect("Failed to initialize knowledge store");

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
            "clear_context" => {
                let chat_id = req.payload.get("chat_id").and_then(|v| v.as_str()).unwrap_or("");
                if chat_id.is_empty() {
                    serde_json::json!({ "error": "Missing 'chat_id' in payload" })
                } else {
                    let result = sqlx::query("DELETE FROM memento_memory WHERE chat_id = ?")
                        .bind(chat_id)
                        .execute(&pool)
                        .await;
                    match result {
                        Ok(_) => serde_json::json!({ "status": "success" }),
                        Err(e) => serde_json::json!({ "error": format!("DB error: {}", e) }),
                    }
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
                    // Safety: only allow SELECT or WITH queries (read-only)
                    let trimmed = query.trim().to_uppercase();
                    if !trimmed.starts_with("SELECT") && !trimmed.starts_with("WITH") {
                        serde_json::json!({ "error": "Only SELECT or WITH queries are allowed" })
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
                                        // Try JSON first, then string, then integer, then bool
                                        if let Ok(v) = row.try_get::<serde_json::Value, _>(name) {
                                            obj.insert(name.to_string(), v);
                                        } else if let Ok(v) = row.try_get::<String, _>(name) {
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

            // ─── Knowledge Store Actions ─────────────────────────────
            "store_knowledge" => {
                let key = req.payload.get("key").and_then(|v| v.as_str()).unwrap_or("");
                let content = req.payload.get("content").and_then(|v| v.as_str()).unwrap_or("");
                let tags = req.payload.get("tags").and_then(|v| v.as_str()).unwrap_or("");

                if key.is_empty() || content.is_empty() {
                    serde_json::json!({ "error": "Missing 'key' or 'content' in payload" })
                } else {
                    knowledge::store(&pool, key, content, tags).await
                }
            }

            "get_knowledge" => {
                let key = req.payload.get("key").and_then(|v| v.as_str()).unwrap_or("");
                if key.is_empty() {
                    serde_json::json!({ "error": "Missing 'key' in payload" })
                } else {
                    knowledge::get(&pool, key).await
                }
            }

            "list_knowledge" => {
                knowledge::list(&pool).await
            }

            "search_knowledge" => {
                let query = req.payload.get("query").and_then(|v| v.as_str()).unwrap_or("");
                if query.is_empty() {
                    serde_json::json!({ "error": "Missing 'query' in payload" })
                } else {
                    knowledge::search(&pool, query).await
                }
            }

            "delete_knowledge" => {
                let key = req.payload.get("key").and_then(|v| v.as_str()).unwrap_or("");
                if key.is_empty() {
                    serde_json::json!({ "error": "Missing 'key' in payload" })
                } else {
                    knowledge::delete(&pool, key).await
                }
            }

            // ─── Bayesian Interaction Tracking ────────────────────────
            "log_interaction" => {
                let session_id = req.payload.get("session_id").and_then(|v| v.as_str()).unwrap_or("");
                let user_id = req.payload.get("user_id").and_then(|v| v.as_str()).unwrap_or("");
                let domain = req.payload.get("domain").and_then(|v| v.as_str()).unwrap_or("");
                let round = req.payload.get("round").and_then(|v| v.as_i64()).unwrap_or(0);
                let options_json = req.payload.get("options_json").and_then(|v| v.as_str()).unwrap_or("[]");
                let choice_index = req.payload.get("choice_index").and_then(|v| v.as_i64()).unwrap_or(0);
                let prior_json = req.payload.get("prior_json").and_then(|v| v.as_str());
                let posterior_json = req.payload.get("posterior_json").and_then(|v| v.as_str());

                if session_id.is_empty() || user_id.is_empty() || domain.is_empty() {
                    serde_json::json!({ "error": "Missing session_id, user_id, or domain" })
                } else {
                    let result = sqlx::query(
                        "INSERT INTO bayesian_interactions (session_id, user_id, domain, round, options_json, choice_index, prior_json, posterior_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
                    )
                    .bind(session_id)
                    .bind(user_id)
                    .bind(domain)
                    .bind(round)
                    .bind(options_json)
                    .bind(choice_index)
                    .bind(prior_json)
                    .bind(posterior_json)
                    .execute(&pool)
                    .await;

                    match result {
                        Ok(_) => serde_json::json!({ "status": "success", "action": "logged" }),
                        Err(e) => serde_json::json!({ "error": format!("DB error: {}", e) }),
                    }
                }
            }

            "get_user_prior" => {
                let user_id = req.payload.get("user_id").and_then(|v| v.as_str()).unwrap_or("");
                let domain = req.payload.get("domain").and_then(|v| v.as_str()).unwrap_or("");

                if user_id.is_empty() || domain.is_empty() {
                    serde_json::json!({ "error": "Missing user_id or domain" })
                } else {
                    let result = sqlx::query(
                        "SELECT prior_json, updated_at FROM user_priors WHERE user_id = ? AND domain = ?"
                    )
                    .bind(user_id)
                    .bind(domain)
                    .fetch_optional(&pool)
                    .await;

                    match result {
                        Ok(Some(row)) => {
                            let prior: String = row.get("prior_json");
                            let updated: String = row.get("updated_at");
                            serde_json::json!({
                                "status": "success",
                                "user_id": user_id,
                                "domain": domain,
                                "prior_json": prior,
                                "updated_at": updated
                            })
                        }
                        Ok(None) => serde_json::json!({
                            "status": "not_found",
                            "user_id": user_id,
                            "domain": domain
                        }),
                        Err(e) => serde_json::json!({ "error": format!("DB error: {}", e) }),
                    }
                }
            }

            "save_user_prior" => {
                let user_id = req.payload.get("user_id").and_then(|v| v.as_str()).unwrap_or("");
                let domain = req.payload.get("domain").and_then(|v| v.as_str()).unwrap_or("");
                let prior_json = req.payload.get("prior_json").and_then(|v| v.as_str()).unwrap_or("");

                if user_id.is_empty() || domain.is_empty() || prior_json.is_empty() {
                    serde_json::json!({ "error": "Missing user_id, domain, or prior_json" })
                } else {
                    let result = sqlx::query(
                        r#"
                        INSERT INTO user_priors (user_id, domain, prior_json, updated_at)
                        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                        ON CONFLICT(user_id, domain) DO UPDATE SET
                            prior_json = excluded.prior_json,
                            updated_at = CURRENT_TIMESTAMP
                        "#
                    )
                    .bind(user_id)
                    .bind(domain)
                    .bind(prior_json)
                    .execute(&pool)
                    .await;

                    match result {
                        Ok(_) => serde_json::json!({
                            "status": "success",
                            "action": "saved",
                            "user_id": user_id,
                            "domain": domain
                        }),
                        Err(e) => serde_json::json!({ "error": format!("DB error: {}", e) }),
                    }
                }
            }

            // ─── Virtual Office: Scoped Memory ────────────────────────
            "save_scoped_memory" => {
                let user_id = req.payload.get("user_id").and_then(|v| v.as_str()).unwrap_or("");
                let tenant_id = req.payload.get("tenant_id").and_then(|v| v.as_str()).unwrap_or("default");
                let app_id = req.payload.get("app_id").and_then(|v| v.as_str()).unwrap_or("os");
                let expert_id = req.payload.get("expert_id").and_then(|v| v.as_str()).unwrap_or("ava");
                let session_id = req.payload.get("session_id").and_then(|v| v.as_str()).unwrap_or("");
                let device_id = req.payload.get("device_id").and_then(|v| v.as_str()).unwrap_or("server");
                let scope = req.payload.get("scope").and_then(|v| v.as_str()).unwrap_or("personal");
                let source = req.payload.get("source").and_then(|v| v.as_str()).unwrap_or("chat");
                let content = req.payload.get("content").and_then(|v| v.as_str()).unwrap_or("");

                if user_id.is_empty() || content.is_empty() {
                    serde_json::json!({ "error": "Missing 'user_id' or 'content' in payload" })
                } else {
                    let result = sqlx::query(
                        "INSERT INTO scoped_memory (user_id, tenant_id, app_id, expert_id, session_id, device_id, scope, source, content) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
                    )
                    .bind(user_id)
                    .bind(tenant_id)
                    .bind(app_id)
                    .bind(expert_id)
                    .bind(session_id)
                    .bind(device_id)
                    .bind(scope)
                    .bind(source)
                    .bind(content)
                    .execute(&pool)
                    .await;

                    match result {
                        Ok(_) => serde_json::json!({
                            "status": "success",
                            "action": "scoped_memory_saved",
                            "scope": scope,
                            "user_id": user_id
                        }),
                        Err(e) => serde_json::json!({ "error": format!("DB error: {}", e) }),
                    }
                }
            }

            "get_scoped_memory" => {
                // At least one filter must be set — global reads are forbidden.
                let user_id = req.payload.get("user_id").and_then(|v| v.as_str());
                let tenant_id = req.payload.get("tenant_id").and_then(|v| v.as_str());
                let app_id = req.payload.get("app_id").and_then(|v| v.as_str());
                let expert_id = req.payload.get("expert_id").and_then(|v| v.as_str());
                let session_id = req.payload.get("session_id").and_then(|v| v.as_str());
                let scope = req.payload.get("scope").and_then(|v| v.as_str());
                let limit = req.payload.get("limit").and_then(|v| v.as_i64()).unwrap_or(50);

                // Reject global reads — at least one meaningful filter required
                if user_id.is_none()
                    && tenant_id.is_none()
                    && app_id.is_none()
                    && expert_id.is_none()
                    && session_id.is_none()
                    && scope.is_none()
                {
                    serde_json::json!({ "error": "At least one filter (user_id, tenant_id, app_id, expert_id, session_id, scope) is required. Global reads are forbidden." })
                } else {
                    // Build dynamic WHERE clause
                    let mut conditions = Vec::new();
                    let mut bind_values: Vec<String> = Vec::new();

                    if let Some(v) = user_id {
                        conditions.push("user_id = ?");
                        bind_values.push(v.to_string());
                    }
                    if let Some(v) = tenant_id {
                        conditions.push("tenant_id = ?");
                        bind_values.push(v.to_string());
                    }
                    if let Some(v) = app_id {
                        conditions.push("app_id = ?");
                        bind_values.push(v.to_string());
                    }
                    if let Some(v) = expert_id {
                        conditions.push("expert_id = ?");
                        bind_values.push(v.to_string());
                    }
                    if let Some(v) = session_id {
                        conditions.push("session_id = ?");
                        bind_values.push(v.to_string());
                    }
                    if let Some(v) = scope {
                        conditions.push("scope = ?");
                        bind_values.push(v.to_string());
                    }

                    let where_clause = conditions.join(" AND ");
                    let sql = format!(
                        "SELECT user_id, tenant_id, app_id, expert_id, session_id, device_id, scope, source, content, timestamp FROM scoped_memory WHERE {} ORDER BY timestamp DESC LIMIT {}",
                        where_clause, limit
                    );

                    let mut query = sqlx::query(&sql);
                    for val in &bind_values {
                        query = query.bind(val);
                    }

                    match query.fetch_all(&pool).await {
                        Ok(rows) => {
                            let results: Vec<serde_json::Value> = rows.iter().map(|row| {
                                serde_json::json!({
                                    "user_id": row.get::<String, _>("user_id"),
                                    "tenant_id": row.get::<String, _>("tenant_id"),
                                    "app_id": row.get::<String, _>("app_id"),
                                    "expert_id": row.get::<String, _>("expert_id"),
                                    "session_id": row.get::<String, _>("session_id"),
                                    "device_id": row.get::<String, _>("device_id"),
                                    "scope": row.get::<String, _>("scope"),
                                    "source": row.get::<String, _>("source"),
                                    "content": row.get::<String, _>("content"),
                                    "timestamp": row.get::<String, _>("timestamp"),
                                })
                            }).collect();
                            serde_json::json!({
                                "status": "success",
                                "count": results.len(),
                                "entries": results
                            })
                        }
                        Err(e) => serde_json::json!({ "error": format!("Query error: {}", e) }),
                    }
                }
            }

            // ─── Virtual Office: Audit Log ─────────────────────────────
            "audit_log" => {
                let actor = req.payload.get("actor").and_then(|v| v.as_str()).unwrap_or("");
                let expert_identity = req.payload.get("expert_identity").and_then(|v| v.as_str()).unwrap_or("");
                let capability_used = req.payload.get("capability_used").and_then(|v| v.as_str()).unwrap_or("");
                let sensitive_action = req.payload.get("sensitive_action").and_then(|v| v.as_str());
                let target_app = req.payload.get("target_app").and_then(|v| v.as_str());
                let target_page = req.payload.get("target_page").and_then(|v| v.as_str());
                let mutation_description = req.payload.get("mutation_description").and_then(|v| v.as_str()).unwrap_or("");
                let tenant_id = req.payload.get("tenant_id").and_then(|v| v.as_str());
                let session_id = req.payload.get("session_id").and_then(|v| v.as_str());

                if actor.is_empty() || expert_identity.is_empty() || mutation_description.is_empty() {
                    serde_json::json!({ "error": "Missing required fields: actor, expert_identity, mutation_description" })
                } else {
                    let result = sqlx::query(
                        "INSERT INTO audit_log (actor, expert_identity, capability_used, sensitive_action, target_app, target_page, mutation_description, tenant_id, session_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
                    )
                    .bind(actor)
                    .bind(expert_identity)
                    .bind(capability_used)
                    .bind(sensitive_action)
                    .bind(target_app)
                    .bind(target_page)
                    .bind(mutation_description)
                    .bind(tenant_id)
                    .bind(session_id)
                    .execute(&pool)
                    .await;

                    match result {
                        Ok(_) => serde_json::json!({
                            "status": "success",
                            "action": "audit_logged",
                            "actor": actor
                        }),
                        Err(e) => serde_json::json!({ "error": format!("DB error: {}", e) }),
                    }
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
