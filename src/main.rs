use std::os::unix::fs::PermissionsExt;
use std::collections::HashMap;
use std::collections::HashSet;
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
mod document_index;
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AdaptiveMemoryProfile {
    recent_limit: i64,
    candidate_limit: i64,
    max_message_chars: i64,
    max_total_chars: i64,
    recency_weight: f64,
    overlap_weight: f64,
    assistant_weight: f64,
}

#[derive(Debug, Clone)]
struct ScoredMemoryMessage {
    role: String,
    content: String,
    score: f64,
    recency_score: f64,
    overlap_score: f64,
}

fn default_memory_profile() -> AdaptiveMemoryProfile {
    AdaptiveMemoryProfile {
        recent_limit: 4,
        candidate_limit: 18,
        max_message_chars: 900,
        max_total_chars: 2600,
        recency_weight: 0.35,
        overlap_weight: 0.55,
        assistant_weight: 0.10,
    }
}

fn clamp_i64(value: i64, min: i64, max: i64) -> i64 {
    value.max(min).min(max)
}

fn clamp_f64(value: f64, min: f64, max: f64) -> f64 {
    value.max(min).min(max)
}

async fn ensure_sqlite_column(
    pool: &SqlitePool,
    table: &str,
    column: &str,
    column_definition: &str,
) -> anyhow::Result<()> {
    let pragma = format!("PRAGMA table_info({table})");
    let rows = sqlx::query(&pragma).fetch_all(pool).await?;
    let exists = rows.iter().any(|row| row.get::<String, _>("name") == column);
    if !exists {
        let sql = format!("ALTER TABLE {table} ADD COLUMN {column} {column_definition}");
        sqlx::query(&sql).execute(pool).await?;
    }
    Ok(())
}

fn trim_message_for_budget(content: &str, max_chars: usize) -> String {
    let char_count = content.chars().count();
    if char_count <= max_chars {
        return content.to_string();
    }

    let keep = max_chars.saturating_sub(48);
    let trimmed: String = content.chars().take(keep).collect();
    format!("{} [trimmed {} chars]", trimmed, char_count.saturating_sub(keep))
}

fn tokenize_memory(text: &str) -> HashSet<String> {
    text.to_lowercase()
        .split(|c: char| !c.is_alphanumeric())
        .filter(|token| token.len() >= 3)
        .take(96)
        .map(|token| token.to_string())
        .collect()
}

fn score_memory_message(
    role: &str,
    content: &str,
    recency_rank: usize,
    total_candidates: usize,
    query_tokens: &HashSet<String>,
    profile: &AdaptiveMemoryProfile,
) -> ScoredMemoryMessage {
    let content_tokens = tokenize_memory(content);
    let overlap_hits = if query_tokens.is_empty() {
        0
    } else {
        query_tokens.intersection(&content_tokens).count()
    };
    let overlap_score = if query_tokens.is_empty() {
        0.0
    } else {
        overlap_hits as f64 / query_tokens.len() as f64
    };
    let recency_score = if total_candidates <= 1 {
        1.0
    } else {
        1.0 - (recency_rank as f64 / (total_candidates - 1) as f64)
    };
    let assistant_bonus = if role == "assistant" { 1.0 } else { 0.0 };
    let score = (recency_score * profile.recency_weight)
        + (overlap_score * profile.overlap_weight)
        + (assistant_bonus * profile.assistant_weight);

    ScoredMemoryMessage {
        role: role.to_string(),
        content: content.to_string(),
        score,
        recency_score,
        overlap_score,
    }
}

async fn load_memory_profile(pool: &SqlitePool, chat_id: &str) -> anyhow::Result<AdaptiveMemoryProfile> {
    let default_profile = default_memory_profile();
    let row = sqlx::query(
        r#"
        SELECT recent_limit, candidate_limit, max_message_chars, max_total_chars,
               recency_weight, overlap_weight, assistant_weight
        FROM adaptive_memory_profiles
        WHERE chat_id = ?
        "#,
    )
    .bind(chat_id)
    .fetch_optional(pool)
    .await?;

    if let Some(row) = row {
        Ok(AdaptiveMemoryProfile {
            recent_limit: clamp_i64(row.get::<i64, _>("recent_limit"), 2, 8),
            candidate_limit: clamp_i64(row.get::<i64, _>("candidate_limit"), 8, 32),
            max_message_chars: clamp_i64(row.get::<i64, _>("max_message_chars"), 300, 1600),
            max_total_chars: clamp_i64(row.get::<i64, _>("max_total_chars"), 1200, 5000),
            recency_weight: clamp_f64(row.get::<f64, _>("recency_weight"), 0.1, 0.8),
            overlap_weight: clamp_f64(row.get::<f64, _>("overlap_weight"), 0.1, 0.8),
            assistant_weight: clamp_f64(row.get::<f64, _>("assistant_weight"), 0.0, 0.3),
        })
    } else {
        sqlx::query(
            r#"
            INSERT INTO adaptive_memory_profiles (
                chat_id, recent_limit, candidate_limit, max_message_chars, max_total_chars,
                recency_weight, overlap_weight, assistant_weight
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(chat_id)
        .bind(default_profile.recent_limit)
        .bind(default_profile.candidate_limit)
        .bind(default_profile.max_message_chars)
        .bind(default_profile.max_total_chars)
        .bind(default_profile.recency_weight)
        .bind(default_profile.overlap_weight)
        .bind(default_profile.assistant_weight)
        .execute(pool)
        .await?;
        Ok(default_profile)
    }
}

async fn store_memory_profile(pool: &SqlitePool, chat_id: &str, profile: &AdaptiveMemoryProfile) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        INSERT INTO adaptive_memory_profiles (
            chat_id, recent_limit, candidate_limit, max_message_chars, max_total_chars,
            recency_weight, overlap_weight, assistant_weight, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(chat_id) DO UPDATE SET
            recent_limit = excluded.recent_limit,
            candidate_limit = excluded.candidate_limit,
            max_message_chars = excluded.max_message_chars,
            max_total_chars = excluded.max_total_chars,
            recency_weight = excluded.recency_weight,
            overlap_weight = excluded.overlap_weight,
            assistant_weight = excluded.assistant_weight,
            updated_at = CURRENT_TIMESTAMP
        "#,
    )
    .bind(chat_id)
    .bind(profile.recent_limit)
    .bind(profile.candidate_limit)
    .bind(profile.max_message_chars)
    .bind(profile.max_total_chars)
    .bind(profile.recency_weight)
    .bind(profile.overlap_weight)
    .bind(profile.assistant_weight)
    .execute(pool)
    .await?;

    Ok(())
}

async fn record_feedback(
    pool: &SqlitePool,
    chat_id: &str,
    signal: &str,
    observed_chars: Option<i64>,
    query: Option<&str>,
) -> anyhow::Result<AdaptiveMemoryProfile> {
    sqlx::query(
        r#"
        INSERT INTO adaptive_memory_feedback (chat_id, signal, observed_chars, query)
        VALUES (?, ?, ?, ?)
        "#,
    )
    .bind(chat_id)
    .bind(signal)
    .bind(observed_chars)
    .bind(query)
    .execute(pool)
    .await?;

    let mut profile = load_memory_profile(pool, chat_id).await?;
    match signal {
        "overflow" | "cloud_fallback" => {
            profile.recent_limit = clamp_i64(profile.recent_limit - 1, 2, 8);
            profile.candidate_limit = clamp_i64(profile.candidate_limit - 2, 8, 32);
            profile.max_message_chars = clamp_i64(profile.max_message_chars - 120, 300, 1600);
            profile.max_total_chars = clamp_i64(profile.max_total_chars - 240, 1200, 5000);
            profile.recency_weight = clamp_f64(profile.recency_weight + 0.05, 0.1, 0.8);
            profile.overlap_weight = clamp_f64(profile.overlap_weight + 0.03, 0.1, 0.8);
        }
        "local_success" => {
            profile.recent_limit = clamp_i64(profile.recent_limit + 1, 2, 8);
            profile.candidate_limit = clamp_i64(profile.candidate_limit + 1, 8, 32);
            profile.max_message_chars = clamp_i64(profile.max_message_chars + 40, 300, 1600);
            profile.max_total_chars = clamp_i64(profile.max_total_chars + 120, 1200, 5000);
            profile.recency_weight = clamp_f64(profile.recency_weight - 0.02, 0.1, 0.8);
            profile.overlap_weight = clamp_f64(profile.overlap_weight + 0.01, 0.1, 0.8);
        }
        _ => {}
    }

    profile.assistant_weight = clamp_f64(1.0 - profile.recency_weight - profile.overlap_weight, 0.0, 0.3);
    store_memory_profile(pool, chat_id, &profile).await?;
    Ok(profile)
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

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS adaptive_memory_profiles (
            chat_id TEXT PRIMARY KEY,
            recent_limit INTEGER NOT NULL DEFAULT 4,
            candidate_limit INTEGER NOT NULL DEFAULT 18,
            max_message_chars INTEGER NOT NULL DEFAULT 900,
            max_total_chars INTEGER NOT NULL DEFAULT 2600,
            recency_weight REAL NOT NULL DEFAULT 0.35,
            overlap_weight REAL NOT NULL DEFAULT 0.55,
            assistant_weight REAL NOT NULL DEFAULT 0.10,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        "#,
    )
    .execute(&pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS adaptive_memory_feedback (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id TEXT NOT NULL,
            signal TEXT NOT NULL,
            observed_chars INTEGER,
            query TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        "#,
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

    ensure_sqlite_column(&pool, "scoped_memory", "memory_type", "TEXT NOT NULL DEFAULT 'event'").await?;
    ensure_sqlite_column(&pool, "scoped_memory", "content_json", "TEXT").await?;
    ensure_sqlite_column(&pool, "scoped_memory", "confidence", "REAL").await?;
    ensure_sqlite_column(&pool, "scoped_memory", "provenance_refs", "TEXT").await?;
    ensure_sqlite_column(&pool, "scoped_memory", "derivation_method", "TEXT").await?;
    ensure_sqlite_column(&pool, "scoped_memory", "status", "TEXT NOT NULL DEFAULT 'active'").await?;
    ensure_sqlite_column(&pool, "scoped_memory", "expires_at", "DATETIME").await?;

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

    // ─── Paulo Bio Data Tables ────────────────────────────────
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS paulo_bio_experience (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            slug TEXT UNIQUE NOT NULL,
            title TEXT NOT NULL,
            company TEXT NOT NULL,
            duration TEXT NOT NULL,
            tag TEXT NOT NULL,
            summary TEXT NOT NULL,
            sort_order INTEGER DEFAULT 0
        )
        "#
    )
    .execute(&pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS paulo_bio_education (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            slug TEXT UNIQUE NOT NULL,
            degree TEXT NOT NULL,
            institution TEXT NOT NULL,
            duration TEXT NOT NULL,
            tag TEXT NOT NULL,
            summary TEXT,
            sort_order INTEGER DEFAULT 0
        )
        "#
    )
    .execute(&pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS paulo_bio_skills (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category TEXT NOT NULL,
            name TEXT NOT NULL,
            level TEXT DEFAULT 'expert'
        )
        "#
    )
    .execute(&pool)
    .await?;

    document_index::init_tables(&pool).await?;

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
                let limit_hint = req.payload.get("limit").and_then(|v| v.as_i64()).unwrap_or(6);
                let query_text = req.payload.get("query").and_then(|v| v.as_str()).unwrap_or("");

                match load_memory_profile(&pool, chat_id).await {
                    Ok(profile) => {
                        let effective_limit = clamp_i64(limit_hint.min(profile.recent_limit), 2, 8);
                        let candidate_limit = clamp_i64(profile.candidate_limit.max(effective_limit * 2), 8, 32);
                        let rows_result = sqlx::query(
                            "SELECT role, content FROM memento_memory WHERE chat_id = ? ORDER BY timestamp DESC LIMIT ?"
                        )
                        .bind(chat_id)
                        .bind(candidate_limit)
                        .fetch_all(&pool)
                        .await;

                        match rows_result {
                            Ok(rows) => {
                                let query_tokens = tokenize_memory(query_text);
                                let total_candidates = rows.len();
                                let mut scored = Vec::new();

                                for (recency_rank, row) in rows.iter().enumerate() {
                                    let role: String = row.get("role");
                                    let content: String = row.get("content");
                                    scored.push(score_memory_message(
                                        &role,
                                        &content,
                                        recency_rank,
                                        total_candidates,
                                        &query_tokens,
                                        &profile,
                                    ));
                                }

                                scored.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));

                                let mut picked = Vec::new();
                                if query_tokens.is_empty() {
                                    picked = scored
                                        .into_iter()
                                        .take(effective_limit as usize)
                                        .collect::<Vec<_>>();
                                } else {
                                    for message in scored.iter().filter(|message| message.overlap_score > 0.0) {
                                        if picked.len() >= effective_limit as usize {
                                            break;
                                        }
                                        picked.push(message.clone());
                                    }

                                    for message in scored.iter().filter(|message| message.recency_score >= 0.75) {
                                        if picked.len() >= effective_limit as usize {
                                            break;
                                        }
                                        if picked.iter().any(|existing| existing.role == message.role && existing.content == message.content) {
                                            continue;
                                        }
                                        picked.push(message.clone());
                                    }
                                }

                                picked.sort_by(|a, b| a.recency_score.partial_cmp(&b.recency_score).unwrap_or(std::cmp::Ordering::Equal));

                                let mut total_chars = 0usize;
                                let mut messages = Vec::new();
                                let max_total_chars = profile.max_total_chars as usize;
                                let max_message_chars = profile.max_message_chars as usize;

                                for message in picked {
                                    if total_chars >= max_total_chars {
                                        break;
                                    }

                                    let mut content = trim_message_for_budget(&message.content, max_message_chars);
                                    let remaining = max_total_chars.saturating_sub(total_chars);
                                    if content.chars().count() > remaining {
                                        content = trim_message_for_budget(&content, remaining.max(80));
                                    }

                                    if content.trim().is_empty() {
                                        continue;
                                    }

                                    total_chars += content.chars().count();
                                    messages.push(serde_json::json!({
                                        "role": message.role,
                                        "content": content,
                                        "score": message.score,
                                        "overlap_score": message.overlap_score,
                                        "recency_score": message.recency_score
                                    }));
                                }

                                serde_json::json!({
                                    "status": "success",
                                    "messages": messages,
                                    "profile": {
                                        "recent_limit": effective_limit,
                                        "candidate_limit": candidate_limit,
                                        "max_message_chars": profile.max_message_chars,
                                        "max_total_chars": profile.max_total_chars,
                                        "recency_weight": profile.recency_weight,
                                        "overlap_weight": profile.overlap_weight,
                                        "assistant_weight": profile.assistant_weight
                                    }
                                })
                            }
                            Err(e) => serde_json::json!({ "error": format!("DB error: {}", e) }),
                        }
                    }
                    Err(e) => serde_json::json!({ "error": format!("Profile error: {}", e) }),
                }
            }
            "record_context_feedback" => {
                let chat_id = req.payload.get("chat_id").and_then(|v| v.as_str()).unwrap_or("");
                let signal = req.payload.get("signal").and_then(|v| v.as_str()).unwrap_or("");
                let observed_chars = req.payload.get("observed_chars").and_then(|v| v.as_i64());
                let query = req.payload.get("query").and_then(|v| v.as_str());

                if chat_id.is_empty() || signal.is_empty() {
                    serde_json::json!({ "error": "Missing 'chat_id' or 'signal' in payload" })
                } else {
                    match record_feedback(&pool, chat_id, signal, observed_chars, query).await {
                        Ok(profile) => serde_json::json!({
                            "status": "success",
                            "profile": {
                                "recent_limit": profile.recent_limit,
                                "candidate_limit": profile.candidate_limit,
                                "max_message_chars": profile.max_message_chars,
                                "max_total_chars": profile.max_total_chars,
                                "recency_weight": profile.recency_weight,
                                "overlap_weight": profile.overlap_weight,
                                "assistant_weight": profile.assistant_weight
                            }
                        }),
                        Err(e) => serde_json::json!({ "error": format!("Feedback error: {}", e) }),
                    }
                }
            }
            "get_context_profile" => {
                let chat_id = req.payload.get("chat_id").and_then(|v| v.as_str()).unwrap_or("");
                if chat_id.is_empty() {
                    serde_json::json!({ "error": "Missing 'chat_id' in payload" })
                } else {
                    match load_memory_profile(&pool, chat_id).await {
                        Ok(profile) => serde_json::json!({
                            "status": "success",
                            "profile": {
                                "recent_limit": profile.recent_limit,
                                "candidate_limit": profile.candidate_limit,
                                "max_message_chars": profile.max_message_chars,
                                "max_total_chars": profile.max_total_chars,
                                "recency_weight": profile.recency_weight,
                                "overlap_weight": profile.overlap_weight,
                                "assistant_weight": profile.assistant_weight
                            }
                        }),
                        Err(e) => serde_json::json!({ "error": format!("Profile error: {}", e) }),
                    }
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
                                        // Try JSON first, then string, then numerics, then bool
                                        if let Ok(v) = row.try_get::<serde_json::Value, _>(name) {
                                            obj.insert(name.to_string(), v);
                                        } else if let Ok(v) = row.try_get::<String, _>(name) {
                                            obj.insert(name.to_string(), serde_json::json!(v));
                                        } else if let Ok(v) = row.try_get::<i64, _>(name) {
                                            obj.insert(name.to_string(), serde_json::json!(v));
                                        } else if let Ok(v) = row.try_get::<i32, _>(name) {
                                            obj.insert(name.to_string(), serde_json::json!(v));
                                        } else if let Ok(v) = row.try_get::<f64, _>(name) {
                                            obj.insert(name.to_string(), serde_json::json!(v));
                                        } else if let Ok(v) = row.try_get::<f32, _>(name) {
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

            // ─── Schema Auto-Discovery ──────────────────────────────
            "describe_app" => {
                let app_slug = req.payload.get("app")
                    .and_then(|v| v.as_str()).unwrap_or("");
                
                if let Some(app_conn) = apps.get(app_slug) {
                    // Auto-discover ALL public tables + their columns
                    let schema_query = r#"
                        SELECT c.table_name, c.column_name, c.data_type, c.is_nullable
                        FROM information_schema.columns c
                        JOIN information_schema.tables t ON c.table_name = t.table_name AND c.table_schema = t.table_schema
                        WHERE c.table_schema = 'public' AND t.table_type = 'BASE TABLE'
                        ORDER BY c.table_name, c.ordinal_position
                    "#;
                    
                    match sqlx::query(schema_query)
                        .fetch_all(&app_conn.pool)
                        .await
                    {
                        Ok(rows) => {
                            let mut tables: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();
                            for row in &rows {
                                let table: String = row.get("table_name");
                                let col: String = row.get("column_name");
                                let dtype: String = row.get("data_type");
                                let entry = tables.entry(table).or_insert_with(|| serde_json::json!([]));
                                if let Some(arr) = entry.as_array_mut() {
                                    arr.push(serde_json::json!({"column": col, "type": dtype}));
                                }
                            }
                            serde_json::json!({
                                "status": "success",
                                "app": app_slug,
                                "table_count": tables.len(),
                                "schema": tables
                            })
                        }
                        Err(e) => serde_json::json!({ "error": format!("Schema query error: {}", e) }),
                    }
                } else {
                    let available: Vec<&String> = apps.keys().collect();
                    serde_json::json!({
                        "error": format!("App '{}' not found", app_slug),
                        "available_apps": available
                    })
                }
            }

            "describe_all_apps" => {
                // Superuser (Ava): return schemas for ALL apps
                let mut all_schemas = serde_json::Map::new();
                for (slug, app_conn) in apps.iter() {
                    let schema_query = r#"
                        SELECT c.table_name, c.column_name, c.data_type
                        FROM information_schema.columns c
                        JOIN information_schema.tables t ON c.table_name = t.table_name AND c.table_schema = t.table_schema
                        WHERE c.table_schema = 'public' AND t.table_type = 'BASE TABLE'
                        ORDER BY c.table_name, c.ordinal_position
                    "#;
                    match sqlx::query(schema_query).fetch_all(&app_conn.pool).await {
                        Ok(rows) => {
                            let mut tables: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();
                            for row in &rows {
                                let table: String = row.get("table_name");
                                let col: String = row.get("column_name");
                                let dtype: String = row.get("data_type");
                                let entry = tables.entry(table).or_insert_with(|| serde_json::json!([]));
                                if let Some(arr) = entry.as_array_mut() {
                                    arr.push(serde_json::json!({"column": col, "type": dtype}));
                                }
                            }
                            all_schemas.insert(slug.clone(), serde_json::json!(tables));
                        }
                        Err(e) => {
                            all_schemas.insert(slug.clone(), serde_json::json!({"error": format!("{}", e)}));
                        }
                    }
                }
                serde_json::json!({
                    "status": "success",
                    "apps": all_schemas
                })
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
            "save_scoped_memory" | "save_memory_record" => {
                let user_id = req.payload.get("user_id").and_then(|v| v.as_str()).unwrap_or("");
                let tenant_id = req.payload.get("tenant_id").and_then(|v| v.as_str()).unwrap_or("default");
                let app_id = req.payload.get("app_id").and_then(|v| v.as_str()).unwrap_or("os");
                let expert_id = req.payload.get("expert_id").and_then(|v| v.as_str()).unwrap_or("ava");
                let session_id = req.payload.get("session_id").and_then(|v| v.as_str()).unwrap_or("");
                let device_id = req.payload.get("device_id").and_then(|v| v.as_str()).unwrap_or("server");
                let scope = req.payload.get("scope").and_then(|v| v.as_str()).unwrap_or("personal");
                let source = req.payload.get("source").and_then(|v| v.as_str()).unwrap_or("chat");
                let memory_type = req.payload.get("memory_type").and_then(|v| v.as_str()).unwrap_or("event");
                let content = req.payload.get("content").and_then(|v| v.as_str()).unwrap_or("");
                let content_json = req.payload.get("content_json").filter(|v| !v.is_null()).cloned();
                let confidence = req.payload.get("confidence").and_then(|v| v.as_f64());
                let provenance_refs = req.payload.get("provenance_refs").filter(|v| !v.is_null()).cloned();
                let derivation_method = req.payload.get("derivation_method").and_then(|v| v.as_str());
                let status = req.payload.get("status").and_then(|v| v.as_str()).unwrap_or("active");
                let expires_at = req.payload.get("expires_at").and_then(|v| v.as_str());

                if user_id.is_empty() || content.is_empty() {
                    serde_json::json!({ "error": "Missing 'user_id' or 'content' in payload" })
                } else {
                    let result = sqlx::query(
                        "INSERT INTO scoped_memory (user_id, tenant_id, app_id, expert_id, session_id, device_id, scope, source, memory_type, content, content_json, confidence, provenance_refs, derivation_method, status, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                    )
                    .bind(user_id)
                    .bind(tenant_id)
                    .bind(app_id)
                    .bind(expert_id)
                    .bind(session_id)
                    .bind(device_id)
                    .bind(scope)
                    .bind(source)
                    .bind(memory_type)
                    .bind(content)
                    .bind(content_json.as_ref().map(|value| value.to_string()))
                    .bind(confidence)
                    .bind(provenance_refs.as_ref().map(|value| value.to_string()))
                    .bind(derivation_method)
                    .bind(status)
                    .bind(expires_at)
                    .execute(&pool)
                    .await;

                    match result {
                        Ok(_) => serde_json::json!({
                            "status": "success",
                            "action": "memory_record_saved",
                            "scope": scope,
                            "user_id": user_id,
                            "memory_type": memory_type
                        }),
                        Err(e) => serde_json::json!({ "error": format!("DB error: {}", e) }),
                    }
                }
            }

            "get_scoped_memory" | "query_memory_records" => {
                // At least one filter must be set — global reads are forbidden.
                let user_id = req.payload.get("user_id").and_then(|v| v.as_str());
                let tenant_id = req.payload.get("tenant_id").and_then(|v| v.as_str());
                let app_id = req.payload.get("app_id").and_then(|v| v.as_str());
                let expert_id = req.payload.get("expert_id").and_then(|v| v.as_str());
                let session_id = req.payload.get("session_id").and_then(|v| v.as_str());
                let scope = req.payload.get("scope").and_then(|v| v.as_str());
                let memory_type = req.payload.get("memory_type").and_then(|v| v.as_str());
                let status = req.payload.get("status").and_then(|v| v.as_str());
                let limit = req.payload.get("limit").and_then(|v| v.as_i64()).unwrap_or(50);

                // Reject global reads — at least one meaningful filter required
                if user_id.is_none()
                    && tenant_id.is_none()
                    && app_id.is_none()
                    && expert_id.is_none()
                    && session_id.is_none()
                    && scope.is_none()
                    && memory_type.is_none()
                {
                    serde_json::json!({ "error": "At least one filter (user_id, tenant_id, app_id, expert_id, session_id, scope, memory_type) is required. Global reads are forbidden." })
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
                    if let Some(v) = memory_type {
                        conditions.push("memory_type = ?");
                        bind_values.push(v.to_string());
                    }
                    if let Some(v) = status {
                        conditions.push("status = ?");
                        bind_values.push(v.to_string());
                    }

                    let where_clause = conditions.join(" AND ");
                    let sql = format!(
                        "SELECT user_id, tenant_id, app_id, expert_id, session_id, device_id, scope, source, memory_type, content, content_json, confidence, provenance_refs, derivation_method, status, expires_at, timestamp FROM scoped_memory WHERE {} ORDER BY timestamp DESC LIMIT {}",
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
                                    "memory_type": row.get::<String, _>("memory_type"),
                                    "content": row.get::<String, _>("content"),
                                    "content_json": row.get::<Option<String>, _>("content_json")
                                        .and_then(|value| serde_json::from_str::<serde_json::Value>(&value).ok()),
                                    "confidence": row.get::<Option<f64>, _>("confidence"),
                                    "provenance_refs": row.get::<Option<String>, _>("provenance_refs")
                                        .and_then(|value| serde_json::from_str::<serde_json::Value>(&value).ok()),
                                    "derivation_method": row.get::<Option<String>, _>("derivation_method"),
                                    "status": row.get::<String, _>("status"),
                                    "expires_at": row.get::<Option<String>, _>("expires_at"),
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
            "upsert_document_index" => {
                match serde_json::from_value::<document_index::DocumentIndexUpsert>(req.payload) {
                    Ok(payload) => match document_index::upsert(&pool, payload).await {
                        Ok(value) => value,
                        Err(e) => serde_json::json!({ "error": format!("document index upsert error: {}", e) }),
                    },
                    Err(e) => serde_json::json!({ "error": format!("Invalid payload for upsert_document_index: {}", e) }),
                }
            }
            "get_document_index" => {
                let document_id = req.payload.get("document_id").and_then(|v| v.as_str()).unwrap_or("");
                let app_id = req.payload.get("app_id").and_then(|v| v.as_str());
                if document_id.is_empty() {
                    serde_json::json!({ "error": "Missing 'document_id' in payload" })
                } else {
                    match document_index::get(&pool, document_id, app_id).await {
                        Ok(value) => value,
                        Err(e) => serde_json::json!({ "error": format!("document index get error: {}", e) }),
                    }
                }
            }
            "list_document_indexes" => {
                let app_id = req.payload.get("app_id").and_then(|v| v.as_str());
                let tenant_id = req.payload.get("tenant_id").and_then(|v| v.as_str());
                let index_type = req.payload.get("index_type").and_then(|v| v.as_str());
                let limit = req.payload.get("limit").and_then(|v| v.as_i64()).unwrap_or(20);
                match document_index::list(&pool, app_id, tenant_id, index_type, limit).await {
                    Ok(value) => value,
                    Err(e) => serde_json::json!({ "error": format!("document index list error: {}", e) }),
                }
            }
            "query_document_index" => {
                let query_text = req.payload.get("query").and_then(|v| v.as_str()).unwrap_or("");
                let app_id = req.payload.get("app_id").and_then(|v| v.as_str());
                let tenant_id = req.payload.get("tenant_id").and_then(|v| v.as_str());
                let document_id = req.payload.get("document_id").and_then(|v| v.as_str());
                let limit = req.payload.get("limit").and_then(|v| v.as_i64()).unwrap_or(8);
                match document_index::query(&pool, query_text, app_id, tenant_id, document_id, limit).await {
                    Ok(value) => value,
                    Err(e) => serde_json::json!({ "error": format!("document index query error: {}", e) }),
                }
            }

            // ─── Paulo Bio Data Actions ───────────────────────────────
            "query_bio" => {
                let section = req.payload.get("section")
                    .and_then(|v| v.as_str()).unwrap_or("experience");

                match section {
                    "experience" => {
                        let rows = sqlx::query(
                            "SELECT slug, title, company, duration, tag, summary, sort_order FROM paulo_bio_experience ORDER BY sort_order"
                        )
                        .fetch_all(&pool)
                        .await;

                        match rows {
                            Ok(rows) => {
                                let items: Vec<serde_json::Value> = rows.iter().map(|row| {
                                    serde_json::json!({
                                        "id": row.get::<String, _>("slug"),
                                        "title": row.get::<String, _>("title"),
                                        "company": row.get::<String, _>("company"),
                                        "duration": row.get::<String, _>("duration"),
                                        "tag": row.get::<String, _>("tag"),
                                        "summary": row.get::<String, _>("summary"),
                                    })
                                }).collect();
                                serde_json::json!({ "status": "success", "section": section, "count": items.len(), "items": items })
                            }
                            Err(e) => serde_json::json!({ "error": format!("DB error: {}", e) }),
                        }
                    }
                    "education" => {
                        let rows = sqlx::query(
                            "SELECT slug, degree, institution, duration, tag, summary, sort_order FROM paulo_bio_education ORDER BY sort_order"
                        )
                        .fetch_all(&pool)
                        .await;

                        match rows {
                            Ok(rows) => {
                                let items: Vec<serde_json::Value> = rows.iter().map(|row| {
                                    serde_json::json!({
                                        "id": row.get::<String, _>("slug"),
                                        "title": row.get::<String, _>("degree"),
                                        "company": row.get::<String, _>("institution"),
                                        "duration": row.get::<String, _>("duration"),
                                        "tag": row.get::<String, _>("tag"),
                                        "summary": row.get::<Option<String>, _>("summary").unwrap_or_default(),
                                    })
                                }).collect();
                                serde_json::json!({ "status": "success", "section": section, "count": items.len(), "items": items })
                            }
                            Err(e) => serde_json::json!({ "error": format!("DB error: {}", e) }),
                        }
                    }
                    "skills" => {
                        let rows = sqlx::query(
                            "SELECT category, name, level FROM paulo_bio_skills ORDER BY category, name"
                        )
                        .fetch_all(&pool)
                        .await;

                        match rows {
                            Ok(rows) => {
                                let items: Vec<serde_json::Value> = rows.iter().map(|row| {
                                    serde_json::json!({
                                        "category": row.get::<String, _>("category"),
                                        "name": row.get::<String, _>("name"),
                                        "level": row.get::<String, _>("level"),
                                    })
                                }).collect();
                                serde_json::json!({ "status": "success", "section": section, "count": items.len(), "items": items })
                            }
                            Err(e) => serde_json::json!({ "error": format!("DB error: {}", e) }),
                        }
                    }
                    _ => serde_json::json!({ "error": format!("Unknown section: {}", section) }),
                }
            }

            "seed_bio" => {
                let section = req.payload.get("section")
                    .and_then(|v| v.as_str()).unwrap_or("");
                let items = req.payload.get("items")
                    .and_then(|v| v.as_array());

                if section.is_empty() || items.is_none() {
                    serde_json::json!({ "error": "Missing 'section' or 'items' in payload" })
                } else {
                    let items = items.unwrap();
                    let mut inserted = 0usize;
                    let mut errors = Vec::new();

                    for item in items {
                        let result = match section {
                            "experience" => {
                                let slug = item.get("slug").and_then(|v| v.as_str()).unwrap_or("");
                                let title = item.get("title").and_then(|v| v.as_str()).unwrap_or("");
                                let company = item.get("company").and_then(|v| v.as_str()).unwrap_or("");
                                let duration = item.get("duration").and_then(|v| v.as_str()).unwrap_or("");
                                let tag = item.get("tag").and_then(|v| v.as_str()).unwrap_or("");
                                let summary = item.get("summary").and_then(|v| v.as_str()).unwrap_or("");
                                let sort_order = item.get("sort_order").and_then(|v| v.as_i64()).unwrap_or(0);

                                sqlx::query(
                                    "INSERT OR REPLACE INTO paulo_bio_experience (slug, title, company, duration, tag, summary, sort_order) VALUES (?, ?, ?, ?, ?, ?, ?)"
                                )
                                .bind(slug).bind(title).bind(company).bind(duration).bind(tag).bind(summary).bind(sort_order)
                                .execute(&pool)
                                .await
                            }
                            "education" => {
                                let slug = item.get("slug").and_then(|v| v.as_str()).unwrap_or("");
                                let degree = item.get("degree").and_then(|v| v.as_str()).unwrap_or("");
                                let institution = item.get("institution").and_then(|v| v.as_str()).unwrap_or("");
                                let duration = item.get("duration").and_then(|v| v.as_str()).unwrap_or("");
                                let tag = item.get("tag").and_then(|v| v.as_str()).unwrap_or("");
                                let summary = item.get("summary").and_then(|v| v.as_str()).unwrap_or("");
                                let sort_order = item.get("sort_order").and_then(|v| v.as_i64()).unwrap_or(0);

                                sqlx::query(
                                    "INSERT OR REPLACE INTO paulo_bio_education (slug, degree, institution, duration, tag, summary, sort_order) VALUES (?, ?, ?, ?, ?, ?, ?)"
                                )
                                .bind(slug).bind(degree).bind(institution).bind(duration).bind(tag).bind(summary).bind(sort_order)
                                .execute(&pool)
                                .await
                            }
                            "skills" => {
                                let category = item.get("category").and_then(|v| v.as_str()).unwrap_or("");
                                let name = item.get("name").and_then(|v| v.as_str()).unwrap_or("");
                                let level = item.get("level").and_then(|v| v.as_str()).unwrap_or("expert");

                                sqlx::query(
                                    "INSERT INTO paulo_bio_skills (category, name, level) VALUES (?, ?, ?)"
                                )
                                .bind(category).bind(name).bind(level)
                                .execute(&pool)
                                .await
                            }
                            _ => {
                                errors.push("Unknown section".to_string());
                                continue;
                            }
                        };

                        match result {
                            Ok(_) => inserted += 1,
                            Err(e) => errors.push(format!("{}", e)),
                        }
                    }

                    serde_json::json!({
                        "status": "success",
                        "inserted": inserted,
                        "errors": errors
                    })
                }
            }

            "delete_bio" => {
                let section = req.payload.get("section")
                    .and_then(|v| v.as_str()).unwrap_or("");
                let slug = req.payload.get("slug")
                    .and_then(|v| v.as_str()).unwrap_or("");

                if section.is_empty() || slug.is_empty() {
                    serde_json::json!({ "error": "Missing 'section' or 'slug' in payload" })
                } else {
                    let result = match section {
                        "experience" => {
                            sqlx::query("DELETE FROM paulo_bio_experience WHERE slug = ?")
                                .bind(slug).execute(&pool).await
                        }
                        "education" => {
                            sqlx::query("DELETE FROM paulo_bio_education WHERE slug = ?")
                                .bind(slug).execute(&pool).await
                        }
                        "skills" => {
                            // For skills, slug is the id
                            sqlx::query("DELETE FROM paulo_bio_skills WHERE id = ?")
                                .bind(slug.parse::<i64>().unwrap_or(0)).execute(&pool).await
                        }
                        _ => {
                            return Ok(());
                        }
                    };

                    match result {
                        Ok(r) => serde_json::json!({
                            "status": "success",
                            "deleted": r.rows_affected()
                        }),
                        Err(e) => serde_json::json!({ "error": format!("DB error: {}", e) }),
                    }
                }
            }

            "vector_search" => {
                let query = req.payload.get("query").and_then(|v| v.as_str()).unwrap_or("");

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

        if let Err(e) = stream.write_all(response.to_string().as_bytes()).await {
            error!("Failed to send IPC response: {}", e);
        }
    }
    
    Ok(())
}
