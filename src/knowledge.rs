/// Knowledge Store — persistent, tagged key-value memory for AI agents.
///
/// Inspired by ContextKeep's memory model but implemented natively in Rust
/// with SQLite storage and UDS IPC transport.
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use sqlx::Row;
use tracing::{info, error};

/// Represents a knowledge entry stored in the `knowledge_store` table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnowledgeEntry {
    pub key: String,
    pub content: String,
    #[serde(default)]
    pub tags: String,
}

/// A compact index entry returned by `list()`.
#[derive(Debug, Serialize)]
pub struct KnowledgeIndex {
    pub key: String,
    pub title: String,
    pub tags: String,
    pub char_count: usize,
    pub updated_at: String,
}

/// Creates the `knowledge_store` table if it doesn't exist.
pub async fn init_knowledge_table(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS knowledge_store (
            key TEXT PRIMARY KEY,
            content TEXT NOT NULL,
            tags TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        "#,
    )
    .execute(pool)
    .await?;

    info!("📚 Knowledge store table ready");
    Ok(())
}

/// Upserts a knowledge entry. If the key already exists, content and tags
/// are updated and `updated_at` is refreshed.
pub async fn store(
    pool: &sqlx::PgPool,
    key: &str,
    content: &str,
    tags: &str,
) -> serde_json::Value {
    let result = sqlx::query(
        r#"
        INSERT INTO knowledge_store (key, content, tags, updated_at)
        VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
        ON CONFLICT(key) DO UPDATE SET
            content = excluded.content,
            tags = excluded.tags,
            updated_at = CURRENT_TIMESTAMP
        "#,
    )
    .bind(key)
    .bind(content)
    .bind(tags)
    .execute(pool)
    .await;

    match result {
        Ok(_) => {
            info!("📚 Stored knowledge: {}", key);
            serde_json::json!({
                "status": "success",
                "key": key,
                "action": "stored"
            })
        }
        Err(e) => {
            error!("❌ Failed to store knowledge {}: {}", key, e);
            serde_json::json!({ "error": format!("DB error: {}", e) })
        }
    }
}

/// Retrieves a single knowledge entry by exact key.
pub async fn get(pool: &sqlx::PgPool, key: &str) -> serde_json::Value {
    let result = sqlx::query("SELECT key, content, tags, created_at, updated_at FROM knowledge_store WHERE key = $1",
    )
    .bind(key)
    .fetch_optional(pool)
    .await;

    match result {
        Ok(Some(row)) => {
            let content: String = row.get("content");
            let tags: String = row.get("tags");
            let created_at: String = row.get("created_at");
            let updated_at: String = row.get("updated_at");
            serde_json::json!({
                "status": "success",
                "key": key,
                "content": content,
                "tags": tags,
                "char_count": content.len(),
                "created_at": created_at,
                "updated_at": updated_at
            })
        }
        Ok(None) => {
            serde_json::json!({
                "status": "not_found",
                "error": format!("No knowledge entry with key '{}'", key)
            })
        }
        Err(e) => {
            serde_json::json!({ "error": format!("DB error: {}", e) })
        }
    }
}

/// Returns a compact index of all knowledge entries (key, title, tags, char_count, updated_at).
/// Title is derived from the first 80 characters of content.
pub async fn list(pool: &sqlx::PgPool) -> serde_json::Value {
    let result = sqlx::query("SELECT key, content, tags, updated_at FROM knowledge_store ORDER BY updated_at DESC",
    )
    .fetch_all(pool)
    .await;

    match result {
        Ok(rows) => {
            let entries: Vec<serde_json::Value> = rows
                .iter()
                .map(|row| {
                    let key: String = row.get("key");
                    let content: String = row.get("content");
                    let tags: String = row.get("tags");
                    let updated_at: String = row.get("updated_at");
                    // Title = first 80 chars of content, trimmed at word boundary
                    let title = if content.len() <= 80 {
                        content.clone()
                    } else {
                        let truncated = &content[..80];
                        match truncated.rfind(' ') {
                            Some(pos) => format!("{}…", &truncated[..pos]),
                            None => format!("{}…", truncated),
                        }
                    };
                    serde_json::json!({
                        "key": key,
                        "title": title,
                        "tags": tags,
                        "char_count": content.len(),
                        "updated_at": updated_at
                    })
                })
                .collect();

            serde_json::json!({
                "status": "success",
                "total": entries.len(),
                "memories": entries
            })
        }
        Err(e) => {
            serde_json::json!({ "error": format!("DB error: {}", e) })
        }
    }
}

/// Searches knowledge entries by keyword across key, content, and tags.
/// Uses SQLite LIKE for broad matching.
pub async fn search(pool: &sqlx::PgPool, query: &str) -> serde_json::Value {
    let tokens: Vec<String> = query
        .replace(&['\'', '"', ',', '.', '!', '?'][..], "")
        .split_whitespace()
        .filter(|w| w.len() > 3) // Ignore short stop words like "the", "and", "for"
        .map(|w| format!("%{}%", w))
        .collect();

    // If no meaningful tokens, use the full query as a fallback
    let search_tokens = if tokens.is_empty() {
        vec![format!("%{}%", query)]
    } else {
        tokens
    };

    let mut ks_query = String::from("SELECT key, content, tags, updated_at FROM knowledge_store WHERE ");
    let mut bio_query = String::from("SELECT slug, title, company, duration, tag, summary FROM paulo_bio_experience WHERE ");
    
    let mut ks_conditions = Vec::new();
    let mut bio_conditions = Vec::new();
    
    for (i, _) in search_tokens.iter().enumerate() {
        // Build SQL string placeholders dynamically 
        // e.g., (key LIKE ? OR content LIKE ? OR tags LIKE ?)
        let bind_idx = i + 1;
        ks_conditions.push(format!("(key LIKE $1{} OR content LIKE $2{} OR tags LIKE $3{})", bind_idx, bind_idx, bind_idx));
        bio_conditions.push(format!("(title LIKE $1{} OR company LIKE $2{} OR tag LIKE $3{} OR summary LIKE $4{})", bind_idx, bind_idx, bind_idx, bind_idx));
    }

    let ks_final_sql = format!("SELECT key, content, tags, updated_at FROM knowledge_store WHERE {} ORDER BY updated_at DESC LIMIT 25",
        ks_conditions.join(" OR ")
    );

    let bio_final_sql = format!("SELECT slug, title, company, duration, tag, summary FROM paulo_bio_experience WHERE {} LIMIT 25",
        bio_conditions.join(" OR ")
    );

    let mut ks_fetch = sqlx::query(&ks_final_sql);
    for token in &search_tokens {
        ks_fetch = ks_fetch.bind(token);
    }
    let ks_result = ks_fetch.fetch_all(pool).await;

    let mut bio_fetch = sqlx::query(&bio_final_sql);
    for token in &search_tokens {
        bio_fetch = bio_fetch.bind(token);
    }
    let bio_result = bio_fetch.fetch_all(pool).await;

    let mut entries: Vec<serde_json::Value> = Vec::new();

    if let Ok(rows) = ks_result {
        for row in rows {
            let key: String = row.get("key");
            let content: String = row.get("content");
            let tags: String = row.get("tags");
            let updated_at: String = row.get("updated_at");
            
            let snippet = if content.len() <= 200 {
                content.clone()
            } else {
                format!("{}…", &content[..200])
            };
            
            entries.push(serde_json::json!({
                "key": key,
                "snippet": snippet,
                "tags": tags,
                "char_count": content.len(),
                "updated_at": updated_at,
                "source": "knowledge_store"
            }));
        }
    }

    if let Ok(rows) = bio_result {
        for row in rows {
            let slug: String = row.get("slug");
            let title: String = row.get("title");
            let company: String = row.get("company");
            let duration: String = row.get("duration");
            let tag: String = row.get("tag");
            let summary: String = row.get("summary");
            
            let content = format!("Role: {} at {}\nDuration: {}\nTag: {}\nSummary: {}", title, company, duration, tag, summary);
            
            let snippet = if content.len() <= 200 {
                content.clone()
            } else {
                format!("{}…", &content[..200])
            };
            
            entries.push(serde_json::json!({
                "key": format!("bio_experience_{}", slug),
                "snippet": snippet,
                "tags": tag,
                "char_count": content.len(),
                "updated_at": "",
                "source": "paulo_bio_experience",
                "full_content": content 
            }));
        }
    }

    serde_json::json!({
        "status": "success",
        "query": query,
        "results": entries.len(),
        "memories": entries
    })
}

/// Deletes a knowledge entry by exact key.
pub async fn delete(pool: &sqlx::PgPool, key: &str) -> serde_json::Value {
    let result = sqlx::query("DELETE FROM knowledge_store WHERE key = $1")
        .bind(key)
        .execute(pool)
        .await;

    match result {
        Ok(res) => {
            if res.rows_affected() > 0 {
                info!("🗑️ Deleted knowledge: {}", key);
                serde_json::json!({
                    "status": "success",
                    "key": key,
                    "action": "deleted"
                })
            } else {
                serde_json::json!({
                    "status": "not_found",
                    "error": format!("No knowledge entry with key '{}'", key)
                })
            }
        }
        Err(e) => {
            serde_json::json!({ "error": format!("DB error: {}", e) })
        }
    }
}
