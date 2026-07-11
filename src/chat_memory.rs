use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::Row;
use std::collections::HashSet;

#[derive(Serialize, Deserialize, Debug)]
pub struct MemoryEntry {
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

fn trim_message_for_budget(content: &str, max_chars: usize) -> String {
    let char_count = content.chars().count();
    if char_count <= max_chars {
        return content.to_string();
    }

    let keep = max_chars.saturating_sub(48);
    let trimmed: String = content.chars().take(keep).collect();
    format!(
        "{} [trimmed {} chars]",
        trimmed,
        char_count.saturating_sub(keep)
    )
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

async fn load_memory_profile(
    pool: &sqlx::PgPool,
    chat_id: &str,
) -> anyhow::Result<AdaptiveMemoryProfile> {
    let default_profile = default_memory_profile();
    let row = sqlx::query(
        r#"
        SELECT recent_limit, candidate_limit, max_message_chars, max_total_chars,
               recency_weight, overlap_weight, assistant_weight
        FROM adaptive_memory_profiles
        WHERE chat_id = $1
        "#,
    )
    .bind(chat_id)
    .fetch_optional(pool)
    .await?;

    if let Some(row) = row {
        Ok(AdaptiveMemoryProfile {
            recent_limit: clamp_i64(row.get::<i32, _>("recent_limit") as i64, 2, 8),
            candidate_limit: clamp_i64(row.get::<i32, _>("candidate_limit") as i64, 8, 32),
            max_message_chars: clamp_i64(row.get::<i32, _>("max_message_chars") as i64, 300, 1600),
            max_total_chars: clamp_i64(row.get::<i32, _>("max_total_chars") as i64, 1200, 5000),
            recency_weight: clamp_f64(row.get::<f32, _>("recency_weight") as f64, 0.1, 0.8),
            overlap_weight: clamp_f64(row.get::<f32, _>("overlap_weight") as f64, 0.1, 0.8),
            assistant_weight: clamp_f64(row.get::<f32, _>("assistant_weight") as f64, 0.0, 0.3),
        })
    } else {
        sqlx::query(
            r#"
            INSERT INTO adaptive_memory_profiles (
                chat_id, recent_limit, candidate_limit, max_message_chars, max_total_chars,
                recency_weight, overlap_weight, assistant_weight
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
        )
        .bind(chat_id)
        .bind(default_profile.recent_limit as i32)
        .bind(default_profile.candidate_limit as i32)
        .bind(default_profile.max_message_chars as i32)
        .bind(default_profile.max_total_chars as i32)
        .bind(default_profile.recency_weight as f32)
        .bind(default_profile.overlap_weight as f32)
        .bind(default_profile.assistant_weight as f32)
        .execute(pool)
        .await?;
        Ok(default_profile)
    }
}

async fn store_memory_profile(
    pool: &sqlx::PgPool,
    chat_id: &str,
    profile: &AdaptiveMemoryProfile,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        INSERT INTO adaptive_memory_profiles (
            chat_id, recent_limit, candidate_limit, max_message_chars, max_total_chars,
            recency_weight, overlap_weight, assistant_weight, updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CURRENT_TIMESTAMP)
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
    .bind(profile.recent_limit as i32)
    .bind(profile.candidate_limit as i32)
    .bind(profile.max_message_chars as i32)
    .bind(profile.max_total_chars as i32)
    .bind(profile.recency_weight as f32)
    .bind(profile.overlap_weight as f32)
    .bind(profile.assistant_weight as f32)
    .execute(pool)
    .await?;

    Ok(())
}

async fn record_feedback(
    pool: &sqlx::PgPool,
    chat_id: &str,
    signal: &str,
    observed_chars: Option<i64>,
    query: Option<&str>,
) -> anyhow::Result<AdaptiveMemoryProfile> {
    sqlx::query(
        r#"
        INSERT INTO adaptive_memory_feedback (chat_id, signal, observed_chars, query)
        VALUES ($1, $2, $3, $4)
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

    profile.assistant_weight = clamp_f64(
        1.0 - profile.recency_weight - profile.overlap_weight,
        0.0,
        0.3,
    );
    store_memory_profile(pool, chat_id, &profile).await?;
    Ok(profile)
}

fn profile_json(profile: &AdaptiveMemoryProfile) -> Value {
    serde_json::json!({
        "recent_limit": profile.recent_limit,
        "candidate_limit": profile.candidate_limit,
        "max_message_chars": profile.max_message_chars,
        "max_total_chars": profile.max_total_chars,
        "recency_weight": profile.recency_weight,
        "overlap_weight": profile.overlap_weight,
        "assistant_weight": profile.assistant_weight
    })
}

pub async fn save_memory(pool: &sqlx::PgPool, payload: Value) -> Value {
    if let Ok(entry) = serde_json::from_value::<MemoryEntry>(payload) {
        let result =
            sqlx::query("INSERT INTO memento_memory (chat_id, role, content) VALUES ($1, $2, $3)")
                .bind(&entry.chat_id)
                .bind(&entry.role)
                .bind(&entry.content)
                .execute(pool)
                .await;

        match result {
            Ok(_) => serde_json::json!({ "status": "success" }),
            Err(e) => serde_json::json!({ "error": format!("DB error: {}", e) }),
        }
    } else {
        serde_json::json!({ "error": "Invalid payload for save_memory" })
    }
}

pub async fn get_context(pool: &sqlx::PgPool, payload: Value) -> Value {
    let chat_id = payload
        .get("chat_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let limit_hint = payload.get("limit").and_then(|v| v.as_i64()).unwrap_or(6);
    let query_text = payload.get("query").and_then(|v| v.as_str()).unwrap_or("");

    match load_memory_profile(pool, chat_id).await {
        Ok(profile) => {
            let effective_limit = clamp_i64(limit_hint.min(profile.recent_limit), 2, 8);
            let candidate_limit =
                clamp_i64(profile.candidate_limit.max(effective_limit * 2), 8, 32);
            let rows_result = sqlx::query(
                "SELECT role, content FROM memento_memory WHERE chat_id = $1 ORDER BY timestamp DESC LIMIT $2",
            )
            .bind(chat_id)
            .bind(candidate_limit)
            .fetch_all(pool)
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

                    scored.sort_by(|a, b| {
                        b.score
                            .partial_cmp(&a.score)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    });

                    let mut picked = scored
                        .into_iter()
                        .take(effective_limit as usize)
                        .collect::<Vec<_>>();

                    picked.sort_by(|a, b| {
                        a.recency_score
                            .partial_cmp(&b.recency_score)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    });

                    let mut total_chars = 0usize;
                    let mut messages = Vec::new();
                    let max_total_chars = profile.max_total_chars as usize;
                    let max_message_chars = profile.max_message_chars as usize;

                    for message in picked {
                        if total_chars >= max_total_chars {
                            break;
                        }

                        let mut content =
                            trim_message_for_budget(&message.content, max_message_chars);
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

pub async fn record_context_feedback(pool: &sqlx::PgPool, payload: Value) -> Value {
    let chat_id = payload
        .get("chat_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let signal = payload.get("signal").and_then(|v| v.as_str()).unwrap_or("");
    let observed_chars = payload.get("observed_chars").and_then(|v| v.as_i64());
    let query = payload.get("query").and_then(|v| v.as_str());

    if chat_id.is_empty() || signal.is_empty() {
        return serde_json::json!({ "error": "Missing 'chat_id' or 'signal' in payload" });
    }

    match record_feedback(pool, chat_id, signal, observed_chars, query).await {
        Ok(profile) => serde_json::json!({
            "status": "success",
            "profile": profile_json(&profile)
        }),
        Err(e) => serde_json::json!({ "error": format!("Feedback error: {}", e) }),
    }
}

pub async fn get_context_profile(pool: &sqlx::PgPool, payload: Value) -> Value {
    let chat_id = payload
        .get("chat_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if chat_id.is_empty() {
        return serde_json::json!({ "error": "Missing 'chat_id' in payload" });
    }

    match load_memory_profile(pool, chat_id).await {
        Ok(profile) => serde_json::json!({
            "status": "success",
            "profile": profile_json(&profile)
        }),
        Err(e) => serde_json::json!({ "error": format!("Profile error: {}", e) }),
    }
}

pub async fn clear_context(pool: &sqlx::PgPool, payload: Value) -> Value {
    let chat_id = payload
        .get("chat_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if chat_id.is_empty() {
        return serde_json::json!({ "error": "Missing 'chat_id' in payload" });
    }

    let result = sqlx::query("DELETE FROM memento_memory WHERE chat_id = $1")
        .bind(chat_id)
        .execute(pool)
        .await;
    match result {
        Ok(_) => serde_json::json!({ "status": "success" }),
        Err(e) => serde_json::json!({ "error": format!("DB error: {}", e) }),
    }
}
