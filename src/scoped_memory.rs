use serde_json::Value;
use sqlx::Row;
use std::collections::HashSet;

fn maybe_cached(action: &str, payload: &Value) -> Option<Value> {
    crate::query_cache::get(action, payload)
}

fn store_cache(action: &str, payload: &Value, value: &Value) {
    if value.get("error").is_none() {
        crate::query_cache::put(action, payload, value);
    }
}

const SCOPED_MEMORY_SELECT_COLUMNS: &str = "id, user_id, tenant_id, app_id, expert_id, session_id, device_id, scope, source, wing, hall, room, entry_title, memory_type, content, tags_json, content_json, confidence, provenance_refs, derivation_method, status, expires_at, timestamp, usage_count, last_used_at, promoted_from";
const REQUIRED_SCOPE_ERROR: &str = "At least one filter (user_id, tenant_id, app_id, expert_id, session_id, scope, wing, hall, room, memory_type) is required. Global reads are forbidden.";
const TIMELINE_SCOPE_ERROR: &str = "At least one filter (user_id, tenant_id, app_id, session_id, wing, hall, room) is required. Global reads are forbidden.";

#[derive(Debug, Clone)]
struct ScopedMemorySearchCandidate {
    id: i32,
    user_id: String,
    tenant_id: String,
    app_id: String,
    expert_id: String,
    session_id: String,
    device_id: String,
    scope: String,
    source: String,
    wing: String,
    hall: String,
    room: String,
    entry_title: String,
    memory_type: String,
    content: String,
    tags: Vec<String>,
    confidence: Option<f64>,
    status: String,
    timestamp: String,
    score: f64,
    overlap_hits: usize,
}

#[derive(Debug, Clone, Default)]
struct ScopedMemoryFilters {
    user_id: Option<String>,
    tenant_id: Option<String>,
    app_id: Option<String>,
    expert_id: Option<String>,
    session_id: Option<String>,
    scope: Option<String>,
    wing: Option<String>,
    hall: Option<String>,
    room: Option<String>,
    memory_type: Option<String>,
    status: Option<String>,
}

fn clamp_i64(value: i64, min: i64, max: i64) -> i64 {
    value.max(min).min(max)
}

fn push_text_condition(
    conditions: &mut Vec<String>,
    bind_values: &mut Vec<String>,
    param_idx: &mut i32,
    column: &str,
    value: Option<&str>,
) {
    if let Some(value) = value {
        conditions.push(format!("{column} = ${}", *param_idx));
        bind_values.push(value.to_string());
        *param_idx += 1;
    }
}

impl ScopedMemoryFilters {
    fn from_payload(payload: &Value) -> Self {
        Self {
            user_id: payload
                .get("user_id")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            tenant_id: payload
                .get("tenant_id")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            app_id: payload
                .get("app_id")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            expert_id: payload
                .get("expert_id")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            session_id: payload
                .get("session_id")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            scope: payload
                .get("scope")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            wing: payload
                .get("wing")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            hall: payload
                .get("hall")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            room: payload
                .get("room")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            memory_type: payload
                .get("memory_type")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            status: payload
                .get("status")
                .and_then(|v| v.as_str())
                .map(str::to_string),
        }
    }

    fn has_required_scope_filter(&self) -> bool {
        self.user_id.is_some()
            || self.tenant_id.is_some()
            || self.app_id.is_some()
            || self.expert_id.is_some()
            || self.session_id.is_some()
            || self.scope.is_some()
            || self.wing.is_some()
            || self.hall.is_some()
            || self.room.is_some()
            || self.memory_type.is_some()
    }

    fn has_timeline_scope_filter(&self) -> bool {
        self.user_id.is_some()
            || self.tenant_id.is_some()
            || self.app_id.is_some()
            || self.session_id.is_some()
            || self.wing.is_some()
            || self.hall.is_some()
            || self.room.is_some()
    }

    fn build_where_clause(&self) -> (String, Vec<String>) {
        let mut conditions = Vec::new();
        let mut bind_values = Vec::new();
        let mut param_idx = 1;

        push_text_condition(
            &mut conditions,
            &mut bind_values,
            &mut param_idx,
            "user_id",
            self.user_id.as_deref(),
        );
        push_text_condition(
            &mut conditions,
            &mut bind_values,
            &mut param_idx,
            "tenant_id",
            self.tenant_id.as_deref(),
        );
        push_text_condition(
            &mut conditions,
            &mut bind_values,
            &mut param_idx,
            "app_id",
            self.app_id.as_deref(),
        );
        push_text_condition(
            &mut conditions,
            &mut bind_values,
            &mut param_idx,
            "expert_id",
            self.expert_id.as_deref(),
        );
        push_text_condition(
            &mut conditions,
            &mut bind_values,
            &mut param_idx,
            "session_id",
            self.session_id.as_deref(),
        );
        push_text_condition(
            &mut conditions,
            &mut bind_values,
            &mut param_idx,
            "scope",
            self.scope.as_deref(),
        );
        push_text_condition(
            &mut conditions,
            &mut bind_values,
            &mut param_idx,
            "wing",
            self.wing.as_deref(),
        );
        push_text_condition(
            &mut conditions,
            &mut bind_values,
            &mut param_idx,
            "hall",
            self.hall.as_deref(),
        );
        push_text_condition(
            &mut conditions,
            &mut bind_values,
            &mut param_idx,
            "room",
            self.room.as_deref(),
        );
        push_text_condition(
            &mut conditions,
            &mut bind_values,
            &mut param_idx,
            "memory_type",
            self.memory_type.as_deref(),
        );
        push_text_condition(
            &mut conditions,
            &mut bind_values,
            &mut param_idx,
            "status",
            self.status.as_deref(),
        );

        (conditions.join(" AND "), bind_values)
    }
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

fn normalize_tags(value: Option<&Value>) -> Vec<String> {
    match value {
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|item| item.as_str())
            .map(|tag| tag.trim().to_lowercase())
            .filter(|tag| !tag.is_empty())
            .collect(),
        Some(Value::String(tags)) => tags
            .split(',')
            .map(|tag| tag.trim().to_lowercase())
            .filter(|tag| !tag.is_empty())
            .collect(),
        _ => Vec::new(),
    }
}

fn parse_json_string_array(raw: Option<String>) -> Vec<String> {
    raw.and_then(|value| serde_json::from_str::<Vec<String>>(&value).ok())
        .unwrap_or_default()
}

fn parse_json_value(raw: Option<String>) -> Option<Value> {
    raw.and_then(|value| serde_json::from_str::<Value>(&value).ok())
}

fn scope_error_response(message: &str) -> Value {
    serde_json::json!({ "error": message })
}

fn format_timestamp(value: chrono::NaiveDateTime) -> String {
    value.format("%Y-%m-%d %H:%M:%S").to_string()
}

fn row_timestamp_string(row: &sqlx::postgres::PgRow, column: &str) -> String {
    format_timestamp(row.get::<chrono::NaiveDateTime, _>(column))
}

fn row_optional_timestamp_string(row: &sqlx::postgres::PgRow, column: &str) -> Option<String> {
    row.get::<Option<chrono::NaiveDateTime>, _>(column)
        .map(format_timestamp)
}

fn derive_entry_title(content: &str) -> String {
    let first_line = content.lines().next().unwrap_or("").trim();
    if first_line.is_empty() {
        return "memory entry".to_string();
    }
    trim_message_for_budget(first_line, 96)
}

fn build_memory_snippet(content: &str) -> String {
    trim_message_for_budget(content, 220)
}

fn row_tags(row: &sqlx::postgres::PgRow) -> Vec<String> {
    parse_json_string_array(row.get::<Option<String>, _>("tags_json"))
}

fn row_content_json(row: &sqlx::postgres::PgRow) -> Option<Value> {
    parse_json_value(row.get::<Option<String>, _>("content_json"))
}

fn has_any_tag(tags: &[String], expected: &[&str]) -> bool {
    tags.iter()
        .any(|tag| expected.iter().any(|candidate| tag == candidate))
}

fn is_preference_entry(row: &sqlx::postgres::PgRow, tags: &[String]) -> bool {
    let memory_type = row.get::<String, _>("memory_type").to_lowercase();
    if matches!(memory_type.as_str(), "preference" | "user_profile") {
        return true;
    }

    if has_any_tag(
        tags,
        &["preference", "preferences", "style", "user_profile"],
    ) {
        return true;
    }

    row_content_json(row)
        .and_then(|value| value.as_object().cloned())
        .map(|object| object.contains_key("preference") || object.contains_key("preferences"))
        .unwrap_or(false)
}

fn is_summary_entry(row: &sqlx::postgres::PgRow, tags: &[String]) -> bool {
    let memory_type = row.get::<String, _>("memory_type").to_lowercase();
    matches!(memory_type.as_str(), "summary" | "working_summary")
        || has_any_tag(tags, &["summary", "working_summary", "session_summary"])
}

fn is_decision_entry(row: &sqlx::postgres::PgRow, tags: &[String]) -> bool {
    let memory_type = row.get::<String, _>("memory_type").to_lowercase();
    matches!(memory_type.as_str(), "decision" | "fact")
        && has_any_tag(tags, &["decision", "resolved", "chosen"])
        || memory_type == "decision"
}

fn is_open_loop_entry(row: &sqlx::postgres::PgRow, tags: &[String]) -> bool {
    let memory_type = row.get::<String, _>("memory_type").to_lowercase();
    matches!(memory_type.as_str(), "open_loop" | "todo" | "task")
        || has_any_tag(tags, &["open_loop", "followup", "pending", "todo"])
}

fn is_durable_fact_entry(row: &sqlx::postgres::PgRow, tags: &[String]) -> bool {
    let memory_type = row.get::<String, _>("memory_type").to_lowercase();
    matches!(
        memory_type.as_str(),
        "fact" | "decision" | "user_profile" | "learned_heuristic"
    ) || has_any_tag(
        tags,
        &[
            "fact",
            "durable_fact",
            "decision",
            "user_profile",
            "heuristic",
        ],
    )
}

fn has_positive_outcome(row: &sqlx::postgres::PgRow, tags: &[String]) -> bool {
    has_any_tag(tags, &["successful", "success", "resolved", "useful"])
        || row_content_json(row)
            .and_then(|value| value.as_object().cloned())
            .and_then(|object| object.get("outcome").cloned())
            .and_then(|value| value.as_str().map(str::to_string))
            .map(|value| {
                matches!(
                    value.to_lowercase().as_str(),
                    "success" | "successful" | "useful"
                )
            })
            .unwrap_or(false)
}

async fn fetch_scoped_rows(
    pool: &sqlx::PgPool,
    filters: &ScopedMemoryFilters,
    limit: i64,
    order_sql: &str,
    extra_conditions: &[&str],
) -> Result<Vec<sqlx::postgres::PgRow>, sqlx::Error> {
    let (where_clause, bind_values) = filters.build_where_clause();
    let mut conditions = vec![where_clause];
    conditions.extend(
        extra_conditions
            .iter()
            .map(|condition| condition.to_string()),
    );
    let sql = format!(
        "SELECT {} FROM scoped_memory WHERE {} ORDER BY {} LIMIT {}",
        SCOPED_MEMORY_SELECT_COLUMNS,
        conditions.join(" AND "),
        order_sql,
        limit
    );

    let mut query = sqlx::query(&sql);
    for val in &bind_values {
        query = query.bind(val);
    }
    query.fetch_all(pool).await
}

async fn fetch_scoped_rows_with_query(
    pool: &sqlx::PgPool,
    filters: &ScopedMemoryFilters,
    query_text: &str,
    limit: i64,
    extra_conditions: &[&str],
) -> Result<Vec<sqlx::postgres::PgRow>, sqlx::Error> {
    let (where_clause, bind_values) = filters.build_where_clause();
    let mut conditions = vec![where_clause];
    conditions.extend(
        extra_conditions
            .iter()
            .map(|condition| condition.to_string()),
    );
    let query_param = bind_values.len() + 1;
    let sql = format!(
        "SELECT {}, ts_rank_cd(to_tsvector('simple', coalesce(entry_title, '') || ' ' || coalesce(content, '') || ' ' || coalesce(memory_type, '') || ' ' || coalesce(tags_json, '')), websearch_to_tsquery('simple', ${})) AS fts_rank \
         FROM scoped_memory \
         WHERE {} AND to_tsvector('simple', coalesce(entry_title, '') || ' ' || coalesce(content, '') || ' ' || coalesce(memory_type, '') || ' ' || coalesce(tags_json, '')) @@ websearch_to_tsquery('simple', ${}) \
         ORDER BY fts_rank DESC, timestamp DESC LIMIT {}",
        SCOPED_MEMORY_SELECT_COLUMNS,
        query_param,
        conditions.join(" AND "),
        query_param,
        limit
    );

    let mut query = sqlx::query(&sql);
    for val in &bind_values {
        query = query.bind(val);
    }
    query.bind(query_text).fetch_all(pool).await
}

fn score_scoped_memory_candidate(
    query_tokens: &HashSet<String>,
    recency_rank: usize,
    total_candidates: usize,
    content: &str,
    entry_title: &str,
    memory_type: &str,
    wing: &str,
    hall: &str,
    room: &str,
    tags: &[String],
) -> (f64, usize) {
    let mut combined = String::new();
    combined.push_str(entry_title);
    combined.push(' ');
    combined.push_str(content);
    combined.push(' ');
    combined.push_str(memory_type);
    combined.push(' ');
    combined.push_str(wing);
    combined.push(' ');
    combined.push_str(hall);
    combined.push(' ');
    combined.push_str(room);
    if !tags.is_empty() {
        combined.push(' ');
        combined.push_str(&tags.join(" "));
    }

    let haystack_tokens = tokenize_memory(&combined);
    let overlap_hits = query_tokens.intersection(&haystack_tokens).count();
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
    let structure_bonus = if !room.is_empty() {
        0.08
    } else if !hall.is_empty() {
        0.05
    } else if !wing.is_empty() {
        0.03
    } else {
        0.0
    };
    let title_bonus = if tokenize_memory(entry_title)
        .intersection(query_tokens)
        .next()
        .is_some()
    {
        0.12
    } else {
        0.0
    };

    (
        (overlap_score * 0.72) + (recency_score * 0.16) + structure_bonus + title_bonus,
        overlap_hits,
    )
}

fn scoped_memory_row_to_json(row: &sqlx::postgres::PgRow) -> Value {
    serde_json::json!({
        "id": row.get::<i32, _>("id"),
        "user_id": row.get::<String, _>("user_id"),
        "tenant_id": row.get::<String, _>("tenant_id"),
        "app_id": row.get::<String, _>("app_id"),
        "expert_id": row.get::<String, _>("expert_id"),
        "session_id": row.get::<String, _>("session_id"),
        "device_id": row.get::<String, _>("device_id"),
        "scope": row.get::<String, _>("scope"),
        "source": row.get::<String, _>("source"),
        "wing": row.get::<String, _>("wing"),
        "hall": row.get::<String, _>("hall"),
        "room": row.get::<String, _>("room"),
        "entry_title": row.get::<String, _>("entry_title"),
        "memory_type": row.get::<String, _>("memory_type"),
        "content": row.get::<String, _>("content"),
        "tags": parse_json_string_array(row.get::<Option<String>, _>("tags_json")),
        "content_json": row.get::<Option<String>, _>("content_json")
            .and_then(|value| serde_json::from_str::<Value>(&value).ok()),
        "confidence": row.get::<Option<f64>, _>("confidence"),
        "provenance_refs": row.get::<Option<String>, _>("provenance_refs")
            .and_then(|value| serde_json::from_str::<Value>(&value).ok()),
        "derivation_method": row.get::<Option<String>, _>("derivation_method"),
        "status": row.get::<String, _>("status"),
        "expires_at": row_optional_timestamp_string(row, "expires_at"),
        "timestamp": row_timestamp_string(row, "timestamp"),
        "created_at": row_timestamp_string(row, "timestamp"),
        "usage_count": row.get::<i32, _>("usage_count"),
        "last_used_at": row_optional_timestamp_string(row, "last_used_at"),
        "promoted_from": row.get::<Option<String>, _>("promoted_from"),
    })
}

fn row_ids(rows: &[sqlx::postgres::PgRow]) -> Vec<i32> {
    rows.iter().map(|row| row.get::<i32, _>("id")).collect()
}

async fn touch_usage(pool: &sqlx::PgPool, ids: &[i32]) -> Result<(), sqlx::Error> {
    if ids.is_empty() {
        return Ok(());
    }

    let deduped: Vec<i32> = ids
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();

    sqlx::query(
        "UPDATE scoped_memory SET usage_count = usage_count + 1, last_used_at = CURRENT_TIMESTAMP WHERE id = ANY($1)",
    )
    .bind(&deduped)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn save_record(pool: &sqlx::PgPool, payload: Value) -> Value {
    let user_id = payload
        .get("user_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let tenant_id = payload
        .get("tenant_id")
        .and_then(|v| v.as_str())
        .unwrap_or("default");
    let app_id = payload
        .get("app_id")
        .and_then(|v| v.as_str())
        .unwrap_or("os");
    let expert_id = payload
        .get("expert_id")
        .and_then(|v| v.as_str())
        .unwrap_or("ava");
    let session_id = payload
        .get("session_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let device_id = payload
        .get("device_id")
        .and_then(|v| v.as_str())
        .unwrap_or("server");
    let scope = payload
        .get("scope")
        .and_then(|v| v.as_str())
        .unwrap_or("personal");
    let source = payload
        .get("source")
        .and_then(|v| v.as_str())
        .unwrap_or("chat");
    let memory_type = payload
        .get("memory_type")
        .and_then(|v| v.as_str())
        .unwrap_or("event");
    let content = payload
        .get("content")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let wing = payload.get("wing").and_then(|v| v.as_str()).unwrap_or("");
    let hall = payload.get("hall").and_then(|v| v.as_str()).unwrap_or("");
    let room = payload.get("room").and_then(|v| v.as_str()).unwrap_or("");
    let entry_title = payload
        .get("entry_title")
        .and_then(|v| v.as_str())
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string())
        .unwrap_or_else(|| derive_entry_title(content));
    let tags = normalize_tags(payload.get("tags"));
    let content_json = payload
        .get("content_json")
        .filter(|v| !v.is_null())
        .cloned();
    let confidence = payload.get("confidence").and_then(|v| v.as_f64());
    let provenance_refs = payload
        .get("provenance_refs")
        .filter(|v| !v.is_null())
        .cloned();
    let derivation_method = payload.get("derivation_method").and_then(|v| v.as_str());
    let status = payload
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("active");
    let expires_at = payload.get("expires_at").and_then(|v| v.as_str());
    let usage_count = payload
        .get("usage_count")
        .and_then(|v| v.as_i64())
        .unwrap_or(0)
        .max(0) as i32;
    let last_used_at = payload.get("last_used_at").and_then(|v| v.as_str());
    let promoted_from = payload
        .get("promoted_from")
        .and_then(|v| v.as_str())
        .map(str::to_string);

    if user_id.is_empty() || content.is_empty() {
        serde_json::json!({ "error": "Missing 'user_id' or 'content' in payload" })
    } else {
        let result = sqlx::query("INSERT INTO scoped_memory (user_id, tenant_id, app_id, expert_id, session_id, device_id, scope, source, wing, hall, room, entry_title, memory_type, content, tags_json, content_json, confidence, provenance_refs, derivation_method, status, expires_at, usage_count, last_used_at, promoted_from) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, CAST($21 AS TIMESTAMP), $22, CAST($23 AS TIMESTAMP), $24)")
            .bind(user_id)
            .bind(tenant_id)
            .bind(app_id)
            .bind(expert_id)
            .bind(session_id)
            .bind(device_id)
            .bind(scope)
            .bind(source)
            .bind(wing)
            .bind(hall)
            .bind(room)
            .bind(&entry_title)
            .bind(memory_type)
            .bind(content)
            .bind(if tags.is_empty() {
                None
            } else {
                Some(Value::Array(
                    tags.iter()
                        .map(|tag| Value::String(tag.clone()))
                        .collect(),
                )
                .to_string())
            })
            .bind(content_json.as_ref().map(|value| value.to_string()))
            .bind(confidence)
            .bind(provenance_refs.as_ref().map(|value| value.to_string()))
            .bind(derivation_method)
            .bind(status)
            .bind(expires_at)
            .bind(usage_count)
            .bind(last_used_at)
            .bind(promoted_from.as_deref())
            .execute(pool)
            .await;

        match result {
            Ok(_) => {
                crate::query_cache::invalidate_all();
                serde_json::json!({
                    "status": "success",
                    "action": "memory_record_saved",
                    "scope": scope,
                    "user_id": user_id,
                    "memory_type": memory_type,
                    "wing": wing,
                    "hall": hall,
                    "room": room,
                    "entry_title": entry_title,
                    "usage_count": usage_count,
                    "promoted_from": promoted_from
                })
            }
            Err(e) => serde_json::json!({ "error": format!("DB error: {}", e) }),
        }
    }
}

pub async fn query_records(pool: &sqlx::PgPool, payload: Value) -> Value {
    let filters = ScopedMemoryFilters::from_payload(&payload);
    let limit = payload.get("limit").and_then(|v| v.as_i64()).unwrap_or(50);

    if !filters.has_required_scope_filter() {
        scope_error_response(REQUIRED_SCOPE_ERROR)
    } else {
        match fetch_scoped_rows(pool, &filters, limit, "timestamp DESC", &[]).await {
            Ok(rows) => {
                let ids = row_ids(&rows);
                let results: Vec<Value> = rows.iter().map(scoped_memory_row_to_json).collect();
                if !ids.is_empty() {
                    let _ = touch_usage(pool, &ids).await;
                }
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

pub async fn get_preferences(pool: &sqlx::PgPool, payload: Value) -> Value {
    if let Some(cached) = maybe_cached("get_preferences", &payload) {
        return cached;
    }
    let filters = ScopedMemoryFilters::from_payload(&payload);
    let limit = clamp_i64(
        payload.get("limit").and_then(|v| v.as_i64()).unwrap_or(12),
        1,
        50,
    );

    if !filters.has_required_scope_filter() {
        return scope_error_response(REQUIRED_SCOPE_ERROR);
    }

    match fetch_scoped_rows(
        pool,
        &filters,
        limit * 3,
        "timestamp DESC",
        &[
            "status = 'active'",
            "(expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)",
            "(memory_type IN ('preference', 'user_profile') OR tags_json ILIKE '%preference%' OR tags_json ILIKE '%user_profile%' OR content_json ILIKE '%preference%')",
        ],
    )
    .await
    {
        Ok(rows) => {
            let matched_rows: Vec<&sqlx::postgres::PgRow> = rows
                .iter()
                .filter(|row| is_preference_entry(row, &row_tags(row)))
                .take(limit as usize)
                .collect();
            let ids: Vec<i32> = matched_rows
                .iter()
                .map(|row| row.get::<i32, _>("id"))
                .collect();
            let entries: Vec<Value> = matched_rows
                .into_iter()
                .map(scoped_memory_row_to_json)
                .collect();
            if !ids.is_empty() {
                let _ = touch_usage(pool, &ids).await;
            }

            let response = serde_json::json!({
                "status": "success",
                "retrieval_strategy": "preferences",
                "count": entries.len(),
                "entries": entries
            });
            store_cache("get_preferences", &payload, &response);
            response
        }
        Err(e) => serde_json::json!({ "error": format!("Query error: {}", e) }),
    }
}

pub async fn get_durable_facts(pool: &sqlx::PgPool, payload: Value) -> Value {
    if let Some(cached) = maybe_cached("get_durable_facts", &payload) {
        return cached;
    }
    let filters = ScopedMemoryFilters::from_payload(&payload);
    let limit = clamp_i64(
        payload.get("limit").and_then(|v| v.as_i64()).unwrap_or(16),
        1,
        64,
    );

    if !filters.has_required_scope_filter() {
        return scope_error_response(REQUIRED_SCOPE_ERROR);
    }

    match fetch_scoped_rows(
        pool,
        &filters,
        limit * 3,
        "usage_count DESC, timestamp DESC",
        &[
            "status = 'active'",
            "(expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)",
            "(memory_type IN ('fact', 'decision', 'user_profile', 'learned_heuristic') OR tags_json ILIKE '%fact%' OR tags_json ILIKE '%decision%' OR tags_json ILIKE '%heuristic%')",
        ],
    )
    .await
    {
        Ok(rows) => {
            let matched_rows: Vec<&sqlx::postgres::PgRow> = rows
                .iter()
                .filter(|row| is_durable_fact_entry(row, &row_tags(row)))
                .take(limit as usize)
                .collect();
            let ids: Vec<i32> = matched_rows
                .iter()
                .map(|row| row.get::<i32, _>("id"))
                .collect();
            let entries: Vec<Value> = matched_rows
                .into_iter()
                .map(scoped_memory_row_to_json)
                .collect();
            if !ids.is_empty() {
                let _ = touch_usage(pool, &ids).await;
            }

            let response = serde_json::json!({
                "status": "success",
                "retrieval_strategy": "durable_facts",
                "count": entries.len(),
                "entries": entries
            });
            store_cache("get_durable_facts", &payload, &response);
            response
        }
        Err(e) => serde_json::json!({ "error": format!("Query error: {}", e) }),
    }
}

pub async fn get_recent_events(pool: &sqlx::PgPool, payload: Value) -> Value {
    if let Some(cached) = maybe_cached("get_recent_events", &payload) {
        return cached;
    }
    let filters = ScopedMemoryFilters::from_payload(&payload);
    let limit = clamp_i64(
        payload.get("limit").and_then(|v| v.as_i64()).unwrap_or(12),
        1,
        64,
    );

    if !filters.has_required_scope_filter() {
        return scope_error_response(REQUIRED_SCOPE_ERROR);
    }

    match fetch_scoped_rows(
        pool,
        &filters,
        limit,
        "timestamp DESC",
        &[
            "status = 'active'",
            "(expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)",
            "memory_type = 'event'",
        ],
    )
    .await
    {
        Ok(rows) => {
            let ids = row_ids(&rows);
            let entries: Vec<Value> = rows.iter().map(scoped_memory_row_to_json).collect();
            if !ids.is_empty() {
                let _ = touch_usage(pool, &ids).await;
            }

            let response = serde_json::json!({
                "status": "success",
                "retrieval_strategy": "recent_events",
                "count": entries.len(),
                "entries": entries
            });
            store_cache("get_recent_events", &payload, &response);
            response
        }
        Err(e) => serde_json::json!({ "error": format!("Query error: {}", e) }),
    }
}

pub async fn get_working_context(pool: &sqlx::PgPool, payload: Value) -> Value {
    if let Some(cached) = maybe_cached("get_working_context", &payload) {
        return cached;
    }
    let filters = ScopedMemoryFilters::from_payload(&payload);
    let summary_limit = clamp_i64(
        payload
            .get("summary_limit")
            .and_then(|v| v.as_i64())
            .unwrap_or(4),
        1,
        12,
    ) as usize;
    let decision_limit = clamp_i64(
        payload
            .get("decision_limit")
            .and_then(|v| v.as_i64())
            .unwrap_or(6),
        1,
        16,
    ) as usize;
    let preference_limit = clamp_i64(
        payload
            .get("preference_limit")
            .and_then(|v| v.as_i64())
            .unwrap_or(6),
        1,
        16,
    ) as usize;
    let open_loop_limit = clamp_i64(
        payload
            .get("open_loop_limit")
            .and_then(|v| v.as_i64())
            .unwrap_or(8),
        1,
        24,
    ) as usize;
    let recent_event_limit = clamp_i64(
        payload
            .get("recent_event_limit")
            .and_then(|v| v.as_i64())
            .unwrap_or(10),
        1,
        30,
    ) as usize;

    if !filters.has_required_scope_filter() {
        return scope_error_response(REQUIRED_SCOPE_ERROR);
    }

    match fetch_scoped_rows(
        pool,
        &filters,
        (summary_limit + decision_limit + preference_limit + open_loop_limit + recent_event_limit)
            as i64
            * 4,
        "timestamp DESC",
        &[
            "status = 'active'",
            "(expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)",
            "(memory_type IN ('summary', 'working_summary', 'decision', 'preference', 'user_profile', 'open_loop', 'todo', 'task', 'event') OR tags_json ILIKE '%summary%' OR tags_json ILIKE '%decision%' OR tags_json ILIKE '%preference%' OR tags_json ILIKE '%open_loop%' OR tags_json ILIKE '%todo%' OR tags_json ILIKE '%event%')",
        ],
    )
    .await
    {
        Ok(rows) => {
            let mut summaries = Vec::new();
            let mut decisions = Vec::new();
            let mut preferences = Vec::new();
            let mut open_loops = Vec::new();
            let mut recent_events = Vec::new();
            let mut touched_ids: Vec<i32> = Vec::new();

            for row in &rows {
                let tags = row_tags(row);
                let row_id = row.get::<i32, _>("id");

                if summaries.len() < summary_limit && is_summary_entry(row, &tags) {
                    summaries.push(scoped_memory_row_to_json(row));
                    touched_ids.push(row_id);
                    continue;
                }
                if decisions.len() < decision_limit && is_decision_entry(row, &tags) {
                    decisions.push(scoped_memory_row_to_json(row));
                    touched_ids.push(row_id);
                    continue;
                }
                if preferences.len() < preference_limit && is_preference_entry(row, &tags) {
                    preferences.push(scoped_memory_row_to_json(row));
                    touched_ids.push(row_id);
                    continue;
                }
                if open_loops.len() < open_loop_limit && is_open_loop_entry(row, &tags) {
                    open_loops.push(scoped_memory_row_to_json(row));
                    touched_ids.push(row_id);
                    continue;
                }
                if recent_events.len() < recent_event_limit
                    && row
                        .get::<String, _>("memory_type")
                        .eq_ignore_ascii_case("event")
                {
                    recent_events.push(scoped_memory_row_to_json(row));
                    touched_ids.push(row_id);
                }
            }

            if !touched_ids.is_empty() {
                touched_ids.sort_unstable();
                touched_ids.dedup();
                let _ = touch_usage(pool, &touched_ids).await;
            }

            let total = summaries.len()
                + decisions.len()
                + preferences.len()
                + open_loops.len()
                + recent_events.len();

            let response = serde_json::json!({
                "status": "success",
                "retrieval_strategy": "working_context",
                "count": total,
                "working_context": {
                    "summaries": summaries,
                    "decisions": decisions,
                    "preferences": preferences,
                    "open_loops": open_loops,
                    "recent_events": recent_events
                }
            });
            store_cache("get_working_context", &payload, &response);
            response
        }
        Err(e) => serde_json::json!({ "error": format!("Query error: {}", e) }),
    }
}

pub async fn search_records(pool: &sqlx::PgPool, payload: Value) -> Value {
    let filters = ScopedMemoryFilters::from_payload(&payload);
    let query_text = payload
        .get("query")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim();
    let limit = clamp_i64(
        payload.get("limit").and_then(|v| v.as_i64()).unwrap_or(12),
        1,
        50,
    );

    if query_text.is_empty() {
        serde_json::json!({ "error": "Missing 'query' in payload" })
    } else if !filters.has_required_scope_filter() {
        scope_error_response(REQUIRED_SCOPE_ERROR)
    } else {
        match fetch_scoped_rows_with_query(
            pool,
            &filters,
            query_text,
            (limit * 8).clamp(24, 250),
            &[
                "status = 'active'",
                "(expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)",
            ],
        )
        .await
        {
            Ok(rows) => {
                let query_tokens = tokenize_memory(query_text);
                let total_candidates = rows.len();
                let mut candidates: Vec<ScopedMemorySearchCandidate> = Vec::new();

                for (recency_rank, row) in rows.iter().enumerate() {
                    let tags = parse_json_string_array(row.get::<Option<String>, _>("tags_json"));
                    let entry_title: String = row.get("entry_title");
                    let content: String = row.get("content");
                    let memory_type_value: String = row.get("memory_type");
                    let wing_value: String = row.get("wing");
                    let hall_value: String = row.get("hall");
                    let room_value: String = row.get("room");
                    let (score, overlap_hits) = score_scoped_memory_candidate(
                        &query_tokens,
                        recency_rank,
                        total_candidates,
                        &content,
                        &entry_title,
                        &memory_type_value,
                        &wing_value,
                        &hall_value,
                        &room_value,
                        &tags,
                    );

                    if overlap_hits == 0 {
                        continue;
                    }

                    candidates.push(ScopedMemorySearchCandidate {
                        id: row.get("id"),
                        user_id: row.get("user_id"),
                        tenant_id: row.get("tenant_id"),
                        app_id: row.get("app_id"),
                        expert_id: row.get("expert_id"),
                        session_id: row.get("session_id"),
                        device_id: row.get("device_id"),
                        scope: row.get("scope"),
                        source: row.get("source"),
                        wing: wing_value,
                        hall: hall_value,
                        room: room_value,
                        entry_title,
                        memory_type: memory_type_value,
                        content,
                        tags,
                        confidence: row.get("confidence"),
                        status: row.get("status"),
                        timestamp: row_timestamp_string(row, "timestamp"),
                        score,
                        overlap_hits,
                    });
                }

                candidates.sort_by(|a, b| {
                    b.score
                        .partial_cmp(&a.score)
                        .unwrap_or(std::cmp::Ordering::Equal)
                        .then_with(|| b.timestamp.cmp(&a.timestamp))
                });
                candidates.truncate(limit as usize);

                let ids: Vec<i32> = candidates.iter().map(|candidate| candidate.id).collect();
                let results: Vec<Value> = candidates
                    .into_iter()
                    .map(|candidate| {
                        serde_json::json!({
                            "id": candidate.id,
                            "user_id": candidate.user_id,
                            "tenant_id": candidate.tenant_id,
                            "app_id": candidate.app_id,
                            "expert_id": candidate.expert_id,
                            "session_id": candidate.session_id,
                            "device_id": candidate.device_id,
                            "scope": candidate.scope,
                            "source": candidate.source,
                            "wing": candidate.wing,
                            "hall": candidate.hall,
                            "room": candidate.room,
                            "entry_title": candidate.entry_title,
                            "memory_type": candidate.memory_type,
                            "snippet": build_memory_snippet(&candidate.content),
                            "tags": candidate.tags,
                            "confidence": candidate.confidence,
                            "status": candidate.status,
                            "timestamp": candidate.timestamp,
                            "score": candidate.score,
                            "overlap_hits": candidate.overlap_hits
                        })
                    })
                    .collect();
                if !ids.is_empty() {
                    let _ = touch_usage(pool, &ids).await;
                }

                serde_json::json!({
                    "status": "success",
                    "query": query_text,
                    "count": results.len(),
                    "retrieval_strategy": "verbatim_palace",
                    "entries": results
                })
            }
            Err(e) => serde_json::json!({ "error": format!("Query error: {}", e) }),
        }
    }
}

pub async fn get_timeline(pool: &sqlx::PgPool, payload: Value) -> Value {
    let filters = ScopedMemoryFilters::from_payload(&payload);
    let limit = clamp_i64(
        payload.get("limit").and_then(|v| v.as_i64()).unwrap_or(40),
        1,
        200,
    );

    if !filters.has_timeline_scope_filter() {
        scope_error_response(TIMELINE_SCOPE_ERROR)
    } else {
        match fetch_scoped_rows(pool, &filters, limit, "timestamp ASC", &[]).await {
            Ok(rows) => {
                let ids = row_ids(&rows);
                let entries: Vec<Value> = rows.iter().map(scoped_memory_row_to_json).collect();
                if !ids.is_empty() {
                    let _ = touch_usage(pool, &ids).await;
                }
                serde_json::json!({
                    "status": "success",
                    "count": entries.len(),
                    "entries": entries
                })
            }
            Err(e) => serde_json::json!({ "error": format!("Query error: {}", e) }),
        }
    }
}

pub async fn memory_promote(pool: &sqlx::PgPool, payload: Value) -> Value {
    let filters = ScopedMemoryFilters::from_payload(&payload);
    let record_id = payload
        .get("record_id")
        .and_then(|v| v.as_i64())
        .map(|v| v as i32);
    let target_memory_type = payload
        .get("target_memory_type")
        .and_then(|v| v.as_str())
        .unwrap_or("learned_heuristic");
    let explicit_content = payload
        .get("content")
        .and_then(|v| v.as_str())
        .map(str::trim);
    let explicit_title = payload
        .get("entry_title")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|v| !v.is_empty());

    if record_id.is_none() && !filters.has_required_scope_filter() {
        return scope_error_response(REQUIRED_SCOPE_ERROR);
    }

    let source_rows = if let Some(record_id) = record_id {
        match sqlx::query(&format!(
            "SELECT {} FROM scoped_memory WHERE id = $1 LIMIT 1",
            SCOPED_MEMORY_SELECT_COLUMNS
        ))
        .bind(record_id)
        .fetch_all(pool)
        .await
        {
            Ok(rows) => rows,
            Err(e) => return serde_json::json!({ "error": format!("Query error: {}", e) }),
        }
    } else {
        match fetch_scoped_rows(
            pool,
            &filters,
            80,
            "timestamp DESC",
            &[
                "status = 'active'",
                "(expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)",
            ],
        )
        .await
        {
            Ok(rows) => rows,
            Err(e) => return serde_json::json!({ "error": format!("Query error: {}", e) }),
        }
    };

    if source_rows.is_empty() {
        return serde_json::json!({ "error": "No source memory records matched for promotion" });
    }

    let promotable_rows: Vec<&sqlx::postgres::PgRow> = source_rows
        .iter()
        .filter(|row| {
            row.get::<i32, _>("usage_count") >= 2
                || has_positive_outcome(row, &row_tags(row))
                || matches!(
                    row.get::<String, _>("memory_type").to_lowercase().as_str(),
                    "preference" | "fact" | "decision" | "user_profile"
                )
        })
        .collect();

    if promotable_rows.is_empty() {
        return serde_json::json!({
            "error": "No promotable records met the heuristic threshold"
        });
    }

    let source = promotable_rows[0];
    let source_id = source.get::<i32, _>("id");
    let source_content = source.get::<String, _>("content");
    let source_tags = row_tags(source);
    let promotion_reason = payload
        .get("promotion_reason")
        .and_then(|v| v.as_str())
        .unwrap_or("manual_promotion");

    let content = explicit_content
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| source_content.clone());
    let entry_title = explicit_title
        .map(str::to_string)
        .unwrap_or_else(|| derive_entry_title(&content));
    let mut tags = source_tags.clone();
    if !tags.iter().any(|tag| tag == "promoted") {
        tags.push("promoted".to_string());
    }
    if target_memory_type == "learned_heuristic" && !tags.iter().any(|tag| tag == "heuristic") {
        tags.push("heuristic".to_string());
    }

    let result = save_record(
        pool,
        serde_json::json!({
            "user_id": source.get::<String, _>("user_id"),
            "tenant_id": source.get::<String, _>("tenant_id"),
            "app_id": source.get::<String, _>("app_id"),
            "expert_id": source.get::<String, _>("expert_id"),
            "session_id": source.get::<String, _>("session_id"),
            "device_id": source.get::<String, _>("device_id"),
            "scope": source.get::<String, _>("scope"),
            "source": source.get::<String, _>("source"),
            "wing": source.get::<String, _>("wing"),
            "hall": source.get::<String, _>("hall"),
            "room": source.get::<String, _>("room"),
            "entry_title": entry_title,
            "memory_type": target_memory_type,
            "content": content,
            "tags": tags,
            "confidence": source.get::<Option<f64>, _>("confidence").unwrap_or(0.8),
            "derivation_method": promotion_reason,
            "promoted_from": serde_json::json!([source_id]).to_string(),
            "content_json": row_content_json(source)
        }),
    )
    .await;

    if result.get("error").is_none() {
        let _ = sqlx::query(
            "UPDATE scoped_memory SET usage_count = usage_count + 1, last_used_at = CURRENT_TIMESTAMP WHERE id = $1",
        )
        .bind(source_id)
        .execute(pool)
        .await;
    }

    let response = serde_json::json!({
        "status": if result.get("error").is_none() { "success" } else { "error" },
        "promotion": result,
        "source_id": source_id,
        "target_memory_type": target_memory_type
    });
    if response["status"] == "success" {
        crate::query_cache::invalidate_all();
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrations;
    use sqlx::postgres::PgPoolOptions;
    use std::process::Command;
    use std::thread;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    struct TestPostgresContainer {
        id: String,
        db_url: String,
    }

    impl Drop for TestPostgresContainer {
        fn drop(&mut self) {
            let _ = Command::new("docker").args(["rm", "-f", &self.id]).status();
        }
    }

    fn docker_available() -> bool {
        Command::new("docker")
            .arg("info")
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false)
    }

    fn start_test_postgres() -> TestPostgresContainer {
        assert!(
            docker_available(),
            "docker is required for scoped_memory integration tests"
        );

        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let container_name = format!("memento-scoped-test-{unique}");
        let password = "memento_test_pw";
        let user = "memento_test";
        let database = "memento_test";

        let run_output = Command::new("docker")
            .args([
                "run",
                "-d",
                "--rm",
                "--name",
                &container_name,
                "-e",
                &format!("POSTGRES_PASSWORD={password}"),
                "-e",
                &format!("POSTGRES_USER={user}"),
                "-e",
                &format!("POSTGRES_DB={database}"),
                "-p",
                "127.0.0.1::5432",
                "postgres:16-alpine",
            ])
            .output()
            .expect("failed to start postgres test container");

        assert!(
            run_output.status.success(),
            "docker run failed: {}",
            String::from_utf8_lossy(&run_output.stderr)
        );

        let container_id = String::from_utf8_lossy(&run_output.stdout)
            .trim()
            .to_string();
        let port_output = Command::new("docker")
            .args(["port", &container_id, "5432/tcp"])
            .output()
            .expect("failed to inspect postgres test container port");

        assert!(
            port_output.status.success(),
            "docker port failed: {}",
            String::from_utf8_lossy(&port_output.stderr)
        );

        let port_text = String::from_utf8_lossy(&port_output.stdout);
        let host_port = port_text
            .trim()
            .rsplit(':')
            .next()
            .expect("missing mapped postgres port")
            .trim()
            .to_string();

        let db_url = format!("postgresql://{user}:{password}@127.0.0.1:{host_port}/{database}");

        let mut ready = false;
        for _ in 0..30 {
            let ready_output = Command::new("docker")
                .args([
                    "exec",
                    &container_id,
                    "pg_isready",
                    "-U",
                    user,
                    "-d",
                    database,
                ])
                .output()
                .expect("failed to probe postgres test container readiness");

            if ready_output.status.success() {
                ready = true;
                break;
            }

            thread::sleep(Duration::from_millis(500));
        }

        assert!(
            ready,
            "postgres test container did not become ready in time"
        );

        TestPostgresContainer {
            id: container_id,
            db_url,
        }
    }

    async fn test_pool() -> (sqlx::PgPool, TestPostgresContainer) {
        let container = start_test_postgres();
        let mut last_error = String::new();
        let mut pool = None;
        for _ in 0..20 {
            match PgPoolOptions::new()
                .max_connections(1)
                .connect(&container.db_url)
                .await
            {
                Ok(connection) => {
                    pool = Some(connection);
                    break;
                }
                Err(error) => {
                    let error_text = error.to_string();
                    last_error = error_text.clone();
                    if error_text.contains("Connection refused")
                        || error_text.contains("Connection reset by peer")
                        || error_text.contains("the database system is starting up")
                    {
                        thread::sleep(Duration::from_millis(500));
                        continue;
                    }
                    panic!(
                        "failed to connect to postgres test container after readiness probe: {}",
                        error_text
                    );
                }
            }
        }
        let pool = pool.unwrap_or_else(|| {
            panic!(
                "failed to connect to postgres test container after retries: {}",
                last_error
            )
        });
        migrations::run_all(&pool).await.unwrap();
        (pool, container)
    }

    #[tokio::test]
    async fn get_preferences_filters_to_live_preference_like_entries() {
        let (pool, _container) = test_pool().await;

        let saved = save_record(
            &pool,
            serde_json::json!({
                "user_id": "user-1",
                "app_id": "vetra",
                "scope": "personal",
                "content": "User prefers concise contract summaries.",
                "memory_type": "preference",
                "tags": ["preference", "style"],
                "content_json": {"preference": "concise"}
            }),
        )
        .await;
        assert_eq!(saved["status"], "success", "save_record failed: {}", saved);

        let expired = save_record(
            &pool,
            serde_json::json!({
                "user_id": "user-1",
                "app_id": "vetra",
                "scope": "personal",
                "content": "Old preference that should expire.",
                "memory_type": "preference",
                "tags": ["preference"],
                "expires_at": "2000-01-01 00:00:00"
            }),
        )
        .await;
        assert_eq!(
            expired["status"], "success",
            "save_record failed: {}",
            expired
        );

        let event = save_record(
            &pool,
            serde_json::json!({
                "user_id": "user-1",
                "app_id": "vetra",
                "scope": "personal",
                "content": "This is just an event.",
                "memory_type": "event"
            }),
        )
        .await;
        assert_eq!(event["status"], "success", "save_record failed: {}", event);

        let raw = query_records(
            &pool,
            serde_json::json!({
                "user_id": "user-1",
                "app_id": "vetra",
                "limit": 10
            }),
        )
        .await;
        assert_eq!(raw["count"], 3, "raw query mismatch: {}", raw);

        let result = get_preferences(
            &pool,
            serde_json::json!({
                "user_id": "user-1",
                "app_id": "vetra",
                "limit": 10
            }),
        )
        .await;

        assert_eq!(result["status"], "success");
        assert_eq!(result["retrieval_strategy"], "preferences");
        assert_eq!(result["count"], 1);
        let entries = result["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0]["memory_type"], "preference");
        assert_eq!(
            entries[0]["content"],
            "User prefers concise contract summaries."
        );
    }

    #[tokio::test]
    async fn get_working_context_groups_records_by_operational_purpose() {
        let (pool, _container) = test_pool().await;

        let common_scope = serde_json::json!({
            "user_id": "user-2",
            "app_id": "vetra",
            "session_id": "session-7",
            "scope": "workspace"
        });

        let records = vec![
            serde_json::json!({
                "content": "Negotiation summary for the current MSA round.",
                "memory_type": "working_summary",
                "tags": ["summary", "session_summary"]
            }),
            serde_json::json!({
                "content": "Accepted liability cap at 2x annual fees.",
                "memory_type": "decision",
                "tags": ["decision", "resolved"]
            }),
            serde_json::json!({
                "content": "User prefers redlines in bullet form.",
                "memory_type": "preference",
                "tags": ["preference"]
            }),
            serde_json::json!({
                "content": "Need confirmation on governing law fallback.",
                "memory_type": "open_loop",
                "tags": ["open_loop", "pending"]
            }),
            serde_json::json!({
                "content": "Counterparty requested turnaround by Friday.",
                "memory_type": "event",
                "tags": ["event"]
            }),
        ];

        for record in records {
            let mut payload = common_scope.clone();
            payload
                .as_object_mut()
                .unwrap()
                .extend(record.as_object().unwrap().clone());
            let saved = save_record(&pool, payload).await;
            assert_eq!(saved["status"], "success", "save_record failed: {}", saved);
        }

        let raw = query_records(
            &pool,
            serde_json::json!({
                "user_id": "user-2",
                "app_id": "vetra",
                "session_id": "session-7"
            }),
        )
        .await;
        assert_eq!(raw["count"], 5, "raw query mismatch: {}", raw);

        let result = get_working_context(
            &pool,
            serde_json::json!({
                "user_id": "user-2",
                "app_id": "vetra",
                "session_id": "session-7"
            }),
        )
        .await;

        assert_eq!(result["status"], "success");
        assert_eq!(result["retrieval_strategy"], "working_context");
        let working = result["working_context"].as_object().unwrap();
        assert_eq!(working["summaries"].as_array().unwrap().len(), 1);
        assert_eq!(working["decisions"].as_array().unwrap().len(), 1);
        assert_eq!(working["preferences"].as_array().unwrap().len(), 1);
        assert_eq!(working["open_loops"].as_array().unwrap().len(), 1);
        assert_eq!(working["recent_events"].as_array().unwrap().len(), 1);
        assert_eq!(
            working["decisions"].as_array().unwrap()[0]["content"],
            "Accepted liability cap at 2x annual fees."
        );
    }

    #[tokio::test]
    async fn reading_memory_updates_usage_metadata() {
        let (pool, _container) = test_pool().await;

        let saved = save_record(
            &pool,
            serde_json::json!({
                "user_id": "user-usage",
                "app_id": "vetra",
                "content": "User prefers concise answers.",
                "memory_type": "preference",
                "tags": ["preference"]
            }),
        )
        .await;
        assert_eq!(saved["status"], "success", "save_record failed: {}", saved);

        let before = sqlx::query(
            "SELECT usage_count, last_used_at FROM scoped_memory WHERE user_id = $1 LIMIT 1",
        )
        .bind("user-usage")
        .fetch_one(&pool)
        .await
        .expect("fetch usage before read");
        assert_eq!(before.get::<i32, _>("usage_count"), 0);
        assert!(before
            .get::<Option<chrono::NaiveDateTime>, _>("last_used_at")
            .is_none());

        let result = get_preferences(
            &pool,
            serde_json::json!({
                "user_id": "user-usage",
                "app_id": "vetra"
            }),
        )
        .await;
        assert_eq!(result["status"], "success");
        assert_eq!(result["count"], 1);

        let after = sqlx::query(
            "SELECT usage_count, last_used_at FROM scoped_memory WHERE user_id = $1 LIMIT 1",
        )
        .bind("user-usage")
        .fetch_one(&pool)
        .await
        .expect("fetch usage after read");
        assert_eq!(after.get::<i32, _>("usage_count"), 1);
        assert!(after
            .get::<Option<chrono::NaiveDateTime>, _>("last_used_at")
            .is_some());
    }
}
