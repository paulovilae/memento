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

fn row_confidence(row: &sqlx::postgres::PgRow) -> Option<f64> {
    row.get::<Option<f32>, _>("confidence")
        .map(|value| value as f64)
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

fn bullet_lines(rows: &[&sqlx::postgres::PgRow], limit: usize) -> Vec<String> {
    rows.iter()
        .take(limit)
        .map(|row| {
            let title = row.get::<String, _>("entry_title");
            let content = row.get::<String, _>("content");
            if !title.trim().is_empty() && title != "memory entry" {
                format!("- {}: {}", title, trim_message_for_budget(&content, 140))
            } else {
                format!("- {}", trim_message_for_budget(&content, 160))
            }
        })
        .collect()
}

fn join_lines(lines: &[String], empty_fallback: &str) -> String {
    if lines.is_empty() {
        empty_fallback.to_string()
    } else {
        lines.join("\n")
    }
}

fn compression_kind_tag(kind: &str) -> &'static str {
    match kind {
        "session" => "session_summary",
        "room" => "room_summary",
        "project" => "project_summary",
        _ => "recursive_summary",
    }
}

#[derive(Debug, Clone)]
struct DerivationSeed {
    user_id: String,
    tenant_id: String,
    app_id: String,
    session_id: String,
    scope: String,
    source: String,
    wing: String,
    hall: String,
    room: String,
    memory_type: String,
    derivation_method: Option<String>,
}

#[derive(Debug, Clone)]
struct SaveRecordInput {
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
    content_json: Option<Value>,
    confidence: Option<f64>,
    provenance_refs: Option<Value>,
    derivation_method: Option<String>,
    status: String,
    expires_at: Option<String>,
    usage_count: i32,
    last_used_at: Option<String>,
    promoted_from: Option<String>,
}

impl SaveRecordInput {
    fn from_payload(payload: &Value) -> Result<Self, Value> {
        let user_id = payload
            .get("user_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let content = payload
            .get("content")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if user_id.is_empty() || content.is_empty() {
            return Err(
                serde_json::json!({ "error": "Missing 'user_id' or 'content' in payload" }),
            );
        }

        Ok(Self {
            user_id,
            tenant_id: payload
                .get("tenant_id")
                .and_then(|v| v.as_str())
                .unwrap_or("default")
                .to_string(),
            app_id: payload
                .get("app_id")
                .and_then(|v| v.as_str())
                .unwrap_or("os")
                .to_string(),
            expert_id: payload
                .get("expert_id")
                .and_then(|v| v.as_str())
                .unwrap_or("ava")
                .to_string(),
            session_id: payload
                .get("session_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            device_id: payload
                .get("device_id")
                .and_then(|v| v.as_str())
                .unwrap_or("server")
                .to_string(),
            scope: payload
                .get("scope")
                .and_then(|v| v.as_str())
                .unwrap_or("personal")
                .to_string(),
            source: payload
                .get("source")
                .and_then(|v| v.as_str())
                .unwrap_or("chat")
                .to_string(),
            wing: payload
                .get("wing")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            hall: payload
                .get("hall")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            room: payload
                .get("room")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            entry_title: payload
                .get("entry_title")
                .and_then(|v| v.as_str())
                .map(|value| value.trim())
                .filter(|value| !value.is_empty())
                .map(|value| value.to_string())
                .unwrap_or_else(|| derive_entry_title(&content)),
            memory_type: payload
                .get("memory_type")
                .and_then(|v| v.as_str())
                .unwrap_or("event")
                .to_string(),
            content,
            tags: normalize_tags(payload.get("tags")),
            content_json: payload
                .get("content_json")
                .filter(|v| !v.is_null())
                .cloned(),
            confidence: payload.get("confidence").and_then(|v| v.as_f64()),
            provenance_refs: payload
                .get("provenance_refs")
                .filter(|v| !v.is_null())
                .cloned(),
            derivation_method: payload
                .get("derivation_method")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            status: payload
                .get("status")
                .and_then(|v| v.as_str())
                .unwrap_or("active")
                .to_string(),
            expires_at: payload
                .get("expires_at")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            usage_count: payload
                .get("usage_count")
                .and_then(|v| v.as_i64())
                .unwrap_or(0)
                .max(0) as i32,
            last_used_at: payload
                .get("last_used_at")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            promoted_from: payload
                .get("promoted_from")
                .and_then(|v| v.as_str())
                .map(str::to_string),
        })
    }

    fn seed(&self) -> DerivationSeed {
        DerivationSeed {
            user_id: self.user_id.clone(),
            tenant_id: self.tenant_id.clone(),
            app_id: self.app_id.clone(),
            session_id: self.session_id.clone(),
            scope: self.scope.clone(),
            source: self.source.clone(),
            wing: self.wing.clone(),
            hall: self.hall.clone(),
            room: self.room.clone(),
            memory_type: self.memory_type.clone(),
            derivation_method: self.derivation_method.clone(),
        }
    }
}

async fn insert_record_only(
    pool: &sqlx::PgPool,
    input: &SaveRecordInput,
) -> Result<i32, sqlx::Error> {
    sqlx::query_scalar::<_, i32>("INSERT INTO scoped_memory (user_id, tenant_id, app_id, expert_id, session_id, device_id, scope, source, wing, hall, room, entry_title, memory_type, content, tags_json, content_json, confidence, provenance_refs, derivation_method, status, expires_at, usage_count, last_used_at, promoted_from) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, CAST($21 AS TIMESTAMP), $22, CAST($23 AS TIMESTAMP), $24) RETURNING id")
        .bind(&input.user_id)
        .bind(&input.tenant_id)
        .bind(&input.app_id)
        .bind(&input.expert_id)
        .bind(&input.session_id)
        .bind(&input.device_id)
        .bind(&input.scope)
        .bind(&input.source)
        .bind(&input.wing)
        .bind(&input.hall)
        .bind(&input.room)
        .bind(&input.entry_title)
        .bind(&input.memory_type)
        .bind(&input.content)
        .bind(if input.tags.is_empty() {
            None
        } else {
            Some(
                Value::Array(
                    input
                        .tags
                        .iter()
                        .map(|tag| Value::String(tag.clone()))
                        .collect(),
                )
                .to_string(),
            )
        })
        .bind(input.content_json.as_ref().map(|value| value.to_string()))
        .bind(input.confidence)
        .bind(input.provenance_refs.as_ref().map(|value| value.to_string()))
        .bind(input.derivation_method.as_deref())
        .bind(&input.status)
        .bind(input.expires_at.as_deref())
        .bind(input.usage_count)
        .bind(input.last_used_at.as_deref())
        .bind(input.promoted_from.as_deref())
        .fetch_one(pool)
        .await
}

fn save_record_response(input: &SaveRecordInput, record_id: i32, derived: Vec<Value>) -> Value {
    serde_json::json!({
        "status": "success",
        "action": "memory_record_saved",
        "record_id": record_id,
        "scope": input.scope,
        "user_id": input.user_id,
        "memory_type": input.memory_type,
        "wing": input.wing,
        "hall": input.hall,
        "room": input.room,
        "entry_title": input.entry_title,
        "usage_count": input.usage_count,
        "promoted_from": input.promoted_from,
        "derived": derived
    })
}

fn should_skip_auto_derivation(seed: &DerivationSeed) -> bool {
    seed.source == "recursive_compression"
        || matches!(
            seed.memory_type.as_str(),
            "working_summary" | "summary" | "learned_heuristic"
        )
        || seed
            .derivation_method
            .as_deref()
            .map(|value| value.starts_with("recursive_"))
            .unwrap_or(false)
}

fn derivation_filters(kind: &str, seed: &DerivationSeed) -> Value {
    match kind {
        "session" => serde_json::json!({
            "user_id": seed.user_id,
            "tenant_id": seed.tenant_id,
            "app_id": seed.app_id,
            "session_id": seed.session_id,
            "scope": seed.scope
        }),
        "room" => serde_json::json!({
            "user_id": seed.user_id,
            "tenant_id": seed.tenant_id,
            "app_id": seed.app_id,
            "wing": seed.wing,
            "hall": seed.hall,
            "room": seed.room,
            "scope": seed.scope
        }),
        "project" => serde_json::json!({
            "user_id": seed.user_id,
            "tenant_id": seed.tenant_id,
            "app_id": seed.app_id,
            "wing": seed.wing,
            "scope": seed.scope
        }),
        _ => serde_json::json!({
            "user_id": seed.user_id,
            "tenant_id": seed.tenant_id,
            "app_id": seed.app_id,
            "scope": seed.scope
        }),
    }
}

fn raw_record_conditions() -> &'static [&'static str] {
    &[
        "status = 'active'",
        "(expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)",
        "source <> 'recursive_compression'",
        "memory_type <> 'working_summary'",
    ]
}

async fn fetch_latest_summary_timestamp(
    pool: &sqlx::PgPool,
    filters: &ScopedMemoryFilters,
    tag: &str,
) -> Result<Option<chrono::NaiveDateTime>, sqlx::Error> {
    let (where_clause, bind_values) = filters.build_where_clause();
    let sql = format!(
        "SELECT timestamp FROM scoped_memory WHERE {} AND status = 'active' AND source = 'recursive_compression' AND tags_json ILIKE '%{}%' ORDER BY timestamp DESC LIMIT 1",
        where_clause, tag
    );
    let mut query = sqlx::query(&sql);
    for value in &bind_values {
        query = query.bind(value);
    }
    query
        .fetch_optional(pool)
        .await
        .map(|row| row.map(|row| row.get::<chrono::NaiveDateTime, _>("timestamp")))
}

async fn count_rows_since(
    pool: &sqlx::PgPool,
    filters: &ScopedMemoryFilters,
    extra_conditions: &[&str],
    since: Option<chrono::NaiveDateTime>,
) -> Result<i64, sqlx::Error> {
    let (where_clause, bind_values) = filters.build_where_clause();
    let mut conditions = vec![where_clause];
    conditions.extend(extra_conditions.iter().map(|value| value.to_string()));
    if since.is_some() {
        conditions.push(format!("timestamp > ${}", bind_values.len() + 1));
    }
    let sql = format!(
        "SELECT COUNT(*) AS count FROM scoped_memory WHERE {}",
        conditions.join(" AND ")
    );
    let mut query = sqlx::query_scalar::<_, i64>(&sql);
    for value in &bind_values {
        query = query.bind(value);
    }
    if let Some(since) = since {
        query = query.bind(since);
    }
    query.fetch_one(pool).await
}

async fn should_derive_kind(
    pool: &sqlx::PgPool,
    kind: &str,
    seed: &DerivationSeed,
    force: bool,
) -> Result<bool, sqlx::Error> {
    if force {
        return Ok(true);
    }

    let payload = derivation_filters(kind, seed);
    let filters = ScopedMemoryFilters::from_payload(&payload);
    let latest_summary =
        fetch_latest_summary_timestamp(pool, &filters, compression_kind_tag(kind)).await?;
    let raw_count =
        count_rows_since(pool, &filters, raw_record_conditions(), latest_summary).await?;

    let threshold_met = match kind {
        "session" => !seed.session_id.is_empty() && raw_count >= 6,
        "room" => !seed.room.is_empty() && raw_count >= 10,
        "project" => !seed.wing.is_empty() && raw_count >= 14,
        _ => false,
    };

    if threshold_met {
        return Ok(true);
    }

    match kind {
        "room" => {
            let session_summary_count = count_rows_since(
                pool,
                &filters,
                &[
                    "status = 'active'",
                    "(expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)",
                    "source = 'recursive_compression'",
                    "tags_json ILIKE '%session_summary%'",
                ],
                latest_summary,
            )
            .await?;
            Ok(session_summary_count >= 2)
        }
        "project" => {
            let room_summary_count = count_rows_since(
                pool,
                &filters,
                &[
                    "status = 'active'",
                    "(expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)",
                    "source = 'recursive_compression'",
                    "tags_json ILIKE '%room_summary%'",
                ],
                latest_summary,
            )
            .await?;
            Ok(room_summary_count >= 2)
        }
        _ => Ok(false),
    }
}

async fn maybe_run_continuous_derivation(
    pool: &sqlx::PgPool,
    seed: &DerivationSeed,
    force: bool,
) -> Vec<Value> {
    if should_skip_auto_derivation(seed) {
        return Vec::new();
    }

    let mut derived = Vec::new();

    if should_derive_kind(pool, "session", seed, force)
        .await
        .unwrap_or(false)
    {
        let payload = derivation_filters("session", seed);
        if let Ok(value) = build_recursive_summary(pool, "session", &payload).await {
            if value.get("error").is_none() {
                derived.push(value);
            }
        }
    }

    if should_derive_kind(pool, "room", seed, force)
        .await
        .unwrap_or(false)
    {
        let payload = derivation_filters("room", seed);
        if let Ok(value) = build_recursive_summary(pool, "room", &payload).await {
            if value.get("error").is_none() {
                derived.push(value);
            }
        }
    }

    if should_derive_kind(pool, "project", seed, force)
        .await
        .unwrap_or(false)
    {
        let payload = derivation_filters("project", seed);
        if let Ok(value) = build_recursive_summary(pool, "project", &payload).await {
            if value.get("error").is_none() {
                derived.push(value);
            }
        }
    }

    derived
}

async fn build_recursive_summary(
    pool: &sqlx::PgPool,
    kind: &str,
    payload: &Value,
) -> Result<Value, sqlx::Error> {
    let filters = ScopedMemoryFilters::from_payload(payload);
    let source_limit = clamp_i64(
        payload
            .get("source_limit")
            .and_then(|value| value.as_i64())
            .unwrap_or(80),
        10,
        200,
    );
    let title_override = payload
        .get("entry_title")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);

    let rows = fetch_scoped_rows(
        pool,
        &filters,
        source_limit,
        "timestamp DESC",
        &[
            "status = 'active'",
            "(expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)",
        ],
    )
    .await?;

    if rows.is_empty() {
        return Ok(serde_json::json!({ "error": "No source records found to compress" }));
    }

    let summaries: Vec<&sqlx::postgres::PgRow> = rows
        .iter()
        .filter(|row| is_summary_entry(row, &row_tags(row)))
        .collect();
    let decisions: Vec<&sqlx::postgres::PgRow> = rows
        .iter()
        .filter(|row| is_decision_entry(row, &row_tags(row)))
        .collect();
    let preferences: Vec<&sqlx::postgres::PgRow> = rows
        .iter()
        .filter(|row| is_preference_entry(row, &row_tags(row)))
        .collect();
    let open_loops: Vec<&sqlx::postgres::PgRow> = rows
        .iter()
        .filter(|row| is_open_loop_entry(row, &row_tags(row)))
        .collect();
    let events: Vec<&sqlx::postgres::PgRow> = rows
        .iter()
        .filter(|row| {
            row.get::<String, _>("memory_type")
                .eq_ignore_ascii_case("event")
        })
        .collect();

    let operational = join_lines(
        &bullet_lines(&summaries, 4),
        "No prior operational summaries captured.",
    );
    let decisions_text = join_lines(
        &bullet_lines(&decisions, 6),
        "No durable decisions captured.",
    );
    let preferences_text = join_lines(
        &bullet_lines(&preferences, 6),
        "No stable preferences detected.",
    );
    let loops_text = join_lines(&bullet_lines(&open_loops, 8), "No open loops pending.");
    let events_text = join_lines(
        &bullet_lines(&events, 8),
        "No recent events worth compressing.",
    );

    let summary_content = format!(
        "Operational Summary\n{}\n\nDecisions Taken\n{}\n\nPreferences Detected\n{}\n\nOpen Loops\n{}\n\nRecent Events\n{}",
        operational, decisions_text, preferences_text, loops_text, events_text
    );

    let source = &rows[0];
    let promoted_from: Vec<i32> = rows.iter().map(|row| row.get::<i32, _>("id")).collect();
    let scope = source.get::<String, _>("scope");
    let app_id = source.get::<String, _>("app_id");
    let session_id = source.get::<String, _>("session_id");
    let wing = source.get::<String, _>("wing");
    let hall = source.get::<String, _>("hall");
    let room = source.get::<String, _>("room");
    let default_title = match kind {
        "session" => format!("session summary {}", session_id),
        "room" => format!("room summary {} / {}", hall, room),
        "project" => format!("project summary {}", wing),
        _ => "recursive summary".to_string(),
    };

    let summary_payload = serde_json::json!({
        "user_id": source.get::<String, _>("user_id"),
        "tenant_id": source.get::<String, _>("tenant_id"),
        "app_id": app_id,
        "expert_id": source.get::<String, _>("expert_id"),
        "session_id": session_id,
        "device_id": source.get::<String, _>("device_id"),
        "scope": scope,
        "source": "recursive_compression",
        "wing": wing,
        "hall": hall,
        "room": room,
        "entry_title": title_override.unwrap_or(default_title),
        "memory_type": "working_summary",
        "content": summary_content,
        "tags": ["summary", "working_summary", "recursive_summary", compression_kind_tag(kind)],
        "content_json": {
            "compression_kind": kind,
            "source_count": rows.len(),
            "sections": {
                "operational_summary": operational,
                "decisions_taken": decisions_text,
                "preferences_detected": preferences_text,
                "open_loops": loops_text,
                "recent_events": events_text
            }
        },
        "confidence": 0.78,
        "derivation_method": format!("recursive_{}_compression", kind),
        "promoted_from": serde_json::json!(promoted_from).to_string()
    });
    let input = match SaveRecordInput::from_payload(&summary_payload) {
        Ok(input) => input,
        Err(error) => return Ok(error),
    };
    let response = match insert_record_only(pool, &input).await {
        Ok(record_id) => {
            crate::query_cache::invalidate_all();
            save_record_response(&input, record_id, Vec::new())
        }
        Err(error) => serde_json::json!({ "error": format!("DB error: {}", error) }),
    };

    Ok(serde_json::json!({
        "status": if response.get("error").is_some() { "error" } else { "success" },
        "compression_kind": kind,
        "source_count": rows.len(),
        "summary": response
    }))
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

struct ScopedMemoryCandidateView<'a> {
    content: &'a str,
    entry_title: &'a str,
    memory_type: &'a str,
    wing: &'a str,
    hall: &'a str,
    room: &'a str,
    tags: &'a [String],
}

fn score_scoped_memory_candidate(
    query_tokens: &HashSet<String>,
    recency_rank: usize,
    total_candidates: usize,
    candidate: ScopedMemoryCandidateView<'_>,
) -> (f64, usize) {
    let mut combined = String::new();
    combined.push_str(candidate.entry_title);
    combined.push(' ');
    combined.push_str(candidate.content);
    combined.push(' ');
    combined.push_str(candidate.memory_type);
    combined.push(' ');
    combined.push_str(candidate.wing);
    combined.push(' ');
    combined.push_str(candidate.hall);
    combined.push(' ');
    combined.push_str(candidate.room);
    if !candidate.tags.is_empty() {
        combined.push(' ');
        combined.push_str(&candidate.tags.join(" "));
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
    let structure_bonus = if !candidate.room.is_empty() {
        0.08
    } else if !candidate.hall.is_empty() {
        0.05
    } else if !candidate.wing.is_empty() {
        0.03
    } else {
        0.0
    };
    let title_bonus = if tokenize_memory(candidate.entry_title)
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
        "confidence": row_confidence(row),
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
    let auto_derive = payload
        .get("auto_derive")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);

    let input = match SaveRecordInput::from_payload(&payload) {
        Ok(input) => input,
        Err(error) => return error,
    };

    match insert_record_only(pool, &input).await {
        Ok(record_id) => {
            crate::query_cache::invalidate_all();
            let derived = if auto_derive {
                maybe_run_continuous_derivation(pool, &input.seed(), false).await
            } else {
                Vec::new()
            };
            save_record_response(&input, record_id, derived)
        }
        Err(e) => serde_json::json!({ "error": format!("DB error: {}", e) }),
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

pub async fn compress_session(pool: &sqlx::PgPool, payload: Value) -> Value {
    match build_recursive_summary(pool, "session", &payload).await {
        Ok(value) => value,
        Err(e) => serde_json::json!({ "error": format!("Compression error: {}", e) }),
    }
}

pub async fn compress_room(pool: &sqlx::PgPool, payload: Value) -> Value {
    match build_recursive_summary(pool, "room", &payload).await {
        Ok(value) => value,
        Err(e) => serde_json::json!({ "error": format!("Compression error: {}", e) }),
    }
}

pub async fn compress_project(pool: &sqlx::PgPool, payload: Value) -> Value {
    match build_recursive_summary(pool, "project", &payload).await {
        Ok(value) => value,
        Err(e) => serde_json::json!({ "error": format!("Compression error: {}", e) }),
    }
}

pub async fn derive_memory(pool: &sqlx::PgPool, payload: Value) -> Value {
    let filters = ScopedMemoryFilters::from_payload(&payload);
    if !filters.has_required_scope_filter() {
        return scope_error_response(REQUIRED_SCOPE_ERROR);
    }

    let level = payload
        .get("level")
        .and_then(|value| value.as_str())
        .unwrap_or("all");
    let force = payload
        .get("force")
        .and_then(|value| value.as_bool())
        .unwrap_or(true);

    let seed = DerivationSeed {
        user_id: filters.user_id.clone().unwrap_or_default(),
        tenant_id: filters
            .tenant_id
            .clone()
            .unwrap_or_else(|| "default".to_string()),
        app_id: filters.app_id.clone().unwrap_or_else(|| "os".to_string()),
        session_id: filters.session_id.clone().unwrap_or_default(),
        scope: filters
            .scope
            .clone()
            .unwrap_or_else(|| "personal".to_string()),
        source: "manual_derivation".to_string(),
        wing: filters.wing.clone().unwrap_or_default(),
        hall: filters.hall.clone().unwrap_or_default(),
        room: filters.room.clone().unwrap_or_default(),
        memory_type: payload
            .get("memory_type")
            .and_then(|value| value.as_str())
            .unwrap_or("event")
            .to_string(),
        derivation_method: Some("manual_derivation".to_string()),
    };

    let derived = match level {
        "session" => {
            let payload = derivation_filters("session", &seed);
            build_recursive_summary(pool, "session", &payload)
                .await
                .map(|value| vec![value])
        }
        "room" => {
            let payload = derivation_filters("room", &seed);
            build_recursive_summary(pool, "room", &payload)
                .await
                .map(|value| vec![value])
        }
        "project" => {
            let payload = derivation_filters("project", &seed);
            build_recursive_summary(pool, "project", &payload)
                .await
                .map(|value| vec![value])
        }
        _ => Ok(maybe_run_continuous_derivation(pool, &seed, force).await),
    };

    match derived {
        Ok(entries) => serde_json::json!({
            "status": "success",
            "level": level,
            "derived": entries
        }),
        Err(e) => serde_json::json!({ "error": format!("Derivation error: {}", e) }),
    }
}

pub async fn recall_recursive_context(pool: &sqlx::PgPool, payload: Value) -> Value {
    if let Some(cached) = maybe_cached("recall_recursive_context", &payload) {
        return cached;
    }

    let filters = ScopedMemoryFilters::from_payload(&payload);
    if !filters.has_required_scope_filter() {
        return scope_error_response(REQUIRED_SCOPE_ERROR);
    }

    let summary_filters = filters.clone();
    let summary_rows = match fetch_scoped_rows(
        pool,
        &summary_filters,
        60,
        "timestamp DESC",
        &[
            "status = 'active'",
            "(expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)",
            "(memory_type IN ('summary', 'working_summary') OR tags_json ILIKE '%summary%')",
        ],
    )
    .await
    {
        Ok(rows) => rows,
        Err(e) => return serde_json::json!({ "error": format!("Query error: {}", e) }),
    };

    let mut session_summaries = Vec::new();
    let mut room_summaries = Vec::new();
    let mut project_summaries = Vec::new();
    let mut touched_ids = Vec::new();

    for row in &summary_rows {
        let tags = row_tags(row);
        let entry = scoped_memory_row_to_json(row);
        if has_any_tag(&tags, &["session_summary"]) && session_summaries.len() < 3 {
            touched_ids.push(row.get::<i32, _>("id"));
            session_summaries.push(entry);
            continue;
        }
        if has_any_tag(&tags, &["room_summary"]) && room_summaries.len() < 3 {
            touched_ids.push(row.get::<i32, _>("id"));
            room_summaries.push(entry);
            continue;
        }
        if has_any_tag(&tags, &["project_summary"]) && project_summaries.len() < 3 {
            touched_ids.push(row.get::<i32, _>("id"));
            project_summaries.push(entry);
        }
    }

    let working_context = get_working_context(pool, payload.clone()).await;
    let durable_facts = get_durable_facts(pool, payload.clone()).await;
    let recent_events = get_recent_events(pool, payload.clone()).await;

    if !touched_ids.is_empty() {
        let _ = touch_usage(pool, &touched_ids).await;
    }

    let response = serde_json::json!({
        "status": "success",
        "retrieval_strategy": "recursive_context",
        "recursive_context": {
            "project_summaries": project_summaries,
            "room_summaries": room_summaries,
            "session_summaries": session_summaries,
            "working_context": working_context.get("working_context").cloned().unwrap_or(Value::Null),
            "durable_facts": durable_facts.get("entries").cloned().unwrap_or(Value::Array(Vec::new())),
            "recent_events": recent_events.get("entries").cloned().unwrap_or(Value::Array(Vec::new()))
        }
    });
    store_cache("recall_recursive_context", &payload, &response);
    response
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
                        ScopedMemoryCandidateView {
                            content: &content,
                            entry_title: &entry_title,
                            memory_type: &memory_type_value,
                            wing: &wing_value,
                            hall: &hall_value,
                            room: &room_value,
                            tags: &tags,
                        },
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
                        confidence: row_confidence(row),
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
            "confidence": row_confidence(source).unwrap_or(0.8),
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

        let db_url = format!(
            "postgresql://{user}:{password}@127.0.0.1:{host_port}/{database}?sslmode=disable"
        );

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
                        || error_text.contains("unexpected response from SSLRequest")
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

    #[tokio::test]
    async fn saving_enough_events_triggers_automatic_session_derivation() {
        let (pool, _container) = test_pool().await;

        for idx in 0..6 {
            let saved = save_record(
                &pool,
                serde_json::json!({
                    "user_id": "user-derived",
                    "app_id": "vetra",
                    "session_id": "session-derived",
                    "scope": "workspace",
                    "wing": "vetra",
                    "hall": "contracts",
                    "room": "msa",
                    "memory_type": "event",
                    "content": format!("Negotiation event {}", idx)
                }),
            )
            .await;
            assert_eq!(saved["status"], "success", "save_record failed: {}", saved);
        }

        let result = query_records(
            &pool,
            serde_json::json!({
                "user_id": "user-derived",
                "app_id": "vetra",
                "session_id": "session-derived",
                "memory_type": "working_summary"
            }),
        )
        .await;

        let entries = result["entries"].as_array().unwrap();
        assert!(
            entries.iter().any(|entry| {
                let tags = entry["tags"].as_array().cloned().unwrap_or_default();
                tags.iter().any(|tag| tag == "session_summary")
            }),
            "expected an automatically derived session_summary entry: {}",
            result
        );
    }
}
