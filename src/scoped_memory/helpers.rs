use serde_json::Value;
use sqlx::Row;
use std::collections::HashSet;

pub(super) fn maybe_cached(action: &str, payload: &Value) -> Option<Value> {
    crate::query_cache::get(action, payload)
}

pub(super) fn store_cache(action: &str, payload: &Value, value: &Value) {
    if value.get("error").is_none() {
        crate::query_cache::put(action, payload, value);
    }
}

pub(super) const SCOPED_MEMORY_SELECT_COLUMNS: &str = "id, user_id, tenant_id, app_id, expert_id, session_id, device_id, scope, source, wing, hall, room, entry_title, memory_type, content, tags_json, content_json, confidence, provenance_refs, derivation_method, status, expires_at, timestamp, usage_count, last_used_at, promoted_from";
pub(super) const REQUIRED_SCOPE_ERROR: &str = "At least one filter (user_id, tenant_id, app_id, expert_id, session_id, scope, wing, hall, room, memory_type) is required. Global reads are forbidden.";
pub(super) const TIMELINE_SCOPE_ERROR: &str = "At least one filter (user_id, tenant_id, app_id, session_id, wing, hall, room) is required. Global reads are forbidden.";

#[derive(Debug, Clone)]
pub(super) struct ScopedMemorySearchCandidate {
    pub(super) id: i32,
    pub(super) user_id: String,
    pub(super) tenant_id: String,
    pub(super) app_id: String,
    pub(super) expert_id: String,
    pub(super) session_id: String,
    pub(super) device_id: String,
    pub(super) scope: String,
    pub(super) source: String,
    pub(super) wing: String,
    pub(super) hall: String,
    pub(super) room: String,
    pub(super) entry_title: String,
    pub(super) memory_type: String,
    pub(super) content: String,
    pub(super) tags: Vec<String>,
    pub(super) confidence: Option<f64>,
    pub(super) status: String,
    pub(super) timestamp: String,
    pub(super) score: f64,
    pub(super) overlap_hits: usize,
}

#[derive(Debug, Clone, Default)]
pub(super) struct ScopedMemoryFilters {
    pub(super) user_id: Option<String>,
    pub(super) tenant_id: Option<String>,
    pub(super) app_id: Option<String>,
    pub(super) expert_id: Option<String>,
    pub(super) session_id: Option<String>,
    pub(super) scope: Option<String>,
    pub(super) wing: Option<String>,
    pub(super) hall: Option<String>,
    pub(super) room: Option<String>,
    pub(super) memory_type: Option<String>,
    pub(super) status: Option<String>,
}

pub(super) fn clamp_i64(value: i64, min: i64, max: i64) -> i64 {
    value.max(min).min(max)
}

pub(super) fn push_text_condition(
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
    pub(super) fn from_payload(payload: &Value) -> Self {
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

    pub(super) fn has_required_scope_filter(&self) -> bool {
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

    pub(super) fn has_timeline_scope_filter(&self) -> bool {
        self.user_id.is_some()
            || self.tenant_id.is_some()
            || self.app_id.is_some()
            || self.session_id.is_some()
            || self.wing.is_some()
            || self.hall.is_some()
            || self.room.is_some()
    }

    pub(super) fn build_where_clause(&self) -> (String, Vec<String>) {
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

pub(super) fn trim_message_for_budget(content: &str, max_chars: usize) -> String {
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

pub(super) fn tokenize_memory(text: &str) -> HashSet<String> {
    text.to_lowercase()
        .split(|c: char| !c.is_alphanumeric())
        .filter(|token| token.len() >= 3)
        .take(96)
        .map(|token| token.to_string())
        .collect()
}

pub(super) fn normalize_tags(value: Option<&Value>) -> Vec<String> {
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

pub(super) fn parse_json_string_array(raw: Option<String>) -> Vec<String> {
    raw.and_then(|value| serde_json::from_str::<Vec<String>>(&value).ok())
        .unwrap_or_default()
}

pub(super) fn parse_json_value(raw: Option<String>) -> Option<Value> {
    raw.and_then(|value| serde_json::from_str::<Value>(&value).ok())
}

pub(super) fn scope_error_response(message: &str) -> Value {
    serde_json::json!({ "error": message })
}

pub(super) fn format_timestamp(value: chrono::NaiveDateTime) -> String {
    value.format("%Y-%m-%d %H:%M:%S").to_string()
}

pub(super) fn row_timestamp_string(row: &sqlx::postgres::PgRow, column: &str) -> String {
    format_timestamp(row.get::<chrono::NaiveDateTime, _>(column))
}

pub(super) fn row_optional_timestamp_string(
    row: &sqlx::postgres::PgRow,
    column: &str,
) -> Option<String> {
    row.get::<Option<chrono::NaiveDateTime>, _>(column)
        .map(format_timestamp)
}

pub(super) fn derive_entry_title(content: &str) -> String {
    let first_line = content.lines().next().unwrap_or("").trim();
    if first_line.is_empty() {
        return "memory entry".to_string();
    }
    trim_message_for_budget(first_line, 96)
}

pub(super) fn build_memory_snippet(content: &str) -> String {
    trim_message_for_budget(content, 220)
}

pub(super) fn row_tags(row: &sqlx::postgres::PgRow) -> Vec<String> {
    parse_json_string_array(row.get::<Option<String>, _>("tags_json"))
}

pub(super) fn row_content_json(row: &sqlx::postgres::PgRow) -> Option<Value> {
    parse_json_value(row.get::<Option<String>, _>("content_json"))
}

pub(super) fn row_confidence(row: &sqlx::postgres::PgRow) -> Option<f64> {
    row.get::<Option<f32>, _>("confidence")
        .map(|value| value as f64)
}

pub(super) fn has_any_tag(tags: &[String], expected: &[&str]) -> bool {
    tags.iter()
        .any(|tag| expected.iter().any(|candidate| tag == candidate))
}

pub(super) fn is_preference_entry(row: &sqlx::postgres::PgRow, tags: &[String]) -> bool {
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

pub(super) fn is_summary_entry(row: &sqlx::postgres::PgRow, tags: &[String]) -> bool {
    let memory_type = row.get::<String, _>("memory_type").to_lowercase();
    matches!(memory_type.as_str(), "summary" | "working_summary")
        || has_any_tag(tags, &["summary", "working_summary", "session_summary"])
}

pub(super) fn is_decision_entry(row: &sqlx::postgres::PgRow, tags: &[String]) -> bool {
    let memory_type = row.get::<String, _>("memory_type").to_lowercase();
    matches!(memory_type.as_str(), "decision" | "fact")
        && has_any_tag(tags, &["decision", "resolved", "chosen"])
        || memory_type == "decision"
}

pub(super) fn is_open_loop_entry(row: &sqlx::postgres::PgRow, tags: &[String]) -> bool {
    let memory_type = row.get::<String, _>("memory_type").to_lowercase();
    matches!(memory_type.as_str(), "open_loop" | "todo" | "task")
        || has_any_tag(tags, &["open_loop", "followup", "pending", "todo"])
}

pub(super) fn bullet_lines(rows: &[&sqlx::postgres::PgRow], limit: usize) -> Vec<String> {
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

/// Bullet list for prior summary rows. Unlike `bullet_lines`, this NEVER copies
/// the body — only the title + timestamp. Body-copying causes a recursive
/// amplification flywheel: every summary's content begins with a boilerplate
/// header ("Operational Summary\n..."), so embedding the prefix verbatim made
/// each new summary read like "- session summary X: Operational Summary, -
/// session summary Y: Operational Summary, ..." compounding each pass and
/// blowing up the prompt context. Reference-only entries break the cycle.
pub(super) fn summary_reference_lines(
    rows: &[&sqlx::postgres::PgRow],
    limit: usize,
) -> Vec<String> {
    rows.iter()
        .take(limit)
        .map(|row| {
            let title = row.get::<String, _>("entry_title");
            let ts = row_timestamp_string(row, "timestamp");
            if title.trim().is_empty() {
                format!("- (untitled summary @ {})", ts)
            } else {
                format!("- {} @ {}", title.trim(), ts)
            }
        })
        .collect()
}

pub(super) fn join_lines(lines: &[String], empty_fallback: &str) -> String {
    if lines.is_empty() {
        empty_fallback.to_string()
    } else {
        lines.join("\n")
    }
}

pub(super) fn compression_kind_tag(kind: &str) -> &'static str {
    match kind {
        "session" => "session_summary",
        "room" => "room_summary",
        "project" => "project_summary",
        _ => "recursive_summary",
    }
}
