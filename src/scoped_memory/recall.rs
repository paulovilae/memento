//! Context recall: recent events, working context, and the combined recursive
//! context (project/room/session summaries + working context + durable facts +
//! recent events in one call). Extracted from `mod.rs` to keep that file under
//! the size guard.

use super::derivation::*;
use super::helpers::*;
use serde_json::Value;
use sqlx::Row;

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

    // Optional compaction: skip_working_context eliminates ~40% token duplication
    // (working_context mirrors top-level durable_facts + recent_events).
    // max_durable_facts / max_recent_events cap sub-call limits for smaller contexts.
    let skip_wc = payload
        .get("skip_working_context")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let mut durable_payload = payload.clone();
    if let Some(n) = payload.get("max_durable_facts").and_then(|v| v.as_i64()) {
        durable_payload["limit"] = n.into();
    }
    let mut events_payload = payload.clone();
    if let Some(n) = payload.get("max_recent_events").and_then(|v| v.as_i64()) {
        events_payload["limit"] = n.into();
    }
    let working_context_data = if skip_wc {
        Value::Null
    } else {
        get_working_context(pool, payload.clone())
            .await
            .get("working_context")
            .cloned()
            .unwrap_or(Value::Null)
    };
    let durable_facts = super::get_durable_facts(pool, durable_payload).await;
    let recent_events = get_recent_events(pool, events_payload).await;

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
            "working_context": working_context_data,
            "durable_facts": durable_facts.get("entries").cloned().unwrap_or(Value::Array(Vec::new())),
            "recent_events": recent_events.get("entries").cloned().unwrap_or(Value::Array(Vec::new()))
        }
    });
    store_cache("recall_recursive_context", &payload, &response);
    response
}
