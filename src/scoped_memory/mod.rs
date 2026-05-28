// Module declarations
mod helpers;
mod parsing;
mod derivation;

// Import internal types and functions
use helpers::*;
use parsing::*;
use derivation::*;

// External dependencies
use serde_json::Value;
use sqlx::Row;

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

/// Cosine similarity between two equal-length f32 vectors. Vectors from Hera's
/// embed action arrive L2-normalized, so this reduces to a dot product, but we
/// compute the full cosine for robustness against unnormalized callers.
fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }
    let mut dot = 0.0f32;
    let mut na = 0.0f32;
    let mut nb = 0.0f32;
    for i in 0..a.len() {
        dot += a[i] * b[i];
        na += a[i] * a[i];
        nb += b[i] * b[i];
    }
    if na == 0.0 || nb == 0.0 {
        return 0.0;
    }
    dot / (na.sqrt() * nb.sqrt())
}

fn parse_embedding(raw: &str) -> Option<Vec<f32>> {
    let parsed: Vec<f32> = serde_json::from_str(raw).ok()?;
    if parsed.is_empty() {
        None
    } else {
        Some(parsed)
    }
}

/// Semantic recall: given a `query_embedding`, rerank the caller's scope-filtered
/// rows by cosine similarity and return the top-k. Filtering by scope first keeps
/// the candidate set small (tens to hundreds of rows), so a brute-force cosine in
/// Rust needs no ANN index or pgvector extension.
///
/// Telemetry: each call is logged to `recall_log` (best-effort) and the returned
/// JSON includes a `request_id` that downstream consumers should pass back via
/// `recall_feedback` once they know which of the returned ids were useful.
pub async fn semantic_recall(pool: &sqlx::PgPool, payload: Value) -> Value {
    let filters = ScopedMemoryFilters::from_payload(&payload);
    if !filters.has_required_scope_filter() {
        return scope_error_response(REQUIRED_SCOPE_ERROR);
    }

    let query_embedding: Vec<f32> = match payload.get("query_embedding").and_then(|v| v.as_array())
    {
        Some(arr) => arr
            .iter()
            .filter_map(|v| v.as_f64().map(|f| f as f32))
            .collect(),
        None => return serde_json::json!({ "error": "Missing 'query_embedding' array in payload" }),
    };
    if query_embedding.is_empty() {
        return serde_json::json!({ "error": "Empty 'query_embedding'" });
    }

    let request_id = payload
        .get("request_id")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .unwrap_or_else(crate::recall_telemetry::generate_request_id);
    let query_text = payload
        .get("query_text")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let limit = clamp_i64(payload.get("limit").and_then(|v| v.as_i64()).unwrap_or(6), 1, 24);
    // Pull a bounded candidate window (already scope-filtered) to rerank.
    let candidate_cap = clamp_i64(
        payload
            .get("candidate_cap")
            .and_then(|v| v.as_i64())
            .unwrap_or(400),
        24,
        2000,
    );
    let min_score = payload
        .get("min_score")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.25) as f32;

    let (where_clause, bind_values) = filters.build_where_clause();
    let sql = format!(
        "SELECT id, content, memory_type, entry_title, timestamp, embedding \
         FROM scoped_memory \
         WHERE {} AND embedding IS NOT NULL AND status = 'active' \
         AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP) \
         ORDER BY timestamp DESC LIMIT {}",
        where_clause, candidate_cap
    );
    let mut query = sqlx::query(&sql);
    for val in &bind_values {
        query = query.bind(val);
    }
    let rows = match query.fetch_all(pool).await {
        Ok(rows) => rows,
        Err(e) => return serde_json::json!({ "error": format!("Query error: {}", e) }),
    };

    let mut scored: Vec<(f32, Value)> = Vec::new();
    for row in &rows {
        let raw: Option<String> = row.get("embedding");
        let Some(raw) = raw else { continue };
        let Some(vec) = parse_embedding(&raw) else {
            continue;
        };
        let score = cosine_similarity(&query_embedding, &vec);
        if score < min_score {
            continue;
        }
        let content: String = row.get("content");
        let entry = serde_json::json!({
            "id": row.get::<i32, _>("id"),
            "memory_type": row.get::<String, _>("memory_type"),
            "entry_title": row.get::<String, _>("entry_title"),
            "content": content,
            "score": score,
        });
        scored.push((score, entry));
    }

    scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    let entries: Vec<Value> = scored
        .into_iter()
        .take(limit as usize)
        .map(|(_, e)| e)
        .collect();

    // Returned-ids manifest for telemetry: keep id + score only (content lives in
    // scoped_memory and is retrievable by id; storing it again would 5× the row).
    let returned_ids: Vec<Value> = entries
        .iter()
        .filter_map(|e| {
            let id = e.get("id")?;
            let score = e.get("score")?;
            Some(serde_json::json!({ "id": id, "score": score }))
        })
        .collect();
    let returned_ids_value = Value::Array(returned_ids);
    crate::recall_telemetry::log_recall(
        pool,
        &request_id,
        filters.app_id.as_deref(),
        filters.user_id.as_deref(),
        filters.tenant_id.as_deref(),
        filters.session_id.as_deref(),
        query_text.as_deref(),
        &query_embedding,
        &returned_ids_value,
        rows.len() as i32,
    )
    .await;

    serde_json::json!({
        "status": "success",
        "retrieval_strategy": "semantic_cosine",
        "request_id": request_id,
        "count": entries.len(),
        "candidates_scanned": rows.len(),
        "entries": entries
    })
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

    #[tokio::test]
    async fn semantic_recall_logs_telemetry_and_feedback_round_trips() {
        let (pool, _container) = test_pool().await;

        // Seed one row with a known embedding so semantic_recall has a hit.
        let saved = save_record(
            &pool,
            serde_json::json!({
                "user_id": "user-recall",
                "app_id": "vetra",
                "session_id": "sess-1",
                "scope": "personal",
                "content": "Contracts with margin > 30% should be flagged.",
                "memory_type": "preference",
                "embedding": [1.0, 0.0, 0.0, 0.0]
            }),
        )
        .await;
        assert_eq!(saved["status"], "success", "save_record failed: {}", saved);

        // Recall with a query embedding that matches the seeded vector exactly.
        let recall = semantic_recall(
            &pool,
            serde_json::json!({
                "user_id": "user-recall",
                "app_id": "vetra",
                "query_embedding": [1.0, 0.0, 0.0, 0.0],
                "query_text": "flag high-margin contracts",
                "limit": 5
            }),
        )
        .await;
        assert_eq!(recall["status"], "success", "semantic_recall: {}", recall);
        let request_id = recall["request_id"]
            .as_str()
            .expect("request_id missing from semantic_recall response")
            .to_string();
        assert!(!request_id.is_empty());
        let entries = recall["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 1);
        let cited_id = entries[0]["id"].clone();

        // recall_log should now hold one row keyed by request_id.
        let log_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM recall_log WHERE request_id = $1",
        )
        .bind(&request_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(log_count, 1, "recall_log did not record the call");

        // Feedback round-trip: report which id was cited.
        let fb = crate::recall_telemetry::recall_feedback(
            &pool,
            serde_json::json!({
                "request_id": request_id,
                "cited_ids": [cited_id],
                "feedback_kind": "cited"
            }),
        )
        .await;
        assert_eq!(fb["status"], "success", "recall_feedback: {}", fb);
        assert_eq!(fb["request_id"], request_id);

        let fb_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM recall_feedback WHERE request_id = $1",
        )
        .bind(&request_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(fb_count, 1, "recall_feedback did not insert the row");
    }
}
