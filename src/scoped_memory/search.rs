//! Search and timeline reads: semantic (embedding cosine) recall, verbatim/FTS
//! search, and the raw scope timeline. Extracted from `mod.rs` to keep that
//! file under the size guard.

use super::derivation::*;
use super::embedding::{cosine_similarity, parse_embedding, unpack_embedding};
use super::helpers::*;
use serde_json::Value;
use sqlx::Row;

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

    let limit = clamp_i64(
        payload.get("limit").and_then(|v| v.as_i64()).unwrap_or(6),
        1,
        24,
    );
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
        "SELECT id, content, memory_type, entry_title, timestamp, embedding, embedding_b \
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
        // Prefer the BYTEA fast-path (zero JSON parse); fall back to the TEXT column
        // for rows written before migration 10.
        let vec = match row
            .get::<Option<Vec<u8>>, _>("embedding_b")
            .as_deref()
            .and_then(unpack_embedding)
        {
            Some(v) => v,
            None => {
                let raw: Option<String> = row.get("embedding");
                let Some(raw) = raw else { continue };
                let Some(v) = parse_embedding(&raw) else {
                    continue;
                };
                v
            }
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
