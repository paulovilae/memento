#[allow(unused_imports)]
use super::*;
use super::helpers::*;
use super::parsing::*;
use serde_json::Value;
use sqlx::Row;
use std::collections::HashSet;

pub(super) fn should_skip_auto_derivation(seed: &DerivationSeed) -> bool {
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

pub(super) fn derivation_filters(kind: &str, seed: &DerivationSeed) -> Value {
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

pub(super) fn raw_record_conditions() -> &'static [&'static str] {
    &[
        "status = 'active'",
        "(expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)",
        "source <> 'recursive_compression'",
        "memory_type <> 'working_summary'",
    ]
}

pub(super) async fn fetch_latest_summary_timestamp(
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

pub(super) async fn count_rows_since(
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

pub(super) async fn should_derive_kind(
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

pub(super) async fn maybe_run_continuous_derivation(
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

pub(super) async fn build_recursive_summary(
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
        &summary_reference_lines(&summaries, 4),
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

pub(super) fn is_durable_fact_entry(row: &sqlx::postgres::PgRow, tags: &[String]) -> bool {
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

pub(super) fn has_positive_outcome(row: &sqlx::postgres::PgRow, tags: &[String]) -> bool {
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

pub(super) async fn fetch_scoped_rows(
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

pub(super) async fn fetch_scoped_rows_with_query(
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

pub(super) struct ScopedMemoryCandidateView<'a> {
    pub(super) content: &'a str,
    pub(super) entry_title: &'a str,
    pub(super) memory_type: &'a str,
    pub(super) wing: &'a str,
    pub(super) hall: &'a str,
    pub(super) room: &'a str,
    pub(super) tags: &'a [String],
}

pub(super) fn score_scoped_memory_candidate(
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

pub(super) fn scoped_memory_row_to_json(row: &sqlx::postgres::PgRow) -> Value {
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

pub(super) fn row_ids(rows: &[sqlx::postgres::PgRow]) -> Vec<i32> {
    rows.iter().map(|row| row.get::<i32, _>("id")).collect()
}

pub(super) async fn touch_usage(pool: &sqlx::PgPool, ids: &[i32]) -> Result<(), sqlx::Error> {
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

