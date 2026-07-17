//! Recursive compression + promotion: session/room/project summary building,
//! manual derivation, and memory promotion up the hierarchy. Extracted from
//! `mod.rs` to keep that file under the size guard.

use super::derivation::*;
use super::helpers::*;
use super::parsing::*;
use serde_json::Value;
use sqlx::Row;

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

    let result = super::save_record(
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
