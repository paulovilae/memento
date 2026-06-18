//! Recall telemetry: closes the data loop for embedder reranker fine-tune.
//!
//! Two surfaces:
//! - `log_recall` (internal): called from `semantic_recall` to persist the
//!   query embedding + returned ids. Best-effort; never blocks the recall.
//! - `recall_feedback` (IPC action): called by downstream consumers (Hera,
//!   apps) to report which of the returned ids were actually useful. Joined
//!   on `request_id`, these pairs become contrastive training tuples.
//!
//! The schema lives in `migrations::migration_9_recall_telemetry`.

use serde_json::Value;
use tracing::warn;

/// Build a request_id when the caller didn't supply one. Nanosecond resolution
/// is collision-free in a single process; we include the pid as a suffix to
/// keep multi-process deployments safe.
pub fn generate_request_id() -> String {
    let nanos = chrono::Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_else(|| chrono::Utc::now().timestamp_millis() * 1_000_000);
    format!("recall-{}-{}", nanos, std::process::id())
}

/// Persist a recall event. Best-effort: a write failure logs a warning but
/// does not surface to the caller (the recall itself already succeeded).
#[allow(clippy::too_many_arguments)]
pub async fn log_recall(
    pool: &sqlx::PgPool,
    request_id: &str,
    app_id: Option<&str>,
    user_id: Option<&str>,
    tenant_id: Option<&str>,
    session_id: Option<&str>,
    query_text: Option<&str>,
    query_embedding: &[f32],
    returned_ids: &Value,
    candidates_scanned: i32,
) {
    let embedding_json = match serde_json::to_string(query_embedding) {
        Ok(s) => s,
        Err(e) => {
            warn!(error=%e, "recall_telemetry: failed to serialize query_embedding; skip log");
            return;
        }
    };
    let res = sqlx::query(
        "INSERT INTO recall_log \
         (request_id, app_id, user_id, tenant_id, session_id, query_text, \
          query_embedding, returned_ids, candidates_scanned) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
    )
    .bind(request_id)
    .bind(app_id.unwrap_or("os"))
    .bind(user_id)
    .bind(tenant_id)
    .bind(session_id)
    .bind(query_text)
    .bind(&embedding_json)
    .bind(returned_ids)
    .bind(candidates_scanned)
    .execute(pool)
    .await;
    if let Err(e) = res {
        warn!(error=%e, request_id=%request_id, "recall_telemetry: log_recall insert failed");
    }
}

/// IPC handler for `recall_feedback`. Caller reports which returned ids were
/// actually used (cited in response, accepted by user, etc.). Idempotent at
/// the row level — multiple feedback events for the same request_id are
/// allowed and append.
pub async fn recall_feedback(pool: &sqlx::PgPool, payload: Value) -> Value {
    let request_id = match payload.get("request_id").and_then(|v| v.as_str()) {
        Some(s) if !s.is_empty() => s.to_string(),
        _ => {
            return serde_json::json!({ "error": "Missing or empty 'request_id'" });
        }
    };
    let cited_ids = match payload.get("cited_ids") {
        Some(v) if v.is_array() => v.clone(),
        _ => serde_json::json!([]),
    };
    let feedback_kind = payload
        .get("feedback_kind")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or("cited")
        .to_string();
    let notes = payload
        .get("notes")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let res = sqlx::query(
        "INSERT INTO recall_feedback (request_id, cited_ids, feedback_kind, notes) \
         VALUES ($1, $2, $3, $4) RETURNING id",
    )
    .bind(&request_id)
    .bind(&cited_ids)
    .bind(&feedback_kind)
    .bind(notes.as_deref())
    .fetch_one(pool)
    .await;

    match res {
        Ok(row) => {
            use sqlx::Row;
            let id: i64 = row.get("id");
            serde_json::json!({
                "status": "success",
                "id": id,
                "request_id": request_id,
                "feedback_kind": feedback_kind,
            })
        }
        Err(e) => serde_json::json!({ "error": format!("recall_feedback insert failed: {}", e) }),
    }
}
