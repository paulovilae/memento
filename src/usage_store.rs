//! Hera usage measurement store.
//!
//! Three actions:
//!   - `hera_log_usage`    — INSERT one usage event (best-effort, never panics)
//!   - `hera_check_limit`  — verify today's usage vs configured limits for app/user
//!   - `hera_usage_stats`  — aggregated daily stats by app/user
//!
//! All functions take a `&sqlx::PgPool` and a `&serde_json::Value` payload,
//! and return a `serde_json::Value` — same contract as every other store.

use serde_json::{json, Value};
use sqlx::Row;
use tracing::warn;

/// INSERT one usage event. Best-effort: SQL errors are logged but never propagated.
/// payload fields (all optional except described):
///   app_id, user_id, session_id, route_profile, model,
///   prompt_tokens, completion_tokens, total_tokens, is_cloud, latency_ms
pub async fn hera_log_usage(pool: &sqlx::PgPool, payload: &Value) -> Value {
    let app_id = payload
        .get("app_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let user_id = payload
        .get("user_id")
        .and_then(|v| v.as_str())
        .unwrap_or("anonymous")
        .to_string();
    let session_id = payload
        .get("session_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let route_profile = payload
        .get("route_profile")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let model = payload
        .get("model")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let prompt_tokens = payload
        .get("prompt_tokens")
        .and_then(|v| v.as_i64())
        .unwrap_or(0) as i32;
    let completion_tokens = payload
        .get("completion_tokens")
        .and_then(|v| v.as_i64())
        .unwrap_or(0) as i32;
    let total_tokens = payload
        .get("total_tokens")
        .and_then(|v| v.as_i64())
        .unwrap_or(0) as i32;
    let is_cloud = payload
        .get("is_cloud")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let latency_ms: Option<i32> = payload
        .get("latency_ms")
        .and_then(|v| v.as_i64())
        .map(|v| v as i32);

    let result = sqlx::query(
        r#"
        INSERT INTO hera_usage_events
            (app_id, user_id, session_id, route_profile, model,
             prompt_tokens, completion_tokens, total_tokens, is_cloud, latency_ms)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        "#,
    )
    .bind(&app_id)
    .bind(&user_id)
    .bind(&session_id)
    .bind(&route_profile)
    .bind(&model)
    .bind(prompt_tokens)
    .bind(completion_tokens)
    .bind(total_tokens)
    .bind(is_cloud)
    .bind(latency_ms)
    .execute(pool)
    .await;

    if let Err(e) = result {
        warn!(error = %e, app_id = %app_id, "hera_log_usage: INSERT failed (best-effort, ignored)");
    }

    json!({ "ok": true })
}

/// Check today's usage against configured limits for (app_id, user_id?).
/// Returns `{"ok": true}` when within limits or no limit is configured.
/// Returns `{"ok": false, "reason": "..."}` when a limit is exceeded.
pub async fn hera_check_limit(pool: &sqlx::PgPool, payload: &Value) -> Value {
    let app_id = payload
        .get("app_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let user_id = payload
        .get("user_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    // Fetch limits that apply: rows where app_id matches AND
    // (user_id IS NULL  → app-wide limit)  OR  (user_id = $user_id → per-user limit)
    let limits_result = sqlx::query(
        r#"
        SELECT limit_kind, limit_value, user_id
        FROM hera_usage_limits
        WHERE app_id = $1
          AND (user_id IS NULL OR user_id = $2)
        "#,
    )
    .bind(&app_id)
    .bind(user_id.as_deref().unwrap_or(""))
    .fetch_all(pool)
    .await;

    let limits = match limits_result {
        Ok(rows) => rows,
        Err(e) => {
            warn!(error = %e, "hera_check_limit: failed to fetch limits");
            // Fail open on DB error — don't block requests
            return json!({ "ok": true });
        }
    };

    if limits.is_empty() {
        return json!({ "ok": true });
    }

    // Fetch today's aggregated usage for this app+user
    let usage_result = sqlx::query(
        r#"
        SELECT
            COALESCE(SUM(total_tokens), 0)::BIGINT  AS total_tokens,
            COUNT(*)::BIGINT                         AS request_count
        FROM hera_usage_events
        WHERE app_id = $1
          AND ($2 = '' OR user_id = $2)
          AND ts >= CURRENT_DATE
          AND ts <  CURRENT_DATE + INTERVAL '1 day'
        "#,
    )
    .bind(&app_id)
    .bind(user_id.as_deref().unwrap_or(""))
    .fetch_one(pool)
    .await;

    let (used_tokens, used_requests): (i64, i64) = match usage_result {
        Ok(row) => (
            row.try_get::<i64, _>("total_tokens").unwrap_or(0),
            row.try_get::<i64, _>("request_count").unwrap_or(0),
        ),
        Err(e) => {
            warn!(error = %e, "hera_check_limit: failed to fetch today's usage");
            return json!({ "ok": true });
        }
    };

    for row in &limits {
        let kind: String = row.try_get("limit_kind").unwrap_or_default();
        let limit_value: i32 = row.try_get("limit_value").unwrap_or(i32::MAX);
        let limit_value_i64 = limit_value as i64;

        match kind.as_str() {
            "daily_tokens" if used_tokens >= limit_value_i64 => {
                return json!({
                    "ok": false,
                    "reason": format!("daily token limit exceeded ({}/{})", used_tokens, limit_value_i64)
                });
            }
            "daily_requests" if used_requests >= limit_value_i64 => {
                return json!({
                    "ok": false,
                    "reason": format!("daily request limit exceeded ({}/{})", used_requests, limit_value_i64)
                });
            }
            _ => {}
        }
    }

    json!({ "ok": true })
}

/// Aggregated daily stats by app/user.
/// payload: { app_id?, user_id?, days?: 7 }
/// Returns: { "rows": [{app_id, user_id, day, prompt_tokens, completion_tokens,
///                       total_tokens, request_count, cloud_requests}] }
pub async fn hera_usage_stats(pool: &sqlx::PgPool, payload: &Value) -> Value {
    let app_id = payload
        .get("app_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let user_id = payload
        .get("user_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let days = payload
        .get("days")
        .and_then(|v| v.as_i64())
        .unwrap_or(7)
        .clamp(1, 365) as i32;

    let rows_result = sqlx::query(
        r#"
        SELECT
            app_id,
            user_id,
            DATE(ts)                      AS day,
            SUM(prompt_tokens)::BIGINT    AS prompt_tokens,
            SUM(completion_tokens)::BIGINT AS completion_tokens,
            SUM(total_tokens)::BIGINT     AS total_tokens,
            COUNT(*)::BIGINT              AS request_count,
            SUM(CASE WHEN is_cloud THEN 1 ELSE 0 END)::BIGINT AS cloud_requests
        FROM hera_usage_events
        WHERE ts >= CURRENT_DATE - ($1::INTEGER - 1) * INTERVAL '1 day'
          AND ($2 = '' OR app_id  = $2)
          AND ($3 = '' OR user_id = $3)
        GROUP BY app_id, user_id, DATE(ts)
        ORDER BY day DESC, app_id, user_id
        "#,
    )
    .bind(days)
    .bind(&app_id)
    .bind(&user_id)
    .fetch_all(pool)
    .await;

    let rows = match rows_result {
        Ok(rows) => rows,
        Err(e) => {
            warn!(error = %e, "hera_usage_stats: query failed");
            return json!({ "rows": [], "error": e.to_string() });
        }
    };

    let data: Vec<Value> = rows
        .iter()
        .map(|row| {
            let day: chrono::NaiveDate = row
                .try_get::<chrono::NaiveDate, _>("day")
                .unwrap_or_default();
            json!({
                "app_id":             row.try_get::<String, _>("app_id").unwrap_or_default(),
                "user_id":            row.try_get::<String, _>("user_id").unwrap_or_default(),
                "day":                day.to_string(),
                "prompt_tokens":      row.try_get::<i64, _>("prompt_tokens").unwrap_or(0),
                "completion_tokens":  row.try_get::<i64, _>("completion_tokens").unwrap_or(0),
                "total_tokens":       row.try_get::<i64, _>("total_tokens").unwrap_or(0),
                "request_count":      row.try_get::<i64, _>("request_count").unwrap_or(0),
                "cloud_requests":     row.try_get::<i64, _>("cloud_requests").unwrap_or(0),
            })
        })
        .collect();

    json!({ "rows": data })
}
