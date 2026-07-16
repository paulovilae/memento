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
    let trace_id = payload
        .get("trace_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let node = payload
        .get("node")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    // Drop unattributed zero-token probe events. A background health/warmup
    // probe hits Hera `generate` every ~22s with NO identity (empty app_id,
    // route=default, user=anonymous) and — because the local llama.cpp engine
    // does not surface usage stats — Path B logs it as 0/0/0. That flood was
    // 4008 of 4017 rows in 24h (99.8%), inflating the billing/attribution
    // denominator so /hera-stats + the Hera-usage % lie (they crushed
    // claude_code's share from ~18% to ~12%).
    //
    // We can't key on 0-token alone: real local generations ALSO report 0
    // tokens today (the engine bug above), so that would erase attributed work.
    // Key on unattributed AND zero-token — an event with no app, no session and
    // an anonymous user carrying no tokens is pure noise; anything with any
    // identity still logs (attributes a request) even at 0 tokens.
    let unattributed = app_id.is_empty()
        && session_id.is_empty()
        && (user_id.is_empty() || user_id == "anonymous");
    if unattributed && prompt_tokens == 0 && completion_tokens == 0 && total_tokens == 0 {
        return json!({ "ok": true, "skipped": "unattributed_zero_token" });
    }

    let result = sqlx::query(
        r#"
        INSERT INTO hera_usage_events
            (app_id, user_id, session_id, route_profile, model,
             prompt_tokens, completion_tokens, total_tokens, is_cloud, latency_ms,
             trace_id, node)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
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
    .bind(&trace_id)
    .bind(&node)
    .execute(pool)
    .await;

    if let Err(e) = result {
        warn!(error = %e, app_id = %app_id, "hera_log_usage: INSERT failed (best-effort, ignored)");
    }

    json!({ "ok": true })
}

/// INSERT one per-tool-call telemetry row. Best-effort: SQL errors are logged
/// but never propagated (mirrors `hera_log_usage`).
/// payload fields (all optional):
///   trace_id, session_id, app_id, route_profile, node, seq, tool_name,
///   args_preview, result_preview, duration_ms, success, error
pub async fn hera_log_tool_call(pool: &sqlx::PgPool, payload: &Value) -> Value {
    let s = |key: &str| -> String {
        payload
            .get(key)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string()
    };
    let trace_id = s("trace_id");
    let session_id = s("session_id");
    let app_id = s("app_id");
    let route_profile = s("route_profile");
    let node = s("node");
    let tool_name = s("tool_name");
    let args_preview = s("args_preview");
    let result_preview = s("result_preview");
    let seq = payload.get("seq").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
    let duration_ms = payload
        .get("duration_ms")
        .and_then(|v| v.as_i64())
        .unwrap_or(0) as i32;
    let success = payload
        .get("success")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let error: Option<String> = payload
        .get("error")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());

    let result = sqlx::query(
        r#"
        INSERT INTO hera_tool_calls
            (trace_id, session_id, app_id, route_profile, node, seq,
             tool_name, args_preview, result_preview, duration_ms, success, error)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        "#,
    )
    .bind(&trace_id)
    .bind(&session_id)
    .bind(&app_id)
    .bind(&route_profile)
    .bind(&node)
    .bind(seq)
    .bind(&tool_name)
    .bind(&args_preview)
    .bind(&result_preview)
    .bind(duration_ms)
    .bind(success)
    .bind(&error)
    .execute(pool)
    .await;

    if let Err(e) = result {
        warn!(error = %e, tool_name = %tool_name, "hera_log_tool_call: INSERT failed (best-effort, ignored)");
    }

    json!({ "ok": true })
}

/// INSERT one direct-MCP-usage telemetry row (the `memento-mcp` stdio bridge
/// choke point in `send_ipc`). Best-effort: SQL errors are logged but never
/// propagated (mirrors `hera_log_usage` / `hera_log_tool_call`).
/// payload fields (all optional):
///   session_id, tool_name, action, app_id, duration_ms, success, error
pub async fn mcp_log_usage(pool: &sqlx::PgPool, payload: &Value) -> Value {
    let s = |key: &str| -> String {
        payload
            .get(key)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string()
    };
    let session_id = s("session_id");
    let tool_name = s("tool_name");
    let action = s("action");
    let app_id_raw = s("app_id");
    let app_id = if app_id_raw.is_empty() {
        "memento-mcp".to_string()
    } else {
        app_id_raw
    };
    let duration_ms = payload
        .get("duration_ms")
        .and_then(|v| v.as_i64())
        .unwrap_or(0) as i32;
    let success = payload
        .get("success")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let error: Option<String> = payload
        .get("error")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());

    let result = sqlx::query(
        r#"
        INSERT INTO memento_mcp_usage_events
            (session_id, tool_name, action, app_id, duration_ms, success, error)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        "#,
    )
    .bind(&session_id)
    .bind(&tool_name)
    .bind(&action)
    .bind(&app_id)
    .bind(duration_ms)
    .bind(success)
    .bind(&error)
    .execute(pool)
    .await;

    if let Err(e) = result {
        warn!(error = %e, tool_name = %tool_name, "mcp_log_usage: INSERT failed (best-effort, ignored)");
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

/// Map a `hera_tool_calls` row to the JSON shape used by all three endpoints
/// below — avoids tripling the same field-by-field mapping.
fn tool_call_row_to_json(row: &sqlx::postgres::PgRow) -> Value {
    let ts = row
        .try_get::<chrono::NaiveDateTime, _>("ts")
        .map(|d| d.to_string())
        .unwrap_or_default();
    json!({
        "id":             row.try_get::<i64, _>("id").unwrap_or(0),
        "ts":             ts,
        "trace_id":       row.try_get::<String, _>("trace_id").unwrap_or_default(),
        "session_id":     row.try_get::<String, _>("session_id").unwrap_or_default(),
        "app_id":         row.try_get::<String, _>("app_id").unwrap_or_default(),
        "route_profile":  row.try_get::<String, _>("route_profile").unwrap_or_default(),
        "node":           row.try_get::<String, _>("node").unwrap_or_default(),
        "seq":            row.try_get::<i32, _>("seq").unwrap_or(0),
        "tool_name":      row.try_get::<String, _>("tool_name").unwrap_or_default(),
        "args_preview":   row.try_get::<String, _>("args_preview").unwrap_or_default(),
        "result_preview": row.try_get::<String, _>("result_preview").unwrap_or_default(),
        "duration_ms":    row.try_get::<i32, _>("duration_ms").unwrap_or(0),
        "success":        row.try_get::<bool, _>("success").unwrap_or(true),
        "error":          row.try_get::<Option<String>, _>("error").unwrap_or_default(),
    })
}

/// Recent tool-call telemetry, filterable by tool_name/app_id/success/age.
/// payload: { tool_name?, app_id?, success?, since_minutes?, limit?: 100 }
pub async fn hera_tool_calls_recent(pool: &sqlx::PgPool, payload: &Value) -> Value {
    let tool_name = payload
        .get("tool_name")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let app_id = payload
        .get("app_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let succ_filter: i32 = match payload.get("success").and_then(|v| v.as_bool()) {
        Some(true) => 1,
        Some(false) => 0,
        None => -1,
    };
    let since_min = payload
        .get("since_minutes")
        .and_then(|v| v.as_i64())
        .unwrap_or(0) as i32;
    let limit = payload
        .get("limit")
        .and_then(|v| v.as_i64())
        .unwrap_or(100)
        .clamp(1, 500) as i32;

    let rows_result = sqlx::query(
        r#"
        SELECT id, ts, trace_id, session_id, app_id, route_profile, node, seq,
               tool_name, args_preview, result_preview, duration_ms, success, error
        FROM hera_tool_calls
        WHERE ($1 = '' OR tool_name = $1)
          AND ($2 = '' OR app_id = $2)
          AND ($3 = -1 OR success = ($3 = 1))
          AND ($4 = 0 OR ts >= now() - make_interval(mins => $4))
        ORDER BY id DESC
        LIMIT $5
        "#,
    )
    .bind(&tool_name)
    .bind(&app_id)
    .bind(succ_filter)
    .bind(since_min)
    .bind(limit)
    .fetch_all(pool)
    .await;

    let rows = match rows_result {
        Ok(rows) => rows,
        Err(e) => {
            warn!(error = %e, "hera_tool_calls_recent: query failed");
            return json!({ "ok": false, "error": e.to_string() });
        }
    };

    let data: Vec<Value> = rows.iter().map(tool_call_row_to_json).collect();
    json!({ "ok": true, "rows": data })
}

/// Full timeline for one trace_id: tool calls (id ASC = authoritative order)
/// plus any usage events sharing the same trace_id.
/// payload: { trace_id } (required)
pub async fn hera_trace_timeline(pool: &sqlx::PgPool, payload: &Value) -> Value {
    let trace_id = payload
        .get("trace_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    if trace_id.is_empty() {
        return json!({ "ok": false, "error": "trace_id required" });
    }

    let calls_result = sqlx::query(
        r#"
        SELECT id, ts, trace_id, session_id, app_id, route_profile, node, seq,
               tool_name, args_preview, result_preview, duration_ms, success, error
        FROM hera_tool_calls
        WHERE trace_id = $1
        ORDER BY id ASC
        "#,
    )
    .bind(&trace_id)
    .fetch_all(pool)
    .await;

    let calls = match calls_result {
        Ok(rows) => rows,
        Err(e) => {
            warn!(error = %e, "hera_trace_timeline: tool_calls query failed");
            return json!({ "ok": false, "error": e.to_string() });
        }
    };
    let calls_data: Vec<Value> = calls.iter().map(tool_call_row_to_json).collect();

    let usage_result = sqlx::query(
        r#"
        SELECT ts, app_id, user_id, session_id, route_profile, model, prompt_tokens, completion_tokens, total_tokens, is_cloud, latency_ms, node
        FROM hera_usage_events
        WHERE trace_id = $1
        ORDER BY ts ASC
        "#,
    )
    .bind(&trace_id)
    .fetch_all(pool)
    .await;

    let usage = match usage_result {
        Ok(rows) => rows,
        Err(e) => {
            warn!(error = %e, "hera_trace_timeline: usage_events query failed");
            return json!({ "ok": false, "error": e.to_string() });
        }
    };
    let usage_data: Vec<Value> = usage
        .iter()
        .map(|row| {
            let ts = row
                .try_get::<chrono::NaiveDateTime, _>("ts")
                .map(|d| d.to_string())
                .unwrap_or_default();
            json!({
                "ts":                ts,
                "app_id":            row.try_get::<String, _>("app_id").unwrap_or_default(),
                "user_id":           row.try_get::<String, _>("user_id").unwrap_or_default(),
                "session_id":        row.try_get::<String, _>("session_id").unwrap_or_default(),
                "route_profile":     row.try_get::<String, _>("route_profile").unwrap_or_default(),
                "model":             row.try_get::<String, _>("model").unwrap_or_default(),
                "prompt_tokens":     row.try_get::<i32, _>("prompt_tokens").unwrap_or(0),
                "completion_tokens": row.try_get::<i32, _>("completion_tokens").unwrap_or(0),
                "total_tokens":      row.try_get::<i32, _>("total_tokens").unwrap_or(0),
                "is_cloud":          row.try_get::<bool, _>("is_cloud").unwrap_or(false),
                "latency_ms":        row.try_get::<Option<i32>, _>("latency_ms").unwrap_or_default(),
                "node":              row.try_get::<String, _>("node").unwrap_or_default(),
            })
        })
        .collect();

    json!({ "ok": true, "tool_calls": calls_data, "usage": usage_data })
}

/// List recent traces (grouped by trace_id) with summary stats.
/// payload: { limit?: 50 }
pub async fn hera_trace_list(pool: &sqlx::PgPool, payload: &Value) -> Value {
    let limit = payload
        .get("limit")
        .and_then(|v| v.as_i64())
        .unwrap_or(50)
        .clamp(1, 200) as i32;

    let rows_result = sqlx::query(
        r#"
        SELECT trace_id,
               min(ts) AS started, max(ts) AS ended,
               count(*)::BIGINT AS calls,
               bool_and(success) AS all_ok,
               COALESCE(sum(duration_ms),0)::BIGINT AS total_ms
        FROM hera_tool_calls
        WHERE trace_id <> ''
        GROUP BY trace_id
        ORDER BY started DESC
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await;

    let rows = match rows_result {
        Ok(rows) => rows,
        Err(e) => {
            warn!(error = %e, "hera_trace_list: query failed");
            return json!({ "ok": false, "error": e.to_string() });
        }
    };

    let data: Vec<Value> = rows
        .iter()
        .map(|row| {
            let started = row
                .try_get::<chrono::NaiveDateTime, _>("started")
                .map(|d| d.to_string())
                .unwrap_or_default();
            let ended = row
                .try_get::<chrono::NaiveDateTime, _>("ended")
                .map(|d| d.to_string())
                .unwrap_or_default();
            json!({
                "trace_id":  row.try_get::<String, _>("trace_id").unwrap_or_default(),
                "started":   started,
                "ended":     ended,
                "calls":     row.try_get::<i64, _>("calls").unwrap_or(0),
                "all_ok":    row.try_get::<bool, _>("all_ok").unwrap_or(true),
                "total_ms":  row.try_get::<i64, _>("total_ms").unwrap_or(0),
            })
        })
        .collect();

    json!({ "ok": true, "traces": data })
}
