use serde_json::{json, Value};
use sqlx::Row;
use std::fs;
use std::path::Path;

const MAX_RECENT_RUNTIME_ROWS: i64 = 24;
const PROMPT_INFLATION_CHARS: u64 = 20_000;
const TOOL_SCHEMA_INFLATION_CHARS: u64 = 18_000;
const DB_SCHEMA_INFLATION_CHARS: u64 = 8_000;
const LATENCY_REGRESSION_THRESHOLD_PCT: f64 = 20.0;
const FIRST_TOKEN_REGRESSION_THRESHOLD_PCT: f64 = 20.0;
const POSITIVE_HINT_TTL_HOURS: i64 = 24;
const NEGATIVE_HINT_TTL_HOURS: i64 = 12;

#[derive(Debug, Clone, Default)]
struct RuntimeSnapshot {
    duration_ms: u64,
    first_token_ms: Option<u64>,
    prompt_chars: u64,
    tool_schema_chars: u64,
    db_schema_chars: u64,
    success: bool,
    persona_drift: bool,
    route_profile: String,
    recommended_budget_mode: Option<String>,
}

fn baseline_path_for(app_id: &str) -> Option<&'static str> {
    match app_id {
        "cartera" => {
            Some("/home/paulo/Programs/apps/OS/benchmarks/baselines/cartera_http_stream.json")
        }
        "movilo" => {
            Some("/home/paulo/Programs/apps/OS/benchmarks/baselines/movilo_http_stream.json")
        }
        "consulting" => {
            Some("/home/paulo/Programs/apps/OS/benchmarks/baselines/consulting_http_stream.json")
        }
        "latinos" => {
            Some("/home/paulo/Programs/apps/OS/benchmarks/baselines/latinos_http_stream.json")
        }
        "vetra" => Some("/home/paulo/Programs/apps/OS/benchmarks/baselines/vetra_http_stream.json"),
        "hera" => {
            Some("/home/paulo/Programs/apps/OS/benchmarks/baselines/hera_vetra_greeting.json")
        }
        _ => None,
    }
}

fn load_baseline(app_id: &str) -> Option<Value> {
    let path = baseline_path_for(app_id)?;
    let content = fs::read_to_string(Path::new(path)).ok()?;
    serde_json::from_str::<Value>(&content).ok()
}

fn baseline_metric_u64(baseline: &Value, keys: &[&str]) -> Option<u64> {
    let metrics = baseline.get("metrics")?;
    for key in keys {
        if let Some(value) = metrics.get(*key).and_then(|value| value.as_u64()) {
            return Some(value);
        }
        if let Some(value) = metrics.get(*key).and_then(|value| value.as_f64()) {
            if value.is_finite() && value >= 0.0 {
                return Some(value.round() as u64);
            }
        }
    }
    None
}

fn pct_regression(actual: u64, baseline: Option<u64>) -> Option<f64> {
    let baseline = baseline?;
    if baseline == 0 {
        return None;
    }
    Some(((actual as f64 - baseline as f64) / baseline as f64) * 100.0)
}

fn parse_snapshot(payload: &Value) -> RuntimeSnapshot {
    RuntimeSnapshot {
        duration_ms: payload
            .get("duration_ms")
            .and_then(|value| value.as_u64())
            .unwrap_or(0),
        first_token_ms: payload
            .get("first_token_ms")
            .and_then(|value| value.as_u64()),
        prompt_chars: payload
            .get("prompt_chars")
            .and_then(|value| value.as_u64())
            .unwrap_or(0),
        tool_schema_chars: payload
            .get("tool_schema_chars")
            .and_then(|value| value.as_u64())
            .unwrap_or(0),
        db_schema_chars: payload
            .get("db_schema_chars")
            .and_then(|value| value.as_u64())
            .unwrap_or(0),
        success: payload
            .get("success")
            .and_then(|value| value.as_bool())
            .unwrap_or(false),
        persona_drift: payload
            .get("persona_drift")
            .and_then(|value| value.as_bool())
            .unwrap_or(false),
        route_profile: payload
            .get("route_profile")
            .and_then(|value| value.as_str())
            .unwrap_or("default")
            .trim()
            .to_string(),
        recommended_budget_mode: payload
            .get("recommended_budget_mode")
            .and_then(|value| value.as_str())
            .map(str::to_string),
    }
}

fn classify_runtime_anomalies(snapshot: &RuntimeSnapshot, baseline: Option<&Value>) -> Vec<String> {
    let mut anomalies = Vec::new();
    let baseline = baseline.cloned();

    if !snapshot.success {
        anomalies.push("execution_failure".to_string());
    }
    if snapshot.persona_drift {
        anomalies.push("persona_drift".to_string());
    }
    if snapshot.prompt_chars > PROMPT_INFLATION_CHARS {
        anomalies.push("prompt_inflation".to_string());
    }
    if snapshot.tool_schema_chars > TOOL_SCHEMA_INFLATION_CHARS {
        anomalies.push("tool_schema_inflation".to_string());
    }
    if snapshot.db_schema_chars > DB_SCHEMA_INFLATION_CHARS {
        anomalies.push("db_schema_inflation".to_string());
    }

    if let Some(baseline) = baseline {
        if let Some(regression_pct) = pct_regression(
            snapshot.duration_ms,
            baseline_metric_u64(&baseline, &["p95_ms", "duration_ms"]),
        ) {
            if regression_pct > LATENCY_REGRESSION_THRESHOLD_PCT {
                anomalies.push("latency_regression".to_string());
            }
        }

        if let Some(first_token_ms) = snapshot.first_token_ms {
            if let Some(regression_pct) = pct_regression(
                first_token_ms,
                baseline_metric_u64(&baseline, &["first_token_p95_ms", "first_token_ms"]),
            ) {
                if regression_pct > FIRST_TOKEN_REGRESSION_THRESHOLD_PCT {
                    anomalies.push("first_token_regression".to_string());
                }
            }
        }
    }

    anomalies
}

fn summarize_snapshot(snapshot: &RuntimeSnapshot, anomalies: &[String]) -> String {
    let anomaly_summary = if anomalies.is_empty() {
        "anomalies=none".to_string()
    } else {
        format!("anomalies={}", anomalies.join(","))
    };

    format!(
        "duration_ms={} first_token_ms={} prompt_chars={} tool_schema_chars={} db_schema_chars={} persona_drift={} success={} {}",
        snapshot.duration_ms,
        snapshot.first_token_ms.unwrap_or(0),
        snapshot.prompt_chars,
        snapshot.tool_schema_chars,
        snapshot.db_schema_chars,
        snapshot.persona_drift,
        snapshot.success,
        anomaly_summary
    )
}

fn append_warning_if_missing(items: &mut Vec<String>, warning: String) {
    if !items.iter().any(|existing| existing == &warning) {
        items.push(warning);
    }
}

fn default_hint_ttl_hours(hint_kind: &str) -> i64 {
    if hint_kind == "negative" {
        NEGATIVE_HINT_TTL_HOURS
    } else {
        POSITIVE_HINT_TTL_HOURS
    }
}

fn infer_hint_kind(entry_title: &str, content: &str, content_json: Option<&Value>) -> String {
    if let Some(kind) = content_json
        .and_then(|value| value.get("hint_kind"))
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return kind.to_string();
    }

    let haystack = format!(
        "{} {}",
        entry_title.to_ascii_lowercase(),
        content.to_ascii_lowercase()
    );
    if haystack.contains("avoid ")
        || haystack.contains("avoid_")
        || haystack.contains("do not")
        || haystack.contains("negative hint")
        || haystack.contains("degraded until")
    {
        "negative".to_string()
    } else {
        "positive".to_string()
    }
}

fn effective_hint_expiry(
    timestamp: chrono::NaiveDateTime,
    expires_at: Option<chrono::NaiveDateTime>,
    hint_kind: &str,
    content_json: Option<&Value>,
) -> chrono::NaiveDateTime {
    if let Some(expires_at) = expires_at {
        return expires_at;
    }
    let ttl_hours = content_json
        .and_then(|value| value.get("hint_ttl_hours"))
        .and_then(|value| value.as_i64())
        .unwrap_or_else(|| default_hint_ttl_hours(hint_kind));
    timestamp + chrono::Duration::hours(ttl_hours.max(1))
}

fn route_matches(snapshot: &RuntimeSnapshot, requested_route_profile: &str) -> bool {
    requested_route_profile.is_empty()
        || snapshot.route_profile.is_empty()
        || snapshot.route_profile == requested_route_profile
}

pub async fn get_runtime_preflight(pool: &sqlx::PgPool, payload: Value) -> Value {
    let app_id = payload
        .get("app_id")
        .and_then(|value| value.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    let route_profile = payload
        .get("route_profile")
        .and_then(|value| value.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    let persona_path = payload
        .get("persona_path")
        .and_then(|value| value.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    let mode = payload
        .get("mode")
        .and_then(|value| value.as_str())
        .unwrap_or("generate")
        .trim()
        .to_string();

    if app_id.is_empty() {
        return json!({ "error": "app_id is required" });
    }

    let rows = match sqlx::query(
        "SELECT id, entry_title, memory_type, content, content_json, timestamp, expires_at \
         FROM scoped_memory \
         WHERE app_id = $1 AND status IN ('active', 'needs_review') \
         AND memory_type IN ('runtime_hint', 'regression_event', 'runtime_observation') \
         ORDER BY timestamp DESC LIMIT $2",
    )
    .bind(&app_id)
    .bind(MAX_RECENT_RUNTIME_ROWS)
    .fetch_all(pool)
    .await
    {
        Ok(rows) => rows,
        Err(error) => {
            return json!({ "error": format!("failed to query runtime memory: {error}") });
        }
    };

    let baseline = load_baseline(&app_id);
    let baseline_p95 = baseline
        .as_ref()
        .and_then(|value| baseline_metric_u64(value, &["p95_ms", "duration_ms"]));
    let baseline_first_token = baseline
        .as_ref()
        .and_then(|value| baseline_metric_u64(value, &["first_token_p95_ms", "first_token_ms"]));

    let mut warnings = Vec::new();
    let mut learned_hints = Vec::new();
    let mut known_regressions = Vec::new();
    let mut recommended_budget_mode: Option<String> = None;
    let mut latest_observation: Option<Value> = None;
    let mut matching_observation_count = 0usize;
    let mut unhealthy_count = 0usize;
    let now = chrono::Utc::now().naive_utc();

    for row in &rows {
        let entry_title = row.get::<String, _>("entry_title");
        let memory_type = row.get::<String, _>("memory_type");
        let content = row.get::<String, _>("content");
        let record_id = row.get::<i32, _>("id");
        let timestamp = row.get::<chrono::NaiveDateTime, _>("timestamp");
        let expires_at = row
            .try_get::<Option<chrono::NaiveDateTime>, _>("expires_at")
            .ok()
            .flatten();
        let content_json = row
            .try_get::<Option<String>, _>("content_json")
            .ok()
            .flatten()
            .and_then(|raw| serde_json::from_str::<Value>(&raw).ok());
        let snapshot = content_json
            .as_ref()
            .map(parse_snapshot)
            .unwrap_or_default();

        if !route_matches(&snapshot, &route_profile) {
            continue;
        }
        matching_observation_count += 1;

        if memory_type == "runtime_hint" {
            let hint_kind = infer_hint_kind(&entry_title, &content, content_json.as_ref());
            let effective_expires_at =
                effective_hint_expiry(timestamp, expires_at, &hint_kind, content_json.as_ref());
            if effective_expires_at <= now {
                continue;
            }
            let hint_ttl_hours = content_json
                .as_ref()
                .and_then(|value| value.get("hint_ttl_hours"))
                .and_then(|value| value.as_i64())
                .unwrap_or_else(|| default_hint_ttl_hours(&hint_kind));
            if recommended_budget_mode.is_none() {
                recommended_budget_mode = snapshot.recommended_budget_mode.clone().or_else(|| {
                    content_json
                        .as_ref()
                        .and_then(|value| value.get("recommended_budget_mode"))
                        .and_then(|value| value.as_str())
                        .map(str::to_string)
                });
            }
            let mut normalized_data = content_json.clone().unwrap_or_else(|| json!({}));
            if let Some(object) = normalized_data.as_object_mut() {
                object.insert("hint_kind".to_string(), json!(hint_kind));
                object.insert("hint_ttl_hours".to_string(), json!(hint_ttl_hours));
                object.insert(
                    "effective_expires_at".to_string(),
                    json!(effective_expires_at.format("%Y-%m-%d %H:%M:%S").to_string()),
                );
            }
            learned_hints.push(json!({
                "record_id": record_id,
                "title": entry_title,
                "content": content,
                "timestamp": timestamp.format("%Y-%m-%d %H:%M:%S").to_string(),
                "expires_at": effective_expires_at.format("%Y-%m-%d %H:%M:%S").to_string(),
                "data": normalized_data
            }));
            continue;
        }

        let anomalies = classify_runtime_anomalies(&snapshot, baseline.as_ref());
        if !anomalies.is_empty() {
            unhealthy_count += 1;
        }

        if memory_type == "regression_event" || !anomalies.is_empty() {
            known_regressions.push(json!({
                "record_id": record_id,
                "title": entry_title,
                "content": content,
                "timestamp": timestamp.format("%Y-%m-%d %H:%M:%S").to_string(),
                "anomalies": anomalies,
                "data": content_json
            }));
        }

        if latest_observation.is_none() {
            latest_observation = Some(json!({
                "record_id": record_id,
                "title": entry_title,
                "content": content,
                "timestamp": timestamp.format("%Y-%m-%d %H:%M:%S").to_string(),
                "anomalies": anomalies,
                "data": content_json
            }));
        }
    }

    if recommended_budget_mode.is_none() {
        let recent_prompt_inflation = known_regressions.iter().any(|item| {
            item.get("anomalies")
                .and_then(|value| value.as_array())
                .map(|items| {
                    items.iter().any(|value| {
                        matches!(
                            value.as_str(),
                            Some(
                                "prompt_inflation"
                                    | "tool_schema_inflation"
                                    | "db_schema_inflation"
                            )
                        )
                    })
                })
                .unwrap_or(false)
        });
        if recent_prompt_inflation || mode == "generate_stream" {
            recommended_budget_mode = Some("lightweight".to_string());
        }
    }

    if known_regressions.iter().any(|item| {
        item.get("anomalies")
            .and_then(|value| value.as_array())
            .map(|items| {
                items
                    .iter()
                    .any(|value| value.as_str() == Some("persona_drift"))
            })
            .unwrap_or(false)
    }) {
        append_warning_if_missing(
            &mut warnings,
            "Route drift detected in runtime memory; validate persona_path against route_profile."
                .to_string(),
        );
    }
    if known_regressions.iter().any(|item| {
        item.get("anomalies")
            .and_then(|value| value.as_array())
            .map(|items| {
                items.iter().any(|value| {
                    matches!(
                        value.as_str(),
                        Some("prompt_inflation" | "tool_schema_inflation" | "db_schema_inflation")
                    )
                })
            })
            .unwrap_or(false)
    }) {
        append_warning_if_missing(
            &mut warnings,
            "Prompt or schema inflation observed recently; keep context budgets constrained."
                .to_string(),
        );
    }
    if let Some(latest) = &latest_observation {
        if let Some(data) = latest.get("data") {
            let snapshot = parse_snapshot(data);
            if let Some(regression_pct) = pct_regression(snapshot.duration_ms, baseline_p95) {
                if regression_pct > LATENCY_REGRESSION_THRESHOLD_PCT {
                    append_warning_if_missing(
                        &mut warnings,
                        format!(
                            "Latest duration regressed {:.1}% over baseline; inspect route integration.",
                            regression_pct
                        ),
                    );
                }
            }
            if let Some(first_token_ms) = snapshot.first_token_ms {
                if let Some(regression_pct) = pct_regression(first_token_ms, baseline_first_token) {
                    if regression_pct > FIRST_TOKEN_REGRESSION_THRESHOLD_PCT {
                        append_warning_if_missing(
                            &mut warnings,
                            format!(
                                "Latest first-token latency regressed {:.1}% over baseline; inspect streaming path.",
                                regression_pct
                            ),
                        );
                    }
                }
            }
        }
    }

    let health_status = if matching_observation_count == 0 {
        "unknown"
    } else if unhealthy_count >= 3 || known_regressions.len() >= 3 {
        "degraded"
    } else if unhealthy_count > 0 || !known_regressions.is_empty() {
        "watch"
    } else {
        "healthy"
    };

    json!({
        "status": "success",
        "app_id": app_id,
        "route_profile": route_profile,
        "persona_path": persona_path,
        "mode": mode,
        "health_status": health_status,
        "matching_observation_count": matching_observation_count,
        "last_healthy_baseline": baseline,
        "recommended_budget_mode": recommended_budget_mode,
        "warnings": warnings,
        "learned_hints": learned_hints,
        "known_regressions": known_regressions,
        "latest_observation": latest_observation
    })
}

pub async fn record_runtime_observation(pool: &sqlx::PgPool, payload: Value) -> Value {
    let app_id = payload
        .get("app_id")
        .and_then(|value| value.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    if app_id.is_empty() {
        return json!({ "error": "app_id is required" });
    }

    let baseline = load_baseline(&app_id);
    let snapshot = parse_snapshot(&payload);
    let anomalies = classify_runtime_anomalies(&snapshot, baseline.as_ref());
    let is_regression = !anomalies.is_empty();
    let route_profile = if snapshot.route_profile.is_empty() {
        "default".to_string()
    } else {
        snapshot.route_profile.clone()
    };
    let observation_type = if is_regression {
        "regression_event"
    } else {
        "runtime_observation"
    };
    let status = if snapshot.success && !is_regression {
        "active"
    } else {
        "needs_review"
    };
    let entry_title = format!(
        "{} {} {}",
        app_id,
        route_profile,
        if is_regression {
            "runtime regression"
        } else {
            "runtime observation"
        }
    );

    let mut tags = vec![
        "runtime".to_string(),
        "hera".to_string(),
        route_profile.clone(),
        observation_type.to_string(),
    ];
    tags.extend(anomalies.iter().cloned());

    crate::scoped_memory::save_record(
        pool,
        json!({
            "user_id": payload.get("user_id").and_then(|value| value.as_str()).unwrap_or("system"),
            "tenant_id": payload.get("tenant_id").and_then(|value| value.as_str()).unwrap_or("imaginos"),
            "app_id": app_id,
            "session_id": payload.get("session_id").and_then(|value| value.as_str()).unwrap_or(""),
            "scope": "app",
            "source": "hera_runtime",
            "entry_title": entry_title,
            "memory_type": observation_type,
            "content": summarize_snapshot(&snapshot, &anomalies),
            "content_json": payload,
            "confidence": if is_regression { 0.92 } else { 0.74 },
            "derivation_method": if is_regression { "runtime_anomaly_classification" } else { "runtime_observation_capture" },
            "status": status,
            "tags": tags
        }),
    )
    .await
}

pub async fn promote_runtime_hint(pool: &sqlx::PgPool, payload: Value) -> Value {
    let app_id = payload
        .get("app_id")
        .and_then(|value| value.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    if app_id.is_empty() {
        return json!({ "error": "app_id is required" });
    }

    let route_profile = payload
        .get("route_profile")
        .and_then(|value| value.as_str())
        .unwrap_or("default")
        .trim()
        .to_string();
    let hint_kind = payload
        .get("hint_kind")
        .and_then(|value| value.as_str())
        .unwrap_or("positive")
        .trim()
        .to_string();
    let recommended_budget_mode = payload
        .get("recommended_budget_mode")
        .and_then(|value| value.as_str())
        .map(str::to_string);
    let ttl_hours = payload
        .get("hint_ttl_hours")
        .and_then(|value| value.as_i64())
        .unwrap_or_else(|| default_hint_ttl_hours(&hint_kind));
    let expires_at = chrono::Utc::now() + chrono::Duration::hours(ttl_hours.max(1));
    let title = payload
        .get("title")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| format!("{} {} runtime hint", app_id, route_profile));
    let content = payload
        .get("content")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| {
            if let Some(mode) = &recommended_budget_mode {
                format!(
                    "Promoted runtime hint for {} / {} with recommended budget mode {}.",
                    app_id, route_profile, mode
                )
            } else {
                format!("Promoted runtime hint for {} / {}.", app_id, route_profile)
            }
        });

    crate::scoped_memory::save_record(
        pool,
        json!({
            "user_id": payload.get("user_id").and_then(|value| value.as_str()).unwrap_or("system"),
            "tenant_id": payload.get("tenant_id").and_then(|value| value.as_str()).unwrap_or("imaginos"),
            "app_id": app_id,
            "session_id": payload.get("session_id").and_then(|value| value.as_str()).unwrap_or(""),
            "scope": "app",
            "source": "runtime_promotion",
            "entry_title": title,
            "memory_type": "runtime_hint",
            "content": content,
            "content_json": payload,
            "confidence": payload.get("confidence").and_then(|value| value.as_f64()).or(Some(0.88)),
            "derivation_method": "runtime_hint_promotion",
            "promoted_from": payload.get("source_record_id").and_then(|value| value.as_i64()).map(|value| json!([value]).to_string()),
            "status": "active",
            "expires_at": expires_at.format("%Y-%m-%d %H:%M:%S").to_string(),
            "tags": ["runtime", "hera", "runtime_hint", route_profile, hint_kind]
        }),
    )
    .await
}

pub async fn save_agent_run_summary(pool: &sqlx::PgPool, payload: Value) -> Value {
    let app_id = payload
        .get("app_id")
        .and_then(|value| value.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    if app_id.is_empty() {
        return json!({ "error": "app_id is required" });
    }

    let run_id = payload
        .get("run_id")
        .and_then(|value| value.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    if run_id.is_empty() {
        return json!({ "error": "run_id is required" });
    }

    let route_profile = payload
        .get("route_profile")
        .and_then(|value| value.as_str())
        .unwrap_or("delegation")
        .trim()
        .to_string();
    let status = payload
        .get("status")
        .and_then(|value| value.as_str())
        .unwrap_or("completed")
        .trim()
        .to_string();
    let title = payload
        .get("title")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| format!("{} {} agent run", app_id, run_id));
    let content = payload
        .get("summary")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .or_else(|| {
            payload
                .get("aggregate_result")
                .and_then(|value| value.as_str())
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
        })
        .unwrap_or_else(|| {
            format!(
                "Agent run {} for {} finished with status {}.",
                run_id, app_id, status
            )
        });

    crate::scoped_memory::save_record(
        pool,
        json!({
            "user_id": payload.get("user_id").and_then(|value| value.as_str()).unwrap_or("system"),
            "tenant_id": payload.get("tenant_id").and_then(|value| value.as_str()).unwrap_or("imaginos"),
            "app_id": app_id,
            "session_id": payload.get("session_id").and_then(|value| value.as_str()).unwrap_or(""),
            "scope": "app",
            "source": "hera_delegation",
            "entry_title": title,
            "memory_type": "agent_run_summary",
            "content": content,
            "content_json": payload,
            "confidence": payload.get("confidence").and_then(|value| value.as_f64()).or(Some(0.86)),
            "derivation_method": "agent_run_capture",
            "status": if status == "completed" { "active" } else { "needs_review" },
            "tags": ["runtime", "hera", "delegation", route_profile, status]
        }),
    )
    .await
}
