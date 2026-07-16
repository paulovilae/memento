// Module declarations
mod compression;
mod deletion;
mod derivation;
mod embedding;
mod helpers;
mod parsing;
mod recall;
mod search;
mod stats;

pub use compression::{compress_project, compress_room, compress_session, derive_memory, memory_promote};
pub use deletion::delete_records;
pub use recall::{get_recent_events, get_working_context, recall_recursive_context};
pub use search::{get_timeline, search_records, semantic_recall};
pub use stats::app_stats;

// Import internal types and functions
use derivation::*;
use helpers::*;
use parsing::*;

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
        let log_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM recall_log WHERE request_id = $1")
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

        let fb_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM recall_feedback WHERE request_id = $1")
                .bind(&request_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(fb_count, 1, "recall_feedback did not insert the row");
    }
}
