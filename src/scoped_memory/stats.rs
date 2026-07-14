//! Per-app aggregate counts of scoped_memory rows.
//!
//! Backs the OS-v3 admin GUI discoverability: instead of forcing the operator
//! to guess an `app_id`, the GUI lists every app that actually holds memories
//! with its row count, busiest first.

use serde_json::Value;

/// Count scoped_memory rows grouped by `app_id`, busiest first.
pub async fn app_stats(pool: &sqlx::PgPool) -> Value {
    match sqlx::query_as::<_, (String, i64)>(
        "SELECT app_id, COUNT(*) AS n FROM scoped_memory GROUP BY app_id ORDER BY n DESC, app_id ASC",
    )
    .fetch_all(pool)
    .await
    {
        Ok(rows) => {
            let stats: Vec<Value> = rows
                .into_iter()
                .map(|(app_id, count)| serde_json::json!({ "app_id": app_id, "count": count }))
                .collect();
            serde_json::json!({
                "status": "success",
                "action": "scoped_memory_app_stats",
                "stats": stats
            })
        }
        Err(e) => serde_json::json!({ "status": "error", "message": format!("DB error: {}", e) }),
    }
}
