//! Bulk deletion of scoped_memory rows by id.

use serde_json::Value;

/// Delete one or more scoped_memory rows by id.
///
/// Accepts either `{ "ids": [1, 2, 3] }` or `{ "id": 1 }` (normalised to a list
/// by the dispatcher before reaching here). Returning the count of deleted rows
/// lets the caller verify that all requested ids were actually present.
///
/// An empty id list is rejected before any SQL is executed.
pub async fn delete_records(pool: &sqlx::PgPool, ids: Vec<i32>) -> Value {
    if ids.is_empty() {
        return serde_json::json!({ "status": "error", "message": "no ids provided" });
    }

    match sqlx::query_scalar::<_, i64>(
        "DELETE FROM scoped_memory WHERE id = ANY($1) RETURNING id",
    )
    .bind(&ids)
    .fetch_all(pool)
    .await
    {
        Ok(deleted_ids) => {
            crate::query_cache::invalidate_all();
            let deleted_count = deleted_ids.len() as i64;
            serde_json::json!({
                "status": "success",
                "action": "scoped_memory_deleted",
                "deleted": deleted_count,
                "ids": ids
            })
        }
        Err(e) => serde_json::json!({ "status": "error", "message": format!("DB error: {}", e) }),
    }
}
