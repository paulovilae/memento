#![allow(dead_code)]

use crate::document_index::{self, DocumentIndexUpsert};
use serde_json::Value;

pub async fn upsert(pool: &sqlx::PgPool, payload: Value) -> Value {
    match serde_json::from_value::<DocumentIndexUpsert>(payload) {
        Ok(payload) => match document_index::upsert(pool, payload).await {
            Ok(value) => value,
            Err(e) => serde_json::json!({ "error": format!("document index upsert error: {}", e) }),
        },
        Err(e) => {
            serde_json::json!({ "error": format!("Invalid payload for upsert_document_index: {}", e) })
        }
    }
}

pub async fn get(pool: &sqlx::PgPool, payload: Value) -> Value {
    let document_id = payload
        .get("document_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let app_id = payload.get("app_id").and_then(|v| v.as_str());
    if document_id.is_empty() {
        return serde_json::json!({ "error": "Missing 'document_id' in payload" });
    }

    match document_index::get(pool, document_id, app_id).await {
        Ok(value) => value,
        Err(e) => serde_json::json!({ "error": format!("document index get error: {}", e) }),
    }
}

pub async fn list(pool: &sqlx::PgPool, payload: Value) -> Value {
    let app_id = payload.get("app_id").and_then(|v| v.as_str());
    let tenant_id = payload.get("tenant_id").and_then(|v| v.as_str());
    let index_type = payload.get("index_type").and_then(|v| v.as_str());
    let limit = payload.get("limit").and_then(|v| v.as_i64()).unwrap_or(20);

    match document_index::list(pool, app_id, tenant_id, index_type, limit).await {
        Ok(value) => value,
        Err(e) => serde_json::json!({ "error": format!("document index list error: {}", e) }),
    }
}

pub async fn query(pool: &sqlx::PgPool, payload: Value) -> Value {
    let query_text = payload.get("query").and_then(|v| v.as_str()).unwrap_or("");
    let app_id = payload.get("app_id").and_then(|v| v.as_str());
    let tenant_id = payload.get("tenant_id").and_then(|v| v.as_str());
    let document_id = payload.get("document_id").and_then(|v| v.as_str());
    let limit = payload.get("limit").and_then(|v| v.as_i64()).unwrap_or(8);

    match document_index::query(pool, query_text, app_id, tenant_id, document_id, limit).await {
        Ok(value) => value,
        Err(e) => serde_json::json!({ "error": format!("document index query error: {}", e) }),
    }
}
