use super::*;
use serde_json::Value;

#[derive(Debug, Clone)]
pub(super) struct DerivationSeed {
    pub(super) user_id: String,
    pub(super) tenant_id: String,
    pub(super) app_id: String,
    pub(super) session_id: String,
    pub(super) scope: String,
    pub(super) source: String,
    pub(super) wing: String,
    pub(super) hall: String,
    pub(super) room: String,
    pub(super) memory_type: String,
    pub(super) derivation_method: Option<String>,
}

#[derive(Debug, Clone)]
pub(super) struct SaveRecordInput {
    pub(super) user_id: String,
    pub(super) tenant_id: String,
    pub(super) app_id: String,
    pub(super) expert_id: String,
    pub(super) session_id: String,
    pub(super) device_id: String,
    pub(super) scope: String,
    pub(super) source: String,
    pub(super) wing: String,
    pub(super) hall: String,
    pub(super) room: String,
    pub(super) entry_title: String,
    pub(super) memory_type: String,
    pub(super) content: String,
    pub(super) tags: Vec<String>,
    pub(super) content_json: Option<Value>,
    pub(super) confidence: Option<f64>,
    pub(super) provenance_refs: Option<Value>,
    pub(super) derivation_method: Option<String>,
    pub(super) status: String,
    pub(super) expires_at: Option<String>,
    pub(super) usage_count: i32,
    pub(super) last_used_at: Option<String>,
    pub(super) promoted_from: Option<String>,
}

impl SaveRecordInput {
    pub(super) fn from_payload(payload: &Value) -> Result<Self, Value> {
        let user_id = payload
            .get("user_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let content = payload
            .get("content")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if user_id.is_empty() || content.is_empty() {
            return Err(
                serde_json::json!({ "error": "Missing 'user_id' or 'content' in payload" }),
            );
        }

        Ok(Self {
            user_id,
            tenant_id: payload
                .get("tenant_id")
                .and_then(|v| v.as_str())
                .unwrap_or("default")
                .to_string(),
            app_id: payload
                .get("app_id")
                .and_then(|v| v.as_str())
                .unwrap_or("os")
                .to_string(),
            expert_id: payload
                .get("expert_id")
                .and_then(|v| v.as_str())
                .unwrap_or("ava")
                .to_string(),
            session_id: payload
                .get("session_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            device_id: payload
                .get("device_id")
                .and_then(|v| v.as_str())
                .unwrap_or("server")
                .to_string(),
            scope: payload
                .get("scope")
                .and_then(|v| v.as_str())
                .unwrap_or("personal")
                .to_string(),
            source: payload
                .get("source")
                .and_then(|v| v.as_str())
                .unwrap_or("chat")
                .to_string(),
            wing: payload
                .get("wing")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            hall: payload
                .get("hall")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            room: payload
                .get("room")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            entry_title: payload
                .get("entry_title")
                .and_then(|v| v.as_str())
                .map(|value| value.trim())
                .filter(|value| !value.is_empty())
                .map(|value| value.to_string())
                .unwrap_or_else(|| derive_entry_title(&content)),
            memory_type: payload
                .get("memory_type")
                .and_then(|v| v.as_str())
                .unwrap_or("event")
                .to_string(),
            content,
            tags: normalize_tags(payload.get("tags")),
            content_json: payload
                .get("content_json")
                .filter(|v| !v.is_null())
                .cloned(),
            confidence: payload.get("confidence").and_then(|v| v.as_f64()),
            provenance_refs: payload
                .get("provenance_refs")
                .filter(|v| !v.is_null())
                .cloned(),
            derivation_method: payload
                .get("derivation_method")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            status: payload
                .get("status")
                .and_then(|v| v.as_str())
                .unwrap_or("active")
                .to_string(),
            expires_at: payload
                .get("expires_at")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            usage_count: payload
                .get("usage_count")
                .and_then(|v| v.as_i64())
                .unwrap_or(0)
                .max(0) as i32,
            last_used_at: payload
                .get("last_used_at")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            promoted_from: payload
                .get("promoted_from")
                .and_then(|v| v.as_str())
                .map(str::to_string),
        })
    }

    pub(super) fn seed(&self) -> DerivationSeed {
        DerivationSeed {
            user_id: self.user_id.clone(),
            tenant_id: self.tenant_id.clone(),
            app_id: self.app_id.clone(),
            session_id: self.session_id.clone(),
            scope: self.scope.clone(),
            source: self.source.clone(),
            wing: self.wing.clone(),
            hall: self.hall.clone(),
            room: self.room.clone(),
            memory_type: self.memory_type.clone(),
            derivation_method: self.derivation_method.clone(),
        }
    }
}

pub(super) async fn insert_record_only(
    pool: &sqlx::PgPool,
    input: &SaveRecordInput,
) -> Result<i32, sqlx::Error> {
    sqlx::query_scalar::<_, i32>("INSERT INTO scoped_memory (user_id, tenant_id, app_id, expert_id, session_id, device_id, scope, source, wing, hall, room, entry_title, memory_type, content, tags_json, content_json, confidence, provenance_refs, derivation_method, status, expires_at, usage_count, last_used_at, promoted_from) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, CAST($21 AS TIMESTAMP), $22, CAST($23 AS TIMESTAMP), $24) RETURNING id")
        .bind(&input.user_id)
        .bind(&input.tenant_id)
        .bind(&input.app_id)
        .bind(&input.expert_id)
        .bind(&input.session_id)
        .bind(&input.device_id)
        .bind(&input.scope)
        .bind(&input.source)
        .bind(&input.wing)
        .bind(&input.hall)
        .bind(&input.room)
        .bind(&input.entry_title)
        .bind(&input.memory_type)
        .bind(&input.content)
        .bind(if input.tags.is_empty() {
            None
        } else {
            Some(
                Value::Array(
                    input
                        .tags
                        .iter()
                        .map(|tag| Value::String(tag.clone()))
                        .collect(),
                )
                .to_string(),
            )
        })
        .bind(input.content_json.as_ref().map(|value| value.to_string()))
        .bind(input.confidence)
        .bind(input.provenance_refs.as_ref().map(|value| value.to_string()))
        .bind(input.derivation_method.as_deref())
        .bind(&input.status)
        .bind(input.expires_at.as_deref())
        .bind(input.usage_count)
        .bind(input.last_used_at.as_deref())
        .bind(input.promoted_from.as_deref())
        .fetch_one(pool)
        .await
}

pub(super) fn save_record_response(input: &SaveRecordInput, record_id: i32, derived: Vec<Value>) -> Value {
    serde_json::json!({
        "status": "success",
        "action": "memory_record_saved",
        "record_id": record_id,
        "scope": input.scope,
        "user_id": input.user_id,
        "memory_type": input.memory_type,
        "wing": input.wing,
        "hall": input.hall,
        "room": input.room,
        "entry_title": input.entry_title,
        "usage_count": input.usage_count,
        "promoted_from": input.promoted_from,
        "derived": derived
    })
}
