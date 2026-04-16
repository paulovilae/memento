use hmac::{Hmac, Mac};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::HashMap;

type HmacSha256 = Hmac<Sha256>;

pub async fn audit_log(pool: &sqlx::PgPool, payload: Value) -> Value {
    let actor = payload.get("actor").and_then(|v| v.as_str()).unwrap_or("");
    let expert_identity = payload
        .get("expert_identity")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let capability_used = payload
        .get("capability_used")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let sensitive_action = payload.get("sensitive_action").and_then(|v| v.as_str());
    let target_app = payload.get("target_app").and_then(|v| v.as_str());
    let target_page = payload.get("target_page").and_then(|v| v.as_str());
    let mutation_description = payload
        .get("mutation_description")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let tenant_id = payload.get("tenant_id").and_then(|v| v.as_str());
    let session_id = payload.get("session_id").and_then(|v| v.as_str());

    if actor.is_empty() || expert_identity.is_empty() || mutation_description.is_empty() {
        return serde_json::json!({ "error": "Missing required fields: actor, expert_identity, mutation_description" });
    }

    if let Err(error) = verify_trace_signature(&payload) {
        return serde_json::json!({ "error": error });
    }

    let payload_json = payload.clone();
    let signature_verified = payload
        .get("trace_signature")
        .and_then(|value| value.as_str())
        .is_some();
    let retention_until = retention_deadline();

    let mut tx = match pool.begin().await {
        Ok(tx) => tx,
        Err(error) => {
            return serde_json::json!({ "error": format!("DB transaction error: {}", error) });
        }
    };

    let previous_hash: Option<String> = match sqlx::query_scalar(
        "SELECT entry_hash FROM audit_log WHERE entry_hash IS NOT NULL ORDER BY id DESC LIMIT 1",
    )
    .fetch_optional(&mut *tx)
    .await
    {
        Ok(value) => value,
        Err(error) => {
            return serde_json::json!({ "error": format!("DB error: {}", error) });
        }
    };

    let entry_hash = compute_audit_entry_hash(previous_hash.as_deref(), &payload_json);
    let result = sqlx::query("INSERT INTO audit_log (actor, expert_identity, capability_used, sensitive_action, target_app, target_page, mutation_description, tenant_id, session_id, payload_json, prev_entry_hash, entry_hash, signature_verified, retention_until) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)")
        .bind(actor)
        .bind(expert_identity)
        .bind(capability_used)
        .bind(sensitive_action)
        .bind(target_app)
        .bind(target_page)
        .bind(mutation_description)
        .bind(tenant_id)
        .bind(session_id)
        .bind(payload_json)
        .bind(previous_hash)
        .bind(&entry_hash)
        .bind(signature_verified)
        .bind(retention_until)
        .execute(&mut *tx)
        .await;

    match result {
        Ok(_) => match tx.commit().await {
            Ok(_) => serde_json::json!({
                "status": "success",
                "action": "audit_logged",
                "actor": actor,
                "entry_hash": entry_hash
            }),
            Err(error) => serde_json::json!({ "error": format!("DB commit error: {}", error) }),
        },
        Err(e) => serde_json::json!({ "error": format!("DB error: {}", e) }),
    }
}

pub async fn purge_expired_audit_entries(pool: &sqlx::PgPool) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(
        "DELETE FROM audit_log WHERE retention_until IS NOT NULL AND retention_until < NOW()",
    )
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

fn verify_trace_signature(payload: &Value) -> Result<(), String> {
    let Some(trace_payload) = payload.get("trace_payload") else {
        return Ok(());
    };

    let target_app = payload
        .get("actor")
        .and_then(|value| value.as_str())
        .unwrap_or("sentinel")
        .trim()
        .to_lowercase();

    let Some(key) = signature_key_for(&target_app) else {
        return Ok(());
    };

    let Some(signature) = payload
        .get("trace_signature")
        .and_then(|value| value.as_str())
    else {
        return Err("trace signature missing".into());
    };
    let Some(signed_at) = payload
        .get("trace_signed_at")
        .and_then(|value| value.as_str())
    else {
        return Err("trace signed timestamp missing".into());
    };
    let algorithm = payload
        .get("trace_signature_alg")
        .and_then(|value| value.as_str())
        .unwrap_or("hmac-sha256");
    if algorithm != "hmac-sha256" {
        return Err(format!(
            "unsupported trace signature algorithm: {}",
            algorithm
        ));
    }

    let serialized = serde_json::to_string(trace_payload)
        .map_err(|error| format!("failed to serialize trace payload: {}", error))?;
    let signing_input = format!("{}\n{}", signed_at, serialized);
    let mut mac = HmacSha256::new_from_slice(key.as_bytes())
        .map_err(|error| format!("invalid HMAC key: {}", error))?;
    mac.update(signing_input.as_bytes());
    let expected = hex::encode(mac.finalize().into_bytes());

    if expected == signature {
        Ok(())
    } else {
        Err("trace signature verification failed".into())
    }
}

fn signature_key_for(app: &str) -> Option<String> {
    let raw = std::env::var("MEMENTO_AUDIT_SIGNATURE_KEYS")
        .ok()
        .or_else(|| {
            let path = std::env::var("MEMENTO_AUDIT_SIGNATURE_KEYS_FILE").ok()?;
            std::fs::read_to_string(path).ok()
        })?;
    if let Ok(map) = serde_json::from_str::<HashMap<String, String>>(&raw) {
        return map
            .into_iter()
            .find(|(name, _)| name.trim().eq_ignore_ascii_case(app))
            .map(|(_, key)| key);
    }

    raw.split(',')
        .filter_map(|pair| pair.split_once('='))
        .find(|(name, _)| name.trim().eq_ignore_ascii_case(app))
        .map(|(_, key)| key.trim().to_string())
}

fn retention_deadline() -> Option<chrono::DateTime<chrono::Utc>> {
    let days = std::env::var("MEMENTO_AUDIT_RETENTION_DAYS")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(365);
    (days > 0).then(|| chrono::Utc::now() + chrono::Duration::days(days))
}

fn compute_audit_entry_hash(previous_hash: Option<&str>, payload: &Value) -> String {
    let previous_hash = previous_hash.unwrap_or("");
    let serialized = serde_json::to_string(payload).unwrap_or_else(|_| "null".into());
    let mut hasher = Sha256::new();
    hasher.update(previous_hash.as_bytes());
    hasher.update(b"\n");
    hasher.update(serialized.as_bytes());
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verification_is_optional_without_configured_key() {
        std::env::remove_var("MEMENTO_AUDIT_SIGNATURE_KEYS");
        let payload = serde_json::json!({
            "actor": "sentinel",
            "trace_payload": {"host": "imaginos.ai"},
            "trace_signature": "ignored",
            "trace_signed_at": "2026-01-01T00:00:00Z",
            "trace_signature_alg": "hmac-sha256"
        });
        assert!(verify_trace_signature(&payload).is_ok());
    }
}
