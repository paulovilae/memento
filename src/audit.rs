use hmac::{Hmac, Mac};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::Row;
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

    // `signature_verified` must reflect an ACTUAL cryptographic verification —
    // not the mere presence of a `trace_signature` field (the old is_some()
    // check flagged rows as verified even when no signing key was configured, so
    // nothing was ever checked). verify_trace_signature returns Ok(true) only
    // when a signature was validated against a configured key.
    let signature_verified = match verify_trace_signature(&payload) {
        Ok(verified) => verified,
        Err(error) => return serde_json::json!({ "error": error }),
    };

    let payload_json = payload.clone();
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

// ─── query ────────────────────────────────────────────────────────────────
//
// Read path for audit_log — used by incident diagnosis (e.g. "what did actor
// X touch on target_app Y since Z"). Deliberately excludes payload_json and
// the hash-chain columns (prev_entry_hash/entry_hash/signature_verified):
// those are internal to the tamper-evidence mechanism, not needed for a
// diagnosis read, and payload_json can carry arbitrary caller-supplied data.
//
// Payload (all optional): target_app: String, since: String (ISO8601,
// filters timestamp >= since), sensitive_action: bool (the column is TEXT —
// a free-form label set by callers like Sentinel's "system_event" — so the
// bool here means "was a label present at all": true = only rows where
// sensitive_action IS NOT NULL, false = only rows where it IS NULL), and
// limit: i64 (default 50, clamped to 500).
pub async fn query(pool: &sqlx::PgPool, payload: Value) -> Value {
    let target_app = payload
        .get("target_app")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let since = payload
        .get("since")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let sensitive_action = payload.get("sensitive_action").and_then(|v| v.as_bool());
    let limit = payload
        .get("limit")
        .and_then(|v| v.as_i64())
        .unwrap_or(50)
        .clamp(1, 500);

    let rows = match sqlx::query(
        "SELECT id, actor, expert_identity, capability_used, sensitive_action, target_app, \
         target_page, mutation_description, tenant_id, session_id, timestamp \
         FROM audit_log \
         WHERE ($1::text IS NULL OR target_app = $1) \
           AND ($2::text IS NULL OR timestamp >= $2::timestamp) \
           AND ($3::bool IS NULL OR (sensitive_action IS NOT NULL) = $3) \
         ORDER BY id DESC LIMIT $4",
    )
    .bind(&target_app)
    .bind(&since)
    .bind(sensitive_action)
    .bind(limit)
    .fetch_all(pool)
    .await
    {
        Ok(rows) => rows,
        Err(error) => {
            return serde_json::json!({ "status": "error", "message": format!("DB error: {}", error) });
        }
    };

    let entries: Vec<Value> = rows
        .iter()
        .map(|r| {
            serde_json::json!({
                "id": r.get::<i32, _>("id"),
                "actor": r.get::<String, _>("actor"),
                "expert_identity": r.get::<String, _>("expert_identity"),
                "capability_used": r.get::<String, _>("capability_used"),
                "sensitive_action": r.get::<Option<String>, _>("sensitive_action"),
                "target_app": r.get::<Option<String>, _>("target_app"),
                "target_page": r.get::<Option<String>, _>("target_page"),
                "mutation_description": r.get::<String, _>("mutation_description"),
                "tenant_id": r.get::<Option<String>, _>("tenant_id"),
                "session_id": r.get::<Option<String>, _>("session_id"),
                "timestamp": ts_str(r.get::<Option<chrono::NaiveDateTime>, _>("timestamp")),
            })
        })
        .collect();
    let count = entries.len();

    serde_json::json!({ "status": "success", "data": { "entries": entries, "count": count } })
}

fn ts_str(dt: Option<chrono::NaiveDateTime>) -> Option<String> {
    dt.map(|t| t.format("%Y-%m-%dT%H:%M:%S").to_string())
}

pub async fn purge_expired_audit_entries(pool: &sqlx::PgPool) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(
        "DELETE FROM audit_log WHERE retention_until IS NOT NULL AND retention_until < NOW()",
    )
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

/// Returns `Ok(true)` when a `trace_signature` was actually validated against a
/// configured signing key, `Ok(false)` when there is nothing to verify (no
/// `trace_payload`, or no key configured for the actor), and `Err` when a
/// signature was required but is missing/malformed/wrong.
fn verify_trace_signature(payload: &Value) -> Result<bool, String> {
    let Some(trace_payload) = payload.get("trace_payload") else {
        return Ok(false);
    };

    let target_app = payload
        .get("actor")
        .and_then(|value| value.as_str())
        .unwrap_or("sentinel")
        .trim()
        .to_lowercase();

    let Some(key) = signature_key_for(&target_app) else {
        return Ok(false);
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
        Ok(true)
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

    // Both cases share the `MEMENTO_AUDIT_SIGNATURE_KEYS` env var, so they live
    // in one test to avoid a parallel-execution race on the global.
    #[test]
    fn signature_verified_reflects_actual_verification() {
        // 1. No key configured → nothing is verified, but it is not an error.
        std::env::remove_var("MEMENTO_AUDIT_SIGNATURE_KEYS");
        let unsigned = serde_json::json!({
            "actor": "sentinel",
            "trace_payload": {"host": "imaginos.ai"},
            "trace_signature": "ignored",
            "trace_signed_at": "2026-01-01T00:00:00Z",
            "trace_signature_alg": "hmac-sha256"
        });
        assert_eq!(verify_trace_signature(&unsigned), Ok(false));

        // 2. Key configured + matching signature → genuinely verified.
        let key = "test-secret-key";
        let trace_payload = serde_json::json!({"host": "imaginos.ai"});
        let signed_at = "2026-01-01T00:00:00Z";
        let serialized = serde_json::to_string(&trace_payload).unwrap();
        let signing_input = format!("{}\n{}", signed_at, serialized);
        let mut mac = HmacSha256::new_from_slice(key.as_bytes()).unwrap();
        mac.update(signing_input.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());

        std::env::set_var("MEMENTO_AUDIT_SIGNATURE_KEYS", format!("sentinel={}", key));
        let signed = serde_json::json!({
            "actor": "sentinel",
            "trace_payload": trace_payload,
            "trace_signature": signature,
            "trace_signed_at": signed_at,
            "trace_signature_alg": "hmac-sha256"
        });
        let ok = verify_trace_signature(&signed);
        // 3. Key configured + wrong signature → error (blocks the insert).
        let tampered = serde_json::json!({
            "actor": "sentinel",
            "trace_payload": {"host": "evil.example"},
            "trace_signature": signature,
            "trace_signed_at": signed_at,
            "trace_signature_alg": "hmac-sha256"
        });
        let bad = verify_trace_signature(&tampered);
        std::env::remove_var("MEMENTO_AUDIT_SIGNATURE_KEYS");
        assert_eq!(ok, Ok(true));
        assert!(bad.is_err());
    }
}
