use serde_json::Value;

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

    let result = sqlx::query("INSERT INTO audit_log (actor, expert_identity, capability_used, sensitive_action, target_app, target_page, mutation_description, tenant_id, session_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)")
        .bind(actor)
        .bind(expert_identity)
        .bind(capability_used)
        .bind(sensitive_action)
        .bind(target_app)
        .bind(target_page)
        .bind(mutation_description)
        .bind(tenant_id)
        .bind(session_id)
        .execute(pool)
        .await;

    match result {
        Ok(_) => serde_json::json!({
            "status": "success",
            "action": "audit_logged",
            "actor": actor
        }),
        Err(e) => serde_json::json!({ "error": format!("DB error: {}", e) }),
    }
}
