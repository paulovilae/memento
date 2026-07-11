use serde_json::Value;
use sqlx::Row;

pub async fn log_interaction(pool: &sqlx::PgPool, payload: Value) -> Value {
    let session_id = payload
        .get("session_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let user_id = payload
        .get("user_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let domain = payload.get("domain").and_then(|v| v.as_str()).unwrap_or("");
    let round = payload.get("round").and_then(|v| v.as_i64()).unwrap_or(0);
    let options_json = payload
        .get("options_json")
        .and_then(|v| v.as_str())
        .unwrap_or("[]");
    let choice_index = payload
        .get("choice_index")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let prior_json = payload.get("prior_json").and_then(|v| v.as_str());
    let posterior_json = payload.get("posterior_json").and_then(|v| v.as_str());

    if session_id.is_empty() || user_id.is_empty() || domain.is_empty() {
        return serde_json::json!({ "error": "Missing session_id, user_id, or domain" });
    }

    let result = sqlx::query("INSERT INTO bayesian_interactions (session_id, user_id, domain, round, options_json, choice_index, prior_json, posterior_json) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)")
        .bind(session_id)
        .bind(user_id)
        .bind(domain)
        .bind(round)
        .bind(options_json)
        .bind(choice_index)
        .bind(prior_json)
        .bind(posterior_json)
        .execute(pool)
        .await;

    match result {
        Ok(_) => serde_json::json!({ "status": "success", "action": "logged" }),
        Err(e) => serde_json::json!({ "error": format!("DB error: {}", e) }),
    }
}

pub async fn get_user_prior(pool: &sqlx::PgPool, payload: Value) -> Value {
    let user_id = payload
        .get("user_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let domain = payload.get("domain").and_then(|v| v.as_str()).unwrap_or("");

    if user_id.is_empty() || domain.is_empty() {
        return serde_json::json!({ "error": "Missing user_id or domain" });
    }

    let result = sqlx::query(
        "SELECT prior_json, updated_at FROM user_priors WHERE user_id = $1 AND domain = $2",
    )
    .bind(user_id)
    .bind(domain)
    .fetch_optional(pool)
    .await;

    match result {
        Ok(Some(row)) => {
            let prior: String = row.get("prior_json");
            let updated: String = row.get("updated_at");
            serde_json::json!({
                "status": "success",
                "user_id": user_id,
                "domain": domain,
                "prior_json": prior,
                "updated_at": updated
            })
        }
        Ok(None) => serde_json::json!({
            "status": "not_found",
            "user_id": user_id,
            "domain": domain
        }),
        Err(e) => serde_json::json!({ "error": format!("DB error: {}", e) }),
    }
}

pub async fn save_user_prior(pool: &sqlx::PgPool, payload: Value) -> Value {
    let user_id = payload
        .get("user_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let domain = payload.get("domain").and_then(|v| v.as_str()).unwrap_or("");
    let prior_json = payload
        .get("prior_json")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    if user_id.is_empty() || domain.is_empty() || prior_json.is_empty() {
        return serde_json::json!({ "error": "Missing user_id, domain, or prior_json" });
    }

    let result = sqlx::query(
        r#"
        INSERT INTO user_priors (user_id, domain, prior_json, updated_at)
        VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
        ON CONFLICT(user_id, domain) DO UPDATE SET
            prior_json = excluded.prior_json,
            updated_at = CURRENT_TIMESTAMP
        "#,
    )
    .bind(user_id)
    .bind(domain)
    .bind(prior_json)
    .execute(pool)
    .await;

    match result {
        Ok(_) => serde_json::json!({
            "status": "success",
            "action": "saved",
            "user_id": user_id,
            "domain": domain
        }),
        Err(e) => serde_json::json!({ "error": format!("DB error: {}", e) }),
    }
}
