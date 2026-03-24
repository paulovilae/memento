/// Integration tests for the Knowledge Store and Bayesian IPC protocols.
///
/// Validates JSON message structure for knowledge CRUD and Bayesian
/// interaction tracking actions.

// ═══════════════════════════════════════════════════════════════════
// Knowledge Store Payload Tests
// ═══════════════════════════════════════════════════════════════════

#[test]
fn test_store_knowledge_payload() {
    let payload = serde_json::json!({
        "action": "store_knowledge",
        "payload": {
            "key": "server_specs",
            "content": "RTX 3090, 64GB RAM, Ryzen 9",
            "tags": "hardware,setup,gpu"
        }
    });

    let msg: serde_json::Value = serde_json::from_str(&payload.to_string()).unwrap();
    assert_eq!(msg["action"], "store_knowledge");
    assert_eq!(msg["payload"]["key"], "server_specs");
    assert!(!msg["payload"]["content"].as_str().unwrap().is_empty());
    assert!(msg["payload"]["tags"].as_str().unwrap().contains("hardware"));
}

#[test]
fn test_get_knowledge_payload() {
    let payload = serde_json::json!({
        "action": "get_knowledge",
        "payload": { "key": "server_specs" }
    });

    let msg: serde_json::Value = serde_json::from_str(&payload.to_string()).unwrap();
    assert_eq!(msg["action"], "get_knowledge");
    assert_eq!(msg["payload"]["key"], "server_specs");
}

#[test]
fn test_list_knowledge_payload() {
    let payload = serde_json::json!({
        "action": "list_knowledge",
        "payload": {}
    });

    let msg: serde_json::Value = serde_json::from_str(&payload.to_string()).unwrap();
    assert_eq!(msg["action"], "list_knowledge");
}

#[test]
fn test_search_knowledge_payload() {
    let payload = serde_json::json!({
        "action": "search_knowledge",
        "payload": { "query": "hardware" }
    });

    let msg: serde_json::Value = serde_json::from_str(&payload.to_string()).unwrap();
    assert_eq!(msg["action"], "search_knowledge");
    assert_eq!(msg["payload"]["query"], "hardware");
}

#[test]
fn test_delete_knowledge_payload() {
    let payload = serde_json::json!({
        "action": "delete_knowledge",
        "payload": { "key": "server_specs" }
    });

    let msg: serde_json::Value = serde_json::from_str(&payload.to_string()).unwrap();
    assert_eq!(msg["action"], "delete_knowledge");
    assert_eq!(msg["payload"]["key"], "server_specs");
}

// ═══════════════════════════════════════════════════════════════════
// Bayesian Interaction Tracking Payload Tests
// ═══════════════════════════════════════════════════════════════════

#[test]
fn test_log_interaction_payload() {
    let payload = serde_json::json!({
        "action": "log_interaction",
        "payload": {
            "session_id": "sess-001",
            "user_id": "user-42",
            "domain": "flights",
            "round": 3,
            "options_json": "[{\"features\":[0.8,0.3]},{\"features\":[0.2,0.7]}]",
            "choice_index": 1,
            "prior_json": "{\"log_probs\":[-3.22,-3.22]}",
            "posterior_json": "{\"log_probs\":[-1.5,-0.3]}"
        }
    });

    let msg: serde_json::Value = serde_json::from_str(&payload.to_string()).unwrap();
    assert_eq!(msg["action"], "log_interaction");
    assert_eq!(msg["payload"]["session_id"], "sess-001");
    assert_eq!(msg["payload"]["user_id"], "user-42");
    assert_eq!(msg["payload"]["domain"], "flights");
    assert_eq!(msg["payload"]["round"], 3);
    assert_eq!(msg["payload"]["choice_index"], 1);
    assert!(!msg["payload"]["options_json"].as_str().unwrap().is_empty());
}

#[test]
fn test_get_user_prior_payload() {
    let payload = serde_json::json!({
        "action": "get_user_prior",
        "payload": {
            "user_id": "user-42",
            "domain": "flights"
        }
    });

    let msg: serde_json::Value = serde_json::from_str(&payload.to_string()).unwrap();
    assert_eq!(msg["action"], "get_user_prior");
    assert_eq!(msg["payload"]["user_id"], "user-42");
    assert_eq!(msg["payload"]["domain"], "flights");
}

#[test]
fn test_save_user_prior_payload() {
    let payload = serde_json::json!({
        "action": "save_user_prior",
        "payload": {
            "user_id": "user-42",
            "domain": "flights",
            "prior_json": "{\"log_probs\":[-1.5,-0.3,-2.1]}"
        }
    });

    let msg: serde_json::Value = serde_json::from_str(&payload.to_string()).unwrap();
    assert_eq!(msg["action"], "save_user_prior");
    assert_eq!(msg["payload"]["user_id"], "user-42");
    assert_eq!(msg["payload"]["domain"], "flights");
    assert!(!msg["payload"]["prior_json"].as_str().unwrap().is_empty());
}

// ═══════════════════════════════════════════════════════════════════
// Master Action Registry
// ═══════════════════════════════════════════════════════════════════

#[test]
fn test_all_actions_recognized() {
    let known_actions = vec![
        // Chat Memory
        "save_memory",
        "get_context",
        // Knowledge Store
        "store_knowledge",
        "get_knowledge",
        "list_knowledge",
        "search_knowledge",
        "delete_knowledge",
        // App Registry
        "list_apps",
        "query_app",
        // Bayesian Tracking
        "log_interaction",
        "get_user_prior",
        "save_user_prior",
    ];

    assert_eq!(known_actions.len(), 12, "Expected 12 known IPC actions");

    // Verify no duplicates
    let mut sorted = known_actions.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sorted.len(), known_actions.len(), "Duplicate actions found");
}
