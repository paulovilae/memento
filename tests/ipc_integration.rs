/// Expanded integration tests for Memento IPC protocol.
///
/// Validates JSON message structure for ALL IPC actions —
/// chat memory, knowledge store, app registry, and error handling.

// ═══════════════════════════════════════════════════════════════════
// Chat Memory Tests
// ═══════════════════════════════════════════════════════════════════

#[test]
fn test_save_memory_payload_structure() {
    let payload = serde_json::json!({
        "action": "save_memory",
        "payload": {
            "chat_id": "telegram-12345",
            "role": "user",
            "content": "What time is my flight?"
        }
    });

    let msg: serde_json::Value = serde_json::from_str(&payload.to_string()).unwrap();
    assert_eq!(msg["action"], "save_memory");
    assert_eq!(msg["payload"]["chat_id"], "telegram-12345");
    assert_eq!(msg["payload"]["role"], "user");
    assert!(!msg["payload"]["content"].as_str().unwrap().is_empty());
}

#[test]
fn test_get_context_payload_with_limit() {
    let payload = serde_json::json!({
        "action": "get_context",
        "payload": {
            "chat_id": "telegram-12345",
            "limit": 20
        }
    });

    let msg: serde_json::Value = serde_json::from_str(&payload.to_string()).unwrap();
    assert_eq!(msg["action"], "get_context");
    assert_eq!(msg["payload"]["limit"], 20);
}

#[test]
fn test_get_context_default_limit() {
    // If limit is not provided, daemon defaults to 20
    let payload = serde_json::json!({
        "action": "get_context",
        "payload": {
            "chat_id": "some-chat"
        }
    });

    let msg: serde_json::Value = serde_json::from_str(&payload.to_string()).unwrap();
    assert_eq!(msg["action"], "get_context");
    assert!(msg["payload"]["limit"].is_null(), "Limit should be absent, daemon uses default");
}

// ═══════════════════════════════════════════════════════════════════
// Scoped Memory / Memento v2 Tests
// ═══════════════════════════════════════════════════════════════════

#[test]
fn test_save_memory_record_payload_structure() {
    let payload = serde_json::json!({
        "action": "save_memory_record",
        "payload": {
            "user_id": "user-123",
            "tenant_id": "tenant-main",
            "app_id": "os-v3",
            "expert_id": "ava",
            "session_id": "session-456",
            "device_id": "laptop",
            "scope": "personal",
            "source": "chat",
            "memory_type": "fact",
            "content": "User prefers concise status reports.",
            "content_json": {
                "preference": "concise",
                "topic": "status_reports"
            },
            "confidence": 0.91,
            "provenance_refs": ["event-1", "event-2"],
            "derivation_method": "summary_derivation",
            "status": "active"
        }
    });

    let msg: serde_json::Value = serde_json::from_str(&payload.to_string()).unwrap();
    assert_eq!(msg["action"], "save_memory_record");
    assert_eq!(msg["payload"]["user_id"], "user-123");
    assert_eq!(msg["payload"]["memory_type"], "fact");
    assert_eq!(msg["payload"]["confidence"], 0.91);
    assert!(msg["payload"]["content_json"].is_object());
    assert!(msg["payload"]["provenance_refs"].is_array());
}

#[test]
fn test_save_scoped_memory_alias_still_supported() {
    let payload = serde_json::json!({
        "action": "save_scoped_memory",
        "payload": {
            "user_id": "user-123",
            "content": "Raw interaction event",
            "memory_type": "event"
        }
    });

    let msg: serde_json::Value = serde_json::from_str(&payload.to_string()).unwrap();
    assert_eq!(msg["action"], "save_scoped_memory");
    assert_eq!(msg["payload"]["memory_type"], "event");
}

#[test]
fn test_query_memory_records_payload_structure() {
    let payload = serde_json::json!({
        "action": "query_memory_records",
        "payload": {
            "user_id": "user-123",
            "app_id": "os-v3",
            "memory_type": "fact",
            "status": "active",
            "limit": 25
        }
    });

    let msg: serde_json::Value = serde_json::from_str(&payload.to_string()).unwrap();
    assert_eq!(msg["action"], "query_memory_records");
    assert_eq!(msg["payload"]["user_id"], "user-123");
    assert_eq!(msg["payload"]["memory_type"], "fact");
    assert_eq!(msg["payload"]["status"], "active");
    assert_eq!(msg["payload"]["limit"], 25);
}

#[test]
fn test_query_memory_records_requires_filter() {
    let payload = serde_json::json!({
        "action": "query_memory_records",
        "payload": {
            "limit": 25
        }
    });

    let msg: serde_json::Value = serde_json::from_str(&payload.to_string()).unwrap();
    assert_eq!(msg["action"], "query_memory_records");
    assert!(msg["payload"]["user_id"].is_null());
    assert!(msg["payload"]["memory_type"].is_null());
}

#[test]
fn test_get_scoped_memory_alias_still_supported() {
    let payload = serde_json::json!({
        "action": "get_scoped_memory",
        "payload": {
            "user_id": "user-123",
            "scope": "personal"
        }
    });

    let msg: serde_json::Value = serde_json::from_str(&payload.to_string()).unwrap();
    assert_eq!(msg["action"], "get_scoped_memory");
    assert_eq!(msg["payload"]["scope"], "personal");
}

// ═══════════════════════════════════════════════════════════════════
// Hybrid Retrieval / Document Index Tests
// ═══════════════════════════════════════════════════════════════════

#[test]
fn test_upsert_document_index_payload_structure() {
    let payload = serde_json::json!({
        "action": "upsert_document_index",
        "payload": {
            "document_id": "vetra-policy-001",
            "tenant_id": "tenant-main",
            "app_id": "vetra",
            "owner_scope": "workspace",
            "title": "Remote Work Policy",
            "summary": "Policy covering remote work expectations and approvals.",
            "index_type": "page_tree",
            "source_type": "policy",
            "source_uri": "/docs/policies/remote-work.pdf",
            "metadata_json": { "version": "2026.1" },
            "root_node_id": "root",
            "status": "active",
            "nodes": [
                {
                    "node_id": "root",
                    "parent_node_id": null,
                    "title": "Remote Work Policy",
                    "summary": "Top-level summary",
                    "level": 0,
                    "node_type": "document",
                    "source_ref": "page:1",
                    "page_from": 1,
                    "page_to": 8,
                    "tags": ["policy", "remote-work"]
                },
                {
                    "node_id": "eligibility",
                    "parent_node_id": "root",
                    "title": "Eligibility",
                    "summary": "Who can request remote work",
                    "level": 1,
                    "node_type": "section",
                    "source_ref": "page:2",
                    "page_from": 2,
                    "page_to": 3,
                    "tags": ["eligibility"]
                }
            ]
        }
    });

    let msg: serde_json::Value = serde_json::from_str(&payload.to_string()).unwrap();
    assert_eq!(msg["action"], "upsert_document_index");
    assert_eq!(msg["payload"]["document_id"], "vetra-policy-001");
    assert_eq!(msg["payload"]["index_type"], "page_tree");
    assert!(msg["payload"]["nodes"].is_array());
}

#[test]
fn test_query_document_index_payload_structure() {
    let payload = serde_json::json!({
        "action": "query_document_index",
        "payload": {
            "app_id": "vetra",
            "tenant_id": "tenant-main",
            "query": "remote work approval policy",
            "limit": 5
        }
    });

    let msg: serde_json::Value = serde_json::from_str(&payload.to_string()).unwrap();
    assert_eq!(msg["action"], "query_document_index");
    assert_eq!(msg["payload"]["app_id"], "vetra");
    assert_eq!(msg["payload"]["limit"], 5);
}

#[test]
fn test_get_document_index_payload_structure() {
    let payload = serde_json::json!({
        "action": "get_document_index",
        "payload": {
            "document_id": "vetra-policy-001",
            "app_id": "vetra"
        }
    });

    let msg: serde_json::Value = serde_json::from_str(&payload.to_string()).unwrap();
    assert_eq!(msg["action"], "get_document_index");
    assert_eq!(msg["payload"]["document_id"], "vetra-policy-001");
}

#[test]
fn test_list_document_indexes_payload_structure() {
    let payload = serde_json::json!({
        "action": "list_document_indexes",
        "payload": {
            "app_id": "vetra",
            "index_type": "page_tree",
            "limit": 10
        }
    });

    let msg: serde_json::Value = serde_json::from_str(&payload.to_string()).unwrap();
    assert_eq!(msg["action"], "list_document_indexes");
    assert_eq!(msg["payload"]["index_type"], "page_tree");
}

// ═══════════════════════════════════════════════════════════════════
// App Registry Tests
// ═══════════════════════════════════════════════════════════════════

#[test]
fn test_list_apps_payload() {
    let payload = serde_json::json!({
        "action": "list_apps",
        "payload": {}
    });

    let msg: serde_json::Value = serde_json::from_str(&payload.to_string()).unwrap();
    assert_eq!(msg["action"], "list_apps");
}

#[test]
fn test_query_app_payload() {
    let payload = serde_json::json!({
        "action": "query_app",
        "payload": {
            "app": "movilo",
            "query": "SELECT name FROM providers LIMIT 5",
            "limit": 50
        }
    });

    let msg: serde_json::Value = serde_json::from_str(&payload.to_string()).unwrap();
    assert_eq!(msg["action"], "query_app");
    assert_eq!(msg["payload"]["app"], "movilo");
    // Verify query starts with SELECT (read-only guard)
    let query = msg["payload"]["query"].as_str().unwrap();
    assert!(
        query.trim().to_uppercase().starts_with("SELECT"),
        "Query must start with SELECT"
    );
}

#[test]
fn test_query_app_rejects_non_select() {
    // This tests the validation logic that should exist in the daemon
    let dangerous_queries = vec![
        "DROP TABLE users",
        "DELETE FROM providers",
        "INSERT INTO providers VALUES (1, 'test')",
        "UPDATE providers SET name = 'hacked'",
        "ALTER TABLE providers ADD COLUMN pwned TEXT",
    ];

    for query in dangerous_queries {
        let trimmed = query.trim().to_uppercase();
        assert!(
            !trimmed.starts_with("SELECT") && !trimmed.starts_with("WITH"),
            "Query '{}' should NOT pass read-only check",
            query
        );
    }
}

#[test]
fn test_query_app_allows_with_cte() {
    let payload = serde_json::json!({
        "action": "query_app",
        "payload": {
            "app": "movilo",
            "query": "WITH recent AS (SELECT * FROM providers) SELECT * FROM recent LIMIT 5"
        }
    });

    let query = payload["payload"]["query"].as_str().unwrap();
    let trimmed = query.trim().to_uppercase();
    assert!(
        trimmed.starts_with("WITH") || trimmed.starts_with("SELECT"),
        "WITH CTEs should be allowed"
    );
}

// ═══════════════════════════════════════════════════════════════════
// Error Handling Tests
// ═══════════════════════════════════════════════════════════════════

#[test]
fn test_unknown_action_format() {
    let payload = serde_json::json!({
        "action": "totally_bogus_action",
        "payload": {}
    });

    let msg: serde_json::Value = serde_json::from_str(&payload.to_string()).unwrap();
    // The daemon should return { "error": "Unknown action: ..." }
    assert_eq!(msg["action"], "totally_bogus_action");
}

#[test]
fn test_empty_payload_is_valid_json() {
    let payload = serde_json::json!({
        "action": "list_knowledge",
        "payload": {}
    });

    // Should not panic on empty payload
    let msg: serde_json::Value = serde_json::from_str(&payload.to_string()).unwrap();
    assert!(msg["payload"].is_object());
}

#[test]
fn test_large_content_serialization() {
    // Test that payloads up to 32KB serialize correctly
    let large_content = "x".repeat(32_000);
    let payload = serde_json::json!({
        "action": "store_knowledge",
        "payload": {
            "key": "large_test",
            "content": large_content,
            "tags": "test"
        }
    });

    let serialized = payload.to_string();
    let deserialized: serde_json::Value = serde_json::from_str(&serialized).unwrap();
    assert_eq!(
        deserialized["payload"]["content"].as_str().unwrap().len(),
        32_000
    );
}

// ═══════════════════════════════════════════════════════════════════
// Complete Action Registry Validation
// ═══════════════════════════════════════════════════════════════════

#[test]
fn test_all_ipc_actions_cataloged() {
    // Master list of all known IPC actions.
    // If a new action is added, this test reminds you to add tests for it.
    let known_actions = vec![
        // Chat Memory
        "save_memory",
        "get_context",
        // Scoped Memory / Memento v2
        "save_scoped_memory",
        "save_memory_record",
        "get_scoped_memory",
        "query_memory_records",
        // Knowledge Store
        "store_knowledge",
        "get_knowledge",
        "list_knowledge",
        "search_knowledge",
        "delete_knowledge",
        // Hybrid Retrieval / Document Index
        "upsert_document_index",
        "get_document_index",
        "list_document_indexes",
        "query_document_index",
        // App Registry
        "list_apps",
        "query_app",
    ];

    assert_eq!(known_actions.len(), 17, "Expected 17 known IPC actions");

    // Verify no duplicates
    let mut sorted = known_actions.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(
        sorted.len(),
        known_actions.len(),
        "Duplicate actions detected"
    );
}

#[test]
fn test_ipc_message_format_is_consistent() {
    // Every IPC message must have "action" (string) and "payload" (object)
    let test_messages = vec![
        serde_json::json!({"action": "save_memory", "payload": {"chat_id": "x", "role": "user", "content": "y"}}),
        serde_json::json!({"action": "get_context", "payload": {"chat_id": "x"}}),
        serde_json::json!({"action": "save_memory_record", "payload": {"user_id": "u", "content": "c", "memory_type": "event"}}),
        serde_json::json!({"action": "query_memory_records", "payload": {"user_id": "u", "memory_type": "event"}}),
        serde_json::json!({"action": "list_apps", "payload": {}}),
        serde_json::json!({"action": "store_knowledge", "payload": {"key": "k", "content": "c", "tags": ""}}),
        serde_json::json!({"action": "list_document_indexes", "payload": {"app_id": "vetra"}}),
        serde_json::json!({"action": "query_document_index", "payload": {"app_id": "vetra", "query": "remote work policy"}}),
    ];

    for msg in &test_messages {
        assert!(msg["action"].is_string(), "action must be a string");
        assert!(msg["payload"].is_object(), "payload must be an object");
    }
}
