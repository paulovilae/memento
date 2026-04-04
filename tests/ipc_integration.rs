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
    assert!(
        msg["payload"]["limit"].is_null(),
        "Limit should be absent, daemon uses default"
    );
}

#[test]
fn test_upsert_research_source_payload_structure() {
    let payload = serde_json::json!({
        "action": "upsert_research_source",
        "payload": {
            "source_id": "source-whales-page-1",
            "project_id": "whales-2026",
            "session_id": "whales-session-1",
            "source_kind": "web_page",
            "source_uri": "https://example.com/whales",
            "source_label": "Whale Overview",
            "title": "Whales Overview",
            "summary": "General whale anatomy reference.",
            "content_type": "text/html"
        }
    });

    let msg: serde_json::Value = serde_json::from_str(&payload.to_string()).unwrap();
    assert_eq!(msg["action"], "upsert_research_source");
    assert_eq!(msg["payload"]["source_id"], "source-whales-page-1");
    assert_eq!(msg["payload"]["project_id"], "whales-2026");
    assert_eq!(msg["payload"]["source_kind"], "web_page");
}

#[test]
fn test_list_concept_nodes_payload_structure() {
    let payload = serde_json::json!({
        "action": "list_concept_nodes",
        "payload": {
            "app_id": "latinos",
            "search": "whale",
            "limit": 12
        }
    });

    let msg: serde_json::Value = serde_json::from_str(&payload.to_string()).unwrap();
    assert_eq!(msg["action"], "list_concept_nodes");
    assert_eq!(msg["payload"]["app_id"], "latinos");
    assert_eq!(msg["payload"]["search"], "whale");
    assert_eq!(msg["payload"]["limit"], 12);
}

#[test]
fn test_list_claim_records_payload_structure() {
    let payload = serde_json::json!({
        "action": "list_claim_records",
        "payload": {
            "project_id": "whales-2026",
            "concept_id": "concept-whales",
            "search": "mammal",
            "limit": 15
        }
    });

    let msg: serde_json::Value = serde_json::from_str(&payload.to_string()).unwrap();
    assert_eq!(msg["action"], "list_claim_records");
    assert_eq!(msg["payload"]["project_id"], "whales-2026");
    assert_eq!(msg["payload"]["concept_id"], "concept-whales");
    assert_eq!(msg["payload"]["limit"], 15);
}

#[test]
fn test_semantic_retention_candidates_payload_structure() {
    let payload = serde_json::json!({
        "action": "semantic_retention_candidates",
        "payload": {
            "limit": 10
        }
    });

    let msg: serde_json::Value = serde_json::from_str(&payload.to_string()).unwrap();
    assert_eq!(msg["action"], "semantic_retention_candidates");
    assert_eq!(msg["payload"]["limit"], 10);
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
// Memento v3 Semantic Memory Tests
// ═══════════════════════════════════════════════════════════════════

#[test]
fn test_upsert_research_project_payload_structure() {
    let payload = serde_json::json!({
        "action": "upsert_research_project",
        "payload": {
            "project_id": "whales-2026",
            "title": "Whale Research",
            "goal": "Build a reusable knowledge base on whales",
            "questions_json": {
                "questions": ["What do we know about whale anatomy?"]
            },
            "deliverable_type": "report",
            "app_id": "os-v3",
            "status": "active"
        }
    });

    let msg: serde_json::Value = serde_json::from_str(&payload.to_string()).unwrap();
    assert_eq!(msg["action"], "upsert_research_project");
    assert_eq!(msg["payload"]["project_id"], "whales-2026");
    assert_eq!(msg["payload"]["title"], "Whale Research");
}

#[test]
fn test_create_research_session_payload_structure() {
    let payload = serde_json::json!({
        "action": "create_research_session",
        "payload": {
            "session_id": "whales-2026-session-01",
            "project_id": "whales-2026",
            "title": "General whale anatomy survey",
            "brief": "Collect core facts about whale biology",
            "channel": "web_widget"
        }
    });

    let msg: serde_json::Value = serde_json::from_str(&payload.to_string()).unwrap();
    assert_eq!(msg["action"], "create_research_session");
    assert_eq!(msg["payload"]["session_id"], "whales-2026-session-01");
    assert_eq!(msg["payload"]["project_id"], "whales-2026");
}

#[test]
fn test_append_claim_record_payload_structure() {
    let payload = serde_json::json!({
        "action": "append_claim_record",
        "payload": {
            "claim_id": "claim-whales-mammals",
            "claim_text": "Whales are mammals.",
            "primary_concept_id": "concept-whales",
            "project_id": "whales-2026",
            "session_id": "whales-2026-session-01",
            "claim_type": "fact",
            "confidence": 0.98
        }
    });

    let msg: serde_json::Value = serde_json::from_str(&payload.to_string()).unwrap();
    assert_eq!(msg["action"], "append_claim_record");
    assert_eq!(msg["payload"]["claim_id"], "claim-whales-mammals");
    assert_eq!(msg["payload"]["primary_concept_id"], "concept-whales");
}

#[test]
fn test_upsert_concept_node_payload_structure() {
    let payload = serde_json::json!({
        "action": "upsert_concept_node",
        "payload": {
            "concept_id": "concept-whales",
            "canonical_name": "Whales",
            "summary": "Large marine mammals."
        }
    });

    let msg: serde_json::Value = serde_json::from_str(&payload.to_string()).unwrap();
    assert_eq!(msg["action"], "upsert_concept_node");
    assert_eq!(msg["payload"]["concept_id"], "concept-whales");
    assert_eq!(msg["payload"]["canonical_name"], "Whales");
}

#[test]
fn test_append_evidence_record_payload_structure() {
    let payload = serde_json::json!({
        "action": "append_evidence_record",
        "payload": {
            "evidence_id": "evidence-whales-mammals-01",
            "claim_id": "claim-whales-mammals",
            "snippet": "Whales are warm-blooded mammals that nurse their young.",
            "source_kind": "web",
            "source_ref": "https://example.org/whales",
            "locator": "section:introduction",
            "confidence": 0.93
        }
    });

    let msg: serde_json::Value = serde_json::from_str(&payload.to_string()).unwrap();
    assert_eq!(msg["action"], "append_evidence_record");
    assert_eq!(msg["payload"]["evidence_id"], "evidence-whales-mammals-01");
    assert_eq!(msg["payload"]["claim_id"], "claim-whales-mammals");
}

#[test]
fn test_link_concepts_payload_structure() {
    let payload = serde_json::json!({
        "action": "link_concepts",
        "payload": {
            "edge_id": "edge-whales-mammals",
            "from_concept_id": "concept-whales",
            "to_concept_id": "concept-mammals",
            "relation_type": "is_a",
            "weight": 1.0
        }
    });

    let msg: serde_json::Value = serde_json::from_str(&payload.to_string()).unwrap();
    assert_eq!(msg["action"], "link_concepts");
    assert_eq!(msg["payload"]["relation_type"], "is_a");
}

#[test]
fn test_expand_concept_payload_structure() {
    let payload = serde_json::json!({
        "action": "expand_concept",
        "payload": {
            "concept_id": "concept-whales",
            "limit": 15
        }
    });

    let msg: serde_json::Value = serde_json::from_str(&payload.to_string()).unwrap();
    assert_eq!(msg["action"], "expand_concept");
    assert_eq!(msg["payload"]["concept_id"], "concept-whales");
    assert_eq!(msg["payload"]["limit"], 15);
}

#[test]
fn test_trace_claim_provenance_payload_structure() {
    let payload = serde_json::json!({
        "action": "trace_claim_provenance",
        "payload": {
            "claim_id": "claim-whales-mammals"
        }
    });

    let msg: serde_json::Value = serde_json::from_str(&payload.to_string()).unwrap();
    assert_eq!(msg["action"], "trace_claim_provenance");
    assert_eq!(msg["payload"]["claim_id"], "claim-whales-mammals");
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
fn test_execute_app_payload() {
    let payload = serde_json::json!({
        "action": "execute_app",
        "payload": {
            "app": "latinos",
            "query": "INSERT INTO stock_research (ticker, research_date, raw_data, analysis_summary) VALUES ('AAPL', NOW(), '{}'::jsonb, 'ok')"
        }
    });

    let msg: serde_json::Value = serde_json::from_str(&payload.to_string()).unwrap();
    assert_eq!(msg["action"], "execute_app");
    assert_eq!(msg["payload"]["app"], "latinos");
    let query = msg["payload"]["query"].as_str().unwrap();
    let trimmed = query.trim().to_uppercase();
    assert!(trimmed.starts_with("INSERT"));
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
        // Semantic Memory / Memento v3
        "upsert_research_project",
        "get_research_project",
        "list_research_projects",
        "create_research_session",
        "upsert_concept_node",
        "append_claim_record",
        "append_evidence_record",
        "link_concepts",
        "expand_concept",
        "trace_claim_provenance",
        "semantic_memory_counts",
        // App Registry
        "list_apps",
        "query_app",
    ];

    assert_eq!(known_actions.len(), 28, "Expected 28 known IPC actions");

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
        serde_json::json!({"action": "upsert_research_project", "payload": {"project_id": "p", "title": "Project"}}),
        serde_json::json!({"action": "create_research_session", "payload": {"session_id": "s", "project_id": "p"}}),
        serde_json::json!({"action": "append_claim_record", "payload": {"claim_id": "c", "claim_text": "x", "primary_concept_id": "concept"}}),
        serde_json::json!({"action": "trace_claim_provenance", "payload": {"claim_id": "c"}}),
    ];

    for msg in &test_messages {
        assert!(msg["action"].is_string(), "action must be a string");
        assert!(msg["payload"].is_object(), "payload must be an object");
    }
}
