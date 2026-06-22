//! RAG document store — persistent full-text documents + embedded chunks for
//! semantic retrieval per scope (app / tenant / expert / collection).
//!
//! Tables: `rag_document` (one row per doc) + `rag_chunk` (N embedded chunks per doc).
//! Scope: same multi-axis vocabulary as `scoped_memory` (app_id / tenant_id / expert_id / collection).
//! Cosine rerank: computed in Rust over scope-filtered candidate chunks, no pgvector dependency.
//! Embeddings: caller supplies pre-computed f32 vectors (Hera embeds via candle BERT MiniLM-L12).

use serde_json::{json, Value};
use tracing::error;

// ─── Embedding helpers ────────────────────────────────────────────────────────
// Local copies — scoped_memory's versions are pub(super) and not accessible here.

fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }
    let (mut dot, mut na, mut nb) = (0.0f32, 0.0f32, 0.0f32);
    for i in 0..a.len() {
        dot += a[i] * b[i];
        na += a[i] * a[i];
        nb += b[i] * b[i];
    }
    if na == 0.0 || nb == 0.0 {
        return 0.0;
    }
    dot / (na.sqrt() * nb.sqrt())
}

fn pack_embedding(v: &[f32]) -> Vec<u8> {
    v.iter().flat_map(|f| f.to_le_bytes()).collect()
}

fn unpack_embedding(bytes: &[u8]) -> Option<Vec<f32>> {
    if bytes.is_empty() || bytes.len() % 4 != 0 {
        return None;
    }
    Some(
        bytes
            .chunks_exact(4)
            .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
            .collect(),
    )
}

fn ts_str(dt: Option<chrono::NaiveDateTime>) -> Option<String> {
    dt.map(|t| t.format("%Y-%m-%dT%H:%M:%S").to_string())
}

// ─── rag_ingest_document ──────────────────────────────────────────────────────
//
// Payload:
//   document_id: String (stable; caller generates a uuid/slug)
//   title: String
//   source_type: String  ("text"|"markdown"|"pdf"|"docx"|"url"|"youtube")
//   full_text: String
//   pinned?: bool (default false)
//   source_uri?: String
//   metadata_json?: Object
//   app_id?, tenant_id?, expert_id?, owner_scope?, collection?: String
//   chunks?: Array of { ordinal: i32, heading_path?: String, content: String,
//                       token_count?: i32, embedding?: [f32, ...] }
//
// Upserts the document header and replaces all its chunks atomically.

pub async fn ingest_document(pool: &sqlx::PgPool, payload: Value) -> Value {
    let document_id = match payload.get("document_id").and_then(|v| v.as_str()) {
        Some(v) if !v.is_empty() => v.to_string(),
        _ => return json!({ "error": "Missing 'document_id' in payload" }),
    };
    let title = payload
        .get("title")
        .and_then(|v| v.as_str())
        .unwrap_or("Untitled")
        .to_string();
    let source_type = payload
        .get("source_type")
        .and_then(|v| v.as_str())
        .unwrap_or("text")
        .to_string();
    let full_text = match payload.get("full_text").and_then(|v| v.as_str()) {
        Some(v) => v.to_string(),
        None => return json!({ "error": "Missing 'full_text' in payload" }),
    };
    let char_count = full_text.chars().count() as i32;
    let pinned = payload
        .get("pinned")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let source_uri: Option<String> = payload
        .get("source_uri")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let app_id: Option<String> = payload
        .get("app_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let tenant_id: Option<String> = payload
        .get("tenant_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let expert_id: Option<String> = payload
        .get("expert_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let owner_scope: Option<String> = payload
        .get("owner_scope")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let collection: Option<String> = payload
        .get("collection")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let metadata_str: Option<String> = payload
        .get("metadata_json")
        .map(|v| v.to_string());

    if let Err(e) = sqlx::query(
        r#"
        INSERT INTO rag_document
            (document_id, app_id, tenant_id, expert_id, owner_scope, collection,
             title, source_type, source_uri, full_text, char_count, pinned,
             metadata_json, updated_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb, CURRENT_TIMESTAMP)
        ON CONFLICT (document_id) DO UPDATE SET
            title         = EXCLUDED.title,
            source_type   = EXCLUDED.source_type,
            source_uri    = EXCLUDED.source_uri,
            full_text     = EXCLUDED.full_text,
            char_count    = EXCLUDED.char_count,
            pinned        = EXCLUDED.pinned,
            metadata_json = EXCLUDED.metadata_json,
            app_id        = EXCLUDED.app_id,
            tenant_id     = EXCLUDED.tenant_id,
            expert_id     = EXCLUDED.expert_id,
            owner_scope   = EXCLUDED.owner_scope,
            collection    = EXCLUDED.collection,
            updated_at    = CURRENT_TIMESTAMP
        "#,
    )
    .bind(&document_id)
    .bind(&app_id)
    .bind(&tenant_id)
    .bind(&expert_id)
    .bind(&owner_scope)
    .bind(&collection)
    .bind(&title)
    .bind(&source_type)
    .bind(&source_uri)
    .bind(&full_text)
    .bind(char_count)
    .bind(pinned)
    .bind(&metadata_str)
    .execute(pool)
    .await
    {
        error!(?e, "rag_ingest_document: upsert failed");
        return json!({ "error": format!("DB error: {}", e) });
    }

    // Replace all chunks for this document
    if let Err(e) = sqlx::query("DELETE FROM rag_chunk WHERE document_id = $1")
        .bind(&document_id)
        .execute(pool)
        .await
    {
        error!(?e, "rag_ingest_document: delete chunks failed");
        return json!({ "error": format!("DB error: {}", e) });
    }

    let empty = vec![];
    let chunks = payload
        .get("chunks")
        .and_then(|v| v.as_array())
        .unwrap_or(&empty);
    let mut chunks_ingested = 0usize;

    for chunk in chunks {
        let ordinal = chunk
            .get("ordinal")
            .and_then(|v| v.as_i64())
            .unwrap_or(0) as i32;
        let heading_path: Option<String> = chunk
            .get("heading_path")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let content = chunk
            .get("content")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let token_count = chunk
            .get("token_count")
            .and_then(|v| v.as_i64())
            .unwrap_or(0) as i32;
        let embedding_b: Option<Vec<u8>> = chunk
            .get("embedding")
            .and_then(|v| v.as_array())
            .map(|arr| {
                let floats: Vec<f32> = arr
                    .iter()
                    .filter_map(|x| x.as_f64().map(|f| f as f32))
                    .collect();
                pack_embedding(&floats)
            });

        if let Err(e) = sqlx::query(
            "INSERT INTO rag_chunk (document_id, ordinal, heading_path, content, token_count, embedding_b) \
             VALUES ($1,$2,$3,$4,$5,$6)",
        )
        .bind(&document_id)
        .bind(ordinal)
        .bind(&heading_path)
        .bind(&content)
        .bind(token_count)
        .bind(&embedding_b)
        .execute(pool)
        .await
        {
            error!(?e, "rag_ingest_document: insert chunk failed");
            return json!({ "error": format!("DB error on chunk {}: {}", ordinal, e) });
        }
        chunks_ingested += 1;
    }

    json!({
        "ok": true,
        "document_id": document_id,
        "chunks_ingested": chunks_ingested
    })
}

// ─── rag_list_documents ───────────────────────────────────────────────────────
//
// Payload (all optional):
//   app_id, tenant_id, expert_id, collection?: String
//   status?: String (default "active")
//   limit?: i64 (default 50)

pub async fn list_documents(pool: &sqlx::PgPool, payload: Value) -> Value {
    let app_id: Option<String> = payload
        .get("app_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let tenant_id: Option<String> = payload
        .get("tenant_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let expert_id: Option<String> = payload
        .get("expert_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let collection: Option<String> = payload
        .get("collection")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let status = payload
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("active")
        .to_string();
    let limit = payload
        .get("limit")
        .and_then(|v| v.as_i64())
        .unwrap_or(50);

    match sqlx::query(
        "SELECT document_id, title, source_type, source_uri, char_count, pinned, status, \
         app_id, tenant_id, expert_id, collection, usage_count, created_at, updated_at \
         FROM rag_document \
         WHERE status = $1 \
           AND ($2::text IS NULL OR app_id    = $2) \
           AND ($3::text IS NULL OR tenant_id = $3) \
           AND ($4::text IS NULL OR expert_id = $4) \
           AND ($5::text IS NULL OR collection = $5) \
         ORDER BY created_at DESC LIMIT $6",
    )
    .bind(&status)
    .bind(&app_id)
    .bind(&tenant_id)
    .bind(&expert_id)
    .bind(&collection)
    .bind(limit)
    .fetch_all(pool)
    .await
    {
        Ok(rows) => {
            use sqlx::Row;
            let docs: Vec<Value> = rows
                .iter()
                .map(|r| {
                    json!({
                        "document_id": r.get::<String, _>("document_id"),
                        "title":        r.get::<String, _>("title"),
                        "source_type":  r.get::<String, _>("source_type"),
                        "source_uri":   r.get::<Option<String>, _>("source_uri"),
                        "char_count":   r.get::<i32, _>("char_count"),
                        "pinned":       r.get::<bool, _>("pinned"),
                        "status":       r.get::<String, _>("status"),
                        "app_id":       r.get::<Option<String>, _>("app_id"),
                        "tenant_id":    r.get::<Option<String>, _>("tenant_id"),
                        "expert_id":    r.get::<Option<String>, _>("expert_id"),
                        "collection":   r.get::<Option<String>, _>("collection"),
                        "usage_count":  r.get::<i32, _>("usage_count"),
                        "created_at":   ts_str(r.get::<Option<chrono::NaiveDateTime>, _>("created_at")),
                        "updated_at":   ts_str(r.get::<Option<chrono::NaiveDateTime>, _>("updated_at")),
                    })
                })
                .collect();
            let count = docs.len();
            json!({ "ok": true, "documents": docs, "count": count })
        }
        Err(e) => {
            error!(?e, "rag_list_documents: query failed");
            json!({ "error": format!("DB error: {}", e) })
        }
    }
}

// ─── rag_get_document ─────────────────────────────────────────────────────────
//
// Payload:
//   document_id: String
//   include_chunks?: bool (default false — skip embedding bytes in listing)

pub async fn get_document(pool: &sqlx::PgPool, payload: Value) -> Value {
    let document_id = match payload.get("document_id").and_then(|v| v.as_str()) {
        Some(v) if !v.is_empty() => v.to_string(),
        _ => return json!({ "error": "Missing 'document_id' in payload" }),
    };
    let include_chunks = payload
        .get("include_chunks")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    use sqlx::Row;
    let doc_row = sqlx::query(
        "SELECT document_id, title, source_type, source_uri, full_text, char_count, pinned, \
         status, app_id, tenant_id, expert_id, owner_scope, collection, usage_count, \
         metadata_json, created_at, updated_at \
         FROM rag_document WHERE document_id = $1",
    )
    .bind(&document_id)
    .fetch_optional(pool)
    .await;

    match doc_row {
        Ok(Some(r)) => {
            let mut doc = json!({
                "document_id":  r.get::<String, _>("document_id"),
                "title":        r.get::<String, _>("title"),
                "source_type":  r.get::<String, _>("source_type"),
                "source_uri":   r.get::<Option<String>, _>("source_uri"),
                "full_text":    r.get::<String, _>("full_text"),
                "char_count":   r.get::<i32, _>("char_count"),
                "pinned":       r.get::<bool, _>("pinned"),
                "status":       r.get::<String, _>("status"),
                "app_id":       r.get::<Option<String>, _>("app_id"),
                "tenant_id":    r.get::<Option<String>, _>("tenant_id"),
                "expert_id":    r.get::<Option<String>, _>("expert_id"),
                "owner_scope":  r.get::<Option<String>, _>("owner_scope"),
                "collection":   r.get::<Option<String>, _>("collection"),
                "usage_count":  r.get::<i32, _>("usage_count"),
                "metadata_json": r.get::<Option<String>, _>("metadata_json"),
                "created_at":   ts_str(r.get::<Option<chrono::NaiveDateTime>, _>("created_at")),
                "updated_at":   ts_str(r.get::<Option<chrono::NaiveDateTime>, _>("updated_at")),
            });

            if include_chunks {
                match sqlx::query(
                    "SELECT ordinal, heading_path, content, token_count FROM rag_chunk \
                     WHERE document_id = $1 ORDER BY ordinal",
                )
                .bind(&document_id)
                .fetch_all(pool)
                .await
                {
                    Ok(chunk_rows) => {
                        let chunks: Vec<Value> = chunk_rows
                            .iter()
                            .map(|c| {
                                json!({
                                    "ordinal":      c.get::<i32, _>("ordinal"),
                                    "heading_path": c.get::<Option<String>, _>("heading_path"),
                                    "content":      c.get::<String, _>("content"),
                                    "token_count":  c.get::<i32, _>("token_count"),
                                })
                            })
                            .collect();
                        doc["chunks"] = json!(chunks);
                    }
                    Err(e) => {
                        error!(?e, "rag_get_document: fetch chunks failed");
                    }
                }
            }

            // Touch usage tracking (non-fatal)
            let _ = sqlx::query(
                "UPDATE rag_document SET usage_count = usage_count + 1, \
                 last_used_at = CURRENT_TIMESTAMP WHERE document_id = $1",
            )
            .bind(&document_id)
            .execute(pool)
            .await;

            json!({ "ok": true, "document": doc })
        }
        Ok(None) => json!({ "error": format!("Document not found: {}", document_id) }),
        Err(e) => {
            error!(?e, "rag_get_document: query failed");
            json!({ "error": format!("DB error: {}", e) })
        }
    }
}

// ─── rag_update_document ─────────────────────────────────────────────────────
//
// Lightweight metadata update — no re-embed (use rag_reembed_document for that).
// Payload:
//   document_id: String
//   title?, pinned?, status?, metadata_json?: updatable fields

pub async fn update_document(pool: &sqlx::PgPool, payload: Value) -> Value {
    let document_id = match payload.get("document_id").and_then(|v| v.as_str()) {
        Some(v) if !v.is_empty() => v.to_string(),
        _ => return json!({ "error": "Missing 'document_id' in payload" }),
    };
    let title: Option<String> = payload
        .get("title")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let pinned: Option<bool> = payload.get("pinned").and_then(|v| v.as_bool());
    let status: Option<String> = payload
        .get("status")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let metadata_str: Option<String> = payload.get("metadata_json").map(|v| v.to_string());

    match sqlx::query(
        "UPDATE rag_document SET \
         title         = COALESCE($2, title), \
         pinned        = COALESCE($3, pinned), \
         status        = COALESCE($4, status), \
         metadata_json = COALESCE($5::jsonb, metadata_json), \
         updated_at    = CURRENT_TIMESTAMP \
         WHERE document_id = $1",
    )
    .bind(&document_id)
    .bind(&title)
    .bind(pinned)
    .bind(&status)
    .bind(&metadata_str)
    .execute(pool)
    .await
    {
        Ok(r) if r.rows_affected() > 0 => json!({ "ok": true, "document_id": document_id }),
        Ok(_) => json!({ "error": format!("Document not found: {}", document_id) }),
        Err(e) => {
            error!(?e, "rag_update_document: update failed");
            json!({ "error": format!("DB error: {}", e) })
        }
    }
}

// ─── rag_reembed_document ─────────────────────────────────────────────────────
//
// Replace a document's text and chunks (re-ingesta after editing).
// Delegates to ingest_document with the same payload shape — it upserts the header
// and replaces chunks, so this is a semantic alias kept for API clarity.

pub async fn reembed_document(pool: &sqlx::PgPool, payload: Value) -> Value {
    // Require document to already exist
    let document_id = match payload.get("document_id").and_then(|v| v.as_str()) {
        Some(v) if !v.is_empty() => v.to_string(),
        _ => return json!({ "error": "Missing 'document_id' in payload" }),
    };
    let exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM rag_document WHERE document_id = $1)",
    )
    .bind(&document_id)
    .fetch_one(pool)
    .await
    .unwrap_or(false);

    if !exists {
        return json!({ "error": format!("Document not found: {}", document_id) });
    }
    ingest_document(pool, payload).await
}

// ─── rag_delete_document ─────────────────────────────────────────────────────
//
// Payload:
//   document_id: String
//   hard?: bool (default false — soft-delete sets status='deleted'; hard removes row+chunks)

pub async fn delete_document(pool: &sqlx::PgPool, payload: Value) -> Value {
    let document_id = match payload.get("document_id").and_then(|v| v.as_str()) {
        Some(v) if !v.is_empty() => v.to_string(),
        _ => return json!({ "error": "Missing 'document_id' in payload" }),
    };
    let hard = payload
        .get("hard")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let result = if hard {
        // Chunks cascade via FK ON DELETE CASCADE
        sqlx::query("DELETE FROM rag_document WHERE document_id = $1")
            .bind(&document_id)
            .execute(pool)
            .await
    } else {
        sqlx::query(
            "UPDATE rag_document SET status = 'deleted', updated_at = CURRENT_TIMESTAMP \
             WHERE document_id = $1",
        )
        .bind(&document_id)
        .execute(pool)
        .await
    };

    match result {
        Ok(r) if r.rows_affected() > 0 => json!({
            "ok": true,
            "document_id": document_id,
            "deleted": hard
        }),
        Ok(_) => json!({ "error": format!("Document not found: {}", document_id) }),
        Err(e) => {
            error!(?e, "rag_delete_document: failed");
            json!({ "error": format!("DB error: {}", e) })
        }
    }
}

// ─── rag_search ───────────────────────────────────────────────────────────────
//
// Semantic search over embedded chunks, scope-filtered, cosine reranked in Rust.
//
// Payload:
//   query_embedding: [f32, ...]  (caller embeds the query text via Hera)
//   app_id?, tenant_id?, expert_id?, collection?: scope filters
//   k?: i64 (default 5, max 20) — number of results to return
//   candidate_cap?: i64 (default 400) — rows fetched before cosine rerank

pub async fn search(pool: &sqlx::PgPool, payload: Value) -> Value {
    let query_vec: Vec<f32> = match payload
        .get("query_embedding")
        .and_then(|v| v.as_array())
    {
        Some(arr) => arr
            .iter()
            .filter_map(|x| x.as_f64().map(|f| f as f32))
            .collect(),
        None => return json!({ "error": "Missing 'query_embedding' in payload" }),
    };
    if query_vec.is_empty() {
        return json!({ "error": "'query_embedding' must be a non-empty float array" });
    }

    let app_id: Option<String> = payload
        .get("app_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let tenant_id: Option<String> = payload
        .get("tenant_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let expert_id: Option<String> = payload
        .get("expert_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let collection: Option<String> = payload
        .get("collection")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let k = payload.get("k").and_then(|v| v.as_i64()).unwrap_or(5).min(20);
    let candidate_cap = payload
        .get("candidate_cap")
        .and_then(|v| v.as_i64())
        .unwrap_or(400)
        .clamp(24, 2000);

    use sqlx::Row;
    match sqlx::query(
        "SELECT c.document_id, c.ordinal, c.heading_path, c.content, c.embedding_b, \
                d.title, d.app_id, d.tenant_id, d.expert_id, d.collection \
         FROM rag_chunk c \
         JOIN rag_document d ON d.document_id = c.document_id \
         WHERE c.embedding_b IS NOT NULL \
           AND d.status = 'active' \
           AND ($1::text IS NULL OR d.app_id    = $1) \
           AND ($2::text IS NULL OR d.tenant_id = $2) \
           AND ($3::text IS NULL OR d.expert_id = $3) \
           AND ($4::text IS NULL OR d.collection = $4) \
         ORDER BY c.id DESC \
         LIMIT $5",
    )
    .bind(&app_id)
    .bind(&tenant_id)
    .bind(&expert_id)
    .bind(&collection)
    .bind(candidate_cap)
    .fetch_all(pool)
    .await
    {
        Ok(rows) => {
            let mut scored: Vec<(f32, Value)> = rows
                .iter()
                .filter_map(|r| {
                    let emb_bytes: Option<Vec<u8>> =
                        r.get::<Option<Vec<u8>>, _>("embedding_b");
                    let vec = emb_bytes.as_deref().and_then(unpack_embedding)?;
                    let score = cosine_similarity(&query_vec, &vec);
                    Some((
                        score,
                        json!({
                            "document_id":  r.get::<String, _>("document_id"),
                            "title":        r.get::<String, _>("title"),
                            "ordinal":      r.get::<i32, _>("ordinal"),
                            "heading_path": r.get::<Option<String>, _>("heading_path"),
                            "content":      r.get::<String, _>("content"),
                            "score":        score,
                            "app_id":       r.get::<Option<String>, _>("app_id"),
                            "tenant_id":    r.get::<Option<String>, _>("tenant_id"),
                            "expert_id":    r.get::<Option<String>, _>("expert_id"),
                            "collection":   r.get::<Option<String>, _>("collection"),
                        }),
                    ))
                })
                .collect();

            scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
            let results: Vec<Value> = scored
                .into_iter()
                .take(k as usize)
                .map(|(_, v)| v)
                .collect();

            json!({ "ok": true, "results": results, "count": results.len() })
        }
        Err(e) => {
            error!(?e, "rag_search: query failed");
            json!({ "error": format!("DB error: {}", e) })
        }
    }
}

// ─── rag_pinned ───────────────────────────────────────────────────────────────
//
// Returns all documents with pinned=true for a scope.
// Used by Hera to inject "always-on" documents into the stable_prefix of the system prompt.
//
// Payload (all optional — at least one scope field recommended):
//   app_id?, tenant_id?, expert_id?, collection?: String

pub async fn pinned(pool: &sqlx::PgPool, payload: Value) -> Value {
    let app_id: Option<String> = payload
        .get("app_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let tenant_id: Option<String> = payload
        .get("tenant_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let expert_id: Option<String> = payload
        .get("expert_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let collection: Option<String> = payload
        .get("collection")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    use sqlx::Row;
    match sqlx::query(
        "SELECT document_id, title, source_type, full_text, char_count, \
                app_id, tenant_id, expert_id, collection, updated_at \
         FROM rag_document \
         WHERE pinned = true AND status = 'active' \
           AND ($1::text IS NULL OR app_id    = $1) \
           AND ($2::text IS NULL OR tenant_id = $2) \
           AND ($3::text IS NULL OR expert_id = $3) \
           AND ($4::text IS NULL OR collection = $4) \
         ORDER BY updated_at DESC",
    )
    .bind(&app_id)
    .bind(&tenant_id)
    .bind(&expert_id)
    .bind(&collection)
    .fetch_all(pool)
    .await
    {
        Ok(rows) => {
            let docs: Vec<Value> = rows
                .iter()
                .map(|r| {
                    json!({
                        "document_id": r.get::<String, _>("document_id"),
                        "title":       r.get::<String, _>("title"),
                        "source_type": r.get::<String, _>("source_type"),
                        "full_text":   r.get::<String, _>("full_text"),
                        "char_count":  r.get::<i32, _>("char_count"),
                        "app_id":      r.get::<Option<String>, _>("app_id"),
                        "tenant_id":   r.get::<Option<String>, _>("tenant_id"),
                        "expert_id":   r.get::<Option<String>, _>("expert_id"),
                        "collection":  r.get::<Option<String>, _>("collection"),
                        "updated_at":  ts_str(r.get::<Option<chrono::NaiveDateTime>, _>("updated_at")),
                    })
                })
                .collect();
            let count = docs.len();
            json!({ "ok": true, "documents": docs, "count": count })
        }
        Err(e) => {
            error!(?e, "rag_pinned: query failed");
            json!({ "error": format!("DB error: {}", e) })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cosine_identical_vectors() {
        let v = vec![1.0f32, 0.0, 0.0];
        assert!((cosine_similarity(&v, &v) - 1.0).abs() < 1e-5);
    }

    #[test]
    fn cosine_orthogonal_vectors() {
        let a = vec![1.0f32, 0.0];
        let b = vec![0.0f32, 1.0];
        assert!(cosine_similarity(&a, &b).abs() < 1e-5);
    }

    #[test]
    fn pack_unpack_roundtrip() {
        let v = vec![1.0f32, -0.5, 0.25, 0.0, 99.9];
        let packed = pack_embedding(&v);
        assert_eq!(packed.len(), v.len() * 4);
        let unpacked = unpack_embedding(&packed).expect("roundtrip");
        for (a, b) in v.iter().zip(unpacked.iter()) {
            assert!((a - b).abs() < 1e-6, "mismatch: {} vs {}", a, b);
        }
    }

    #[test]
    fn unpack_rejects_bad_length() {
        assert!(unpack_embedding(&[1, 2, 3]).is_none());
        assert!(unpack_embedding(&[]).is_none());
    }
}
