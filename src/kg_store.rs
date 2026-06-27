//! Sovereign knowledge graph store (relational / graph RAG).
//!
//! ONE graph per scope (app / tenant / expert / collection), fed by two sources:
//! RAG documents and DURABLE memory (facts/decisions/summaries) — never raw chat
//! turns. Entities are **resolved** (deduped) by normalized name + type within a
//! scope, so the same person / company / law is a single node no matter how many
//! documents or memories mention it.
//!
//! Design mirrors `rag_store`: caller supplies pre-computed f32 embeddings (Hera
//! embeds via candle BERT MiniLM-L12); we pack them to BYTEA. Cosine / PCA / PPR
//! happen in Rust on the consumer side.
//!
//! Live actions (registered in `main.rs`):
//!   - `kg_upsert_triples` — merge entities + relations into the graph
//!   - `kg_graph`          — full scoped subgraph (entities + edges) for the viewer
//!   - `kg_neighbors`      — k-hop expansion from seed entities (retrieval)

use serde_json::{json, Value};
use sqlx::Row;
use tracing::error;

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

/// Deterministic 64-bit FNV-1a — stable across processes (unlike DefaultHasher),
/// so the same (scope, name, type) always resolves to the same entity_id.
fn fnv1a64(s: &str) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in s.bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// Lowercase, trim, collapse internal whitespace, strip surrounding punctuation —
/// the resolution key so "Paulo Vila", "  paulo vila.", "PAULO  VILA" all merge.
fn normalize_name(s: &str) -> String {
    let lowered = s.to_lowercase();
    let collapsed = lowered.split_whitespace().collect::<Vec<_>>().join(" ");
    collapsed
        .trim_matches(|c: char| !c.is_alphanumeric())
        .to_string()
}

fn opt_str(payload: &Value, key: &str) -> Option<String> {
    payload
        .get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

/// Build the scope key used to namespace deterministic ids.
fn scope_key(app: &Option<String>, tenant: &Option<String>, expert: &Option<String>, coll: &Option<String>) -> String {
    format!(
        "{}|{}|{}|{}",
        app.as_deref().unwrap_or(""),
        tenant.as_deref().unwrap_or(""),
        expert.as_deref().unwrap_or(""),
        coll.as_deref().unwrap_or("")
    )
}

fn entity_id_for(scope: &str, norm: &str, etype: &str) -> String {
    format!("e_{:016x}", fnv1a64(&format!("{scope}|{norm}|{etype}")))
}

fn embedding_from(node: &Value) -> Option<Vec<u8>> {
    node.get("embedding")
        .and_then(|v| v.as_array())
        .map(|a| {
            let floats: Vec<f32> = a
                .iter()
                .filter_map(|x| x.as_f64().map(|f| f as f32))
                .collect();
            pack_embedding(&floats)
        })
        .filter(|b| !b.is_empty())
}

// ─── kg_upsert_triples ────────────────────────────────────────────────────────
//
// Payload:
//   app_id?, tenant_id?, expert_id?, collection?   (scope)
//   source_kind?: "rag" | "memory"                 (provenance, default "rag")
//   doc_id?: String                                (provenance ref)
//   entities?: [{ name, type?, embedding?: [f32], summary? }]
//   triples?:  [{ src: {name,type?,embedding?}, rel?, dst: {name,type?,embedding?},
//                 weight?, evidence? }]
//
// Resolves every entity by (scope, normalized name, type) and merges. Returns the
// number of entities + relations touched.
pub async fn upsert_triples(pool: &sqlx::PgPool, payload: Value) -> Value {
    let app = opt_str(&payload, "app_id");
    let tenant = opt_str(&payload, "tenant_id");
    let expert = opt_str(&payload, "expert_id");
    let coll = opt_str(&payload, "collection");
    let source_kind = opt_str(&payload, "source_kind").unwrap_or_else(|| "rag".to_string());
    let doc_id = opt_str(&payload, "doc_id");
    let skey = scope_key(&app, &tenant, &expert, &coll);

    let mut entity_count = 0i64;
    let mut relation_count = 0i64;

    // Inner closure-like helper expressed inline (async, so no real closure): upsert
    // one entity node, returning its resolved entity_id.
    async fn upsert_entity(
        pool: &sqlx::PgPool,
        skey: &str,
        app: &Option<String>,
        tenant: &Option<String>,
        expert: &Option<String>,
        coll: &Option<String>,
        source_kind: &str,
        doc_id: &Option<String>,
        node: &Value,
    ) -> Option<String> {
        let name = node.get("name").and_then(|v| v.as_str())?.trim();
        if name.is_empty() {
            return None;
        }
        let etype = node
            .get("type")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .unwrap_or("concept")
            .to_lowercase();
        let norm = normalize_name(name);
        if norm.is_empty() {
            return None;
        }
        let eid = entity_id_for(skey, &norm, &etype);
        let emb = embedding_from(node);
        let summary = node.get("summary").and_then(|v| v.as_str());
        let source_kinds = json!([source_kind]);
        let doc_ids = doc_id.as_ref().map(|d| json!([d])).unwrap_or_else(|| json!([]));

        let res = sqlx::query(
            "INSERT INTO kg_entity \
               (entity_id, app_id, tenant_id, expert_id, collection, name, norm_name, \
                entity_type, summary, embedding_b, mention_count, source_kinds, doc_ids) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,1,$11,$12) \
             ON CONFLICT (entity_id) DO UPDATE SET \
               mention_count = kg_entity.mention_count + 1, \
               embedding_b   = COALESCE(EXCLUDED.embedding_b, kg_entity.embedding_b), \
               summary       = COALESCE(EXCLUDED.summary, kg_entity.summary), \
               updated_at    = CURRENT_TIMESTAMP",
        )
        .bind(&eid)
        .bind(app)
        .bind(tenant)
        .bind(expert)
        .bind(coll)
        .bind(name)
        .bind(&norm)
        .bind(&etype)
        .bind(summary)
        .bind(emb)
        .bind(&source_kinds)
        .bind(&doc_ids)
        .execute(pool)
        .await;
        match res {
            Ok(_) => Some(eid),
            Err(e) => {
                error!(?e, "kg_upsert_triples: entity upsert failed");
                None
            }
        }
    }

    if let Some(entities) = payload.get("entities").and_then(|v| v.as_array()) {
        for node in entities {
            if upsert_entity(
                pool, &skey, &app, &tenant, &expert, &coll, &source_kind, &doc_id, node,
            )
            .await
            .is_some()
            {
                entity_count += 1;
            }
        }
    }

    if let Some(triples) = payload.get("triples").and_then(|v| v.as_array()) {
        for tr in triples {
            let (Some(src), Some(dst)) = (tr.get("src"), tr.get("dst")) else {
                continue;
            };
            let sid = upsert_entity(
                pool, &skey, &app, &tenant, &expert, &coll, &source_kind, &doc_id, src,
            )
            .await;
            let did = upsert_entity(
                pool, &skey, &app, &tenant, &expert, &coll, &source_kind, &doc_id, dst,
            )
            .await;
            let (Some(sid), Some(did)) = (sid, did) else {
                continue;
            };
            entity_count += 2;
            if sid == did {
                continue; // no self-loops
            }
            let rel = tr
                .get("rel")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .unwrap_or("relates")
                .to_lowercase();
            let weight = tr.get("weight").and_then(|v| v.as_f64()).unwrap_or(1.0) as f32;
            let evidence = tr.get("evidence").and_then(|v| v.as_str());
            let rid = format!("r_{:016x}", fnv1a64(&format!("{skey}|{sid}|{did}|{rel}")));
            let source_kinds = json!([source_kind]);

            let res = sqlx::query(
                "INSERT INTO kg_relation \
                   (relation_id, app_id, tenant_id, expert_id, collection, src_id, dst_id, \
                    rel_type, weight, evidence, source_kinds) \
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) \
                 ON CONFLICT (relation_id) DO UPDATE SET \
                   weight     = kg_relation.weight + EXCLUDED.weight, \
                   evidence   = COALESCE(EXCLUDED.evidence, kg_relation.evidence), \
                   updated_at = CURRENT_TIMESTAMP",
            )
            .bind(&rid)
            .bind(&app)
            .bind(&tenant)
            .bind(&expert)
            .bind(&coll)
            .bind(&sid)
            .bind(&did)
            .bind(&rel)
            .bind(weight)
            .bind(evidence)
            .bind(&source_kinds)
            .execute(pool)
            .await;
            match res {
                Ok(_) => relation_count += 1,
                Err(e) => error!(?e, "kg_upsert_triples: relation upsert failed"),
            }
        }
    }

    json!({ "ok": true, "entities": entity_count, "relations": relation_count })
}

fn entity_row_json(r: &sqlx::postgres::PgRow) -> Value {
    let emb: Option<Vec<u8>> = r.get::<Option<Vec<u8>>, _>("embedding_b");
    json!({
        "entity_id":    r.get::<String, _>("entity_id"),
        "name":         r.get::<String, _>("name"),
        "entity_type":  r.get::<String, _>("entity_type"),
        "summary":      r.get::<Option<String>, _>("summary"),
        "mention_count": r.get::<i32, _>("mention_count"),
        "embedding":    emb.as_deref().and_then(unpack_embedding).unwrap_or_default(),
    })
}

// ─── kg_graph ─────────────────────────────────────────────────────────────────
//
// Full scoped subgraph for the viewer. Payload: scope + max_entities? (default 200)
// + max_relations? (default 400). Returns entities (with embeddings) ranked by
// mention_count, plus the relations whose BOTH endpoints are in the returned set
// (no dangling edges).
pub async fn graph(pool: &sqlx::PgPool, payload: Value) -> Value {
    let app = opt_str(&payload, "app_id");
    let tenant = opt_str(&payload, "tenant_id");
    let expert = opt_str(&payload, "expert_id");
    let coll = opt_str(&payload, "collection");
    let max_entities = payload
        .get("max_entities")
        .and_then(|v| v.as_i64())
        .unwrap_or(200)
        .clamp(1, 2000);
    let max_relations = payload
        .get("max_relations")
        .and_then(|v| v.as_i64())
        .unwrap_or(400)
        .clamp(1, 5000);

    let ents = match sqlx::query(
        "SELECT entity_id, name, entity_type, summary, mention_count, embedding_b \
         FROM kg_entity \
         WHERE ($1::text IS NULL OR app_id    = $1) \
           AND ($2::text IS NULL OR tenant_id = $2) \
           AND ($3::text IS NULL OR expert_id = $3) \
           AND ($4::text IS NULL OR collection = $4) \
         ORDER BY mention_count DESC, name ASC LIMIT $5",
    )
    .bind(&app)
    .bind(&tenant)
    .bind(&expert)
    .bind(&coll)
    .bind(max_entities)
    .fetch_all(pool)
    .await
    {
        Ok(rows) => rows,
        Err(e) => {
            error!(?e, "kg_graph: entity query failed");
            return json!({ "error": format!("DB error: {}", e) });
        }
    };

    let entities: Vec<Value> = ents.iter().map(entity_row_json).collect();
    let id_set: std::collections::HashSet<String> = ents
        .iter()
        .map(|r| r.get::<String, _>("entity_id"))
        .collect();

    let rels = sqlx::query(
        "SELECT src_id, dst_id, rel_type, weight, evidence \
         FROM kg_relation \
         WHERE ($1::text IS NULL OR app_id    = $1) \
           AND ($2::text IS NULL OR tenant_id = $2) \
           AND ($3::text IS NULL OR expert_id = $3) \
           AND ($4::text IS NULL OR collection = $4) \
         ORDER BY weight DESC LIMIT $5",
    )
    .bind(&app)
    .bind(&tenant)
    .bind(&expert)
    .bind(&coll)
    .bind(max_relations)
    .fetch_all(pool)
    .await
    .unwrap_or_default();

    let relations: Vec<Value> = rels
        .iter()
        .filter_map(|r| {
            let src = r.get::<String, _>("src_id");
            let dst = r.get::<String, _>("dst_id");
            if !id_set.contains(&src) || !id_set.contains(&dst) {
                return None;
            }
            Some(json!({
                "src_id":   src,
                "dst_id":   dst,
                "rel_type": r.get::<String, _>("rel_type"),
                "weight":   r.get::<f32, _>("weight"),
                "evidence": r.get::<Option<String>, _>("evidence"),
            }))
        })
        .collect();

    let ec = entities.len();
    let rc = relations.len();
    json!({ "ok": true, "entities": entities, "relations": relations, "entity_count": ec, "relation_count": rc })
}

// ─── kg_neighbors ─────────────────────────────────────────────────────────────
//
// k-hop expansion from seed entity_ids (graph retrieval). Payload: scope +
// seeds: [entity_id] + hops? (default 1, clamp 1..3) + max_entities? (default 60).
// Returns the reachable subgraph (entities + connecting relations).
pub async fn neighbors(pool: &sqlx::PgPool, payload: Value) -> Value {
    let app = opt_str(&payload, "app_id");
    let tenant = opt_str(&payload, "tenant_id");
    let expert = opt_str(&payload, "expert_id");
    let coll = opt_str(&payload, "collection");
    let hops = payload.get("hops").and_then(|v| v.as_i64()).unwrap_or(1).clamp(1, 3);
    let max_entities = payload
        .get("max_entities")
        .and_then(|v| v.as_i64())
        .unwrap_or(60)
        .clamp(1, 1000);
    let seeds: Vec<String> = payload
        .get("seeds")
        .and_then(|v| v.as_array())
        .map(|a| a.iter().filter_map(|x| x.as_str().map(String::from)).collect())
        .unwrap_or_default();
    if seeds.is_empty() {
        return json!({ "ok": true, "entities": [], "relations": [], "entity_count": 0, "relation_count": 0 });
    }

    // BFS over kg_relation (undirected for reachability), bounded by hops + scope.
    let mut frontier: std::collections::HashSet<String> = seeds.iter().cloned().collect();
    let mut visited: std::collections::HashSet<String> = frontier.clone();
    for _ in 0..hops {
        if frontier.is_empty() || visited.len() as i64 >= max_entities {
            break;
        }
        let cur: Vec<String> = frontier.iter().cloned().collect();
        let rows = sqlx::query(
            "SELECT src_id, dst_id FROM kg_relation \
             WHERE (src_id = ANY($1) OR dst_id = ANY($1)) \
               AND ($2::text IS NULL OR app_id    = $2) \
               AND ($3::text IS NULL OR tenant_id = $3) \
               AND ($4::text IS NULL OR expert_id = $4) \
               AND ($5::text IS NULL OR collection = $5)",
        )
        .bind(&cur)
        .bind(&app)
        .bind(&tenant)
        .bind(&expert)
        .bind(&coll)
        .fetch_all(pool)
        .await
        .unwrap_or_default();
        let mut next = std::collections::HashSet::new();
        for r in &rows {
            for id in [r.get::<String, _>("src_id"), r.get::<String, _>("dst_id")] {
                if visited.insert(id.clone()) {
                    next.insert(id);
                }
            }
        }
        frontier = next;
    }

    let ids: Vec<String> = visited.into_iter().take(max_entities as usize).collect();
    let ents = sqlx::query(
        "SELECT entity_id, name, entity_type, summary, mention_count, embedding_b \
         FROM kg_entity WHERE entity_id = ANY($1) ORDER BY mention_count DESC",
    )
    .bind(&ids)
    .fetch_all(pool)
    .await
    .unwrap_or_default();
    let entities: Vec<Value> = ents.iter().map(entity_row_json).collect();
    let id_set: std::collections::HashSet<&String> = ids.iter().collect();

    let rels = sqlx::query(
        "SELECT src_id, dst_id, rel_type, weight, evidence FROM kg_relation \
         WHERE src_id = ANY($1) AND dst_id = ANY($1) ORDER BY weight DESC",
    )
    .bind(&ids)
    .fetch_all(pool)
    .await
    .unwrap_or_default();
    let relations: Vec<Value> = rels
        .iter()
        .filter_map(|r| {
            let src = r.get::<String, _>("src_id");
            let dst = r.get::<String, _>("dst_id");
            if !id_set.contains(&src) || !id_set.contains(&dst) {
                return None;
            }
            Some(json!({
                "src_id": src,
                "dst_id": dst,
                "rel_type": r.get::<String, _>("rel_type"),
                "weight": r.get::<f32, _>("weight"),
                "evidence": r.get::<Option<String>, _>("evidence"),
            }))
        })
        .collect();

    let ec = entities.len();
    let rc = relations.len();
    json!({ "ok": true, "entities": entities, "relations": relations, "entity_count": ec, "relation_count": rc })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_merges_variants() {
        assert_eq!(normalize_name("  Paulo  VILA. "), normalize_name("paulo vila"));
        assert_eq!(normalize_name("AWS!"), "aws");
    }

    #[test]
    fn entity_id_deterministic_and_scope_isolated() {
        let a = entity_id_for("capacita|user:x||", "paulo vila", "person");
        let b = entity_id_for("capacita|user:x||", "paulo vila", "person");
        let c = entity_id_for("capacita|user:y||", "paulo vila", "person");
        assert_eq!(a, b);
        assert_ne!(a, c); // different scope → different node
    }

    #[test]
    fn embedding_roundtrip() {
        let v = vec![0.5f32, -1.0, 2.5];
        assert_eq!(unpack_embedding(&pack_embedding(&v)), Some(v));
        assert_eq!(unpack_embedding(&[1, 2, 3]), None);
    }
}
