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

/// Canonical DISPLAY name: trim + drop a trailing legal/org suffix ("& Company",
/// "Inc", "Ltd", "SAS", "S.A."…) so "McKinsey & Company" and "McKinsey" become one
/// node. Case preserved. This is store-level resolution — every writer dedups the
/// same way, not just one kit.
fn canonical_name(raw: &str) -> String {
    const SUFFIXES: &[&str] = &[
        " & company", " and company", " & co", " & co.", " inc", " inc.", " llc",
        " ltd", " ltd.", " limited", " s.a.", " sa", " s.a.s", " s.a.s.", " sas",
        " corp", " corp.", " corporation", " co.", " gmbh", " plc",
    ];
    let mut s = raw.trim().trim_end_matches(|c: char| matches!(c, '.' | ',' | ';' | ':')).trim();
    loop {
        let lower = s.to_lowercase();
        let cut = SUFFIXES.iter().find_map(|suf| lower.ends_with(suf).then(|| s.len() - suf.len()));
        match cut {
            Some(i) => s = s[..i].trim_end_matches(|c: char| matches!(c, '.' | ',' | ';' | ':')).trim(),
            None => break,
        }
    }
    s.to_string()
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

/// Controlled type vocabulary so the SAME entity doesn't split across
/// "institucion" / "organizacion" / "empresa".
fn canonical_type(raw: &str) -> String {
    let t = raw.trim().to_lowercase();
    let t = t.trim_start_matches("a ").trim();
    match t {
        "organizacion" | "organización" | "organization" | "org" | "company" | "empresa"
        | "institucion" | "institución" | "institution" | "university" | "universidad"
        | "agency" | "agencia" | "gobierno" | "government" => "empresa",
        "person" | "persona" | "people" | "individuo" => "persona",
        "product" | "producto" | "project" | "proyecto" | "app" | "platform" | "plataforma" => "producto",
        "technology" | "tecnologia" | "tecnología" | "tech" | "tool" | "herramienta"
        | "framework" | "language" | "lenguaje" => "tecnologia",
        "skill" | "habilidad" | "competencia" => "habilidad",
        "place" | "lugar" | "city" | "ciudad" | "country" | "pais" | "país" | "region" | "región" => "lugar",
        "role" | "cargo" | "title" | "titulo" | "título" | "job" | "position" | "puesto" => "cargo",
        "certification" | "certificacion" | "certificación" | "certificate" | "certificado" => "certificacion",
        "award" | "premio" | "prize" | "reconocimiento" => "premio",
        "law" | "ley" | "articulo" | "artículo" | "clause" | "clausula" | "cláusula" | "contract" | "contrato" => "ley",
        "date" | "fecha" | "year" | "año" => "fecha",
        "" => "concepto",
        other => {
            if other.len() <= 16 && other.split_whitespace().count() == 1 {
                return other.to_string();
            }
            "concepto"
        }
    }
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
        let raw_name = node.get("name").and_then(|v| v.as_str())?.trim();
        if raw_name.is_empty() {
            return None;
        }
        // Store-level resolution: canonical display name + controlled type so variants
        // collapse to one node regardless of which writer/kit sent them.
        let name = canonical_name(raw_name);
        if name.is_empty() {
            return None;
        }
        let etype = canonical_type(
            node.get("type").and_then(|v| v.as_str()).unwrap_or("concepto"),
        );
        let name = name.as_str();
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
            // Confidence: 'inferred' for deductions/co-mention, else 'extracted'.
            let confidence = match tr.get("confidence").and_then(|v| v.as_str()) {
                Some(c) if c.eq_ignore_ascii_case("inferred") => "inferred",
                _ => "extracted",
            };
            let rid = format!("r_{:016x}", fnv1a64(&format!("{skey}|{sid}|{did}|{rel}")));
            let source_kinds = json!([source_kind]);

            let res = sqlx::query(
                "INSERT INTO kg_relation \
                   (relation_id, app_id, tenant_id, expert_id, collection, src_id, dst_id, \
                    rel_type, weight, evidence, confidence, source_kinds) \
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) \
                 ON CONFLICT (relation_id) DO UPDATE SET \
                   weight     = kg_relation.weight + EXCLUDED.weight, \
                   evidence   = COALESCE(EXCLUDED.evidence, kg_relation.evidence), \
                   confidence = CASE WHEN kg_relation.confidence = 'extracted' THEN kg_relation.confidence ELSE EXCLUDED.confidence END, \
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
            .bind(confidence)
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
        "SELECT src_id, dst_id, rel_type, weight, evidence, confidence \
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
                "confidence": r.get::<String, _>("confidence"),
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
        "SELECT src_id, dst_id, rel_type, weight, evidence, confidence FROM kg_relation \
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
                "confidence": r.get::<String, _>("confidence"),
            }))
        })
        .collect();

    let ec = entities.len();
    let rc = relations.len();
    json!({ "ok": true, "entities": entities, "relations": relations, "entity_count": ec, "relation_count": rc })
}

// ─── kg_centrality ────────────────────────────────────────────────────────────
//
// PageRank over the scoped graph — "which entities matter most" — computed in Rust
// (no LLM). Powers ranking in retrieval and the viewer. Payload: scope + top?
// (default 20) + alpha? (default 0.85). Returns the top-N entities by score.
pub async fn centrality(pool: &sqlx::PgPool, payload: Value) -> Value {
    let app = opt_str(&payload, "app_id");
    let tenant = opt_str(&payload, "tenant_id");
    let expert = opt_str(&payload, "expert_id");
    let coll = opt_str(&payload, "collection");
    let top = payload.get("top").and_then(|v| v.as_i64()).unwrap_or(20).clamp(1, 500) as usize;
    let alpha = payload.get("alpha").and_then(|v| v.as_f64()).unwrap_or(0.85) as f32;

    let ents = sqlx::query(
        "SELECT entity_id, name, entity_type, mention_count FROM kg_entity \
         WHERE ($1::text IS NULL OR app_id = $1) AND ($2::text IS NULL OR tenant_id = $2) \
           AND ($3::text IS NULL OR expert_id = $3) AND ($4::text IS NULL OR collection = $4)",
    )
    .bind(&app).bind(&tenant).bind(&expert).bind(&coll)
    .fetch_all(pool).await.unwrap_or_default();
    let n = ents.len();
    if n == 0 {
        return json!({ "ok": true, "top": [] });
    }
    let mut idx_of = std::collections::HashMap::with_capacity(n);
    for (i, r) in ents.iter().enumerate() {
        idx_of.insert(r.get::<String, _>("entity_id"), i);
    }

    let rels = sqlx::query(
        "SELECT src_id, dst_id, weight FROM kg_relation \
         WHERE ($1::text IS NULL OR app_id = $1) AND ($2::text IS NULL OR tenant_id = $2) \
           AND ($3::text IS NULL OR expert_id = $3) AND ($4::text IS NULL OR collection = $4)",
    )
    .bind(&app).bind(&tenant).bind(&expert).bind(&coll)
    .fetch_all(pool).await.unwrap_or_default();

    let mut adj: Vec<Vec<(usize, f32)>> = vec![Vec::new(); n];
    let mut out_w: Vec<f32> = vec![0.0; n];
    for r in &rels {
        let (Some(&si), Some(&di)) = (
            idx_of.get(&r.get::<String, _>("src_id")),
            idx_of.get(&r.get::<String, _>("dst_id")),
        ) else {
            continue;
        };
        if si == di {
            continue;
        }
        let w = r.get::<f32, _>("weight").max(0.01);
        adj[si].push((di, w));
        adj[di].push((si, w)); // undirected
        out_w[si] += w;
        out_w[di] += w;
    }

    // Power iteration.
    let base = 1.0 / n as f32;
    let mut pr = vec![base; n];
    for _ in 0..60 {
        let mut next = vec![(1.0 - alpha) / n as f32; n];
        let mut dangling = 0.0f32;
        for i in 0..n {
            if out_w[i] <= 0.0 {
                dangling += pr[i];
                continue;
            }
            let share = alpha * pr[i];
            for &(j, w) in &adj[i] {
                next[j] += share * (w / out_w[i]);
            }
        }
        let spill = alpha * dangling / n as f32;
        for v in &mut next {
            *v += spill;
        }
        pr = next;
    }

    let mut order: Vec<usize> = (0..n).collect();
    order.sort_by(|&a, &b| pr[b].partial_cmp(&pr[a]).unwrap_or(std::cmp::Ordering::Equal));
    let top_list: Vec<Value> = order
        .into_iter()
        .take(top)
        .map(|i| {
            let r = &ents[i];
            json!({
                "entity_id": r.get::<String, _>("entity_id"),
                "name": r.get::<String, _>("name"),
                "entity_type": r.get::<String, _>("entity_type"),
                "mention_count": r.get::<i32, _>("mention_count"),
                "score": pr[i],
            })
        })
        .collect();
    json!({ "ok": true, "count": n, "top": top_list })
}

// Load the scoped graph as (entities: id→(name, mention), undirected adjacency).
async fn load_scope_graph(
    pool: &sqlx::PgPool,
    app: &Option<String>,
    tenant: &Option<String>,
    expert: &Option<String>,
    coll: &Option<String>,
) -> (Vec<(String, String, i32)>, std::collections::HashMap<String, Vec<(String, f32)>>) {
    let ents = sqlx::query(
        "SELECT entity_id, name, mention_count FROM kg_entity \
         WHERE ($1::text IS NULL OR app_id = $1) AND ($2::text IS NULL OR tenant_id = $2) \
           AND ($3::text IS NULL OR expert_id = $3) AND ($4::text IS NULL OR collection = $4)",
    )
    .bind(app).bind(tenant).bind(expert).bind(coll)
    .fetch_all(pool).await.unwrap_or_default();
    let entities: Vec<(String, String, i32)> = ents
        .iter()
        .map(|r| (r.get::<String, _>("entity_id"), r.get::<String, _>("name"), r.get::<i32, _>("mention_count")))
        .collect();
    let valid: std::collections::HashSet<&String> = entities.iter().map(|(id, _, _)| id).collect();
    let rels = sqlx::query(
        "SELECT src_id, dst_id, weight FROM kg_relation \
         WHERE ($1::text IS NULL OR app_id = $1) AND ($2::text IS NULL OR tenant_id = $2) \
           AND ($3::text IS NULL OR expert_id = $3) AND ($4::text IS NULL OR collection = $4)",
    )
    .bind(app).bind(tenant).bind(expert).bind(coll)
    .fetch_all(pool).await.unwrap_or_default();
    let mut adj: std::collections::HashMap<String, Vec<(String, f32)>> = std::collections::HashMap::new();
    for r in &rels {
        let s = r.get::<String, _>("src_id");
        let d = r.get::<String, _>("dst_id");
        if s == d || !valid.contains(&s) || !valid.contains(&d) {
            continue;
        }
        let w = r.get::<f32, _>("weight").max(0.01);
        adj.entry(s.clone()).or_default().push((d.clone(), w));
        adj.entry(d).or_default().push((s, w));
    }
    (entities, adj)
}

// ─── kg_path ──────────────────────────────────────────────────────────────────
//
// Shortest path between two entities (BFS over the undirected graph). "How is X
// related to Y?" Payload: scope + from + to (entity_ids) + max_hops? (default 5).
pub async fn path(pool: &sqlx::PgPool, payload: Value) -> Value {
    let app = opt_str(&payload, "app_id");
    let tenant = opt_str(&payload, "tenant_id");
    let expert = opt_str(&payload, "expert_id");
    let coll = opt_str(&payload, "collection");
    let (Some(from), Some(to)) = (opt_str(&payload, "from"), opt_str(&payload, "to")) else {
        return json!({ "error": "kg_path requires from + to entity_ids" });
    };
    let max_hops = payload.get("max_hops").and_then(|v| v.as_i64()).unwrap_or(5).clamp(1, 8) as usize;

    let (entities, adj) = load_scope_graph(pool, &app, &tenant, &expert, &coll).await;
    let name_of: std::collections::HashMap<&str, &str> =
        entities.iter().map(|(id, n, _)| (id.as_str(), n.as_str())).collect();

    // BFS with predecessor tracking, bounded by max_hops.
    let mut prev: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    let mut depth: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    let mut queue = std::collections::VecDeque::new();
    depth.insert(from.clone(), 0);
    queue.push_back(from.clone());
    let mut found = false;
    while let Some(cur) = queue.pop_front() {
        if cur == to {
            found = true;
            break;
        }
        let d = depth[&cur];
        if d >= max_hops {
            continue;
        }
        if let Some(nbrs) = adj.get(&cur) {
            for (nb, _) in nbrs {
                if !depth.contains_key(nb) {
                    depth.insert(nb.clone(), d + 1);
                    prev.insert(nb.clone(), cur.clone());
                    queue.push_back(nb.clone());
                }
            }
        }
    }
    if !found {
        return json!({ "ok": true, "found": false, "path": [] });
    }
    let mut chain = vec![to.clone()];
    let mut node = to;
    while let Some(p) = prev.get(&node) {
        chain.push(p.clone());
        node = p.clone();
    }
    chain.reverse();
    let path: Vec<Value> = chain
        .iter()
        .map(|id| json!({ "entity_id": id, "name": name_of.get(id.as_str()).copied().unwrap_or("?") }))
        .collect();
    let len = path.len().saturating_sub(1);
    json!({ "ok": true, "found": true, "length": len, "path": path })
}

// ─── kg_communities ───────────────────────────────────────────────────────────
//
// Cluster entities by label propagation (no LLM) — "idea groups". Each community is
// named by its highest-mention member. Payload: scope + min_size? (default 2).
pub async fn communities(pool: &sqlx::PgPool, payload: Value) -> Value {
    let app = opt_str(&payload, "app_id");
    let tenant = opt_str(&payload, "tenant_id");
    let expert = opt_str(&payload, "expert_id");
    let coll = opt_str(&payload, "collection");
    let min_size = payload.get("min_size").and_then(|v| v.as_i64()).unwrap_or(2).max(1) as usize;

    let (entities, adj) = load_scope_graph(pool, &app, &tenant, &expert, &coll).await;
    let n = entities.len();
    if n == 0 {
        return json!({ "ok": true, "communities": [] });
    }
    let idx_of: std::collections::HashMap<&str, usize> =
        entities.iter().enumerate().map(|(i, (id, _, _))| (id.as_str(), i)).collect();
    // Label propagation: each node adopts the highest-weight label among neighbors.
    let mut label: Vec<usize> = (0..n).collect();
    for _ in 0..14 {
        let mut changed = false;
        for i in 0..n {
            let id = entities[i].0.as_str();
            let Some(nbrs) = adj.get(id) else { continue };
            let mut tally: std::collections::HashMap<usize, f32> = std::collections::HashMap::new();
            for (nb, w) in nbrs {
                if let Some(&j) = idx_of.get(nb.as_str()) {
                    *tally.entry(label[j]).or_insert(0.0) += *w;
                }
            }
            if let Some((&best, _)) = tally
                .iter()
                .max_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal).then(b.0.cmp(a.0)))
            {
                if label[i] != best {
                    label[i] = best;
                    changed = true;
                }
            }
        }
        if !changed {
            break;
        }
    }
    // Group + name by highest-mention member.
    let mut groups: std::collections::HashMap<usize, Vec<usize>> = std::collections::HashMap::new();
    for i in 0..n {
        groups.entry(label[i]).or_default().push(i);
    }
    let mut out: Vec<Value> = groups
        .values()
        .filter(|members| members.len() >= min_size)
        .map(|members| {
            let top = members
                .iter()
                .max_by_key(|&&i| entities[i].2)
                .copied()
                .unwrap_or(members[0]);
            json!({
                "name": entities[top].1,
                "size": members.len(),
                "members": members.iter().map(|&i| entities[i].0.clone()).collect::<Vec<_>>(),
            })
        })
        .collect();
    out.sort_by(|a, b| b["size"].as_u64().cmp(&a["size"].as_u64()));
    json!({ "ok": true, "count": out.len(), "communities": out })
}

// ─── kg_clear ─────────────────────────────────────────────────────────────────
//
// Delete the whole graph for a scope (entities + relations). Used before a full
// re-extraction so stale / noisy nodes don't accumulate. Requires app_id to be
// present (refuses to wipe the entire table on an empty scope).
pub async fn clear(pool: &sqlx::PgPool, payload: Value) -> Value {
    let app = opt_str(&payload, "app_id");
    if app.is_none() {
        return json!({ "error": "kg_clear requires app_id" });
    }
    let tenant = opt_str(&payload, "tenant_id");
    let expert = opt_str(&payload, "expert_id");
    let coll = opt_str(&payload, "collection");

    let del = |table: &str| {
        format!(
            "DELETE FROM {table} \
             WHERE ($1::text IS NULL OR app_id    = $1) \
               AND ($2::text IS NULL OR tenant_id = $2) \
               AND ($3::text IS NULL OR expert_id = $3) \
               AND ($4::text IS NULL OR collection = $4)"
        )
    };
    let rels = sqlx::query(&del("kg_relation"))
        .bind(&app)
        .bind(&tenant)
        .bind(&expert)
        .bind(&coll)
        .execute(pool)
        .await;
    let ents = sqlx::query(&del("kg_entity"))
        .bind(&app)
        .bind(&tenant)
        .bind(&expert)
        .bind(&coll)
        .execute(pool)
        .await;
    match (rels, ents) {
        (Ok(r), Ok(e)) => json!({
            "ok": true,
            "relations_deleted": r.rows_affected(),
            "entities_deleted": e.rows_affected()
        }),
        _ => json!({ "error": "kg_clear: DB error" }),
    }
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
