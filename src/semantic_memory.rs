use serde::{Deserialize, Serialize};
use sqlx::{Row, SqlitePool};

fn parse_json(raw: Option<String>) -> Option<serde_json::Value> {
    raw.and_then(|value| serde_json::from_str(&value).ok())
}

fn stringify_json(value: Option<&serde_json::Value>) -> Option<String> {
    value.map(|item| item.to_string())
}

#[derive(Debug, Clone, Deserialize)]
pub struct ResearchProjectUpsert {
    pub project_id: String,
    pub title: String,
    #[serde(default)]
    pub goal: String,
    #[serde(default)]
    pub questions_json: Option<serde_json::Value>,
    #[serde(default)]
    pub constraints_json: Option<serde_json::Value>,
    #[serde(default)]
    pub deliverable_type: Option<String>,
    #[serde(default)]
    pub owner: Option<String>,
    #[serde(default)]
    pub user_id: Option<String>,
    #[serde(default)]
    pub tenant_id: Option<String>,
    #[serde(default)]
    pub app_id: Option<String>,
    #[serde(default)]
    pub scope_key: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub importance_score: Option<f64>,
    #[serde(default)]
    pub freshness_score: Option<f64>,
    #[serde(default)]
    pub confidence: Option<f64>,
    #[serde(default)]
    pub tags_json: Option<serde_json::Value>,
    #[serde(default)]
    pub metadata_json: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ResearchSessionCreate {
    pub session_id: String,
    pub project_id: String,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub brief: Option<String>,
    #[serde(default)]
    pub channel: Option<String>,
    #[serde(default)]
    pub tools_json: Option<serde_json::Value>,
    #[serde(default)]
    pub agents_json: Option<serde_json::Value>,
    #[serde(default)]
    pub summary: Option<String>,
    #[serde(default)]
    pub user_id: Option<String>,
    #[serde(default)]
    pub tenant_id: Option<String>,
    #[serde(default)]
    pub app_id: Option<String>,
    #[serde(default)]
    pub scope_key: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub tags_json: Option<serde_json::Value>,
    #[serde(default)]
    pub metadata_json: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ResearchSourceUpsert {
    pub source_id: String,
    pub project_id: String,
    #[serde(default)]
    pub session_id: Option<String>,
    pub source_kind: String,
    #[serde(default)]
    pub source_uri: Option<String>,
    #[serde(default)]
    pub source_label: Option<String>,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub summary: Option<String>,
    #[serde(default)]
    pub content_type: Option<String>,
    #[serde(default)]
    pub user_id: Option<String>,
    #[serde(default)]
    pub tenant_id: Option<String>,
    #[serde(default)]
    pub app_id: Option<String>,
    #[serde(default)]
    pub scope_key: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub confidence: Option<f64>,
    #[serde(default)]
    pub tags_json: Option<serde_json::Value>,
    #[serde(default)]
    pub metadata_json: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConceptNodeUpsert {
    pub concept_id: String,
    pub canonical_name: String,
    #[serde(default)]
    pub summary: Option<String>,
    #[serde(default)]
    pub domain: Option<String>,
    #[serde(default)]
    pub aliases_json: Option<serde_json::Value>,
    #[serde(default)]
    pub user_id: Option<String>,
    #[serde(default)]
    pub tenant_id: Option<String>,
    #[serde(default)]
    pub app_id: Option<String>,
    #[serde(default)]
    pub scope_key: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub importance_score: Option<f64>,
    #[serde(default)]
    pub freshness_score: Option<f64>,
    #[serde(default)]
    pub confidence: Option<f64>,
    #[serde(default)]
    pub tags_json: Option<serde_json::Value>,
    #[serde(default)]
    pub metadata_json: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClaimRecordCreate {
    pub claim_id: String,
    pub claim_text: String,
    pub primary_concept_id: String,
    #[serde(default)]
    pub project_id: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub claim_type: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub confidence: Option<f64>,
    #[serde(default)]
    pub evidence_count: Option<i64>,
    #[serde(default)]
    pub provenance_refs: Option<serde_json::Value>,
    #[serde(default)]
    pub tags_json: Option<serde_json::Value>,
    #[serde(default)]
    pub metadata_json: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EvidenceRecordCreate {
    pub evidence_id: String,
    pub claim_id: String,
    #[serde(default)]
    pub project_id: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub source_kind: Option<String>,
    #[serde(default)]
    pub source_ref: Option<String>,
    pub snippet: String,
    #[serde(default)]
    pub locator: Option<String>,
    #[serde(default)]
    pub extraction_method: Option<String>,
    #[serde(default)]
    pub contradiction_group: Option<String>,
    #[serde(default)]
    pub confidence: Option<f64>,
    #[serde(default)]
    pub provenance_refs: Option<serde_json::Value>,
    #[serde(default)]
    pub tags_json: Option<serde_json::Value>,
    #[serde(default)]
    pub metadata_json: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RelationEdgeCreate {
    pub edge_id: String,
    pub from_concept_id: String,
    pub to_concept_id: String,
    pub relation_type: String,
    #[serde(default)]
    pub project_id: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub weight: Option<f64>,
    #[serde(default)]
    pub confidence: Option<f64>,
    #[serde(default)]
    pub provenance_refs: Option<serde_json::Value>,
    #[serde(default)]
    pub tags_json: Option<serde_json::Value>,
    #[serde(default)]
    pub metadata_json: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SemanticCounts {
    pub project_count: i64,
    pub session_count: i64,
    pub source_count: i64,
    pub concept_count: i64,
    pub claim_count: i64,
    pub evidence_count: i64,
    pub relation_count: i64,
}

pub async fn init_tables(pool: &SqlitePool) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS research_projects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id TEXT NOT NULL UNIQUE,
            title TEXT NOT NULL,
            goal TEXT NOT NULL DEFAULT '',
            questions_json TEXT,
            constraints_json TEXT,
            deliverable_type TEXT NOT NULL DEFAULT 'report',
            owner TEXT NOT NULL DEFAULT 'system',
            user_id TEXT,
            tenant_id TEXT NOT NULL DEFAULT 'default',
            app_id TEXT NOT NULL DEFAULT 'os',
            scope_key TEXT NOT NULL DEFAULT 'personal',
            status TEXT NOT NULL DEFAULT 'active',
            importance_score REAL,
            freshness_score REAL,
            confidence REAL,
            tags_json TEXT,
            metadata_json TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS research_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL UNIQUE,
            project_id TEXT NOT NULL,
            title TEXT,
            brief TEXT,
            channel TEXT,
            tools_json TEXT,
            agents_json TEXT,
            summary TEXT,
            user_id TEXT,
            tenant_id TEXT NOT NULL DEFAULT 'default',
            app_id TEXT NOT NULL DEFAULT 'os',
            scope_key TEXT NOT NULL DEFAULT 'personal',
            status TEXT NOT NULL DEFAULT 'active',
            tags_json TEXT,
            metadata_json TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(project_id) REFERENCES research_projects(project_id)
        )
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS research_sources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id TEXT NOT NULL UNIQUE,
            project_id TEXT NOT NULL,
            session_id TEXT,
            source_kind TEXT NOT NULL DEFAULT 'text',
            source_uri TEXT,
            source_label TEXT,
            title TEXT,
            summary TEXT,
            content_type TEXT,
            user_id TEXT,
            tenant_id TEXT NOT NULL DEFAULT 'default',
            app_id TEXT NOT NULL DEFAULT 'os',
            scope_key TEXT NOT NULL DEFAULT 'personal',
            status TEXT NOT NULL DEFAULT 'active',
            confidence REAL,
            tags_json TEXT,
            metadata_json TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(project_id) REFERENCES research_projects(project_id),
            FOREIGN KEY(session_id) REFERENCES research_sessions(session_id)
        )
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS semantic_concepts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            concept_id TEXT NOT NULL UNIQUE,
            canonical_name TEXT NOT NULL,
            summary TEXT,
            domain TEXT,
            aliases_json TEXT,
            user_id TEXT,
            tenant_id TEXT NOT NULL DEFAULT 'default',
            app_id TEXT NOT NULL DEFAULT 'os',
            scope_key TEXT NOT NULL DEFAULT 'personal',
            status TEXT NOT NULL DEFAULT 'active',
            reuse_count INTEGER NOT NULL DEFAULT 0,
            importance_score REAL,
            freshness_score REAL,
            confidence REAL,
            tags_json TEXT,
            metadata_json TEXT,
            last_consolidated_at DATETIME,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS semantic_claims (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            claim_id TEXT NOT NULL UNIQUE,
            project_id TEXT,
            session_id TEXT,
            primary_concept_id TEXT NOT NULL,
            claim_text TEXT NOT NULL,
            claim_type TEXT NOT NULL DEFAULT 'fact',
            status TEXT NOT NULL DEFAULT 'active',
            confidence REAL,
            evidence_count INTEGER NOT NULL DEFAULT 0,
            provenance_refs TEXT,
            tags_json TEXT,
            metadata_json TEXT,
            last_verified_at DATETIME,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(project_id) REFERENCES research_projects(project_id),
            FOREIGN KEY(session_id) REFERENCES research_sessions(session_id),
            FOREIGN KEY(primary_concept_id) REFERENCES semantic_concepts(concept_id)
        )
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS semantic_evidence (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            evidence_id TEXT NOT NULL UNIQUE,
            claim_id TEXT NOT NULL,
            project_id TEXT,
            session_id TEXT,
            source_kind TEXT NOT NULL DEFAULT 'text',
            source_ref TEXT,
            snippet TEXT NOT NULL,
            locator TEXT,
            extraction_method TEXT,
            contradiction_group TEXT,
            confidence REAL,
            provenance_refs TEXT,
            tags_json TEXT,
            metadata_json TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(claim_id) REFERENCES semantic_claims(claim_id)
        )
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS semantic_relations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            edge_id TEXT NOT NULL UNIQUE,
            from_concept_id TEXT NOT NULL,
            to_concept_id TEXT NOT NULL,
            relation_type TEXT NOT NULL,
            project_id TEXT,
            session_id TEXT,
            weight REAL,
            confidence REAL,
            provenance_refs TEXT,
            tags_json TEXT,
            metadata_json TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(from_concept_id) REFERENCES semantic_concepts(concept_id),
            FOREIGN KEY(to_concept_id) REFERENCES semantic_concepts(concept_id)
        )
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query("CREATE INDEX IF NOT EXISTS idx_research_sessions_project_id ON research_sessions(project_id)")
        .execute(pool)
        .await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_research_sources_project_id ON research_sources(project_id)")
        .execute(pool)
        .await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_research_sources_uri ON research_sources(source_uri)",
    )
    .execute(pool)
    .await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_semantic_claims_concept_id ON semantic_claims(primary_concept_id)")
        .execute(pool)
        .await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_semantic_evidence_claim_id ON semantic_evidence(claim_id)",
    )
    .execute(pool)
    .await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_semantic_relations_from_to ON semantic_relations(from_concept_id, to_concept_id)")
        .execute(pool)
        .await?;

    Ok(())
}

pub async fn upsert_project(
    pool: &SqlitePool,
    payload: ResearchProjectUpsert,
) -> anyhow::Result<serde_json::Value> {
    if payload.project_id.trim().is_empty() || payload.title.trim().is_empty() {
        anyhow::bail!("project_id and title are required");
    }

    sqlx::query(
        r#"
        INSERT INTO research_projects (
            project_id, title, goal, questions_json, constraints_json, deliverable_type, owner,
            user_id, tenant_id, app_id, scope_key, status, importance_score, freshness_score,
            confidence, tags_json, metadata_json, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(project_id) DO UPDATE SET
            title = excluded.title,
            goal = excluded.goal,
            questions_json = excluded.questions_json,
            constraints_json = excluded.constraints_json,
            deliverable_type = excluded.deliverable_type,
            owner = excluded.owner,
            user_id = excluded.user_id,
            tenant_id = excluded.tenant_id,
            app_id = excluded.app_id,
            scope_key = excluded.scope_key,
            status = excluded.status,
            importance_score = excluded.importance_score,
            freshness_score = excluded.freshness_score,
            confidence = excluded.confidence,
            tags_json = excluded.tags_json,
            metadata_json = excluded.metadata_json,
            updated_at = CURRENT_TIMESTAMP
        "#,
    )
    .bind(&payload.project_id)
    .bind(&payload.title)
    .bind(&payload.goal)
    .bind(stringify_json(payload.questions_json.as_ref()))
    .bind(stringify_json(payload.constraints_json.as_ref()))
    .bind(payload.deliverable_type.as_deref().unwrap_or("report"))
    .bind(payload.owner.as_deref().unwrap_or("system"))
    .bind(payload.user_id.as_deref())
    .bind(payload.tenant_id.as_deref().unwrap_or("default"))
    .bind(payload.app_id.as_deref().unwrap_or("os"))
    .bind(payload.scope_key.as_deref().unwrap_or("personal"))
    .bind(payload.status.as_deref().unwrap_or("active"))
    .bind(payload.importance_score)
    .bind(payload.freshness_score)
    .bind(payload.confidence)
    .bind(stringify_json(payload.tags_json.as_ref()))
    .bind(stringify_json(payload.metadata_json.as_ref()))
    .execute(pool)
    .await?;

    get_project(pool, &payload.project_id).await
}

pub async fn create_session(
    pool: &SqlitePool,
    payload: ResearchSessionCreate,
) -> anyhow::Result<serde_json::Value> {
    if payload.session_id.trim().is_empty() || payload.project_id.trim().is_empty() {
        anyhow::bail!("session_id and project_id are required");
    }

    sqlx::query(
        r#"
        INSERT INTO research_sessions (
            session_id, project_id, title, brief, channel, tools_json, agents_json, summary,
            user_id, tenant_id, app_id, scope_key, status, tags_json, metadata_json, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(session_id) DO UPDATE SET
            project_id = excluded.project_id,
            title = excluded.title,
            brief = excluded.brief,
            channel = excluded.channel,
            tools_json = excluded.tools_json,
            agents_json = excluded.agents_json,
            summary = excluded.summary,
            user_id = excluded.user_id,
            tenant_id = excluded.tenant_id,
            app_id = excluded.app_id,
            scope_key = excluded.scope_key,
            status = excluded.status,
            tags_json = excluded.tags_json,
            metadata_json = excluded.metadata_json,
            updated_at = CURRENT_TIMESTAMP
        "#,
    )
    .bind(&payload.session_id)
    .bind(&payload.project_id)
    .bind(payload.title.as_deref())
    .bind(payload.brief.as_deref())
    .bind(payload.channel.as_deref())
    .bind(stringify_json(payload.tools_json.as_ref()))
    .bind(stringify_json(payload.agents_json.as_ref()))
    .bind(payload.summary.as_deref())
    .bind(payload.user_id.as_deref())
    .bind(payload.tenant_id.as_deref().unwrap_or("default"))
    .bind(payload.app_id.as_deref().unwrap_or("os"))
    .bind(payload.scope_key.as_deref().unwrap_or("personal"))
    .bind(payload.status.as_deref().unwrap_or("active"))
    .bind(stringify_json(payload.tags_json.as_ref()))
    .bind(stringify_json(payload.metadata_json.as_ref()))
    .execute(pool)
    .await?;

    Ok(serde_json::json!({
        "status": "success",
        "session_id": payload.session_id,
        "project_id": payload.project_id,
    }))
}

pub async fn upsert_source(
    pool: &SqlitePool,
    payload: ResearchSourceUpsert,
) -> anyhow::Result<serde_json::Value> {
    if payload.source_id.trim().is_empty()
        || payload.project_id.trim().is_empty()
        || payload.source_kind.trim().is_empty()
    {
        anyhow::bail!("source_id, project_id, and source_kind are required");
    }

    sqlx::query(
        r#"
        INSERT INTO research_sources (
            source_id, project_id, session_id, source_kind, source_uri, source_label, title, summary,
            content_type, user_id, tenant_id, app_id, scope_key, status, confidence, tags_json,
            metadata_json, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(source_id) DO UPDATE SET
            project_id = excluded.project_id,
            session_id = excluded.session_id,
            source_kind = excluded.source_kind,
            source_uri = excluded.source_uri,
            source_label = excluded.source_label,
            title = excluded.title,
            summary = excluded.summary,
            content_type = excluded.content_type,
            user_id = excluded.user_id,
            tenant_id = excluded.tenant_id,
            app_id = excluded.app_id,
            scope_key = excluded.scope_key,
            status = excluded.status,
            confidence = excluded.confidence,
            tags_json = excluded.tags_json,
            metadata_json = excluded.metadata_json,
            updated_at = CURRENT_TIMESTAMP
        "#,
    )
    .bind(&payload.source_id)
    .bind(&payload.project_id)
    .bind(payload.session_id.as_deref())
    .bind(&payload.source_kind)
    .bind(payload.source_uri.as_deref())
    .bind(payload.source_label.as_deref())
    .bind(payload.title.as_deref())
    .bind(payload.summary.as_deref())
    .bind(payload.content_type.as_deref())
    .bind(payload.user_id.as_deref())
    .bind(payload.tenant_id.as_deref().unwrap_or("default"))
    .bind(payload.app_id.as_deref().unwrap_or("os"))
    .bind(payload.scope_key.as_deref().unwrap_or("personal"))
    .bind(payload.status.as_deref().unwrap_or("active"))
    .bind(payload.confidence)
    .bind(stringify_json(payload.tags_json.as_ref()))
    .bind(stringify_json(payload.metadata_json.as_ref()))
    .execute(pool)
    .await?;

    get_source(pool, &payload.source_id).await
}

pub async fn get_source(pool: &SqlitePool, source_id: &str) -> anyhow::Result<serde_json::Value> {
    let Some(source) = sqlx::query(
        r#"
        SELECT source_id, project_id, session_id, source_kind, source_uri, source_label, title,
               summary, content_type, user_id, tenant_id, app_id, scope_key, status, confidence,
               tags_json, metadata_json, created_at, updated_at
        FROM research_sources
        WHERE source_id = ?
        "#,
    )
    .bind(source_id)
    .fetch_optional(pool)
    .await?
    else {
        return Ok(serde_json::json!({ "error": "research source not found" }));
    };

    Ok(serde_json::json!({
        "status": "success",
        "source": {
            "source_id": source.get::<String, _>("source_id"),
            "project_id": source.get::<String, _>("project_id"),
            "session_id": source.get::<Option<String>, _>("session_id"),
            "source_kind": source.get::<String, _>("source_kind"),
            "source_uri": source.get::<Option<String>, _>("source_uri"),
            "source_label": source.get::<Option<String>, _>("source_label"),
            "title": source.get::<Option<String>, _>("title"),
            "summary": source.get::<Option<String>, _>("summary"),
            "content_type": source.get::<Option<String>, _>("content_type"),
            "user_id": source.get::<Option<String>, _>("user_id"),
            "tenant_id": source.get::<String, _>("tenant_id"),
            "app_id": source.get::<String, _>("app_id"),
            "scope_key": source.get::<String, _>("scope_key"),
            "status": source.get::<String, _>("status"),
            "confidence": source.get::<Option<f64>, _>("confidence"),
            "tags_json": parse_json(source.get::<Option<String>, _>("tags_json")),
            "metadata_json": parse_json(source.get::<Option<String>, _>("metadata_json")),
            "created_at": source.get::<String, _>("created_at"),
            "updated_at": source.get::<String, _>("updated_at")
        }
    }))
}

pub async fn list_sources(
    pool: &SqlitePool,
    project_id: Option<&str>,
    session_id: Option<&str>,
    source_kind: Option<&str>,
    limit: i64,
) -> anyhow::Result<serde_json::Value> {
    let mut conditions = Vec::new();
    let mut values = Vec::new();

    if let Some(value) = project_id {
        conditions.push("project_id = ?");
        values.push(value.to_string());
    }
    if let Some(value) = session_id {
        conditions.push("session_id = ?");
        values.push(value.to_string());
    }
    if let Some(value) = source_kind {
        conditions.push("source_kind = ?");
        values.push(value.to_string());
    }

    let mut sql = String::from(
        "SELECT source_id, project_id, session_id, source_kind, source_uri, source_label, title, summary, content_type, status, confidence, created_at, updated_at FROM research_sources",
    );
    if !conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conditions.join(" AND "));
    }
    sql.push_str(" ORDER BY updated_at DESC LIMIT ?");

    let mut query = sqlx::query(&sql);
    for value in &values {
        query = query.bind(value);
    }
    query = query.bind(limit.max(1));

    let rows = query.fetch_all(pool).await?;
    Ok(serde_json::json!({
        "status": "success",
        "count": rows.len(),
        "sources": rows.iter().map(|row| {
            serde_json::json!({
                "source_id": row.get::<String, _>("source_id"),
                "project_id": row.get::<String, _>("project_id"),
                "session_id": row.get::<Option<String>, _>("session_id"),
                "source_kind": row.get::<String, _>("source_kind"),
                "source_uri": row.get::<Option<String>, _>("source_uri"),
                "source_label": row.get::<Option<String>, _>("source_label"),
                "title": row.get::<Option<String>, _>("title"),
                "summary": row.get::<Option<String>, _>("summary"),
                "content_type": row.get::<Option<String>, _>("content_type"),
                "status": row.get::<String, _>("status"),
                "confidence": row.get::<Option<f64>, _>("confidence"),
                "created_at": row.get::<String, _>("created_at"),
                "updated_at": row.get::<String, _>("updated_at")
            })
        }).collect::<Vec<_>>()
    }))
}

pub async fn upsert_concept(
    pool: &SqlitePool,
    payload: ConceptNodeUpsert,
) -> anyhow::Result<serde_json::Value> {
    if payload.concept_id.trim().is_empty() || payload.canonical_name.trim().is_empty() {
        anyhow::bail!("concept_id and canonical_name are required");
    }

    sqlx::query(
        r#"
        INSERT INTO semantic_concepts (
            concept_id, canonical_name, summary, domain, aliases_json, user_id, tenant_id, app_id,
            scope_key, status, importance_score, freshness_score, confidence, tags_json,
            metadata_json, last_consolidated_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT(concept_id) DO UPDATE SET
            canonical_name = excluded.canonical_name,
            summary = excluded.summary,
            domain = excluded.domain,
            aliases_json = excluded.aliases_json,
            user_id = excluded.user_id,
            tenant_id = excluded.tenant_id,
            app_id = excluded.app_id,
            scope_key = excluded.scope_key,
            status = excluded.status,
            importance_score = excluded.importance_score,
            freshness_score = excluded.freshness_score,
            confidence = excluded.confidence,
            tags_json = excluded.tags_json,
            metadata_json = excluded.metadata_json,
            last_consolidated_at = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP,
            reuse_count = semantic_concepts.reuse_count + 1
        "#,
    )
    .bind(&payload.concept_id)
    .bind(&payload.canonical_name)
    .bind(payload.summary.as_deref())
    .bind(payload.domain.as_deref())
    .bind(stringify_json(payload.aliases_json.as_ref()))
    .bind(payload.user_id.as_deref())
    .bind(payload.tenant_id.as_deref().unwrap_or("default"))
    .bind(payload.app_id.as_deref().unwrap_or("os"))
    .bind(payload.scope_key.as_deref().unwrap_or("personal"))
    .bind(payload.status.as_deref().unwrap_or("active"))
    .bind(payload.importance_score)
    .bind(payload.freshness_score)
    .bind(payload.confidence)
    .bind(stringify_json(payload.tags_json.as_ref()))
    .bind(stringify_json(payload.metadata_json.as_ref()))
    .execute(pool)
    .await?;

    expand_concept(pool, &payload.concept_id, 20).await
}

pub async fn append_claim(
    pool: &SqlitePool,
    payload: ClaimRecordCreate,
) -> anyhow::Result<serde_json::Value> {
    if payload.claim_id.trim().is_empty()
        || payload.claim_text.trim().is_empty()
        || payload.primary_concept_id.trim().is_empty()
    {
        anyhow::bail!("claim_id, claim_text, and primary_concept_id are required");
    }

    sqlx::query(
        r#"
        INSERT INTO semantic_claims (
            claim_id, project_id, session_id, primary_concept_id, claim_text, claim_type, status,
            confidence, evidence_count, provenance_refs, tags_json, metadata_json, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(claim_id) DO UPDATE SET
            project_id = excluded.project_id,
            session_id = excluded.session_id,
            primary_concept_id = excluded.primary_concept_id,
            claim_text = excluded.claim_text,
            claim_type = excluded.claim_type,
            status = excluded.status,
            confidence = excluded.confidence,
            evidence_count = excluded.evidence_count,
            provenance_refs = excluded.provenance_refs,
            tags_json = excluded.tags_json,
            metadata_json = excluded.metadata_json,
            updated_at = CURRENT_TIMESTAMP
        "#,
    )
    .bind(&payload.claim_id)
    .bind(payload.project_id.as_deref())
    .bind(payload.session_id.as_deref())
    .bind(&payload.primary_concept_id)
    .bind(&payload.claim_text)
    .bind(payload.claim_type.as_deref().unwrap_or("fact"))
    .bind(payload.status.as_deref().unwrap_or("active"))
    .bind(payload.confidence)
    .bind(payload.evidence_count.unwrap_or(0))
    .bind(stringify_json(payload.provenance_refs.as_ref()))
    .bind(stringify_json(payload.tags_json.as_ref()))
    .bind(stringify_json(payload.metadata_json.as_ref()))
    .execute(pool)
    .await?;

    Ok(serde_json::json!({
        "status": "success",
        "claim_id": payload.claim_id,
        "primary_concept_id": payload.primary_concept_id,
    }))
}

pub async fn append_evidence(
    pool: &SqlitePool,
    payload: EvidenceRecordCreate,
) -> anyhow::Result<serde_json::Value> {
    if payload.evidence_id.trim().is_empty()
        || payload.claim_id.trim().is_empty()
        || payload.snippet.trim().is_empty()
    {
        anyhow::bail!("evidence_id, claim_id, and snippet are required");
    }

    sqlx::query(
        r#"
        INSERT INTO semantic_evidence (
            evidence_id, claim_id, project_id, session_id, source_kind, source_ref, snippet,
            locator, extraction_method, contradiction_group, confidence, provenance_refs,
            tags_json, metadata_json, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(evidence_id) DO UPDATE SET
            claim_id = excluded.claim_id,
            project_id = excluded.project_id,
            session_id = excluded.session_id,
            source_kind = excluded.source_kind,
            source_ref = excluded.source_ref,
            snippet = excluded.snippet,
            locator = excluded.locator,
            extraction_method = excluded.extraction_method,
            contradiction_group = excluded.contradiction_group,
            confidence = excluded.confidence,
            provenance_refs = excluded.provenance_refs,
            tags_json = excluded.tags_json,
            metadata_json = excluded.metadata_json,
            updated_at = CURRENT_TIMESTAMP
        "#,
    )
    .bind(&payload.evidence_id)
    .bind(&payload.claim_id)
    .bind(payload.project_id.as_deref())
    .bind(payload.session_id.as_deref())
    .bind(payload.source_kind.as_deref().unwrap_or("text"))
    .bind(payload.source_ref.as_deref())
    .bind(&payload.snippet)
    .bind(payload.locator.as_deref())
    .bind(payload.extraction_method.as_deref())
    .bind(payload.contradiction_group.as_deref())
    .bind(payload.confidence)
    .bind(stringify_json(payload.provenance_refs.as_ref()))
    .bind(stringify_json(payload.tags_json.as_ref()))
    .bind(stringify_json(payload.metadata_json.as_ref()))
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        UPDATE semantic_claims
        SET evidence_count = (
            SELECT COUNT(*) FROM semantic_evidence WHERE semantic_evidence.claim_id = semantic_claims.claim_id
        ),
            updated_at = CURRENT_TIMESTAMP
        WHERE claim_id = ?
        "#,
    )
    .bind(&payload.claim_id)
    .execute(pool)
    .await?;

    Ok(serde_json::json!({
        "status": "success",
        "evidence_id": payload.evidence_id,
        "claim_id": payload.claim_id,
    }))
}

pub async fn link_concepts(
    pool: &SqlitePool,
    payload: RelationEdgeCreate,
) -> anyhow::Result<serde_json::Value> {
    if payload.edge_id.trim().is_empty()
        || payload.from_concept_id.trim().is_empty()
        || payload.to_concept_id.trim().is_empty()
        || payload.relation_type.trim().is_empty()
    {
        anyhow::bail!("edge_id, from_concept_id, to_concept_id, and relation_type are required");
    }

    sqlx::query(
        r#"
        INSERT INTO semantic_relations (
            edge_id, from_concept_id, to_concept_id, relation_type, project_id, session_id, weight,
            confidence, provenance_refs, tags_json, metadata_json, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(edge_id) DO UPDATE SET
            from_concept_id = excluded.from_concept_id,
            to_concept_id = excluded.to_concept_id,
            relation_type = excluded.relation_type,
            project_id = excluded.project_id,
            session_id = excluded.session_id,
            weight = excluded.weight,
            confidence = excluded.confidence,
            provenance_refs = excluded.provenance_refs,
            tags_json = excluded.tags_json,
            metadata_json = excluded.metadata_json,
            updated_at = CURRENT_TIMESTAMP
        "#,
    )
    .bind(&payload.edge_id)
    .bind(&payload.from_concept_id)
    .bind(&payload.to_concept_id)
    .bind(&payload.relation_type)
    .bind(payload.project_id.as_deref())
    .bind(payload.session_id.as_deref())
    .bind(payload.weight.unwrap_or(1.0))
    .bind(payload.confidence)
    .bind(stringify_json(payload.provenance_refs.as_ref()))
    .bind(stringify_json(payload.tags_json.as_ref()))
    .bind(stringify_json(payload.metadata_json.as_ref()))
    .execute(pool)
    .await?;

    Ok(serde_json::json!({
        "status": "success",
        "edge_id": payload.edge_id,
        "from_concept_id": payload.from_concept_id,
        "to_concept_id": payload.to_concept_id,
        "relation_type": payload.relation_type,
    }))
}

pub async fn get_project(pool: &SqlitePool, project_id: &str) -> anyhow::Result<serde_json::Value> {
    let Some(project) = sqlx::query(
        r#"
        SELECT project_id, title, goal, questions_json, constraints_json, deliverable_type, owner,
               user_id, tenant_id, app_id, scope_key, status, importance_score, freshness_score,
               confidence, tags_json, metadata_json, created_at, updated_at
        FROM research_projects
        WHERE project_id = ?
        "#,
    )
    .bind(project_id)
    .fetch_optional(pool)
    .await?
    else {
        return Ok(serde_json::json!({ "error": "research project not found" }));
    };

    let sessions = sqlx::query(
        r#"
        SELECT session_id, title, brief, channel, tools_json, agents_json, summary, status, tags_json, metadata_json, created_at, updated_at
        FROM research_sessions
        WHERE project_id = ?
        ORDER BY created_at DESC
        "#,
    )
    .bind(project_id)
    .fetch_all(pool)
    .await?;

    let concepts = sqlx::query(
        r#"
        SELECT DISTINCT c.concept_id, c.canonical_name, c.summary, c.domain, c.aliases_json, c.status, c.reuse_count, c.importance_score, c.freshness_score, c.confidence
        FROM semantic_concepts c
        JOIN semantic_claims sc ON sc.primary_concept_id = c.concept_id
        WHERE sc.project_id = ?
        ORDER BY c.updated_at DESC
        LIMIT 25
        "#,
    )
    .bind(project_id)
    .fetch_all(pool)
    .await?;

    let sources = sqlx::query(
        r#"
        SELECT source_id, session_id, source_kind, source_uri, source_label, title, summary,
               content_type, status, confidence, tags_json, metadata_json, created_at, updated_at
        FROM research_sources
        WHERE project_id = ?
        ORDER BY updated_at DESC
        LIMIT 50
        "#,
    )
    .bind(project_id)
    .fetch_all(pool)
    .await?;

    Ok(serde_json::json!({
        "status": "success",
        "project": {
            "project_id": project.get::<String, _>("project_id"),
            "title": project.get::<String, _>("title"),
            "goal": project.get::<String, _>("goal"),
            "questions_json": parse_json(project.get::<Option<String>, _>("questions_json")),
            "constraints_json": parse_json(project.get::<Option<String>, _>("constraints_json")),
            "deliverable_type": project.get::<String, _>("deliverable_type"),
            "owner": project.get::<String, _>("owner"),
            "user_id": project.get::<Option<String>, _>("user_id"),
            "tenant_id": project.get::<String, _>("tenant_id"),
            "app_id": project.get::<String, _>("app_id"),
            "scope_key": project.get::<String, _>("scope_key"),
            "status": project.get::<String, _>("status"),
            "importance_score": project.get::<Option<f64>, _>("importance_score"),
            "freshness_score": project.get::<Option<f64>, _>("freshness_score"),
            "confidence": project.get::<Option<f64>, _>("confidence"),
            "tags_json": parse_json(project.get::<Option<String>, _>("tags_json")),
            "metadata_json": parse_json(project.get::<Option<String>, _>("metadata_json")),
            "created_at": project.get::<String, _>("created_at"),
            "updated_at": project.get::<String, _>("updated_at")
        },
        "sessions": sessions.iter().map(|row| {
            serde_json::json!({
                "session_id": row.get::<String, _>("session_id"),
                "title": row.get::<Option<String>, _>("title"),
                "brief": row.get::<Option<String>, _>("brief"),
                "channel": row.get::<Option<String>, _>("channel"),
                "tools_json": parse_json(row.get::<Option<String>, _>("tools_json")),
                "agents_json": parse_json(row.get::<Option<String>, _>("agents_json")),
                "summary": row.get::<Option<String>, _>("summary"),
                "status": row.get::<String, _>("status"),
                "tags_json": parse_json(row.get::<Option<String>, _>("tags_json")),
                "metadata_json": parse_json(row.get::<Option<String>, _>("metadata_json")),
                "created_at": row.get::<String, _>("created_at"),
                "updated_at": row.get::<String, _>("updated_at")
            })
        }).collect::<Vec<_>>(),
        "concepts": concepts.iter().map(|row| {
            serde_json::json!({
                "concept_id": row.get::<String, _>("concept_id"),
                "canonical_name": row.get::<String, _>("canonical_name"),
                "summary": row.get::<Option<String>, _>("summary"),
                "domain": row.get::<Option<String>, _>("domain"),
                "aliases_json": parse_json(row.get::<Option<String>, _>("aliases_json")),
                "status": row.get::<String, _>("status"),
                "reuse_count": row.get::<i64, _>("reuse_count"),
                "importance_score": row.get::<Option<f64>, _>("importance_score"),
                "freshness_score": row.get::<Option<f64>, _>("freshness_score"),
                "confidence": row.get::<Option<f64>, _>("confidence")
            })
        }).collect::<Vec<_>>(),
        "sources": sources.iter().map(|row| {
            serde_json::json!({
                "source_id": row.get::<String, _>("source_id"),
                "session_id": row.get::<Option<String>, _>("session_id"),
                "source_kind": row.get::<String, _>("source_kind"),
                "source_uri": row.get::<Option<String>, _>("source_uri"),
                "source_label": row.get::<Option<String>, _>("source_label"),
                "title": row.get::<Option<String>, _>("title"),
                "summary": row.get::<Option<String>, _>("summary"),
                "content_type": row.get::<Option<String>, _>("content_type"),
                "status": row.get::<String, _>("status"),
                "confidence": row.get::<Option<f64>, _>("confidence"),
                "tags_json": parse_json(row.get::<Option<String>, _>("tags_json")),
                "metadata_json": parse_json(row.get::<Option<String>, _>("metadata_json")),
                "created_at": row.get::<String, _>("created_at"),
                "updated_at": row.get::<String, _>("updated_at")
            })
        }).collect::<Vec<_>>()
    }))
}

pub async fn list_projects(
    pool: &SqlitePool,
    app_id: Option<&str>,
    tenant_id: Option<&str>,
    status: Option<&str>,
    limit: i64,
) -> anyhow::Result<serde_json::Value> {
    let mut conditions = Vec::new();
    let mut values = Vec::new();

    if let Some(value) = app_id {
        conditions.push("app_id = ?");
        values.push(value.to_string());
    }
    if let Some(value) = tenant_id {
        conditions.push("tenant_id = ?");
        values.push(value.to_string());
    }
    if let Some(value) = status {
        conditions.push("status = ?");
        values.push(value.to_string());
    }

    let mut sql = String::from(
        "SELECT project_id, title, goal, deliverable_type, owner, app_id, tenant_id, scope_key, status, importance_score, freshness_score, confidence, created_at, updated_at FROM research_projects",
    );
    if !conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conditions.join(" AND "));
    }
    sql.push_str(" ORDER BY updated_at DESC LIMIT ?");

    let mut query = sqlx::query(&sql);
    for value in &values {
        query = query.bind(value);
    }
    query = query.bind(limit.max(1));

    let rows = query.fetch_all(pool).await?;

    Ok(serde_json::json!({
        "status": "success",
        "count": rows.len(),
        "projects": rows.iter().map(|row| {
            serde_json::json!({
                "project_id": row.get::<String, _>("project_id"),
                "title": row.get::<String, _>("title"),
                "goal": row.get::<String, _>("goal"),
                "deliverable_type": row.get::<String, _>("deliverable_type"),
                "owner": row.get::<String, _>("owner"),
                "app_id": row.get::<String, _>("app_id"),
                "tenant_id": row.get::<String, _>("tenant_id"),
                "scope_key": row.get::<String, _>("scope_key"),
                "status": row.get::<String, _>("status"),
                "importance_score": row.get::<Option<f64>, _>("importance_score"),
                "freshness_score": row.get::<Option<f64>, _>("freshness_score"),
                "confidence": row.get::<Option<f64>, _>("confidence"),
                "created_at": row.get::<String, _>("created_at"),
                "updated_at": row.get::<String, _>("updated_at")
            })
        }).collect::<Vec<_>>()
    }))
}

pub async fn list_concepts(
    pool: &SqlitePool,
    app_id: Option<&str>,
    tenant_id: Option<&str>,
    status: Option<&str>,
    domain: Option<&str>,
    search: Option<&str>,
    limit: i64,
) -> anyhow::Result<serde_json::Value> {
    let mut conditions = Vec::new();
    let mut values = Vec::new();

    if let Some(value) = app_id {
        conditions.push("app_id = ?");
        values.push(value.to_string());
    }
    if let Some(value) = tenant_id {
        conditions.push("tenant_id = ?");
        values.push(value.to_string());
    }
    if let Some(value) = status {
        conditions.push("status = ?");
        values.push(value.to_string());
    }
    if let Some(value) = domain {
        conditions.push("domain = ?");
        values.push(value.to_string());
    }
    if let Some(value) = search {
        let pattern = format!("%{}%", value.trim());
        conditions.push("(canonical_name LIKE ? OR summary LIKE ? OR aliases_json LIKE ?)");
        values.push(pattern.clone());
        values.push(pattern.clone());
        values.push(pattern);
    }

    let mut sql = String::from(
        "SELECT concept_id, canonical_name, summary, domain, status, reuse_count, importance_score, freshness_score, confidence, updated_at FROM semantic_concepts",
    );
    if !conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conditions.join(" AND "));
    }
    sql.push_str(" ORDER BY updated_at DESC LIMIT ?");

    let mut query = sqlx::query(&sql);
    for value in &values {
        query = query.bind(value);
    }
    query = query.bind(limit.max(1));

    let rows = query.fetch_all(pool).await?;
    Ok(serde_json::json!({
        "status": "success",
        "count": rows.len(),
        "concepts": rows.iter().map(|row| {
            serde_json::json!({
                "concept_id": row.get::<String, _>("concept_id"),
                "canonical_name": row.get::<String, _>("canonical_name"),
                "summary": row.get::<Option<String>, _>("summary"),
                "domain": row.get::<Option<String>, _>("domain"),
                "status": row.get::<String, _>("status"),
                "reuse_count": row.get::<i64, _>("reuse_count"),
                "importance_score": row.get::<Option<f64>, _>("importance_score"),
                "freshness_score": row.get::<Option<f64>, _>("freshness_score"),
                "confidence": row.get::<Option<f64>, _>("confidence"),
                "updated_at": row.get::<String, _>("updated_at")
            })
        }).collect::<Vec<_>>()
    }))
}

pub async fn list_claims(
    pool: &SqlitePool,
    project_id: Option<&str>,
    concept_id: Option<&str>,
    status: Option<&str>,
    search: Option<&str>,
    limit: i64,
) -> anyhow::Result<serde_json::Value> {
    let mut conditions = Vec::new();
    let mut values = Vec::new();

    if let Some(value) = project_id {
        conditions.push("project_id = ?");
        values.push(value.to_string());
    }
    if let Some(value) = concept_id {
        conditions.push("primary_concept_id = ?");
        values.push(value.to_string());
    }
    if let Some(value) = status {
        conditions.push("status = ?");
        values.push(value.to_string());
    }
    if let Some(value) = search {
        let pattern = format!("%{}%", value.trim());
        conditions.push("claim_text LIKE ?");
        values.push(pattern);
    }

    let mut sql = String::from(
        "SELECT claim_id, project_id, session_id, primary_concept_id, claim_text, claim_type, status, confidence, evidence_count, last_verified_at, updated_at FROM semantic_claims",
    );
    if !conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conditions.join(" AND "));
    }
    sql.push_str(" ORDER BY updated_at DESC LIMIT ?");

    let mut query = sqlx::query(&sql);
    for value in &values {
        query = query.bind(value);
    }
    query = query.bind(limit.max(1));

    let rows = query.fetch_all(pool).await?;
    Ok(serde_json::json!({
        "status": "success",
        "count": rows.len(),
        "claims": rows.iter().map(|row| {
            serde_json::json!({
                "claim_id": row.get::<String, _>("claim_id"),
                "project_id": row.get::<Option<String>, _>("project_id"),
                "session_id": row.get::<Option<String>, _>("session_id"),
                "primary_concept_id": row.get::<String, _>("primary_concept_id"),
                "claim_text": row.get::<String, _>("claim_text"),
                "claim_type": row.get::<String, _>("claim_type"),
                "status": row.get::<String, _>("status"),
                "confidence": row.get::<Option<f64>, _>("confidence"),
                "evidence_count": row.get::<i64, _>("evidence_count"),
                "last_verified_at": row.get::<Option<String>, _>("last_verified_at"),
                "updated_at": row.get::<String, _>("updated_at")
            })
        }).collect::<Vec<_>>()
    }))
}

pub async fn list_evidence(
    pool: &SqlitePool,
    project_id: Option<&str>,
    claim_id: Option<&str>,
    session_id: Option<&str>,
    source_ref: Option<&str>,
    limit: i64,
) -> anyhow::Result<serde_json::Value> {
    let mut conditions = Vec::new();
    let mut values = Vec::new();

    if let Some(value) = project_id {
        conditions.push("project_id = ?");
        values.push(value.to_string());
    }
    if let Some(value) = claim_id {
        conditions.push("claim_id = ?");
        values.push(value.to_string());
    }
    if let Some(value) = session_id {
        conditions.push("session_id = ?");
        values.push(value.to_string());
    }
    if let Some(value) = source_ref {
        conditions.push("source_ref = ?");
        values.push(value.to_string());
    }

    let mut sql = String::from(
        "SELECT evidence_id, claim_id, project_id, session_id, source_kind, source_ref, snippet, locator, extraction_method, contradiction_group, confidence, updated_at FROM semantic_evidence",
    );
    if !conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conditions.join(" AND "));
    }
    sql.push_str(" ORDER BY updated_at DESC LIMIT ?");

    let mut query = sqlx::query(&sql);
    for value in &values {
        query = query.bind(value);
    }
    query = query.bind(limit.max(1));

    let rows = query.fetch_all(pool).await?;
    Ok(serde_json::json!({
        "status": "success",
        "count": rows.len(),
        "evidence": rows.iter().map(|row| {
            serde_json::json!({
                "evidence_id": row.get::<String, _>("evidence_id"),
                "claim_id": row.get::<String, _>("claim_id"),
                "project_id": row.get::<Option<String>, _>("project_id"),
                "session_id": row.get::<Option<String>, _>("session_id"),
                "source_kind": row.get::<String, _>("source_kind"),
                "source_ref": row.get::<Option<String>, _>("source_ref"),
                "snippet": row.get::<String, _>("snippet"),
                "locator": row.get::<Option<String>, _>("locator"),
                "extraction_method": row.get::<Option<String>, _>("extraction_method"),
                "contradiction_group": row.get::<Option<String>, _>("contradiction_group"),
                "confidence": row.get::<Option<f64>, _>("confidence"),
                "updated_at": row.get::<String, _>("updated_at")
            })
        }).collect::<Vec<_>>()
    }))
}

pub async fn list_relations(
    pool: &SqlitePool,
    concept_id: Option<&str>,
    relation_type: Option<&str>,
    limit: i64,
) -> anyhow::Result<serde_json::Value> {
    let mut conditions = Vec::new();
    let mut values = Vec::new();

    if let Some(value) = concept_id {
        conditions.push("(from_concept_id = ? OR to_concept_id = ?)");
        values.push(value.to_string());
        values.push(value.to_string());
    }
    if let Some(value) = relation_type {
        conditions.push("relation_type = ?");
        values.push(value.to_string());
    }

    let mut sql = String::from(
        "SELECT edge_id, from_concept_id, to_concept_id, relation_type, project_id, session_id, weight, confidence, updated_at FROM semantic_relations",
    );
    if !conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conditions.join(" AND "));
    }
    sql.push_str(" ORDER BY updated_at DESC LIMIT ?");

    let mut query = sqlx::query(&sql);
    for value in &values {
        query = query.bind(value);
    }
    query = query.bind(limit.max(1));

    let rows = query.fetch_all(pool).await?;
    Ok(serde_json::json!({
        "status": "success",
        "count": rows.len(),
        "relations": rows.iter().map(|row| {
            serde_json::json!({
                "edge_id": row.get::<String, _>("edge_id"),
                "from_concept_id": row.get::<String, _>("from_concept_id"),
                "to_concept_id": row.get::<String, _>("to_concept_id"),
                "relation_type": row.get::<String, _>("relation_type"),
                "project_id": row.get::<Option<String>, _>("project_id"),
                "session_id": row.get::<Option<String>, _>("session_id"),
                "weight": row.get::<Option<f64>, _>("weight"),
                "confidence": row.get::<Option<f64>, _>("confidence"),
                "updated_at": row.get::<String, _>("updated_at")
            })
        }).collect::<Vec<_>>()
    }))
}

pub async fn expand_concept(
    pool: &SqlitePool,
    concept_id: &str,
    limit: i64,
) -> anyhow::Result<serde_json::Value> {
    let Some(concept) = sqlx::query(
        r#"
        SELECT concept_id, canonical_name, summary, domain, aliases_json, status, reuse_count,
               importance_score, freshness_score, confidence, tags_json, metadata_json,
               last_consolidated_at, created_at, updated_at
        FROM semantic_concepts
        WHERE concept_id = ?
        "#,
    )
    .bind(concept_id)
    .fetch_optional(pool)
    .await?
    else {
        return Ok(serde_json::json!({ "error": "concept not found" }));
    };

    let claims = sqlx::query(
        r#"
        SELECT claim_id, project_id, session_id, claim_text, claim_type, status, confidence,
               evidence_count, provenance_refs, tags_json, metadata_json, last_verified_at,
               created_at, updated_at
        FROM semantic_claims
        WHERE primary_concept_id = ?
        ORDER BY updated_at DESC
        LIMIT ?
        "#,
    )
    .bind(concept_id)
    .bind(limit.max(1))
    .fetch_all(pool)
    .await?;

    let relations = sqlx::query(
        r#"
        SELECT edge_id, from_concept_id, to_concept_id, relation_type, project_id, session_id,
               weight, confidence, provenance_refs, tags_json, metadata_json, created_at, updated_at
        FROM semantic_relations
        WHERE from_concept_id = ? OR to_concept_id = ?
        ORDER BY updated_at DESC
        LIMIT ?
        "#,
    )
    .bind(concept_id)
    .bind(concept_id)
    .bind(limit.max(1))
    .fetch_all(pool)
    .await?;

    Ok(serde_json::json!({
        "status": "success",
        "concept": {
            "concept_id": concept.get::<String, _>("concept_id"),
            "canonical_name": concept.get::<String, _>("canonical_name"),
            "summary": concept.get::<Option<String>, _>("summary"),
            "domain": concept.get::<Option<String>, _>("domain"),
            "aliases_json": parse_json(concept.get::<Option<String>, _>("aliases_json")),
            "status": concept.get::<String, _>("status"),
            "reuse_count": concept.get::<i64, _>("reuse_count"),
            "importance_score": concept.get::<Option<f64>, _>("importance_score"),
            "freshness_score": concept.get::<Option<f64>, _>("freshness_score"),
            "confidence": concept.get::<Option<f64>, _>("confidence"),
            "tags_json": parse_json(concept.get::<Option<String>, _>("tags_json")),
            "metadata_json": parse_json(concept.get::<Option<String>, _>("metadata_json")),
            "last_consolidated_at": concept.get::<Option<String>, _>("last_consolidated_at"),
            "created_at": concept.get::<String, _>("created_at"),
            "updated_at": concept.get::<String, _>("updated_at")
        },
        "claims": claims.iter().map(|row| {
            serde_json::json!({
                "claim_id": row.get::<String, _>("claim_id"),
                "project_id": row.get::<Option<String>, _>("project_id"),
                "session_id": row.get::<Option<String>, _>("session_id"),
                "claim_text": row.get::<String, _>("claim_text"),
                "claim_type": row.get::<String, _>("claim_type"),
                "status": row.get::<String, _>("status"),
                "confidence": row.get::<Option<f64>, _>("confidence"),
                "evidence_count": row.get::<i64, _>("evidence_count"),
                "provenance_refs": parse_json(row.get::<Option<String>, _>("provenance_refs")),
                "tags_json": parse_json(row.get::<Option<String>, _>("tags_json")),
                "metadata_json": parse_json(row.get::<Option<String>, _>("metadata_json")),
                "last_verified_at": row.get::<Option<String>, _>("last_verified_at"),
                "created_at": row.get::<String, _>("created_at"),
                "updated_at": row.get::<String, _>("updated_at")
            })
        }).collect::<Vec<_>>(),
        "relations": relations.iter().map(|row| {
            serde_json::json!({
                "edge_id": row.get::<String, _>("edge_id"),
                "from_concept_id": row.get::<String, _>("from_concept_id"),
                "to_concept_id": row.get::<String, _>("to_concept_id"),
                "relation_type": row.get::<String, _>("relation_type"),
                "project_id": row.get::<Option<String>, _>("project_id"),
                "session_id": row.get::<Option<String>, _>("session_id"),
                "weight": row.get::<Option<f64>, _>("weight"),
                "confidence": row.get::<Option<f64>, _>("confidence"),
                "provenance_refs": parse_json(row.get::<Option<String>, _>("provenance_refs")),
                "tags_json": parse_json(row.get::<Option<String>, _>("tags_json")),
                "metadata_json": parse_json(row.get::<Option<String>, _>("metadata_json")),
                "created_at": row.get::<String, _>("created_at"),
                "updated_at": row.get::<String, _>("updated_at")
            })
        }).collect::<Vec<_>>()
    }))
}

pub async fn trace_claim_provenance(
    pool: &SqlitePool,
    claim_id: &str,
) -> anyhow::Result<serde_json::Value> {
    let Some(claim) = sqlx::query(
        r#"
        SELECT claim_id, project_id, session_id, primary_concept_id, claim_text, claim_type, status,
               confidence, evidence_count, provenance_refs, tags_json, metadata_json, last_verified_at,
               created_at, updated_at
        FROM semantic_claims
        WHERE claim_id = ?
        "#,
    )
    .bind(claim_id)
    .fetch_optional(pool)
    .await? else {
        return Ok(serde_json::json!({ "error": "claim not found" }));
    };

    let evidence_rows = sqlx::query(
        r#"
        SELECT evidence_id, project_id, session_id, source_kind, source_ref, snippet, locator,
               extraction_method, contradiction_group, confidence, provenance_refs, tags_json,
               metadata_json, created_at, updated_at
        FROM semantic_evidence
        WHERE claim_id = ?
        ORDER BY created_at ASC
        "#,
    )
    .bind(claim_id)
    .fetch_all(pool)
    .await?;

    let project = match claim.get::<Option<String>, _>("project_id") {
        Some(project_id) => sqlx::query(
            "SELECT project_id, title, goal, status, updated_at FROM research_projects WHERE project_id = ?",
        )
        .bind(project_id)
        .fetch_optional(pool)
        .await?,
        None => None,
    };

    let session = match claim.get::<Option<String>, _>("session_id") {
        Some(session_id) => sqlx::query(
            "SELECT session_id, project_id, title, brief, status, updated_at FROM research_sessions WHERE session_id = ?",
        )
        .bind(session_id)
        .fetch_optional(pool)
        .await?,
        None => None,
    };

    let source_refs: Vec<String> = evidence_rows
        .iter()
        .filter_map(|row| row.get::<Option<String>, _>("source_ref"))
        .collect();
    let mut sources = Vec::new();
    for source_ref in source_refs {
        if let Some(row) = sqlx::query(
            r#"
            SELECT source_id, project_id, session_id, source_kind, source_uri, source_label, title,
                   summary, content_type, status, confidence, tags_json, metadata_json, created_at, updated_at
            FROM research_sources
            WHERE source_id = ? OR source_uri = ?
            ORDER BY updated_at DESC
            LIMIT 1
            "#,
        )
        .bind(&source_ref)
        .bind(&source_ref)
        .fetch_optional(pool)
        .await?
        {
            sources.push(serde_json::json!({
                "source_id": row.get::<String, _>("source_id"),
                "project_id": row.get::<String, _>("project_id"),
                "session_id": row.get::<Option<String>, _>("session_id"),
                "source_kind": row.get::<String, _>("source_kind"),
                "source_uri": row.get::<Option<String>, _>("source_uri"),
                "source_label": row.get::<Option<String>, _>("source_label"),
                "title": row.get::<Option<String>, _>("title"),
                "summary": row.get::<Option<String>, _>("summary"),
                "content_type": row.get::<Option<String>, _>("content_type"),
                "status": row.get::<String, _>("status"),
                "confidence": row.get::<Option<f64>, _>("confidence"),
                "tags_json": parse_json(row.get::<Option<String>, _>("tags_json")),
                "metadata_json": parse_json(row.get::<Option<String>, _>("metadata_json")),
                "created_at": row.get::<String, _>("created_at"),
                "updated_at": row.get::<String, _>("updated_at")
            }));
        }
    }

    Ok(serde_json::json!({
        "status": "success",
        "claim": {
            "claim_id": claim.get::<String, _>("claim_id"),
            "project_id": claim.get::<Option<String>, _>("project_id"),
            "session_id": claim.get::<Option<String>, _>("session_id"),
            "primary_concept_id": claim.get::<String, _>("primary_concept_id"),
            "claim_text": claim.get::<String, _>("claim_text"),
            "claim_type": claim.get::<String, _>("claim_type"),
            "status": claim.get::<String, _>("status"),
            "confidence": claim.get::<Option<f64>, _>("confidence"),
            "evidence_count": claim.get::<i64, _>("evidence_count"),
            "provenance_refs": parse_json(claim.get::<Option<String>, _>("provenance_refs")),
            "tags_json": parse_json(claim.get::<Option<String>, _>("tags_json")),
            "metadata_json": parse_json(claim.get::<Option<String>, _>("metadata_json")),
            "last_verified_at": claim.get::<Option<String>, _>("last_verified_at"),
            "created_at": claim.get::<String, _>("created_at"),
            "updated_at": claim.get::<String, _>("updated_at")
        },
        "project": project.map(|row| serde_json::json!({
            "project_id": row.get::<String, _>("project_id"),
            "title": row.get::<String, _>("title"),
            "goal": row.get::<String, _>("goal"),
            "status": row.get::<String, _>("status"),
            "updated_at": row.get::<String, _>("updated_at")
        })),
        "session": session.map(|row| serde_json::json!({
            "session_id": row.get::<String, _>("session_id"),
            "project_id": row.get::<String, _>("project_id"),
            "title": row.get::<Option<String>, _>("title"),
            "brief": row.get::<Option<String>, _>("brief"),
            "status": row.get::<String, _>("status"),
            "updated_at": row.get::<String, _>("updated_at")
        })),
        "evidence": evidence_rows.iter().map(|row| {
            serde_json::json!({
                "evidence_id": row.get::<String, _>("evidence_id"),
                "project_id": row.get::<Option<String>, _>("project_id"),
                "session_id": row.get::<Option<String>, _>("session_id"),
                "source_kind": row.get::<String, _>("source_kind"),
                "source_ref": row.get::<Option<String>, _>("source_ref"),
                "snippet": row.get::<String, _>("snippet"),
                "locator": row.get::<Option<String>, _>("locator"),
                "extraction_method": row.get::<Option<String>, _>("extraction_method"),
                "contradiction_group": row.get::<Option<String>, _>("contradiction_group"),
                "confidence": row.get::<Option<f64>, _>("confidence"),
                "provenance_refs": parse_json(row.get::<Option<String>, _>("provenance_refs")),
                "tags_json": parse_json(row.get::<Option<String>, _>("tags_json")),
                "metadata_json": parse_json(row.get::<Option<String>, _>("metadata_json")),
                "created_at": row.get::<String, _>("created_at"),
                "updated_at": row.get::<String, _>("updated_at")
            })
        }).collect::<Vec<_>>(),
        "sources": sources
    }))
}

pub async fn retention_candidates(
    pool: &SqlitePool,
    limit: i64,
) -> anyhow::Result<serde_json::Value> {
    let limit = limit.max(1);
    let projects = sqlx::query(
        r#"
        SELECT project_id, title, status, importance_score, freshness_score, confidence, updated_at,
               CASE
                   WHEN status IN ('archive_candidate', 'delete_candidate', 'stale') THEN status
                   WHEN (importance_score IS NULL OR importance_score < 0.40)
                        AND (freshness_score IS NULL OR freshness_score < 0.45) THEN 'low importance and low freshness'
                   WHEN julianday('now') - julianday(updated_at) > 45 THEN 'inactive for more than 45 days'
                   ELSE 'review'
               END AS candidate_reason
        FROM research_projects
        WHERE status != 'deleted'
          AND (
            status IN ('archive_candidate', 'delete_candidate', 'stale')
            OR ((importance_score IS NULL OR importance_score < 0.40)
                AND (freshness_score IS NULL OR freshness_score < 0.45))
            OR julianday('now') - julianday(updated_at) > 45
          )
        ORDER BY updated_at ASC
        LIMIT ?
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;

    let concepts = sqlx::query(
        r#"
        SELECT concept_id, canonical_name, status, reuse_count, importance_score, freshness_score, confidence, updated_at,
               CASE
                   WHEN status IN ('archive_candidate', 'delete_candidate', 'stale') THEN status
                   WHEN reuse_count = 0 AND (importance_score IS NULL OR importance_score < 0.35) THEN 'unused low-importance concept'
                   WHEN julianday('now') - julianday(updated_at) > 60 THEN 'unrefreshed concept older than 60 days'
                   ELSE 'review'
               END AS candidate_reason
        FROM semantic_concepts
        WHERE status != 'deleted'
          AND (
            status IN ('archive_candidate', 'delete_candidate', 'stale')
            OR (reuse_count = 0 AND (importance_score IS NULL OR importance_score < 0.35))
            OR julianday('now') - julianday(updated_at) > 60
          )
        ORDER BY updated_at ASC
        LIMIT ?
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;

    let claims = sqlx::query(
        r#"
        SELECT claim_id, project_id, primary_concept_id, claim_text, status, confidence, evidence_count, updated_at,
               CASE
                   WHEN status IN ('archive_candidate', 'delete_candidate', 'stale') THEN status
                   WHEN evidence_count = 0 THEN 'claim has no supporting evidence'
                   WHEN confidence IS NOT NULL AND confidence < 0.35 THEN 'low-confidence claim'
                   ELSE 'review'
               END AS candidate_reason
        FROM semantic_claims
        WHERE status != 'deleted'
          AND (
            status IN ('archive_candidate', 'delete_candidate', 'stale')
            OR evidence_count = 0
            OR (confidence IS NOT NULL AND confidence < 0.35)
          )
        ORDER BY updated_at ASC
        LIMIT ?
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;

    Ok(serde_json::json!({
        "status": "success",
        "projects": projects.iter().map(|row| {
            serde_json::json!({
                "project_id": row.get::<String, _>("project_id"),
                "title": row.get::<String, _>("title"),
                "status": row.get::<String, _>("status"),
                "importance_score": row.get::<Option<f64>, _>("importance_score"),
                "freshness_score": row.get::<Option<f64>, _>("freshness_score"),
                "confidence": row.get::<Option<f64>, _>("confidence"),
                "updated_at": row.get::<String, _>("updated_at"),
                "candidate_reason": row.get::<String, _>("candidate_reason")
            })
        }).collect::<Vec<_>>(),
        "concepts": concepts.iter().map(|row| {
            serde_json::json!({
                "concept_id": row.get::<String, _>("concept_id"),
                "canonical_name": row.get::<String, _>("canonical_name"),
                "status": row.get::<String, _>("status"),
                "reuse_count": row.get::<i64, _>("reuse_count"),
                "importance_score": row.get::<Option<f64>, _>("importance_score"),
                "freshness_score": row.get::<Option<f64>, _>("freshness_score"),
                "confidence": row.get::<Option<f64>, _>("confidence"),
                "updated_at": row.get::<String, _>("updated_at"),
                "candidate_reason": row.get::<String, _>("candidate_reason")
            })
        }).collect::<Vec<_>>(),
        "claims": claims.iter().map(|row| {
            serde_json::json!({
                "claim_id": row.get::<String, _>("claim_id"),
                "project_id": row.get::<Option<String>, _>("project_id"),
                "primary_concept_id": row.get::<String, _>("primary_concept_id"),
                "claim_text": row.get::<String, _>("claim_text"),
                "status": row.get::<String, _>("status"),
                "confidence": row.get::<Option<f64>, _>("confidence"),
                "evidence_count": row.get::<i64, _>("evidence_count"),
                "updated_at": row.get::<String, _>("updated_at"),
                "candidate_reason": row.get::<String, _>("candidate_reason")
            })
        }).collect::<Vec<_>>()
    }))
}

pub async fn counts(pool: &SqlitePool) -> anyhow::Result<SemanticCounts> {
    async fn single_count(pool: &SqlitePool, table: &str) -> anyhow::Result<i64> {
        let sql = format!("SELECT COUNT(*) AS count FROM {table}");
        let row = sqlx::query(&sql).fetch_one(pool).await?;
        Ok(row.get::<i64, _>("count"))
    }

    Ok(SemanticCounts {
        project_count: single_count(pool, "research_projects").await?,
        session_count: single_count(pool, "research_sessions").await?,
        source_count: single_count(pool, "research_sources").await?,
        concept_count: single_count(pool, "semantic_concepts").await?,
        claim_count: single_count(pool, "semantic_claims").await?,
        evidence_count: single_count(pool, "semantic_evidence").await?,
        relation_count: single_count(pool, "semantic_relations").await?,
    })
}
