use serde::{Deserialize, Serialize};
use sqlx::{Row, SqlitePool};
use std::collections::HashSet;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentIndexRecord {
    pub document_id: String,
    pub tenant_id: String,
    pub app_id: String,
    pub owner_scope: String,
    pub title: String,
    pub summary: Option<String>,
    pub index_type: String,
    pub source_type: String,
    pub source_uri: Option<String>,
    pub metadata_json: Option<serde_json::Value>,
    pub root_node_id: Option<String>,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentIndexNode {
    pub node_id: String,
    pub parent_node_id: Option<String>,
    pub title: String,
    pub summary: Option<String>,
    pub level: i64,
    pub node_type: String,
    pub source_ref: Option<String>,
    pub start_offset: Option<i64>,
    pub end_offset: Option<i64>,
    pub page_from: Option<i64>,
    pub page_to: Option<i64>,
    pub tags: Option<Vec<String>>,
    pub metadata_json: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentIndexUpsert {
    #[serde(flatten)]
    pub record: DocumentIndexRecord,
    pub nodes: Vec<DocumentIndexNode>,
}

fn tokenize(text: &str) -> HashSet<String> {
    text.to_lowercase()
        .split(|c: char| !c.is_alphanumeric())
        .filter(|token| token.len() >= 3)
        .map(|token| token.to_string())
        .collect()
}

fn parse_json_value(raw: Option<String>) -> Option<serde_json::Value> {
    raw.and_then(|value| serde_json::from_str(&value).ok())
}

fn score_node(node: &DocumentIndexNode, query_tokens: &HashSet<String>) -> (f64, usize) {
    let mut combined = node.title.to_lowercase();
    if let Some(summary) = &node.summary {
        combined.push(' ');
        combined.push_str(&summary.to_lowercase());
    }
    if let Some(tags) = &node.tags {
        combined.push(' ');
        combined.push_str(&tags.join(" ").to_lowercase());
    }

    let node_tokens = tokenize(&combined);
    let overlap = query_tokens.intersection(&node_tokens).count();
    let overlap_score = if query_tokens.is_empty() {
        0.0
    } else {
        overlap as f64 / query_tokens.len() as f64
    };
    let level_bonus = 1.0 / (node.level.max(0) + 1) as f64;
    let title_bonus = if node
        .title
        .to_lowercase()
        .split_whitespace()
        .any(|token| query_tokens.contains(token))
    {
        0.2
    } else {
        0.0
    };

    (
        (overlap_score * 0.7) + (level_bonus * 0.1) + title_bonus,
        overlap,
    )
}

pub async fn init_tables(pool: &SqlitePool) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS document_indexes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            document_id TEXT NOT NULL UNIQUE,
            tenant_id TEXT NOT NULL DEFAULT 'default',
            app_id TEXT NOT NULL DEFAULT 'os',
            owner_scope TEXT NOT NULL DEFAULT 'shared',
            title TEXT NOT NULL,
            summary TEXT,
            index_type TEXT NOT NULL DEFAULT 'page_tree',
            source_type TEXT NOT NULL DEFAULT 'document',
            source_uri TEXT,
            metadata_json TEXT,
            root_node_id TEXT,
            status TEXT NOT NULL DEFAULT 'active',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS document_index_nodes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            document_id TEXT NOT NULL,
            node_id TEXT NOT NULL,
            parent_node_id TEXT,
            title TEXT NOT NULL,
            summary TEXT,
            level INTEGER NOT NULL DEFAULT 0,
            node_type TEXT NOT NULL DEFAULT 'section',
            source_ref TEXT,
            start_offset INTEGER,
            end_offset INTEGER,
            page_from INTEGER,
            page_to INTEGER,
            tags_json TEXT,
            metadata_json TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(document_id, node_id)
        )
        "#,
    )
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn upsert(
    pool: &SqlitePool,
    payload: DocumentIndexUpsert,
) -> anyhow::Result<serde_json::Value> {
    if payload.record.document_id.trim().is_empty() {
        anyhow::bail!("document_id is required");
    }
    if payload.record.title.trim().is_empty() {
        anyhow::bail!("title is required");
    }
    if payload.nodes.is_empty() {
        anyhow::bail!("nodes must not be empty");
    }

    let mut tx = pool.begin().await?;

    sqlx::query(
        r#"
        INSERT INTO document_indexes (
            document_id, tenant_id, app_id, owner_scope, title, summary, index_type,
            source_type, source_uri, metadata_json, root_node_id, status, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(document_id) DO UPDATE SET
            tenant_id = excluded.tenant_id,
            app_id = excluded.app_id,
            owner_scope = excluded.owner_scope,
            title = excluded.title,
            summary = excluded.summary,
            index_type = excluded.index_type,
            source_type = excluded.source_type,
            source_uri = excluded.source_uri,
            metadata_json = excluded.metadata_json,
            root_node_id = excluded.root_node_id,
            status = excluded.status,
            updated_at = CURRENT_TIMESTAMP
        "#,
    )
    .bind(&payload.record.document_id)
    .bind(&payload.record.tenant_id)
    .bind(&payload.record.app_id)
    .bind(&payload.record.owner_scope)
    .bind(&payload.record.title)
    .bind(&payload.record.summary)
    .bind(&payload.record.index_type)
    .bind(&payload.record.source_type)
    .bind(&payload.record.source_uri)
    .bind(
        payload
            .record
            .metadata_json
            .as_ref()
            .map(|value| value.to_string()),
    )
    .bind(&payload.record.root_node_id)
    .bind(&payload.record.status)
    .execute(&mut *tx)
    .await?;

    sqlx::query("DELETE FROM document_index_nodes WHERE document_id = ?")
        .bind(&payload.record.document_id)
        .execute(&mut *tx)
        .await?;

    for node in &payload.nodes {
        sqlx::query(
            r#"
            INSERT INTO document_index_nodes (
                document_id, node_id, parent_node_id, title, summary, level, node_type,
                source_ref, start_offset, end_offset, page_from, page_to, tags_json, metadata_json,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            "#,
        )
        .bind(&payload.record.document_id)
        .bind(&node.node_id)
        .bind(&node.parent_node_id)
        .bind(&node.title)
        .bind(&node.summary)
        .bind(node.level)
        .bind(&node.node_type)
        .bind(&node.source_ref)
        .bind(node.start_offset)
        .bind(node.end_offset)
        .bind(node.page_from)
        .bind(node.page_to)
        .bind(
            node.tags
                .as_ref()
                .map(|value| serde_json::to_string(value).unwrap_or_default()),
        )
        .bind(node.metadata_json.as_ref().map(|value| value.to_string()))
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;

    Ok(serde_json::json!({
        "status": "success",
        "document_id": payload.record.document_id,
        "index_type": payload.record.index_type,
        "nodes_written": payload.nodes.len()
    }))
}

pub async fn get(
    pool: &SqlitePool,
    document_id: &str,
    app_id: Option<&str>,
) -> anyhow::Result<serde_json::Value> {
    let mut sql = String::from(
        "SELECT document_id, tenant_id, app_id, owner_scope, title, summary, index_type, source_type, source_uri, metadata_json, root_node_id, status FROM document_indexes WHERE document_id = ?",
    );
    if app_id.is_some() {
        sql.push_str(" AND app_id = ?");
    }

    let mut query = sqlx::query(&sql).bind(document_id);
    if let Some(value) = app_id {
        query = query.bind(value);
    }

    let Some(row) = query.fetch_optional(pool).await? else {
        return Ok(serde_json::json!({ "error": "document index not found" }));
    };

    let nodes = sqlx::query(
        r#"
        SELECT node_id, parent_node_id, title, summary, level, node_type, source_ref, start_offset, end_offset, page_from, page_to, tags_json, metadata_json
        FROM document_index_nodes
        WHERE document_id = ?
        ORDER BY level ASC, node_id ASC
        "#,
    )
    .bind(document_id)
    .fetch_all(pool)
    .await?;

    let node_values: Vec<serde_json::Value> = nodes
        .into_iter()
        .map(|node| {
            serde_json::json!({
                "node_id": node.get::<String, _>("node_id"),
                "parent_node_id": node.get::<Option<String>, _>("parent_node_id"),
                "title": node.get::<String, _>("title"),
                "summary": node.get::<Option<String>, _>("summary"),
                "level": node.get::<i64, _>("level"),
                "node_type": node.get::<String, _>("node_type"),
                "source_ref": node.get::<Option<String>, _>("source_ref"),
                "start_offset": node.get::<Option<i64>, _>("start_offset"),
                "end_offset": node.get::<Option<i64>, _>("end_offset"),
                "page_from": node.get::<Option<i64>, _>("page_from"),
                "page_to": node.get::<Option<i64>, _>("page_to"),
                "tags": parse_json_value(node.get::<Option<String>, _>("tags_json")),
                "metadata_json": parse_json_value(node.get::<Option<String>, _>("metadata_json"))
            })
        })
        .collect();

    Ok(serde_json::json!({
        "status": "success",
        "document": {
            "document_id": row.get::<String, _>("document_id"),
            "tenant_id": row.get::<String, _>("tenant_id"),
            "app_id": row.get::<String, _>("app_id"),
            "owner_scope": row.get::<String, _>("owner_scope"),
            "title": row.get::<String, _>("title"),
            "summary": row.get::<Option<String>, _>("summary"),
            "index_type": row.get::<String, _>("index_type"),
            "source_type": row.get::<String, _>("source_type"),
            "source_uri": row.get::<Option<String>, _>("source_uri"),
            "metadata_json": parse_json_value(row.get::<Option<String>, _>("metadata_json")),
            "root_node_id": row.get::<Option<String>, _>("root_node_id"),
            "status": row.get::<String, _>("status")
        },
        "nodes": node_values
    }))
}

pub async fn list(
    pool: &SqlitePool,
    app_id: Option<&str>,
    tenant_id: Option<&str>,
    index_type: Option<&str>,
    limit: i64,
) -> anyhow::Result<serde_json::Value> {
    let mut sql = String::from(
        "SELECT document_id, tenant_id, app_id, owner_scope, title, summary, index_type, source_type, source_uri, root_node_id, status, updated_at FROM document_indexes",
    );
    let mut conditions = Vec::new();
    if app_id.is_some() {
        conditions.push("app_id = ?");
    }
    if tenant_id.is_some() {
        conditions.push("tenant_id = ?");
    }
    if index_type.is_some() {
        conditions.push("index_type = ?");
    }
    if !conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conditions.join(" AND "));
    }
    sql.push_str(" ORDER BY updated_at DESC LIMIT ?");

    let mut query = sqlx::query(&sql);
    if let Some(value) = app_id {
        query = query.bind(value);
    }
    if let Some(value) = tenant_id {
        query = query.bind(value);
    }
    if let Some(value) = index_type {
        query = query.bind(value);
    }
    query = query.bind(limit.max(1).min(100));

    let rows = query.fetch_all(pool).await?;
    let documents: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|row| {
            serde_json::json!({
                "document_id": row.get::<String, _>("document_id"),
                "tenant_id": row.get::<String, _>("tenant_id"),
                "app_id": row.get::<String, _>("app_id"),
                "owner_scope": row.get::<String, _>("owner_scope"),
                "title": row.get::<String, _>("title"),
                "summary": row.get::<Option<String>, _>("summary"),
                "index_type": row.get::<String, _>("index_type"),
                "source_type": row.get::<String, _>("source_type"),
                "source_uri": row.get::<Option<String>, _>("source_uri"),
                "root_node_id": row.get::<Option<String>, _>("root_node_id"),
                "status": row.get::<String, _>("status"),
                "updated_at": row.get::<String, _>("updated_at")
            })
        })
        .collect();

    Ok(serde_json::json!({
        "status": "success",
        "documents": documents
    }))
}

pub async fn query(
    pool: &SqlitePool,
    query_text: &str,
    app_id: Option<&str>,
    tenant_id: Option<&str>,
    document_id: Option<&str>,
    limit: i64,
) -> anyhow::Result<serde_json::Value> {
    let query_tokens = tokenize(query_text);
    if query_tokens.is_empty() {
        return Ok(
            serde_json::json!({ "error": "query must contain at least one alphanumeric token with length >= 3" }),
        );
    }
    if app_id.is_none() && tenant_id.is_none() && document_id.is_none() {
        return Ok(
            serde_json::json!({ "error": "At least one filter (document_id, app_id, tenant_id) is required. Global document reads are forbidden." }),
        );
    }

    let mut sql = String::from(
        r#"
        SELECT
            idx.document_id, idx.app_id, idx.tenant_id, idx.title AS document_title, idx.summary AS document_summary,
            node.node_id, node.parent_node_id, node.title, node.summary, node.level, node.node_type,
            node.source_ref, node.page_from, node.page_to, node.tags_json
        FROM document_index_nodes node
        JOIN document_indexes idx ON idx.document_id = node.document_id
        WHERE idx.index_type = 'page_tree' AND idx.status = 'active'
        "#,
    );
    if document_id.is_some() {
        sql.push_str(" AND idx.document_id = ?");
    }
    if app_id.is_some() {
        sql.push_str(" AND idx.app_id = ?");
    }
    if tenant_id.is_some() {
        sql.push_str(" AND idx.tenant_id = ?");
    }

    let mut query = sqlx::query(&sql);
    if let Some(value) = document_id {
        query = query.bind(value);
    }
    if let Some(value) = app_id {
        query = query.bind(value);
    }
    if let Some(value) = tenant_id {
        query = query.bind(value);
    }

    let rows = query.fetch_all(pool).await?;
    let mut results = Vec::new();
    for row in rows {
        let tags = parse_json_value(row.get::<Option<String>, _>("tags_json"))
            .and_then(|value| serde_json::from_value::<Vec<String>>(value).ok());
        let node = DocumentIndexNode {
            node_id: row.get::<String, _>("node_id"),
            parent_node_id: row.get::<Option<String>, _>("parent_node_id"),
            title: row.get::<String, _>("title"),
            summary: row.get::<Option<String>, _>("summary"),
            level: row.get::<i64, _>("level"),
            node_type: row.get::<String, _>("node_type"),
            source_ref: row.get::<Option<String>, _>("source_ref"),
            start_offset: None,
            end_offset: None,
            page_from: row.get::<Option<i64>, _>("page_from"),
            page_to: row.get::<Option<i64>, _>("page_to"),
            tags,
            metadata_json: None,
        };
        let (score, overlap_hits) = score_node(&node, &query_tokens);
        if overlap_hits == 0 {
            continue;
        }
        results.push(serde_json::json!({
            "document_id": row.get::<String, _>("document_id"),
            "app_id": row.get::<String, _>("app_id"),
            "tenant_id": row.get::<String, _>("tenant_id"),
            "document_title": row.get::<String, _>("document_title"),
            "document_summary": row.get::<Option<String>, _>("document_summary"),
            "node_id": node.node_id,
            "parent_node_id": node.parent_node_id,
            "title": node.title,
            "summary": node.summary,
            "level": node.level,
            "node_type": node.node_type,
            "source_ref": node.source_ref,
            "page_from": node.page_from,
            "page_to": node.page_to,
            "tags": node.tags,
            "score": score,
            "overlap_hits": overlap_hits
        }));
    }

    results.sort_by(|a, b| {
        let left = a
            .get("score")
            .and_then(|value| value.as_f64())
            .unwrap_or(0.0);
        let right = b
            .get("score")
            .and_then(|value| value.as_f64())
            .unwrap_or(0.0);
        right
            .partial_cmp(&left)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    results.truncate(limit.max(1).min(25) as usize);

    Ok(serde_json::json!({
        "status": "success",
        "retrieval_strategy": "page_tree",
        "results": results
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::sqlite::SqlitePoolOptions;

    #[tokio::test]
    async fn page_tree_index_roundtrip_works() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();

        init_tables(&pool).await.unwrap();

        let payload = DocumentIndexUpsert {
            record: DocumentIndexRecord {
                document_id: "policy-1".to_string(),
                tenant_id: "tenant-main".to_string(),
                app_id: "vetra".to_string(),
                owner_scope: "workspace".to_string(),
                title: "Remote Work Policy".to_string(),
                summary: Some("Rules for remote work requests and approvals.".to_string()),
                index_type: "page_tree".to_string(),
                source_type: "policy".to_string(),
                source_uri: Some("/docs/remote-work.pdf".to_string()),
                metadata_json: None,
                root_node_id: Some("root".to_string()),
                status: "active".to_string(),
            },
            nodes: vec![
                DocumentIndexNode {
                    node_id: "root".to_string(),
                    parent_node_id: None,
                    title: "Remote Work Policy".to_string(),
                    summary: Some("Root node".to_string()),
                    level: 0,
                    node_type: "document".to_string(),
                    source_ref: Some("page:1".to_string()),
                    start_offset: None,
                    end_offset: None,
                    page_from: Some(1),
                    page_to: Some(10),
                    tags: Some(vec!["policy".to_string(), "remote-work".to_string()]),
                    metadata_json: None,
                },
                DocumentIndexNode {
                    node_id: "approval".to_string(),
                    parent_node_id: Some("root".to_string()),
                    title: "Approval Workflow".to_string(),
                    summary: Some(
                        "Manager approval is required before remote work begins.".to_string(),
                    ),
                    level: 1,
                    node_type: "section".to_string(),
                    source_ref: Some("page:4".to_string()),
                    start_offset: None,
                    end_offset: None,
                    page_from: Some(4),
                    page_to: Some(5),
                    tags: Some(vec!["approval".to_string()]),
                    metadata_json: None,
                },
            ],
        };

        let upserted = upsert(&pool, payload).await.unwrap();
        assert_eq!(upserted["status"], "success");
        assert_eq!(upserted["nodes_written"], 2);

        let fetched = get(&pool, "policy-1", Some("vetra")).await.unwrap();
        assert_eq!(fetched["status"], "success");
        assert_eq!(fetched["document"]["title"], "Remote Work Policy");
        assert_eq!(fetched["nodes"].as_array().unwrap().len(), 2);

        let queried = query(
            &pool,
            "remote work approval manager",
            Some("vetra"),
            Some("tenant-main"),
            None,
            5,
        )
        .await
        .unwrap();

        assert_eq!(queried["status"], "success");
        assert_eq!(queried["retrieval_strategy"], "page_tree");
        let results = queried["results"].as_array().unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0]["node_id"], "approval");
    }
}
