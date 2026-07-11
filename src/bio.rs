#![allow(dead_code)]

use serde_json::Value;
use sqlx::Row;

enum BioSection {
    Experience,
    Education,
    Skills,
}

fn parse_bio_section(section: &str) -> Option<BioSection> {
    if section.starts_with("experience") {
        Some(BioSection::Experience)
    } else if section.starts_with("education") {
        Some(BioSection::Education)
    } else if section.starts_with("skills") {
        Some(BioSection::Skills)
    } else {
        None
    }
}

pub async fn query_bio(pool: &sqlx::PgPool, payload: Value) -> Value {
    let section = payload
        .get("section")
        .and_then(|v| v.as_str())
        .unwrap_or("experience");

    match parse_bio_section(section) {
        Some(BioSection::Experience) => {
            let query = format!(
                "SELECT slug, title, company, duration, tag, summary, sort_order FROM paulo_bio_{} ORDER BY sort_order",
                section
            );
            match sqlx::query(&query).fetch_all(pool).await {
                Ok(rows) => {
                    let items: Vec<Value> = rows
                        .iter()
                        .map(|row| {
                            serde_json::json!({
                                "id": row.get::<String, _>("slug"),
                                "title": row.get::<String, _>("title"),
                                "company": row.get::<String, _>("company"),
                                "duration": row.get::<String, _>("duration"),
                                "tag": row.get::<String, _>("tag"),
                                "summary": row.get::<String, _>("summary"),
                            })
                        })
                        .collect();
                    serde_json::json!({ "status": "success", "section": section, "count": items.len(), "items": items })
                }
                Err(e) => serde_json::json!({ "error": format!("DB error: {}", e) }),
            }
        }
        Some(BioSection::Education) => {
            let query = format!(
                "SELECT slug, degree, institution, duration, tag, summary, sort_order FROM paulo_bio_{} ORDER BY sort_order",
                section
            );
            match sqlx::query(&query).fetch_all(pool).await {
                Ok(rows) => {
                    let items: Vec<Value> = rows
                        .iter()
                        .map(|row| {
                            serde_json::json!({
                                "id": row.get::<String, _>("slug"),
                                "title": row.get::<String, _>("degree"),
                                "company": row.get::<String, _>("institution"),
                                "duration": row.get::<String, _>("duration"),
                                "tag": row.get::<String, _>("tag"),
                                "summary": row.get::<Option<String>, _>("summary").unwrap_or_default(),
                            })
                        })
                        .collect();
                    serde_json::json!({ "status": "success", "section": section, "count": items.len(), "items": items })
                }
                Err(e) => serde_json::json!({ "error": format!("DB error: {}", e) }),
            }
        }
        Some(BioSection::Skills) => {
            let query = format!(
                "SELECT category, name, level FROM paulo_bio_{} ORDER BY category, name",
                section
            );
            match sqlx::query(&query).fetch_all(pool).await {
                Ok(rows) => {
                    let items: Vec<Value> = rows
                        .iter()
                        .map(|row| {
                            serde_json::json!({
                                "category": row.get::<String, _>("category"),
                                "name": row.get::<String, _>("name"),
                                "level": row.get::<String, _>("level"),
                            })
                        })
                        .collect();
                    serde_json::json!({ "status": "success", "section": section, "count": items.len(), "items": items })
                }
                Err(e) => serde_json::json!({ "error": format!("DB error: {}", e) }),
            }
        }
        None => serde_json::json!({ "error": format!("Unknown section: {}", section) }),
    }
}

pub async fn seed_bio(pool: &sqlx::PgPool, payload: Value) -> Value {
    let section = payload
        .get("section")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let items = payload.get("items").and_then(|v| v.as_array());

    if section.is_empty() || items.is_none() {
        return serde_json::json!({ "error": "Missing 'section' or 'items' in payload" });
    }

    let items = items.expect("checked above");
    let mut inserted = 0usize;
    let mut errors = Vec::new();

    let Some(kind) = parse_bio_section(section) else {
        return serde_json::json!({ "error": format!("Unknown section: {}", section) });
    };

    for item in items {
        let result = match kind {
            BioSection::Experience => {
                let slug = item.get("slug").and_then(|v| v.as_str()).unwrap_or("");
                let title = item.get("title").and_then(|v| v.as_str()).unwrap_or("");
                let company = item.get("company").and_then(|v| v.as_str()).unwrap_or("");
                let duration = item.get("duration").and_then(|v| v.as_str()).unwrap_or("");
                let tag = item.get("tag").and_then(|v| v.as_str()).unwrap_or("");
                let summary = item.get("summary").and_then(|v| v.as_str()).unwrap_or("");
                let sort_order = item.get("sort_order").and_then(|v| v.as_i64()).unwrap_or(0);

                let query = format!("INSERT INTO paulo_bio_{} (slug, title, company, duration, tag, summary, sort_order) VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (slug) DO UPDATE SET title=$2, company=$3, duration=$4, tag=$5, summary=$6, sort_order=$7", section);
                sqlx::query(&query)
                    .bind(slug)
                    .bind(title)
                    .bind(company)
                    .bind(duration)
                    .bind(tag)
                    .bind(summary)
                    .bind(sort_order)
                    .execute(pool)
                    .await
            }
            BioSection::Education => {
                let slug = item.get("slug").and_then(|v| v.as_str()).unwrap_or("");
                let degree = item.get("degree").and_then(|v| v.as_str()).unwrap_or("");
                let institution = item
                    .get("institution")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let duration = item.get("duration").and_then(|v| v.as_str()).unwrap_or("");
                let tag = item.get("tag").and_then(|v| v.as_str()).unwrap_or("");
                let summary = item.get("summary").and_then(|v| v.as_str()).unwrap_or("");
                let sort_order = item.get("sort_order").and_then(|v| v.as_i64()).unwrap_or(0);

                let query = format!("INSERT INTO paulo_bio_{} (slug, degree, institution, duration, tag, summary, sort_order) VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (slug) DO UPDATE SET degree=$2, institution=$3, duration=$4, tag=$5, summary=$6, sort_order=$7", section);
                sqlx::query(&query)
                    .bind(slug)
                    .bind(degree)
                    .bind(institution)
                    .bind(duration)
                    .bind(tag)
                    .bind(summary)
                    .bind(sort_order)
                    .execute(pool)
                    .await
            }
            BioSection::Skills => {
                let category = item.get("category").and_then(|v| v.as_str()).unwrap_or("");
                let name = item.get("name").and_then(|v| v.as_str()).unwrap_or("");
                let level = item
                    .get("level")
                    .and_then(|v| v.as_str())
                    .unwrap_or("expert");

                let query = format!(
                    "INSERT INTO paulo_bio_{} (category, name, level) VALUES ($1, $2, $3)",
                    section
                );
                sqlx::query(&query)
                    .bind(category)
                    .bind(name)
                    .bind(level)
                    .execute(pool)
                    .await
            }
        };

        match result {
            Ok(_) => inserted += 1,
            Err(e) => errors.push(format!("{}", e)),
        }
    }

    serde_json::json!({
        "status": "success",
        "inserted": inserted,
        "errors": errors
    })
}

pub async fn delete_bio(pool: &sqlx::PgPool, payload: Value) -> Value {
    let section = payload
        .get("section")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let slug = payload.get("slug").and_then(|v| v.as_str()).unwrap_or("");

    if section.is_empty() || slug.is_empty() {
        return serde_json::json!({ "error": "Missing 'section' or 'slug' in payload" });
    }

    let result = match parse_bio_section(section) {
        Some(BioSection::Experience) => {
            sqlx::query("DELETE FROM paulo_bio_experience WHERE slug = $1")
                .bind(slug)
                .execute(pool)
                .await
        }
        Some(BioSection::Education) => {
            sqlx::query("DELETE FROM paulo_bio_education WHERE slug = $1")
                .bind(slug)
                .execute(pool)
                .await
        }
        Some(BioSection::Skills) => {
            sqlx::query("DELETE FROM paulo_bio_skills WHERE id = $1")
                .bind(slug.parse::<i64>().unwrap_or(0))
                .execute(pool)
                .await
        }
        None => {
            return serde_json::json!({ "error": format!("Unknown section: {}", section) });
        }
    };

    match result {
        Ok(r) => serde_json::json!({
            "status": "success",
            "deleted": r.rows_affected()
        }),
        Err(e) => serde_json::json!({ "error": format!("DB error: {}", e) }),
    }
}
