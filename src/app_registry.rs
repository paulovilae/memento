/// App Registry — reads OS/etc/apps.toml and provides database connections
/// to external app databases (Movilo, Vetra, Latinos, etc.)
use serde::Deserialize;
use bigdecimal::ToPrimitive;
use serde_json::{Map, Value};
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::{Column, Row};
use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;
use tracing::{error, info, warn};

/// A single app entry in etc/apps.toml
#[derive(Debug, Deserialize)]
struct AppRegistryEntry {
    slug: String,
    #[allow(dead_code)]
    path: String,
    manifest: String,
}

/// The full etc/apps.toml file
#[derive(Debug, Deserialize)]
struct AppRegistry {
    apps: Vec<AppRegistryEntry>,
}

/// Parsed from each app's app.toml
#[derive(Debug, Deserialize)]
struct AppManifest {
    app: AppInfo,
    database: Option<DatabaseConfig>,
}

#[derive(Debug, Deserialize)]
struct AppInfo {
    name: String,
    slug: String,
    description: String,
    #[allow(dead_code)]
    domain: Option<String>,
    #[allow(dead_code)]
    port: Option<u16>,
}

#[derive(Debug, Deserialize)]
struct DatabaseConfig {
    #[serde(rename = "type")]
    db_type: String,
    url_env: String,
    schema_prefix: Option<String>,
    key_tables: Option<Vec<String>>,
}

/// Holds a live Postgres connection pool for an app
pub struct AppConnection {
    pub name: String,
    pub slug: String,
    pub description: String,
    pub schema_prefix: String,
    pub key_tables: Vec<String>,
    pub pool: PgPool,
}

/// Discovers all apps from etc/apps.toml and connects to their databases.
/// Returns a map of slug -> AppConnection.
pub async fn discover_apps(os_root: &str) -> HashMap<String, AppConnection> {
    let mut connections = HashMap::new();

    let registry_path = format!("{}/etc/apps.toml", os_root);
    let registry_content = match std::fs::read_to_string(&registry_path) {
        Ok(c) => c,
        Err(e) => {
            warn!("⚠️ Could not read {}: {}", registry_path, e);
            return connections;
        }
    };

    let registry: AppRegistry = match toml::from_str(&registry_content) {
        Ok(r) => r,
        Err(e) => {
            error!("❌ Failed to parse {}: {}", registry_path, e);
            return connections;
        }
    };

    info!("📋 Found {} apps in registry", registry.apps.len());

    for entry in registry.apps {
        let manifest_path = format!("{}/{}", os_root, entry.manifest);
        if !Path::new(&manifest_path).exists() {
            warn!("⚠️ Manifest not found: {}", manifest_path);
            continue;
        }

        let manifest_content = match std::fs::read_to_string(&manifest_path) {
            Ok(c) => c,
            Err(e) => {
                warn!("⚠️ Could not read {}: {}", manifest_path, e);
                continue;
            }
        };

        let manifest: AppManifest = match toml::from_str(&manifest_content) {
            Ok(m) => m,
            Err(e) => {
                warn!("⚠️ Failed to parse {}: {}", manifest_path, e);
                continue;
            }
        };

        // Only connect to apps that have a postgres database configured
        let db_config = match manifest.database {
            Some(ref db) if db.db_type == "postgres" => db,
            _ => {
                info!(
                    "  ⏭️ {} — no postgres database, skipping",
                    manifest.app.name
                );
                continue;
            }
        };

        // Read the actual database URL from the environment
        let db_url = match std::env::var(&db_config.url_env) {
            Ok(url) => url,
            Err(_) => {
                warn!(
                    "  ⚠️ {} — env var {} not set, skipping",
                    manifest.app.name, db_config.url_env
                );
                continue;
            }
        };

        // Connect to the app's database
        let connect_options = match sqlx::postgres::PgConnectOptions::from_str(&db_url) {
            Ok(options) => options.application_name(&format!("memento-app-{}", entry.slug)),
            Err(error) => {
                error!(
                    "  ❌ {} — invalid database URL: {}",
                    manifest.app.name, error
                );
                continue;
            }
        };

        // Keep app pools lazy so Memento does not pre-consume Postgres connections
        // for every registered app on startup. The pool will open on first actual query.
        let pool = PgPoolOptions::new()
            .min_connections(0)
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(3))
            .idle_timeout(Duration::from_secs(30))
            .max_lifetime(Duration::from_secs(60 * 5))
            .connect_lazy_with(connect_options);

        info!(
            "  ✅ {} — registered lazy Postgres pool via {}",
            manifest.app.name, db_config.url_env
        );
        connections.insert(
            entry.slug.clone(),
            AppConnection {
                name: manifest.app.name,
                slug: manifest.app.slug,
                description: manifest.app.description,
                schema_prefix: db_config.schema_prefix.clone().unwrap_or_default(),
                key_tables: db_config.key_tables.clone().unwrap_or_default(),
                pool,
            },
        );
    }

    info!("🔗 Connected to {} app databases", connections.len());
    connections
}

fn available_apps(apps: &HashMap<String, AppConnection>) -> Vec<&String> {
    apps.keys().collect()
}

fn row_to_json(row: &sqlx::postgres::PgRow) -> Value {
    let columns = row.columns();
    let mut obj = Map::new();
    for col in columns {
        let name = col.name();
        if let Ok(v) = row.try_get::<Value, _>(name) {
            obj.insert(name.to_string(), v);
        } else if let Ok(v) = row.try_get::<String, _>(name) {
            obj.insert(name.to_string(), serde_json::json!(v));
        } else if let Ok(v) = row.try_get::<i64, _>(name) {
            obj.insert(name.to_string(), serde_json::json!(v));
        } else if let Ok(v) = row.try_get::<i32, _>(name) {
            obj.insert(name.to_string(), serde_json::json!(v));
        } else if let Ok(v) = row.try_get::<f64, _>(name) {
            obj.insert(name.to_string(), serde_json::json!(v));
        } else if let Ok(v) = row.try_get::<f32, _>(name) {
            obj.insert(name.to_string(), serde_json::json!(v));
        } else if let Ok(v) = row.try_get::<bigdecimal::BigDecimal, _>(name) {
            let json_value = v
                .to_f64()
                .map(|number| serde_json::json!(number))
                .unwrap_or_else(|| serde_json::json!(v.to_string()));
            obj.insert(name.to_string(), json_value);
        } else if let Ok(v) = row.try_get::<bool, _>(name) {
            obj.insert(name.to_string(), serde_json::json!(v));
        } else if let Ok(v) = row.try_get::<chrono::DateTime<chrono::Utc>, _>(name) {
            obj.insert(name.to_string(), serde_json::json!(v.to_rfc3339()));
        } else if let Ok(v) = row.try_get::<chrono::NaiveDateTime, _>(name) {
            obj.insert(name.to_string(), serde_json::json!(v.to_string()));
        } else {
            obj.insert(name.to_string(), Value::Null);
        }
    }
    Value::Object(obj)
}

fn accumulate_schema(rows: &[sqlx::postgres::PgRow]) -> Map<String, Value> {
    let mut tables = Map::new();
    for row in rows {
        let table: String = row.get("table_name");
        let col: String = row.get("column_name");
        let dtype: String = row.get("data_type");
        let entry = tables.entry(table).or_insert_with(|| serde_json::json!([]));
        if let Some(arr) = entry.as_array_mut() {
            arr.push(serde_json::json!({"column": col, "type": dtype}));
        }
    }
    tables
}

pub fn list_apps_json(apps: &HashMap<String, AppConnection>) -> Value {
    let app_list: Vec<Value> = apps
        .iter()
        .map(|(slug, conn)| {
            serde_json::json!({
                "slug": conn.slug,
                "registry_key": slug,
                "name": conn.name,
                "description": conn.description,
                "schema_prefix": conn.schema_prefix,
                "key_tables": conn.key_tables,
            })
        })
        .collect();
    serde_json::json!({ "status": "success", "apps": app_list })
}

pub async fn query_app(
    apps: &HashMap<String, AppConnection>,
    app_slug: &str,
    query: &str,
    limit: i64,
) -> Value {
    if query.is_empty() {
        return serde_json::json!({ "error": "Missing 'query' in payload" });
    }

    let Some(app_conn) = apps.get(app_slug) else {
        return serde_json::json!({
            "error": format!("App '{}' not found", app_slug),
            "available_apps": available_apps(apps)
        });
    };

    let trimmed = query.trim().to_uppercase();
    if !trimmed.starts_with("SELECT") && !trimmed.starts_with("WITH") {
        return serde_json::json!({ "error": "Only SELECT or WITH queries are allowed" });
    }

    let safe_query = if trimmed.contains("LIMIT") {
        query.to_string()
    } else {
        format!("{} LIMIT {}", query, limit)
    };

    match sqlx::query(&safe_query).fetch_all(&app_conn.pool).await {
        Ok(rows) => {
            let results: Vec<Value> = rows.iter().map(row_to_json).collect();
            serde_json::json!({
                "status": "success",
                "app": app_slug,
                "count": results.len(),
                "rows": results
            })
        }
        Err(e) => serde_json::json!({ "error": format!("Query error: {}", e) }),
    }
}

pub async fn describe_app(apps: &HashMap<String, AppConnection>, app_slug: &str) -> Value {
    let Some(app_conn) = apps.get(app_slug) else {
        return serde_json::json!({
            "error": format!("App '{}' not found", app_slug),
            "available_apps": available_apps(apps)
        });
    };

    let schema_query = r#"
        SELECT c.table_name, c.column_name, c.data_type, c.is_nullable
        FROM information_schema.columns c
        JOIN information_schema.tables t ON c.table_name = t.table_name AND c.table_schema = t.table_schema
        WHERE c.table_schema = 'public' AND t.table_type = 'BASE TABLE'
        ORDER BY c.table_name, c.ordinal_position
    "#;

    match sqlx::query(schema_query).fetch_all(&app_conn.pool).await {
        Ok(rows) => {
            let tables = accumulate_schema(&rows);
            serde_json::json!({
                "status": "success",
                "app": app_slug,
                "table_count": tables.len(),
                "schema": tables
            })
        }
        Err(e) => serde_json::json!({ "error": format!("Schema query error: {}", e) }),
    }
}

pub async fn describe_all_apps(apps: &HashMap<String, AppConnection>) -> Value {
    let schema_query = r#"
        SELECT c.table_name, c.column_name, c.data_type
        FROM information_schema.columns c
        JOIN information_schema.tables t ON c.table_name = t.table_name AND c.table_schema = t.table_schema
        WHERE c.table_schema = 'public' AND t.table_type = 'BASE TABLE'
        ORDER BY c.table_name, c.ordinal_position
    "#;

    let mut all_schemas = Map::new();
    for (slug, app_conn) in apps {
        match sqlx::query(schema_query).fetch_all(&app_conn.pool).await {
            Ok(rows) => {
                all_schemas.insert(slug.clone(), serde_json::json!(accumulate_schema(&rows)));
            }
            Err(e) => {
                all_schemas.insert(
                    slug.clone(),
                    serde_json::json!({ "error": format!("{}", e) }),
                );
            }
        }
    }

    serde_json::json!({
        "status": "success",
        "apps": all_schemas
    })
}
