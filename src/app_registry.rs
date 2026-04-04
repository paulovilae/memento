/// App Registry — reads OS/etc/apps.toml and provides database connections
/// to external app databases (Movilo, Vetra, Latinos, etc.)
use serde::Deserialize;
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::collections::HashMap;
use std::path::Path;
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
    #[allow(dead_code)]
    pub slug: String,
    pub description: String,
    #[allow(dead_code)]
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
        match PgPoolOptions::new()
            .max_connections(2)
            .connect(&db_url)
            .await
        {
            Ok(pool) => {
                info!(
                    "  ✅ {} — connected to Postgres via {}",
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
            Err(e) => {
                error!("  ❌ {} — failed to connect: {}", manifest.app.name, e);
            }
        }
    }

    info!("🔗 Connected to {} app databases", connections.len());
    connections
}
