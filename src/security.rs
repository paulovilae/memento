use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientIdentity {
    pub app: String,
    #[serde(default)]
    pub token: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SecurityConfig {
    pub socket_mode: u32,
    app_tokens: HashMap<String, String>,
    privileged_clients: HashSet<String>,
    knowledge_clients: HashSet<String>,
    app_query_clients: HashSet<String>,
    schema_clients: HashSet<String>,
    document_index_clients: HashSet<String>,
    bio_clients: HashSet<String>,
    audit_clients: HashSet<String>,
}

fn parse_set_env(var: &str, default: &[&str]) -> HashSet<String> {
    std::env::var(var)
        .ok()
        .map(|raw| {
            raw.split(',')
                .map(|item| item.trim().to_lowercase())
                .filter(|item| !item.is_empty())
                .collect()
        })
        .unwrap_or_else(|| default.iter().map(|item| item.to_string()).collect())
}

fn parse_tokens_env(var: &str) -> HashMap<String, String> {
    let raw = match std::env::var(var) {
        Ok(value) => value,
        Err(_) => return HashMap::new(),
    };

    if let Ok(map) = serde_json::from_str::<HashMap<String, String>>(&raw) {
        return map
            .into_iter()
            .map(|(k, v)| (k.to_lowercase(), v))
            .collect();
    }

    raw.split(',')
        .filter_map(|pair| pair.split_once('='))
        .map(|(app, token)| (app.trim().to_lowercase(), token.trim().to_string()))
        .filter(|(app, token)| !app.is_empty() && !token.is_empty())
        .collect()
}

fn parse_socket_mode() -> u32 {
    std::env::var("MEMENTO_SOCKET_MODE")
        .ok()
        .and_then(|value| u32::from_str_radix(value.trim_start_matches("0o"), 8).ok())
        .unwrap_or(0o600)
}

impl SecurityConfig {
    pub fn from_env() -> Self {
        Self {
            socket_mode: parse_socket_mode(),
            app_tokens: parse_tokens_env("MEMENTO_CLIENT_TOKENS"),
            privileged_clients: parse_set_env("MEMENTO_PRIVILEGED_CLIENTS", &["hera", "os-v3"]),
            knowledge_clients: parse_set_env(
                "MEMENTO_KNOWLEDGE_CLIENTS",
                &["hera", "os-v3", "memento-mcp"],
            ),
            app_query_clients: parse_set_env("MEMENTO_APP_QUERY_CLIENTS", &["hera", "os-v3"]),
            schema_clients: parse_set_env("MEMENTO_SCHEMA_CLIENTS", &["hera", "os-v3"]),
            document_index_clients: parse_set_env(
                "MEMENTO_DOCUMENT_INDEX_CLIENTS",
                &["hera", "os-v3", "vetra"],
            ),
            bio_clients: parse_set_env(
                "MEMENTO_BIO_CLIENTS",
                &["paulovila-rust", "paulovila", "os-v3"],
            ),
            audit_clients: parse_set_env(
                "MEMENTO_AUDIT_CLIENTS",
                &["hera", "os-v3", "paulovila-rust"],
            ),
        }
    }

    fn authenticate_client(
        &self,
        client: &Option<ClientIdentity>,
        allowlist: &HashSet<String>,
        action: &str,
    ) -> Result<String, String> {
        let client = client
            .as_ref()
            .ok_or_else(|| format!("client identity required for action '{}'", action))?;
        let app = client.app.trim().to_lowercase();
        if app.is_empty() {
            return Err(format!("client app is required for action '{}'", action));
        }
        if !allowlist.contains(&app) {
            return Err(format!(
                "client '{}' is not allowed to execute action '{}'",
                client.app, action
            ));
        }
        if let Some(expected_token) = self.app_tokens.get(&app) {
            if client.token.as_deref() != Some(expected_token.as_str()) {
                return Err(format!(
                    "client '{}' provided an invalid token for action '{}'",
                    client.app, action
                ));
            }
        }
        Ok(app)
    }

    fn require_payload_app_match(
        &self,
        client_app: &str,
        payload: &Value,
        key: &str,
        action: &str,
    ) -> Result<(), String> {
        if let Some(target_app) = payload.get(key).and_then(|value| value.as_str()) {
            if !target_app.is_empty()
                && target_app.to_lowercase() != client_app
                && !self.privileged_clients.contains(client_app)
            {
                return Err(format!(
                    "client '{}' cannot execute '{}' against app '{}'",
                    client_app, action, target_app
                ));
            }
        }
        Ok(())
    }

    pub fn authorize(
        &self,
        action: &str,
        payload: &Value,
        client: &Option<ClientIdentity>,
    ) -> Result<(), String> {
        match action {
            "query_app" => {
                let client_app =
                    self.authenticate_client(client, &self.app_query_clients, action)?;
                self.require_payload_app_match(&client_app, payload, "app", action)
            }
            "describe_app" => {
                let client_app = self.authenticate_client(client, &self.schema_clients, action)?;
                self.require_payload_app_match(&client_app, payload, "app", action)
            }
            "describe_all_apps" | "list_apps" => {
                self.authenticate_client(client, &self.schema_clients, action)?;
                Ok(())
            }
            "upsert_document_index" | "list_document_indexes" | "query_document_index" => {
                let client_app =
                    self.authenticate_client(client, &self.document_index_clients, action)?;
                self.require_payload_app_match(&client_app, payload, "app_id", action)
            }
            "get_document_index" => {
                let client_app =
                    self.authenticate_client(client, &self.document_index_clients, action)?;
                self.require_payload_app_match(&client_app, payload, "app_id", action)
            }
            "store_knowledge" | "get_knowledge" | "list_knowledge" | "search_knowledge"
            | "delete_knowledge" => {
                self.authenticate_client(client, &self.knowledge_clients, action)?;
                Ok(())
            }
            "query_bio" | "seed_bio" | "delete_bio" => {
                self.authenticate_client(client, &self.bio_clients, action)?;
                Ok(())
            }
            "audit_log" => {
                self.authenticate_client(client, &self.audit_clients, action)?;
                Ok(())
            }
            "get_metrics" => {
                self.authenticate_client(client, &self.audit_clients, action)?;
                Ok(())
            }
            "save_scoped_memory"
            | "save_memory_record"
            | "get_scoped_memory"
            | "query_memory_records"
            | "search_memory_records"
            | "get_memory_timeline"
            | "get_working_context"
            | "get_preferences"
            | "get_durable_facts"
            | "get_recent_events"
            | "memory_promote" => {
                if let Some(client) = client {
                    let client_app = client.app.trim().to_lowercase();
                    if !client_app.is_empty() {
                        self.require_payload_app_match(&client_app, payload, "app_id", action)?;
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}
