use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
struct CacheEntry {
    stored_at: Instant,
    value: Value,
}

#[derive(Debug, Default)]
struct QueryCache {
    entries: HashMap<String, CacheEntry>,
}

static CACHE: OnceLock<Mutex<QueryCache>> = OnceLock::new();

const CACHE_TTL: Duration = Duration::from_secs(3);

fn state() -> &'static Mutex<QueryCache> {
    CACHE.get_or_init(|| Mutex::new(QueryCache::default()))
}

fn make_key(action: &str, payload: &Value) -> String {
    format!("{action}:{}", payload)
}

pub fn get(action: &str, payload: &Value) -> Option<Value> {
    let mut state = state().lock().expect("query cache lock poisoned");
    let key = make_key(action, payload);
    let entry = state.entries.get(&key).cloned();
    match entry {
        Some(entry) if entry.stored_at.elapsed() <= CACHE_TTL => Some(entry.value),
        Some(_) => {
            state.entries.remove(&key);
            None
        }
        None => None,
    }
}

pub fn put(action: &str, payload: &Value, value: &Value) {
    let mut state = state().lock().expect("query cache lock poisoned");
    state.entries.insert(
        make_key(action, payload),
        CacheEntry {
            stored_at: Instant::now(),
            value: value.clone(),
        },
    );
}

pub fn invalidate_all() {
    let mut state = state().lock().expect("query cache lock poisoned");
    state.entries.clear();
}
