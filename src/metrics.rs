use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

#[derive(Debug, Default)]
struct ActionMetric {
    count: u64,
    error_count: u64,
    denied_count: u64,
    total_duration_ms: u128,
    recent_durations_ms: Vec<u128>,
}

#[derive(Debug, Default)]
struct MetricsState {
    total_requests: u64,
    total_errors: u64,
    total_denied: u64,
    actions: HashMap<String, ActionMetric>,
}

static METRICS: OnceLock<Mutex<MetricsState>> = OnceLock::new();

fn state() -> &'static Mutex<MetricsState> {
    METRICS.get_or_init(|| Mutex::new(MetricsState::default()))
}

pub fn record_request(action: &str, duration_ms: u128, success: bool) {
    let mut state = state().lock().expect("metrics lock poisoned");
    state.total_requests += 1;
    if !success {
        state.total_errors += 1;
    }
    let entry = state.actions.entry(action.to_string()).or_default();
    entry.count += 1;
    entry.total_duration_ms += duration_ms;
    entry.recent_durations_ms.push(duration_ms);
    if entry.recent_durations_ms.len() > 256 {
        let overflow = entry.recent_durations_ms.len() - 256;
        entry.recent_durations_ms.drain(0..overflow);
    }
    if !success {
        entry.error_count += 1;
    }
}

pub fn record_denied(action: &str) {
    let mut state = state().lock().expect("metrics lock poisoned");
    state.total_denied += 1;
    let entry = state.actions.entry(action.to_string()).or_default();
    entry.denied_count += 1;
}

pub fn get_metrics() -> Value {
    let state = state().lock().expect("metrics lock poisoned");
    let actions: Vec<Value> = state
        .actions
        .iter()
        .map(|(action, metric)| {
            let avg_duration_ms = if metric.count == 0 {
                0.0
            } else {
                metric.total_duration_ms as f64 / metric.count as f64
            };
            let mut durations = metric.recent_durations_ms.clone();
            durations.sort_unstable();
            let p50_ms = percentile(&durations, 50.0);
            let p95_ms = percentile(&durations, 95.0);
            serde_json::json!({
                "action": action,
                "count": metric.count,
                "error_count": metric.error_count,
                "denied_count": metric.denied_count,
                "total_duration_ms": metric.total_duration_ms,
                "avg_duration_ms": avg_duration_ms,
                "p50_duration_ms": p50_ms,
                "p95_duration_ms": p95_ms,
                "sample_size": durations.len()
            })
        })
        .collect();

    serde_json::json!({
        "status": "success",
        "total_requests": state.total_requests,
        "total_errors": state.total_errors,
        "total_denied": state.total_denied,
        "actions": actions
    })
}

fn percentile(values: &[u128], pct: f64) -> u128 {
    if values.is_empty() {
        return 0;
    }
    let rank = ((pct / 100.0) * (values.len().saturating_sub(1) as f64)).round() as usize;
    values[rank.min(values.len() - 1)]
}
