use crate::metrics::{metrics, MetricsSnapshot};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Simple HTTP-like endpoint system for metrics and status
#[derive(Debug)]
pub struct EndpointServer {
    /// Scoreboard data for ranking
    scoreboard: Arc<RwLock<HashMap<String, ScoreboardEntry>>>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ScoreboardEntry {
    pub mint: String,
    pub program: String,
    pub buy_attempts: u64,
    pub buy_successes: u64,
    pub last_success_timestamp: Option<u64>,
    pub success_rate: f64,
    pub avg_latency_ms: f64,
}

impl EndpointServer {
    pub fn new() -> Self {
        Self {
            scoreboard: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Update scoreboard entry
    pub async fn update_scoreboard(&self, mint: &str, program: &str, success: bool, latency_ms: u64) {
        let mut scoreboard = self.scoreboard.write().await;
        let entry = scoreboard.entry(mint.to_string()).or_insert_with(|| ScoreboardEntry {
            mint: mint.to_string(),
            program: program.to_string(),
            buy_attempts: 0,
            buy_successes: 0,
            last_success_timestamp: None,
            success_rate: 0.0,
            avg_latency_ms: 0.0,
        });

        entry.buy_attempts += 1;
        if success {
            entry.buy_successes += 1;
            entry.last_success_timestamp = Some(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
            );
        }

        entry.success_rate = if entry.buy_attempts > 0 {
            entry.buy_successes as f64 / entry.buy_attempts as f64
        } else {
            0.0
        };

        // Update average latency (simple moving average)
        if entry.avg_latency_ms == 0.0 {
            entry.avg_latency_ms = latency_ms as f64;
        } else {
            entry.avg_latency_ms = (entry.avg_latency_ms + latency_ms as f64) / 2.0;
        }
    }

    /// Get metrics endpoint response
    pub fn get_metrics_response(&self) -> String {
        let metrics_snapshot = metrics().export_metrics();
        self.format_prometheus_metrics(&metrics_snapshot)
    }

    /// Get health endpoint response
    pub fn get_health_response(&self) -> String {
        json!({
            "status": "healthy",
            "timestamp": std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            "version": "0.1.0"
        }).to_string()
    }

    /// Get status endpoint response  
    pub async fn get_status_response(&self) -> String {
        let metrics_snapshot = metrics().export_metrics();
        let scoreboard = self.scoreboard.read().await;
        
        json!({
            "metrics": {
                "counters": metrics_snapshot.counters,
                "gauges": metrics_snapshot.gauges,
                "histograms": metrics_snapshot.histograms
            },
            "scoreboard_entries": scoreboard.len(),
            "system": {
                "uptime_seconds": std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                "memory_usage": "not_implemented"
            }
        }).to_string()
    }

    /// Get scoreboard endpoint response
    pub async fn get_scoreboard_response(&self, limit: Option<usize>) -> String {
        let scoreboard = self.scoreboard.read().await;
        let mut entries: Vec<_> = scoreboard.values().cloned().collect();
        
        // Sort by success rate descending, then by buy attempts
        entries.sort_by(|a, b| {
            b.success_rate.partial_cmp(&a.success_rate)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| b.buy_attempts.cmp(&a.buy_attempts))
        });

        if let Some(limit) = limit {
            entries.truncate(limit);
        }

        json!({
            "scoreboard": entries,
            "total_entries": scoreboard.len(),
            "timestamp": std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
        }).to_string()
    }

    /// Format metrics in Prometheus format
    fn format_prometheus_metrics(&self, snapshot: &MetricsSnapshot) -> String {
        let mut output = String::new();

        // Format counters
        for (name, value) in &snapshot.counters {
            output.push_str(&format!("# TYPE {} counter\n", name));
            output.push_str(&format!("{} {}\n", name, value));
        }

        // Format gauges
        for (name, value) in &snapshot.gauges {
            output.push_str(&format!("# TYPE {} gauge\n", name));
            output.push_str(&format!("{} {}\n", name, value));
        }

        // Format histograms
        for (name, stats) in &snapshot.histograms {
            output.push_str(&format!("# TYPE {}_count counter\n", name));
            output.push_str(&format!("{}_count {}\n", name, stats.count));
            
            output.push_str(&format!("# TYPE {} histogram\n", name));
            output.push_str(&format!("{}_bucket{{le=\"50\"}} {}\n", name, stats.p50));
            output.push_str(&format!("{}_bucket{{le=\"95\"}} {}\n", name, stats.p95));
            output.push_str(&format!("{}_bucket{{le=\"99\"}} {}\n", name, stats.p99));
            output.push_str(&format!("{}_bucket{{le=\"+Inf\"}} {}\n", name, stats.count));
            
            output.push_str(&format!("{}_min {}\n", name, stats.min));
            output.push_str(&format!("{}_max {}\n", name, stats.max));
        }

        output
    }

    /// Cleanup old scoreboard entries
    pub async fn cleanup_scoreboard(&self, max_entries: usize, max_age_hours: u64) {
        let mut scoreboard = self.scoreboard.write().await;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Remove entries older than max_age_hours
        let max_age_secs = max_age_hours * 3600;
        scoreboard.retain(|_mint, entry| {
            if let Some(last_success) = entry.last_success_timestamp {
                now - last_success < max_age_secs
            } else {
                // Keep entries without success timestamp for now
                true
            }
        });

        // If still too many entries, keep only the best performing ones
        if scoreboard.len() > max_entries {
            let mut entries: Vec<_> = scoreboard.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            entries.sort_by(|a, b| {
                b.1.success_rate.partial_cmp(&a.1.success_rate)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| b.1.buy_attempts.cmp(&a.1.buy_attempts))
            });

            scoreboard.clear();
            for (mint, entry) in entries.into_iter().take(max_entries) {
                scoreboard.insert(mint, entry);
            }
        }
    }
}

impl Default for EndpointServer {
    fn default() -> Self {
        Self::new()
    }
}

/// Global endpoint server instance
static GLOBAL_ENDPOINT_SERVER: std::sync::OnceLock<EndpointServer> = std::sync::OnceLock::new();

/// Get global endpoint server
pub fn endpoint_server() -> &'static EndpointServer {
    GLOBAL_ENDPOINT_SERVER.get_or_init(EndpointServer::new)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_scoreboard_updates() {
        let server = EndpointServer::new();
        
        server.update_scoreboard("mint1", "pump.fun", true, 150).await;
        server.update_scoreboard("mint1", "pump.fun", false, 200).await;
        server.update_scoreboard("mint2", "pump.fun", true, 100).await;

        let response = server.get_scoreboard_response(Some(10)).await;
        assert!(response.contains("mint1"));
        assert!(response.contains("mint2"));
    }

    #[test]
    fn test_metrics_response() {
        let server = EndpointServer::new();
        
        // Add some test metrics
        metrics().increment_counter("test_counter");
        metrics().set_gauge("test_gauge", 42);
        
        let response = server.get_metrics_response();
        assert!(response.contains("test_counter"));
        assert!(response.contains("test_gauge"));
    }

    #[test]
    fn test_health_response() {
        let server = EndpointServer::new();
        let response = server.get_health_response();
        assert!(response.contains("healthy"));
    }
}