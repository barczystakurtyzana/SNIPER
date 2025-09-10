//! Basic observability infrastructure for SNIPER bot
//! Provides correlation IDs, structured logging, and basic metrics collection

use serde_json::{json, Value};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{info, warn};
use uuid::Uuid;

/// Global correlation ID counter for unique transaction tracking
static CORRELATION_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Correlation ID for tracking requests through the pipeline
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CorrelationId(String);

impl CorrelationId {
    /// Generate a new correlation ID
    pub fn new() -> Self {
        let counter = CORRELATION_COUNTER.fetch_add(1, Ordering::Relaxed);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        
        Self(format!("sniper-{}-{}", timestamp, counter))
    }

    /// Create correlation ID from string
    pub fn from_string(s: String) -> Self {
        Self(s)
    }

    /// Get the correlation ID as string
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for CorrelationId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Default for CorrelationId {
    fn default() -> Self {
        Self::new()
    }
}

/// Structured logger for JSON formatted logs with correlation IDs
pub struct StructuredLogger;

impl StructuredLogger {
    /// Log buy attempt with structured data
    pub fn log_buy_attempt(
        correlation_id: &CorrelationId,
        mint: &str,
        program: &str,
        nonce_count: usize,
    ) {
        let log_data = json!({
            "event": "buy_attempt",
            "correlation_id": correlation_id.as_str(),
            "mint": mint,
            "program": program,
            "nonce_count": nonce_count,
            "timestamp": SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis()
        });

        info!(%correlation_id, buy_attempt = %log_data, "Buy attempt initiated");
    }

    /// Log buy success with structured data
    pub fn log_buy_success(
        correlation_id: &CorrelationId,
        mint: &str,
        signature: &str,
        execution_price: f64,
        latency_ms: u64,
    ) {
        let log_data = json!({
            "event": "buy_success",
            "correlation_id": correlation_id.as_str(),
            "mint": mint,
            "signature": signature,
            "execution_price": execution_price,
            "latency_ms": latency_ms,
            "timestamp": SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis()
        });

        info!(%correlation_id, buy_success = %log_data, "Buy completed successfully");
    }

    /// Log buy failure with structured data
    pub fn log_buy_failure(
        correlation_id: &CorrelationId,
        mint: &str,
        error: &str,
        latency_ms: u64,
        failure_count: u32,
    ) {
        let log_data = json!({
            "event": "buy_failure",
            "correlation_id": correlation_id.as_str(),
            "mint": mint,
            "error": error,
            "latency_ms": latency_ms,
            "consecutive_failures": failure_count,
            "timestamp": SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis()
        });

        warn!(%correlation_id, buy_failure = %log_data, "Buy attempt failed");
    }

    /// Log RPC broadcast with structured data
    pub fn log_rpc_broadcast(
        correlation_id: &CorrelationId,
        broadcast_mode: &str,
        endpoint_count: usize,
        transaction_count: usize,
    ) {
        let log_data = json!({
            "event": "rpc_broadcast",
            "correlation_id": correlation_id.as_str(),
            "broadcast_mode": broadcast_mode,
            "endpoint_count": endpoint_count,
            "transaction_count": transaction_count,
            "timestamp": SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis()
        });

        info!(%correlation_id, rpc_broadcast = %log_data, "Broadcasting transactions");
    }

    /// Log RPC endpoint result with structured data
    pub fn log_rpc_result(
        correlation_id: &CorrelationId,
        endpoint: &str,
        success: bool,
        latency: Duration,
        signature: Option<&str>,
        error: Option<&str>,
    ) {
        let log_data = json!({
            "event": "rpc_result",
            "correlation_id": correlation_id.as_str(),
            "endpoint": endpoint,
            "success": success,
            "latency_ms": latency.as_millis(),
            "signature": signature,
            "error": error,
            "timestamp": SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis()
        });

        if success {
            info!(%correlation_id, rpc_result = %log_data, "RPC call succeeded");
        } else {
            warn!(%correlation_id, rpc_result = %log_data, "RPC call failed");
        }
    }
}

/// Basic metrics collection trait
pub trait MetricsCollector: Send + Sync {
    /// Record a counter metric
    fn counter(&self, name: &str, value: u64, tags: &[(&str, &str)]);
    
    /// Record a gauge metric
    fn gauge(&self, name: &str, value: f64, tags: &[(&str, &str)]);
    
    /// Record a histogram/timing metric
    fn histogram(&self, name: &str, value: f64, tags: &[(&str, &str)]);
}

/// In-memory metrics collector for basic observability
#[derive(Debug, Default)]
pub struct InMemoryMetrics {
    // Simple counters stored in memory - in production this would be exported to Prometheus
    counters: std::sync::Mutex<std::collections::HashMap<String, u64>>,
}

impl InMemoryMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Get current metric values (for testing/debugging)
    pub fn get_counter(&self, name: &str) -> Option<u64> {
        let counters = self.counters.lock().unwrap();
        counters.get(name).copied()
    }
}

impl MetricsCollector for InMemoryMetrics {
    fn counter(&self, name: &str, value: u64, _tags: &[(&str, &str)]) {
        let mut counters = self.counters.lock().unwrap();
        *counters.entry(name.to_string()).or_insert(0) += value;
    }

    fn gauge(&self, name: &str, value: f64, _tags: &[(&str, &str)]) {
        // For simplicity, treat gauges as counters in this basic implementation
        self.counter(name, value as u64, _tags);
    }

    fn histogram(&self, name: &str, value: f64, _tags: &[(&str, &str)]) {
        // For simplicity, treat histograms as counters in this basic implementation
        self.counter(name, value as u64, _tags);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_correlation_id_generation() {
        let id1 = CorrelationId::new();
        let id2 = CorrelationId::new();
        
        assert_ne!(id1, id2);
        assert!(id1.as_str().starts_with("sniper-"));
        assert!(id2.as_str().starts_with("sniper-"));
        assert!(id1.as_str().len() > 10);
    }

    #[test]
    fn test_correlation_id_from_string() {
        let custom_id = "custom-correlation-123".to_string();
        let id = CorrelationId::from_string(custom_id.clone());
        assert_eq!(id.as_str(), custom_id);
    }

    #[test]
    fn test_metrics_collector() {
        let metrics = InMemoryMetrics::new();
        
        metrics.counter("test_counter", 5, &[]);
        metrics.counter("test_counter", 3, &[]);
        
        assert_eq!(metrics.get_counter("test_counter"), Some(8));
        assert_eq!(metrics.get_counter("nonexistent"), None);
    }
}