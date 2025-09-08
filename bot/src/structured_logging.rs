use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{info, warn, error, debug};

/// Global correlation ID generator
static CORRELATION_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Generate a new correlation ID
pub fn new_correlation_id() -> u64 {
    CORRELATION_ID_COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// Structured logging with correlation ID support
#[derive(Clone, Debug)]
pub struct StructuredLogger {
    correlation_id: u64,
    component: String,
}

impl StructuredLogger {
    pub fn new(component: &str) -> Self {
        Self {
            correlation_id: new_correlation_id(),
            component: component.to_string(),
        }
    }

    pub fn with_correlation_id(component: &str, correlation_id: u64) -> Self {
        Self {
            correlation_id,
            component: component.to_string(),
        }
    }

    pub fn correlation_id(&self) -> u64 {
        self.correlation_id
    }

    fn log_structured(&self, level: &str, message: &str, extra_fields: serde_json::Value) {
        let log_entry = json!({
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "level": level,
            "component": self.component,
            "correlation_id": self.correlation_id,
            "message": message,
            "fields": extra_fields
        });

        let log_string = log_entry.to_string();
        
        // Route to appropriate tracing level
        match level {
            "DEBUG" => debug!(target: "structured", "{}", log_string),
            "INFO" => info!(target: "structured", "{}", log_string),
            "WARN" => warn!(target: "structured", "{}", log_string),
            "ERROR" => error!(target: "structured", "{}", log_string),
            _ => info!(target: "structured", "{}", log_string),
        }
    }

    pub fn info(&self, message: &str, fields: serde_json::Value) {
        self.log_structured("INFO", message, fields);
    }

    pub fn warn(&self, message: &str, fields: serde_json::Value) {
        self.log_structured("WARN", message, fields);
    }

    pub fn error(&self, message: &str, fields: serde_json::Value) {
        self.log_structured("ERROR", message, fields);
    }

    pub fn debug(&self, message: &str, fields: serde_json::Value) {
        self.log_structured("DEBUG", message, fields);
    }

    // Convenience methods for common use cases
    pub fn log_candidate_processed(&self, mint: &str, program: &str, accepted: bool) {
        self.info("candidate_processed", json!({
            "mint": mint,
            "program": program,
            "accepted": accepted,
            "action": "candidate_filter"
        }));
    }

    pub fn log_buy_attempt(&self, mint: &str, nonce_count: usize) {
        self.info("buy_attempt_started", json!({
            "mint": mint,
            "nonce_count": nonce_count,
            "action": "buy_start"
        }));
    }

    pub fn log_buy_success(&self, mint: &str, signature: &str, latency_ms: u64) {
        self.info("buy_success", json!({
            "mint": mint,
            "signature": signature,
            "latency_ms": latency_ms,
            "action": "buy_success"
        }));
    }

    pub fn log_buy_failure(&self, mint: &str, error: &str, latency_ms: u64) {
        self.error("buy_failure", json!({
            "mint": mint,
            "error": error,
            "latency_ms": latency_ms,
            "action": "buy_failure"
        }));
    }

    pub fn log_rpc_request(&self, endpoint: &str, method: &str) {
        self.debug("rpc_request", json!({
            "endpoint": endpoint,
            "method": method,
            "action": "rpc_send"
        }));
    }

    pub fn log_rpc_response(&self, endpoint: &str, method: &str, success: bool, latency_ms: u64) {
        let level = if success { "INFO" } else { "WARN" };
        self.log_structured(level, "rpc_response", json!({
            "endpoint": endpoint,
            "method": method,
            "success": success,
            "latency_ms": latency_ms,
            "action": "rpc_response"
        }));
    }

    pub fn log_nonce_operation(&self, operation: &str, nonce_index: Option<usize>, success: bool) {
        self.debug("nonce_operation", json!({
            "operation": operation,
            "nonce_index": nonce_index,
            "success": success,
            "action": "nonce_mgmt"
        }));
    }

    pub fn log_buffer_operation(&self, operation: &str, buffer_size: usize, candidate_mint: Option<&str>) {
        self.debug("buffer_operation", json!({
            "operation": operation,
            "buffer_size": buffer_size,
            "candidate_mint": candidate_mint,
            "action": "buffer_mgmt"
        }));
    }

    pub fn log_sell_operation(&self, mint: &str, sell_percent: f64, holdings_remaining: f64) {
        self.info("sell_operation", json!({
            "mint": mint,
            "sell_percent": sell_percent,
            "holdings_remaining": holdings_remaining,
            "action": "sell"
        }));
    }

    pub fn log_abort_all(&self, tasks_aborted: usize) {
        self.warn("abort_all_executed", json!({
            "tasks_aborted": tasks_aborted,
            "action": "abort_all"
        }));
    }
}

/// Pipeline context that carries correlation ID through operations
#[derive(Clone, Debug)]
pub struct PipelineContext {
    pub correlation_id: u64,
    pub logger: StructuredLogger,
}

impl PipelineContext {
    pub fn new(component: &str) -> Self {
        let logger = StructuredLogger::new(component);
        let correlation_id = logger.correlation_id();
        Self {
            correlation_id,
            logger,
        }
    }

    pub fn with_correlation_id(component: &str, correlation_id: u64) -> Self {
        let logger = StructuredLogger::with_correlation_id(component, correlation_id);
        Self {
            correlation_id,
            logger,
        }
    }

    pub fn child(&self, component: &str) -> Self {
        Self::with_correlation_id(component, self.correlation_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_correlation_id_generation() {
        let id1 = new_correlation_id();
        let id2 = new_correlation_id();
        assert!(id2 > id1);
    }

    #[test]
    fn test_structured_logger() {
        let logger = StructuredLogger::new("test_component");
        let correlation_id = logger.correlation_id();
        
        // This should not panic
        logger.info("test message", json!({"key": "value"}));
        
        assert!(correlation_id > 0);
    }

    #[test]
    fn test_pipeline_context() {
        let ctx = PipelineContext::new("test");
        let child_ctx = ctx.child("child_component");
        
        assert_eq!(ctx.correlation_id, child_ctx.correlation_id);
    }
}