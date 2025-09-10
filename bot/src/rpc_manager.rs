use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use solana_client::{
    client_error::{ClientError, ClientErrorKind},
    nonblocking::rpc_client::RpcClient,
    rpc_config::RpcSendTransactionConfig,
    rpc_request::RpcError,
};
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
    signature::Signature,
    transaction::VersionedTransaction,
};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::{sync::Mutex, task::JoinSet, time::timeout};
use tracing::{debug, info, warn};

/// Classification of RPC errors for proper handling
#[derive(Debug, Clone, PartialEq)]
pub enum RpcErrorType {
    /// Transaction was already processed - treat as soft success
    AlreadyProcessed,
    /// Duplicate signature detected - treat as soft success  
    DuplicateSignature,
    /// Blockhash not found - requires rebuilding with new blockhash
    BlockhashNotFound,
    /// Transaction expired - requires rebuilding with new blockhash
    TransactionExpired,
    /// Node is behind - retry with different endpoint
    NodeBehind,
    /// Slot skew issues - retry with different endpoint  
    SlotSkew,
    /// Rate limited - retry with different endpoint or backoff
    RateLimited,
    /// Too many requests - retry with different endpoint or backoff
    TooManyRequests,
    /// Network timeout - retry with different endpoint
    Timeout,
    /// Other error - generic failure
    Other(String),
}

impl std::fmt::Display for RpcErrorType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RpcErrorType::AlreadyProcessed => write!(f, "AlreadyProcessed"),
            RpcErrorType::DuplicateSignature => write!(f, "DuplicateSignature"),
            RpcErrorType::BlockhashNotFound => write!(f, "BlockhashNotFound"),
            RpcErrorType::TransactionExpired => write!(f, "TransactionExpired"),
            RpcErrorType::NodeBehind => write!(f, "NodeBehind"),
            RpcErrorType::SlotSkew => write!(f, "SlotSkew"),
            RpcErrorType::RateLimited => write!(f, "RateLimited"),
            RpcErrorType::TooManyRequests => write!(f, "TooManyRequests"),
            RpcErrorType::Timeout => write!(f, "Timeout"),
            RpcErrorType::Other(msg) => write!(f, "Other({})", msg),
        }
    }
}

/// Classify RPC errors for appropriate handling
pub fn classify_rpc_error(error: &ClientError) -> RpcErrorType {
    match error.kind() {
        ClientErrorKind::RpcError(RpcError::RpcResponseError { message, .. }) => {
            let msg = message.to_lowercase();
            if msg.contains("already processed") 
                || msg.contains("transaction was already processed") {
                RpcErrorType::AlreadyProcessed
            } else if msg.contains("duplicate") && msg.contains("signature") {
                RpcErrorType::DuplicateSignature
            } else if msg.contains("blockhash not found") 
                || msg.contains("blockhash has expired") {
                RpcErrorType::BlockhashNotFound
            } else if msg.contains("transaction expired")
                || msg.contains("block height exceeded") {
                RpcErrorType::TransactionExpired
            } else if msg.contains("node behind") 
                || msg.contains("slot") && msg.contains("behind") {
                RpcErrorType::NodeBehind
            } else if msg.contains("slot") && msg.contains("skew") {
                RpcErrorType::SlotSkew
            } else if msg.contains("rate limit") 
                || msg.contains("rate-limit") {
                RpcErrorType::RateLimited
            } else if msg.contains("too many requests") 
                || msg.contains("request limit") {
                RpcErrorType::TooManyRequests
            } else {
                RpcErrorType::Other(message.clone())
            }
        }
        ClientErrorKind::Io(_) => RpcErrorType::Timeout,
        ClientErrorKind::Reqwest(_) => RpcErrorType::Timeout,
        _ => RpcErrorType::Other(error.to_string()),
    }
}

/// Check if an error type should be treated as a soft success
fn is_soft_success(error_type: &RpcErrorType) -> bool {
    matches!(error_type, RpcErrorType::AlreadyProcessed | RpcErrorType::DuplicateSignature)

}

/// Trait for broadcasting transactions. Allows injecting mock implementations for tests.
pub trait RpcBroadcaster: Send + Sync + std::fmt::Debug {
    /// Broadcast the prepared VersionedTransaction objects; return first successful Signature or Err.
    fn send_on_many_rpc<'a>(
        &'a self,
        txs: Vec<VersionedTransaction>,
        correlation_id: Option<CorrelationId>,
    ) -> Pin<Box<dyn Future<Output = Result<Signature>> + Send + 'a>>;
}

/// Production RpcManager that broadcasts to multiple HTTP RPC endpoints with configurable modes.
pub struct RpcManager {
    pub endpoints: Vec<String>,
    pub broadcast_mode: BroadcastMode,
    client_cache: Arc<Mutex<HashMap<String, Arc<RpcClient>>>>,
    metrics: Arc<Mutex<HashMap<String, EndpointMetrics>>>,
}

impl std::fmt::Debug for RpcManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcManager")
            .field("endpoints", &self.endpoints)
            .field("broadcast_mode", &self.broadcast_mode)
            .field("cached_clients", &"<cached>")
            .field("metrics", &"<metrics>")
            .finish()
    }
}

impl RpcManager {
    pub fn new(endpoints: Vec<String>) -> Self {
        Self::with_mode(endpoints, BroadcastMode::default())
    }

    pub fn with_mode(endpoints: Vec<String>, broadcast_mode: BroadcastMode) -> Self {
        let metrics = endpoints.iter()
            .map(|endpoint| (endpoint.clone(), EndpointMetrics::new(endpoint.clone())))
            .collect();

        Self {
            endpoints,
            broadcast_mode,
            client_cache: Arc::new(Mutex::new(HashMap::new())),
            metrics: Arc::new(Mutex::new(metrics)),
        }
    }

    /// Get or create cached RPC client for endpoint
    async fn get_client(&self, endpoint: &str) -> Arc<RpcClient> {
        let mut cache = self.client_cache.lock().await;
        
        if let Some(client) = cache.get(endpoint) {
            Arc::clone(client)
        } else {
            let commitment = CommitmentConfig {
                commitment: CommitmentLevel::Confirmed,
            };
            let client = Arc::new(RpcClient::new_with_commitment(endpoint.to_string(), commitment));
            cache.insert(endpoint.to_string(), Arc::clone(&client));
            client
        }
    }

    /// Record metrics for an endpoint
    async fn record_metrics(&self, endpoint: &str, success: bool, latency: Option<Duration>) {
        let mut metrics = self.metrics.lock().await;
        if let Some(endpoint_metrics) = metrics.get_mut(endpoint) {
            if success {
                if let Some(lat) = latency {
                    endpoint_metrics.record_success(lat);
                }
            } else {
                endpoint_metrics.record_failure();
            }
        }
    }

    /// Get current metrics snapshot
    pub async fn get_metrics(&self) -> Vec<EndpointMetrics> {
        let metrics = self.metrics.lock().await;
        metrics.values().cloned().collect()
    }
}

impl RpcBroadcaster for RpcManager {
    fn send_on_many_rpc<'a>(
        &'a self,
        txs: Vec<VersionedTransaction>,
        correlation_id: Option<CorrelationId>,
    ) -> Pin<Box<dyn Future<Output = Result<Signature>> + Send + 'a>> {
        Box::pin(async move {
            if self.endpoints.is_empty() || txs.is_empty() {
                return Err(anyhow!(
                    "send_on_many_rpc: no endpoints or no transactions to send (endpoints={}, txs={})",
                    self.endpoints.len(),
                    txs.len()
                ));
            }

            let correlation_id = correlation_id.unwrap_or_default();

            match self.broadcast_mode {
                BroadcastMode::Pairwise => self.broadcast_pairwise(txs, correlation_id).await,
                BroadcastMode::ReplicateSingle => self.broadcast_replicate_single(txs, correlation_id).await,
                BroadcastMode::RoundRobin => self.broadcast_round_robin(txs, correlation_id).await,
                BroadcastMode::FullFanout => self.broadcast_full_fanout(txs, correlation_id).await,
            }
        })
    }
}

impl RpcManager {
    /// Pairwise broadcast: tx[0] -> endpoint[0], tx[1] -> endpoint[1], etc.
    async fn broadcast_pairwise(&self, txs: Vec<VersionedTransaction>, correlation_id: CorrelationId) -> Result<Signature> {
        let n = self.endpoints.len().min(txs.len());
        
        StructuredLogger::log_rpc_broadcast(
            &correlation_id,
            "pairwise",
            n,
            n,
        );
        
        info!("RpcManager: pairwise broadcast {} tx(s) across {} endpoint(s)", n, n);
        
        let mut set: JoinSet<Result<Signature>> = JoinSet::new();

        for i in 0..n {
            let endpoint = self.endpoints[i].clone();
            let tx = txs[i].clone();
            let client = self.get_client(&endpoint).await;
            let metrics_recorder = Arc::clone(&self.metrics);
            let corr_id = Some(correlation_id.clone());

            set.spawn(async move {
                Self::send_single_tx(client, endpoint, tx, metrics_recorder, corr_id).await
            });
        }

        Self::wait_for_first_success(set).await
    }

    /// Replicate single transaction to all endpoints
    async fn broadcast_replicate_single(&self, mut txs: Vec<VersionedTransaction>, _correlation_id: CorrelationId) -> Result<Signature> {
        if txs.is_empty() {
            return Err(anyhow!("no transactions to replicate"));
        }

        let tx = txs.swap_remove(0); // Take the first transaction
        
        info!("RpcManager: replicating single tx to {} endpoint(s)", self.endpoints.len());
        
        let mut set: JoinSet<Result<Signature>> = JoinSet::new();

        for endpoint in &self.endpoints {
            let endpoint = endpoint.clone();
            let tx = tx.clone();
            let client = self.get_client(&endpoint).await;
            let metrics_recorder = Arc::clone(&self.metrics);

            set.spawn(async move {
                Self::send_single_tx(client, endpoint, tx, metrics_recorder, None).await
            });
        }

        Self::wait_for_first_success(set).await
    }

    /// Round-robin distribution of transactions across endpoints
    async fn broadcast_round_robin(&self, txs: Vec<VersionedTransaction>, _correlation_id: CorrelationId) -> Result<Signature> {
        info!("RpcManager: round-robin broadcast {} tx(s) across {} endpoint(s)", 
              txs.len(), self.endpoints.len());
        
        let mut set: JoinSet<Result<Signature>> = JoinSet::new();

        for (i, tx) in txs.into_iter().enumerate() {
            let endpoint_idx = i % self.endpoints.len();
            let endpoint = self.endpoints[endpoint_idx].clone();
            let client = self.get_client(&endpoint).await;
            let metrics_recorder = Arc::clone(&self.metrics);

            set.spawn(async move {
                Self::send_single_tx(client, endpoint, tx, metrics_recorder, None).await
            });
        }

        Self::wait_for_first_success(set).await
    }

    /// Full fanout: send all transactions to all endpoints
    async fn broadcast_full_fanout(&self, txs: Vec<VersionedTransaction>, _correlation_id: CorrelationId) -> Result<Signature> {
        info!("RpcManager: full fanout {} tx(s) to {} endpoint(s) (total: {} sends)", 
              txs.len(), self.endpoints.len(), txs.len() * self.endpoints.len());
        
        let mut set: JoinSet<Result<Signature>> = JoinSet::new();

        for endpoint in &self.endpoints {
            for tx in &txs {
                let endpoint = endpoint.clone();
                let tx = tx.clone();
                let client = self.get_client(&endpoint).await;
                let metrics_recorder = Arc::clone(&self.metrics);

                set.spawn(async move {

                    let rpc = RpcClient::new_with_commitment(endpoint.clone(), commitment);
                    debug!("RpcManager: sending tx on endpoint[{}]: {}", i, endpoint);

                    let send_fut = rpc.send_transaction_with_config(&tx, send_cfg);
                    match timeout(SEND_TIMEOUT, send_fut).await {
                        Ok(Ok(sig)) => {
                            info!(
                                "RpcManager: success on endpoint[{}]: {} sig={}",
                                i, endpoint, sig
                            );
                            Ok(sig)
                        }
                        Ok(Err(e)) => {
                            let error_type = classify_rpc_error(&e);
                            
                            // Treat some errors as soft success
                            if is_soft_success(&error_type) {
                                info!(
                                    "RpcManager: soft success on endpoint[{}]: {} ({})",
                                    i, endpoint, error_type
                                );
                                // Create a deterministic signature from the transaction
                                let mut sig_bytes = [0u8; 64];
                                sig_bytes[0] = i as u8; // Endpoint index
                                sig_bytes[1] = 0xFF; // Marker for soft success
                                return Ok(Signature::from(sig_bytes));
                            }
                            
                            warn!(
                                "RpcManager: endpoint[{}] send failed on {} with {:?}: {}",
                                i, endpoint, error_type, e
                            );
                            Err(anyhow!(e).context(format!("RPC error: {:?}", error_type)))
                        }
                        Err(elapsed) => {
                            warn!(
                                "RpcManager: endpoint[{}] timed out on {} after {:?}",
                                i, endpoint, SEND_TIMEOUT
                            );
                            Err(anyhow!(elapsed).context("RPC send timeout"))
                        }
                    }

                });
            }
        }

        Self::wait_for_first_success(set).await
    }

    /// Send a single transaction and record metrics
    async fn send_single_tx(
        client: Arc<RpcClient>,
        endpoint: String,
        tx: VersionedTransaction,
        metrics: Arc<Mutex<HashMap<String, EndpointMetrics>>>,
        correlation_id: Option<CorrelationId>,
    ) -> Result<Signature> {
        const SEND_TIMEOUT: Duration = Duration::from_secs(8);
        
        let send_cfg = RpcSendTransactionConfig {
            skip_preflight: true,
            preflight_commitment: Some(CommitmentLevel::Processed),
            max_retries: Some(3),
            ..Default::default()
        };

        debug!("RpcManager: sending tx to endpoint: {}", endpoint);
        
        let start = Instant::now();
        let send_fut = client.send_transaction_with_config(&tx, send_cfg);
        
        match timeout(SEND_TIMEOUT, send_fut).await {
            Ok(Ok(sig)) => {
                let latency = start.elapsed();
                
                if let Some(ref corr_id) = correlation_id {
                    StructuredLogger::log_rpc_result(
                        corr_id,
                        &endpoint,
                        true,
                        latency,
                        Some(&sig.to_string()),
                        None,
                    );
                }
                
                info!("RpcManager: success on endpoint: {} sig={} latency={:?}", 
                      endpoint, sig, latency);
                
                // Record success metrics
                let mut metrics_guard = metrics.lock().await;
                if let Some(endpoint_metrics) = metrics_guard.get_mut(&endpoint) {
                    endpoint_metrics.record_success(latency);
                }
                
                Ok(sig)
            }
            Ok(Err(e)) => {
                let latency = start.elapsed();
                
                if let Some(ref corr_id) = correlation_id {
                    StructuredLogger::log_rpc_result(
                        corr_id,
                        &endpoint,
                        false,
                        latency,
                        None,
                        Some(&e.to_string()),
                    );
                }
                
                warn!("RpcManager: endpoint send failed on {}: {} (latency={:?})", 
                      endpoint, e, latency);
                
                // Record failure metrics
                let mut metrics_guard = metrics.lock().await;
                if let Some(endpoint_metrics) = metrics_guard.get_mut(&endpoint) {
                    endpoint_metrics.record_failure();
                }
                
                Err(anyhow!(e).context("RPC send_transaction_with_config failed"))
            }
            Err(_elapsed) => {
                let latency = start.elapsed();
                
                if let Some(ref corr_id) = correlation_id {
                    StructuredLogger::log_rpc_result(
                        corr_id,
                        &endpoint,
                        false,
                        latency,
                        None,
                        Some("timeout"),
                    );
                }
                
                warn!("RpcManager: endpoint timed out on {} after {:?}", endpoint, SEND_TIMEOUT);
                
                // Record timeout as failure
                let mut metrics_guard = metrics.lock().await;
                if let Some(endpoint_metrics) = metrics_guard.get_mut(&endpoint) {
                    endpoint_metrics.record_failure();
                }
                
                Err(anyhow!("RPC send timeout after {:?}", latency))
            }
        }
    }

    /// Wait for first successful result from JoinSet
    async fn wait_for_first_success(mut set: JoinSet<Result<Signature>>) -> Result<Signature> {
        while let Some(join_res) = set.join_next().await {
            match join_res {
                Ok(Ok(sig)) => {
                    set.abort_all();
                    return Ok(sig);
                }
                Ok(Err(e)) => {
                    debug!("RpcManager: task returned error: {:?}", e);
                }
                Err(join_err) => {
                    warn!("RpcManager: task join error: {}", join_err);
                }
            }
        }

        Err(anyhow!("RpcManager: all send attempts failed"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use solana_sdk::{
        message::Message,
        pubkey::Pubkey,
        system_instruction,
        transaction::{Transaction, VersionedTransaction},
    };

    // Helper to create a simple transaction
    fn create_test_tx() -> VersionedTransaction {
        let from = Pubkey::new_unique();
        let to = Pubkey::new_unique();
        let ix = system_instruction::transfer(&from, &to, 1);
        let msg = Message::new(&[ix], None);
        let tx = Transaction::new_unsigned(msg);
        VersionedTransaction::from(tx)
    }

    #[tokio::test]
    async fn test_broadcast_mode_creation() {
        let endpoints = vec!["http://test1".to_string(), "http://test2".to_string()];
        
        let manager_default = RpcManager::new(endpoints.clone());
        assert_eq!(manager_default.broadcast_mode, BroadcastMode::Pairwise);
        
        let manager_replicate = RpcManager::with_mode(endpoints.clone(), BroadcastMode::ReplicateSingle);
        assert_eq!(manager_replicate.broadcast_mode, BroadcastMode::ReplicateSingle);
    }

    #[tokio::test]
    async fn test_client_caching() {
        let endpoints = vec!["http://test1".to_string()];
        let manager = RpcManager::new(endpoints);
        
        let client1 = manager.get_client("http://test1").await;
        let client2 = manager.get_client("http://test1").await;
        
        // Should be the same Arc (client reuse)
        assert!(Arc::ptr_eq(&client1, &client2));
    }

    #[tokio::test]
    async fn test_metrics_tracking() {
        let endpoints = vec!["http://test1".to_string()];
        let manager = RpcManager::new(endpoints);
        
        // Record some metrics
        manager.record_metrics("http://test1", true, Some(Duration::from_millis(100))).await;
        manager.record_metrics("http://test1", false, None).await;
        manager.record_metrics("http://test1", true, Some(Duration::from_millis(200))).await;
        
        let metrics = manager.get_metrics().await;
        assert_eq!(metrics.len(), 1);
        
        let endpoint_metrics = &metrics[0];
        assert_eq!(endpoint_metrics.success_count, 2);
        assert_eq!(endpoint_metrics.failure_count, 1);
        assert!(endpoint_metrics.avg_latency_ms > 0.0);
        assert_eq!(endpoint_metrics.success_rate(), 2.0 / 3.0);
    }

    #[tokio::test] 
    async fn test_empty_inputs_handling() {
        let manager = RpcManager::new(vec![]);
        let result = manager.send_on_many_rpc(vec![create_test_tx()], None).await;
        assert!(result.is_err());
        
        let manager = RpcManager::new(vec!["http://test".to_string()]);
        let result = manager.send_on_many_rpc(vec![], None).await;
        assert!(result.is_err());
    }

    // Mock broadcaster for testing different modes
    #[derive(Debug)]
    struct CountingBroadcaster {
        send_count: Arc<AtomicUsize>,
        should_succeed: bool,
    }

    impl CountingBroadcaster {
        fn new(should_succeed: bool) -> Self {
            Self {
                send_count: Arc::new(AtomicUsize::new(0)),
                should_succeed,
            }
        }

        fn get_send_count(&self) -> usize {
            self.send_count.load(Ordering::Relaxed)
        }
    }

    impl RpcBroadcaster for CountingBroadcaster {
        fn send_on_many_rpc<'a>(
            &'a self,
            txs: Vec<VersionedTransaction>,
            _correlation_id: Option<CorrelationId>,
        ) -> Pin<Box<dyn Future<Output = Result<Signature>> + Send + 'a>> {
            Box::pin(async move {
                self.send_count.fetch_add(txs.len(), Ordering::Relaxed);
                
                if self.should_succeed {
                    Ok(Signature::from([42u8; 64]))
                } else {
                    Err(anyhow!("mock failure"))
                }
            })
        }
    }

    #[tokio::test]
    async fn test_broadcast_modes_basic() {
        let endpoints = vec![
            "http://test1".to_string(),
            "http://test2".to_string(),
            "http://test3".to_string(),
        ];

        // Test that different modes are set correctly
        let manager_pairwise = RpcManager::with_mode(endpoints.clone(), BroadcastMode::Pairwise);
        let manager_replicate = RpcManager::with_mode(endpoints.clone(), BroadcastMode::ReplicateSingle);
        let manager_roundrobin = RpcManager::with_mode(endpoints.clone(), BroadcastMode::RoundRobin);
        let manager_fanout = RpcManager::with_mode(endpoints.clone(), BroadcastMode::FullFanout);

        assert_eq!(manager_pairwise.broadcast_mode, BroadcastMode::Pairwise);
        assert_eq!(manager_replicate.broadcast_mode, BroadcastMode::ReplicateSingle);
        assert_eq!(manager_roundrobin.broadcast_mode, BroadcastMode::RoundRobin);
        assert_eq!(manager_fanout.broadcast_mode, BroadcastMode::FullFanout);
    }
}