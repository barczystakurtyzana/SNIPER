use anyhow::{anyhow, Result};
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

use std::{collections::HashMap, future::Future, sync::Arc, time::Instant};
use std::pin::Pin;
use std::time::Duration;

use tokio::{sync::RwLock, task::JoinSet, time::timeout};
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::observability::CorrelationId;

/// Classification of RPC errors for handling logic
#[derive(Debug, PartialEq, Eq)]
pub enum RpcErrorType {
    AlreadyProcessed,
    DuplicateSignature,
    BlockhashNotFound,
    RateLimited,
    Other(String),
}

/// Classify a ClientError into an RpcErrorType for consistent handling
pub fn classify_rpc_error(error: &ClientError) -> RpcErrorType {
    match error.kind() {
        ClientErrorKind::RpcError(rpc_error) => match rpc_error {
            RpcError::RpcResponseError { message, .. } => {
                let msg = message.to_lowercase();
                if msg.contains("already processed") {
                    RpcErrorType::AlreadyProcessed
                } else if msg.contains("duplicate signature") {
                    RpcErrorType::DuplicateSignature
                } else if msg.contains("blockhash not found") {
                    RpcErrorType::BlockhashNotFound
                } else if msg.contains("rate limit") || msg.contains("too many requests") {
                    RpcErrorType::RateLimited
                } else {
                    RpcErrorType::Other(message.clone())
                }
            }
            _ => RpcErrorType::Other("Unknown RPC error".to_string()),
        },
        _ => RpcErrorType::Other(error.to_string()),
    }
}

/// Endpoint performance metrics for adaptive ranking
#[derive(Debug, Clone)]
struct EndpointMetrics {
    success_count: u64,
    error_count: u64,
    total_latency_ms: u64,
    last_success: Option<Instant>,
}

impl EndpointMetrics {
    fn new() -> Self {
        Self {
            success_count: 0,
            error_count: 0,
            total_latency_ms: 0,
            last_success: None,
        }
    }

    fn success_rate(&self) -> f64 {
        let total = self.success_count + self.error_count;
        if total == 0 {
            1.0 // Assume good until proven otherwise
        } else {
            self.success_count as f64 / total as f64
        }
    }

    fn avg_latency_ms(&self) -> f64 {
        if self.success_count == 0 {
            1000.0 // Default to 1s estimate
        } else {
            self.total_latency_ms as f64 / self.success_count as f64
        }
    }

    fn record_success(&mut self, latency_ms: u64) {
        self.success_count += 1;
        self.total_latency_ms += latency_ms;
        self.last_success = Some(Instant::now());
    }

    fn record_error(&mut self) {
        self.error_count += 1;
    }
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


/// Production RpcManager that broadcasts to multiple HTTP RPC endpoints with connection pooling.
pub struct RpcManager {
    pub endpoints: Vec<String>,
    // Connection pool to avoid recreating clients on every request
    client_pool: Arc<RwLock<HashMap<String, Arc<RpcClient>>>>,
    // Configuration for RPC operations
    config: Config,
}

impl std::fmt::Debug for RpcManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcManager")
            .field("endpoints", &self.endpoints)
            .field("client_pool_size", &"<pool>")
            .finish()
    }
}

impl RpcManager {
    pub fn new(endpoints: Vec<String>, config: Config) -> Self {
        Self { 
            endpoints,
            client_pool: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    pub fn new_with_config(endpoints: Vec<String>, config: Config) -> Self {
        Self::new(endpoints, config)
    }

    async fn get_or_create_client(&self, endpoint: &str, commitment: CommitmentConfig) -> Arc<RpcClient> {
        // Try to get existing client first
        {
            let pool = self.client_pool.read().await;
            if let Some(client) = pool.get(endpoint) {
                return client.clone();
            }
        }
        
        // Create new client if not found
        let client = Arc::new(RpcClient::new_with_commitment(endpoint.to_string(), commitment));
        {
            let mut pool = self.client_pool.write().await;
            // Double-check pattern in case another task created it
            if let Some(existing) = pool.get(endpoint) {
                return existing.clone();
            }
            pool.insert(endpoint.to_string(), client.clone());
        }
        client
    }

    /// Check if an error indicates a fatal condition that should trigger early cancellation
    fn is_fatal_error_type(error_msg: &str) -> bool {
        // Simple implementation - consider some common fatal errors
        error_msg.contains("insufficient funds") 
            || error_msg.contains("account not found")
            || error_msg.contains("invalid signature")
            || error_msg.contains("transaction too large")
    }
}

impl Clone for RpcManager {
    fn clone(&self) -> Self {
        Self {
            endpoints: self.endpoints.clone(),
            client_pool: self.client_pool.clone(),
            config: self.config.clone(),
        }
    }
}

impl RpcBroadcaster for RpcManager {
    fn send_on_many_rpc<'a>(
        &'a self,
        txs: Vec<VersionedTransaction>,
        _correlation_id: Option<CorrelationId>,
    ) -> Pin<Box<dyn Future<Output = Result<Signature>> + Send + 'a>> {
        Box::pin(async move {
            if self.endpoints.is_empty() || txs.is_empty() {
                return Err(anyhow!(
                    "send_on_many_rpc: no endpoints or no transactions to send (endpoints={}, txs={})",
                    self.endpoints.len(),
                    txs.len()
                ));
            }

            let timeout_duration = Duration::from_secs(self.config.rpc_timeout_sec);
            
            // Fix commitment mismatch - use Confirmed consistently
            let send_cfg = RpcSendTransactionConfig {
                skip_preflight: true,
                preflight_commitment: Some(CommitmentLevel::Confirmed),
                max_retries: Some(3),
                ..Default::default()
            };

            let mut set: JoinSet<Result<Signature>> = JoinSet::new();
            let mut fatal_errors = 0;

            // Simple pairwise broadcast for now (minimal implementation)
            let num_tasks = std::cmp::min(self.endpoints.len(), txs.len());
            
            for i in 0..num_tasks {
                let endpoint = self.endpoints[i].clone();
                let tx = txs[i].clone();
                let client_pool = self.client_pool.clone();
                let commitment = CommitmentConfig::confirmed();

                set.spawn(async move {
                    // Use the pooled client instead of creating a new one
                    let rpc_manager = RpcManager {
                        endpoints: vec![endpoint.clone()],
                        client_pool,
                        config: Config::default(), // Use default config for spawned tasks
                    };
                    let client = rpc_manager.get_or_create_client(&endpoint, commitment).await;
                    debug!("RpcManager: sending tx on endpoint[{}]: {}", i, endpoint);

                    let start_time = Instant::now();
                    let send_fut = client.send_transaction_with_config(&tx, send_cfg);
                    match timeout(timeout_duration, send_fut).await {
                        Ok(Ok(sig)) => {
                            let latency_ms = start_time.elapsed().as_millis() as u64;
                            info!("RpcManager: success on {}: {} ({}ms)", endpoint, sig, latency_ms);
                            Ok(sig)
                        }
                        Ok(Err(e)) => {
                            let error_msg = e.to_string();
                            warn!("RpcManager: endpoint {} failed: {}", endpoint, error_msg);
                            Err(anyhow!(e).context("RPC failed"))
                        }
                        Err(_elapsed) => {
                            warn!("RpcManager: endpoint {} timed out after {:?}", endpoint, timeout_duration);
                            Err(anyhow!("RPC send timeout"))
                        }
                    }
                });
            }

            // Wait for results with early cancellation
            while let Some(join_res) = set.join_next().await {
                match join_res {
                    Ok(Ok(sig)) => {
                        set.abort_all();
                        return Ok(sig);
                    }
                    Ok(Err(e)) => {
                        let error_str = e.to_string();
                        if Self::is_fatal_error_type(&error_str) {
                            fatal_errors += 1;
                            debug!("RpcManager: fatal error count: {}/{}", fatal_errors, self.config.early_cancel_threshold);
                            
                            // Early cancellation if too many fatal errors
                            if fatal_errors >= self.config.early_cancel_threshold {
                                warn!("RpcManager: cancelling remaining tasks due to {} fatal errors", fatal_errors);
                                set.abort_all();
                                break;
                            }
                        }
                        debug!("RpcManager: task returned error: {:?}", e);
                    }
                    Err(join_err) => {
                        warn!("RpcManager: task join error: {}", join_err);
                    }
                }
            }

            Err(anyhow!(
                "RpcManager: all sends failed (fatal_errors: {})", 
                fatal_errors
            ))
        })
    }
}