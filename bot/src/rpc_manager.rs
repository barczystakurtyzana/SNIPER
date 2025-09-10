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

use std::{collections::HashMap, future::Future, sync::Arc};
use std::pin::Pin;
use std::time::Duration;

use tokio::{sync::RwLock, task::JoinSet, time::timeout};
use tracing::{debug, info, warn};


use crate::config::{BroadcastMode, Config};

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
}


    fn success_rate(&self) -> f64 {
        let total = self.success_count + self.error_count;
        if total == 0 {
            1.0 // Assume good until proven otherwise
        } else {
            self.success_count as f64 / total as f64

        }
        ClientErrorKind::Io(_) => RpcErrorType::Timeout,
        ClientErrorKind::Reqwest(_) => RpcErrorType::Timeout,
        _ => RpcErrorType::Other(error.to_string()),
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
    pub fn new(endpoints: Vec<String>) -> Self {

        Self { 
            endpoints,
            client_pool: Arc::new(RwLock::new(HashMap::new())),
        }
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
}

impl Clone for RpcManager {
    fn clone(&self) -> Self {
        Self {
            endpoints: self.endpoints.clone(),
            client_pool: self.client_pool.clone(),
        }

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


            let timeout_duration = Duration::from_secs(self.config.rpc_timeout_sec);
            
            // Fix commitment mismatch - use Confirmed consistently
            let send_cfg = RpcSendTransactionConfig {
                skip_preflight: true,
                preflight_commitment: Some(CommitmentLevel::Confirmed), // Fixed: was Processed
                max_retries: Some(3),
                ..Default::default()
            };

            // Get ranked endpoints by performance
            let ranked_endpoints = self.get_ranked_endpoints().await;

            // Generate tasks based on broadcast mode
            let tasks = match self.config.broadcast_mode {
                BroadcastMode::Pairwise => self.create_pairwise_tasks(&txs, &ranked_endpoints),
                BroadcastMode::Replicate => self.create_replicate_tasks(&txs, &ranked_endpoints),
                BroadcastMode::RoundRobin => self.create_round_robin_tasks(&txs, &ranked_endpoints),
                BroadcastMode::FullFanout => self.create_fanout_tasks(&txs, &ranked_endpoints),
            };

            info!(
                "RpcManager: broadcasting with {:?} mode - {} task(s) across {} endpoint(s)",
                self.config.broadcast_mode, tasks.len(), self.endpoints.len()
            );

            let mut set: JoinSet<Result<Signature>> = JoinSet::new();
            let mut fatal_errors = 0;


            for i in 0..n {
                let endpoint = self.endpoints[i].clone();
                let tx = txs[i].clone();
                let client_pool = self.client_pool.clone();

                set.spawn(async move {
                    // Use the pooled client instead of creating a new one
                    let rpc_manager = RpcManager {
                        endpoints: vec![endpoint.clone()],
                        client_pool,
                    };
                    let rpc = rpc_manager.get_or_create_client(&endpoint, commitment).await;
                    debug!("RpcManager: sending tx on endpoint[{}]: {}", i, endpoint);


                    let start_time = Instant::now();
                    debug!("RpcManager: sending tx on endpoint: {}", endpoint);

                    let send_fut = client.send_transaction_with_config(&tx, send_cfg);
                    match timeout(timeout_duration, send_fut).await {
                        Ok(Ok(sig)) => {
                            let latency_ms = start_time.elapsed().as_millis() as u64;
                            info!("RpcManager: success on {}: {} ({}ms)", endpoint, sig, latency_ms);
                            
                            // Record success metrics
                            self_clone.record_metrics(&endpoint, true, Some(latency_ms)).await;
                            Ok(sig)
                        }
                        Ok(Err(e)) => {
                            let error_msg = e.to_string();
                            warn!("RpcManager: endpoint {} failed: {}", endpoint, error_msg);
                            
                            // Record error metrics
                            self_clone.record_metrics(&endpoint, false, None).await;
                            
                            // Check if this is a fatal error type
                            let is_fatal = Self::is_fatal_error_type(&error_msg);
                            Err(anyhow!(e).context(format!("RPC failed (fatal: {})", is_fatal)))
                        }
                        Err(_elapsed) => {
                            warn!("RpcManager: endpoint {} timed out after {:?}", endpoint, timeout_duration);
                            
                            // Record timeout as error
                            self_clone.record_metrics(&endpoint, false, None).await;
                            Err(anyhow!("RPC send timeout"))
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
        }


            Err(anyhow!(
                "RpcManager: all sends failed (fatal_errors: {})", 
                fatal_errors
            ))
        })

    }
}