use anyhow::{anyhow, Result};
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::RpcSendTransactionConfig,
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

/// Trait for broadcasting transactions. Allows injecting mock implementations for tests.
pub trait RpcBroadcaster: Send + Sync + std::fmt::Debug {
    /// Broadcast the prepared VersionedTransaction objects; return first successful Signature or Err.
    fn send_on_many_rpc<'a>(
        &'a self,
        txs: Vec<VersionedTransaction>,
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
    ) -> Pin<Box<dyn Future<Output = Result<Signature>> + Send + 'a>> {
        Box::pin(async move {
            let n = self.endpoints.len().min(txs.len());
            if n == 0 {
                return Err(anyhow!(
                    "send_on_many_rpc: no endpoints or no transactions to send (endpoints={}, txs={})",
                    self.endpoints.len(),
                    txs.len()
                ));
            }

            const SEND_TIMEOUT: Duration = Duration::from_secs(8);
            let commitment = CommitmentConfig {
                commitment: CommitmentLevel::Confirmed,
            };

            let send_cfg = RpcSendTransactionConfig {
                skip_preflight: true,
                preflight_commitment: Some(CommitmentLevel::Processed),
                max_retries: Some(3),
                ..Default::default()
            };

            info!(
                "RpcManager: broadcasting {} tx(s) across {} endpoint(s)",
                n, n
            );

            let mut set: JoinSet<Result<Signature>> = JoinSet::new();

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
                            warn!(
                                "RpcManager: endpoint[{}] send failed on {}: {}",
                                i, endpoint, e
                            );
                            Err(anyhow!(e).context("RPC send_transaction_with_config failed"))
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

            Err(anyhow!(
                "RpcManager: all sends failed across {} pair(s)",
                n
            ))
        })
    }
}