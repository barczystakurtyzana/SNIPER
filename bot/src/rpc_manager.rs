use anyhow::{anyhow, Context, Result};
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::RpcSendTransactionConfig,
};
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
    signature::Signature,
    transaction::VersionedTransaction,
};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use tokio::{task::JoinSet, time::timeout};
use tracing::{debug, info, warn};

/// Trait for broadcasting transactions. Allows injecting mock implementations for tests.
pub trait RpcBroadcaster: Send + Sync {
    /// Broadcast the prepared VersionedTransaction objects; return first successful Signature or Err.
    fn send_on_many_rpc<'a>(
        &'a self,
        txs: Vec<VersionedTransaction>,
    ) -> Pin<Box<dyn Future<Output = Result<Signature>> + Send + 'a>>;
}

/// Production RpcManager that broadcasts to multiple HTTP RPC endpoints.
#[derive(Debug, Clone)]
pub struct RpcManager {
    pub endpoints: Vec<String>,
}

impl RpcManager {
    pub fn new(endpoints: Vec<String>) -> Self {
        Self { endpoints }
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