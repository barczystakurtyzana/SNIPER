use std::{
    collections::VecDeque,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use itertools::Itertools;
use tokio::{
    sync::{mpsc::Sender, Notify, RwLock},
    time,
};
use tracing::{debug, error, warn};

use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_client::GetConfirmedSignaturesForAddress2Config;
use solana_client::rpc_config::RpcTransactionConfig;
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
    signature::Signature,
};
use solana_transaction_status::UiTransactionEncoding;

use crate::config::Config;
use crate::sniffer::real::parse_pump_logs;
use crate::sniffer::source::{pump_fun_program_pk, CandidateSource};
use crate::time_utils::now_ms;
use crate::types::{PremintCandidate, ProgramLogEvent};

pub struct HttpSource {
    cfg: Config,
    last_seen: Arc<RwLock<VecDeque<Signature>>>, // simple recent signatures queue
    stop_notify: Arc<Notify>,
    healthy: Arc<RwLock<bool>>,
}

impl HttpSource {
    pub fn new(cfg: Config) -> Self {
        Self {
            cfg,
            last_seen: Arc::new(RwLock::new(VecDeque::with_capacity(2048))),
            stop_notify: Arc::new(Notify::new()),
            healthy: Arc::new(RwLock::new(false)),
        }
    }

    async fn mark_healthy(&self, val: bool) {
        *self.healthy.write().await = val;
    }

    async fn push_seen(&self, sig: Signature) {
        let mut q = self.last_seen.write().await;
        if q.len() >= 2048 {
            q.pop_front();
        }
        q.push_back(sig);
    }

    fn commitment_config(&self) -> CommitmentConfig {
        let level = match self
            .cfg
            .meta_fetch_commitment
            .as_deref()
            .unwrap_or("confirmed")
            .to_ascii_lowercase()
            .as_str()
        {
            "processed" => CommitmentLevel::Processed,
            "finalized" => CommitmentLevel::Finalized,
            _ => CommitmentLevel::Confirmed,
        };
        CommitmentConfig { commitment: level }
    }
}

#[async_trait]
impl CandidateSource for HttpSource {
    async fn run(
        &self,
        cand_tx: Sender<PremintCandidate>,
        raw_log_tx: Option<Sender<ProgramLogEvent>>,
    ) {
        if self.cfg.rpc_endpoints.is_empty() {
            warn!(target:"sniffer", "HTTP source: no rpc_endpoints configured");
            loop {
                tokio::select! {
                    _ = self.stop_notify.notified() => {
                        warn!(target:"sniffer", "HTTP source stop requested (no endpoints)");
                        return;
                    }
                    _ = time::sleep(Duration::from_millis(500)) => {}
                }
            }
        }

        let program = pump_fun_program_pk();
        let http = RpcClient::new_with_commitment(
            self.cfg.rpc_endpoints[0].clone(),
            self.commitment_config(),
        );

        loop {
            let notified = self.stop_notify.notified();
            tokio::pin!(notified);

            tokio::select! {
                _ = &mut notified => {
                    warn!(target:"sniffer", "HTTP poller stop requested");
                    return;
                }
                _ = time::sleep(Duration::from_millis(self.cfg.http_poll_interval_ms)) => {
                    let res = http.get_signatures_for_address_with_config(
                        &program,
                        GetConfirmedSignaturesForAddress2Config {
                            limit: Some(self.cfg.http_sig_depth.min(1000)),
                            ..Default::default()
                        }
                    ).await;

                    let sigs = match res {
                        Ok(v) => {
                            self.mark_healthy(true).await;
                            v.into_iter().filter_map(|x| x.signature.parse::<Signature>().ok()).collect_vec()
                        }
                        Err(e) => {
                            self.mark_healthy(false).await;
                            error!(target:"sniffer", ?e, "getSignaturesForAddress error");
                            continue;
                        }
                    };

                    if sigs.is_empty() { continue; }

                    let new_sigs = {
                        let seen = self.last_seen.read().await;
                        sigs.into_iter().filter(|s| !seen.contains(s)).collect_vec()
                    };

                    if new_sigs.is_empty() { continue; }

                    let sem = Arc::new(tokio::sync::Semaphore::new(self.cfg.http_max_parallel_tx_fetch.max(1)));
                    let mut tasks = Vec::with_capacity(new_sigs.len());
                    for sig in new_sigs {
                        let endpoint = self.cfg.rpc_endpoints[0].clone();
                        let sem = sem.clone();
                        let raw_log_tx = raw_log_tx.clone();
                        let cand_tx = cand_tx.clone();
                        let program_str = program.to_string();
                        let commitment = self.commitment_config();

                        tasks.push(tokio::spawn(async move {
                            let _permit = sem.acquire().await.expect("semaphore");
                            
                            let http = RpcClient::new_with_commitment(endpoint, commitment);
                            let tx = http.get_transaction_with_config(
                                &sig,
                                RpcTransactionConfig {
                                    encoding: Some(UiTransactionEncoding::Json),
                                    commitment: Some(commitment),
                                    max_supported_transaction_version: Some(0),
                                }
                            ).await;

                            if let Ok(txres) = tx {
                                let slot = txres.slot;
                                if let Some(meta) = txres.transaction.meta {
                                    if let Some(logs) = Option::<Vec<String>>::from(meta.log_messages) {
                                        let ts_ms = now_ms();

                                        if let Some(tx_ch) = raw_log_tx.as_ref() {
                                            let _ = tx_ch.send(ProgramLogEvent {
                                                slot,
                                                signature: sig.to_string(),
                                                program: program_str.clone(),
                                                logs: logs.clone(),
                                                ts_ms
                                            }).await;
                                        }

                                        let (maybe_mint, maybe_creator, _k) = parse_pump_logs(&logs);
                                        if let (Some(mint), Some(creator)) = (maybe_mint, maybe_creator) {
                                            let _ = cand_tx.send(PremintCandidate {
                                                mint,
                                                creator,
                                                program: program_str.clone(),
                                                slot,
                                                timestamp: ts_ms / 1000,
                                                instruction_summary: Some("HTTP mint".to_string()),
                                                is_jito_bundle: None,
                                            }).await;
                                        }
                                    }
                                }
                            } else if let Err(e) = tx {
                                debug!(target:"sniffer", ?e, signature=%sig, "getTransaction error");
                            }

                            sig
                        }));
                    }

                    for t in tasks {
                        if let Ok(sig) = t.await {
                            self.push_seen(sig).await;
                        }
                    }
                }
            }
        }
    }

    fn is_healthy(&self) -> bool {
        futures::executor::block_on(self.healthy.read()).to_owned()
    }

    fn request_stop(&self) {
        self.stop_notify.notify_waiters();
    }
}