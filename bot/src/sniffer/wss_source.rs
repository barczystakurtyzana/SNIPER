use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use tokio::{
    sync::{mpsc::Sender, Notify, RwLock},
    time,
};
use tracing::{debug, error, info, warn};

use solana_client::nonblocking::pubsub_client::PubsubClient;
use solana_client::rpc_config::{RpcTransactionLogsConfig, RpcTransactionLogsFilter};
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};

use crate::config::Config;
use crate::sniffer::real::{fetch_meta_from_rpc, parse_pump_logs};
use crate::sniffer::source::{pump_fun_program_pk, CandidateSource};
use crate::time_utils::now_ms;
use crate::types::{PremintCandidate, ProgramLogEvent};

pub struct WssSource {
    cfg: Config,
    last_heartbeat: Arc<RwLock<Instant>>,
    stop_notify: Arc<Notify>,
}

impl WssSource {
    pub fn new(cfg: Config) -> Self {
        Self {
            cfg,
            last_heartbeat: Arc::new(RwLock::new(Instant::now())),
            stop_notify: Arc::new(Notify::new()),
        }
    }

    fn update_heartbeat(&self) {
        let lh = self.last_heartbeat.clone();
        tokio::spawn(async move {
            *lh.write().await = Instant::now();
        });
    }

    fn healthy_window(&self) -> Duration {
        Duration::from_millis(self.cfg.wss_max_silent_ms)
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
impl CandidateSource for WssSource {
    async fn run(
        &self,
        cand_tx: Sender<PremintCandidate>,
        raw_log_tx: Option<Sender<ProgramLogEvent>>,
    ) {
        if self.cfg.rpc_wss_endpoints.is_empty() {
            warn!(target:"sniffer", "WSS source: no rpc_wss_endpoints configured");
            loop {
                tokio::select! {
                    _ = self.stop_notify.notified() => {
                        warn!(target:"sniffer", "WSS source stop requested (no endpoints)");
                        return;
                    }
                    _ = time::sleep(Duration::from_millis(500)) => {}
                }
            }
        }

        let program = pump_fun_program_pk();
        let mut backoff = self.cfg.wss_reconnect_backoff_ms;
        let max_backoff = self.cfg.wss_reconnect_backoff_max_ms;

        loop {
            let notified = self.stop_notify.notified();
            tokio::pin!(notified);

            debug!(target: "sniffer", "WSS connecting…");
            match PubsubClient::new(&self.cfg.rpc_wss_endpoints[0]).await {
                Ok(client) => {
                    info!(target: "sniffer", "WSS connected to {}", &self.cfg.rpc_wss_endpoints[0]);

                    let commitment_cfg = self.commitment_config();
                    let (mut sub, unsub) = match client
                        .logs_subscribe(
                            RpcTransactionLogsFilter::Mentions(vec![program.to_string()]),
                            RpcTransactionLogsConfig {
                                commitment: Some(commitment_cfg),
                            },
                        )
                        .await
                    {
                        Ok((s, u)) => (s, u),
                        Err(e) => {
                            error!(target: "sniffer", ?e, "logs_subscribe failed");
                            time::sleep(Duration::from_millis(backoff)).await;
                            backoff = (backoff.saturating_mul(2)).min(max_backoff);
                            continue;
                        }
                    };

                    self.update_heartbeat();
                    backoff = self.cfg.wss_reconnect_backoff_ms;

                    loop {
                        tokio::select! {
                            _ = &mut notified => {
                                warn!(target:"sniffer", "WSS stop requested");
                                let _ = unsub().await;
                                return;
                            }
                            msg = sub.recv() => {
                                match msg {
                                    Some(ev) => {
                                        self.update_heartbeat();

                                        let sig = ev.value.signature.to_string();
                                        let slot = ev.context.slot;
                                        let logs = ev.value.logs.unwrap_or_default();
                                        let ts_ms = now_ms();

                                        if let Some(tx) = raw_log_tx.as_ref() {
                                            let _ = tx.send(ProgramLogEvent {
                                                slot,
                                                signature: sig.clone(),
                                                program: program.to_string(),
                                                logs: logs.clone(),
                                                ts_ms
                                            }).await;
                                        }

                                        let (maybe_mint, maybe_creator, _keys) = parse_pump_logs(&logs);
                                        if maybe_mint.is_none() || maybe_creator.is_none() {
                                            if self.cfg.meta_fetch_enabled {
                                                if let Ok((m, c)) = fetch_meta_from_rpc(
                                                    &self.cfg.rpc_endpoints[0],
                                                    &sig,
                                                    self.cfg.meta_fetch_commitment.as_deref().unwrap_or("confirmed"),
                                                ).await {
                                                    if let (Some(mint), Some(creator)) = (m, c) {
                                                        let _ = cand_tx.send(PremintCandidate {
                                                            mint,
                                                            creator,
                                                            program: program,
                                                            slot,
                                                            timestamp: ts_ms / 1000,
                                                        }).await;
                                                        continue;
                                                    }
                                                }
                                            }
                                            continue;
                                        }

                                        let _ = cand_tx.send(PremintCandidate {
                                            mint: maybe_mint.unwrap(),
                                            creator: maybe_creator.unwrap(),
                                            program,
                                            slot,
                                            timestamp: ts_ms / 1000,
                                        }).await;
                                    }
                                    None => {
                                        warn!(target: "sniffer", "WSS subscription ended");
                                        break;
                                    }
                                }
                            }
                            _ = time::sleep(Duration::from_millis(self.cfg.wss_heartbeat_ms)) => {
                                let last = *self.last_heartbeat.read().await;
                                if last.elapsed() > self.healthy_window() {
                                    warn!(target: "sniffer", "WSS heartbeat timeout (silent too long)");
                                    let _ = unsub().await;
                                    break;
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    error!(target: "sniffer", ?e, "WSS connect failed");
                }
            }

            time::sleep(Duration::from_millis(backoff)).await;
            backoff = (backoff.saturating_mul(2)).min(max_backoff);
        }
    }

    fn is_healthy(&self) -> bool {
        let last = futures::executor::block_on(self.last_heartbeat.read());
        last.elapsed() < self.healthy_window()
    }

    fn request_stop(&self) {
        self.stop_notify.notify_waiters();
    }
}