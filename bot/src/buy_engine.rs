//! Core logic for auto-buy and one-token state machine.
//!
//! Responsibilities:
//! - Consume candidates from an mpsc receiver while in Sniffing mode.
//! - Filter candidates by simple heuristics (e.g., program == "pump.fun").
//! - Acquire up to N nonces, build N distinct transactions (skeleton), and broadcast via RpcBroadcaster.
//! - On first success, switch to PassiveToken mode (one-token mode) and hold until sold.
//! - Provide a sell(percent) API that reduces holdings and returns to Sniffing when 100% sold.

use std::{sync::Arc, time::Duration};

use anyhow::{anyhow, Context, Result};
use solana_sdk::{
    message::Message,
    pubkey::Pubkey,
    signature::Signature,
    system_instruction,
    transaction::{Transaction, VersionedTransaction},
};
use tokio::sync::Mutex;
use tokio::time::{sleep, timeout};
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::endpoints::endpoint_server;
use crate::metrics::{metrics, Timer};
use crate::nonce_manager::NonceManager;
use crate::rpc_manager::RpcBroadcaster;
use crate::security::validator;
use crate::structured_logging::PipelineContext;
use crate::tx_builder::{TransactionBuilder, TransactionConfig};
use crate::types::{AppState, CandidateReceiver, Mode, PremintCandidate};

pub struct BuyEngine {
    pub rpc: Arc<dyn RpcBroadcaster>,
    pub nonce_manager: Arc<NonceManager>,
    pub candidate_rx: CandidateReceiver,
    pub app_state: Arc<Mutex<AppState>>,
    pub config: Config,
    pub tx_builder: Option<TransactionBuilder>,
}

impl BuyEngine {
    pub async fn run(&mut self) {
        info!("BuyEngine started");
        loop {
            let sniffing = {
                let st = self.app_state.lock().await;
                st.is_sniffing()
            };

            if sniffing {
                match timeout(Duration::from_millis(1000), self.candidate_rx.recv()).await {
                    Ok(Some(candidate)) => {
                        // Validate candidate for security issues
                        let validation = validator().validate_candidate(&candidate);
                        if !validation.is_valid() {
                            metrics().increment_counter("buy_attempts_security_rejected");
                            warn!(mint=%candidate.mint, issues=?validation.issues, "Candidate rejected due to security validation");
                            continue;
                        }

                        // Check rate limiting to prevent spam
                        if !validator().check_mint_rate_limit(&candidate.mint, 60, 5) {
                            metrics().increment_counter("buy_attempts_rate_limited");
                            debug!(mint=%candidate.mint, "Candidate rate limited");
                            continue;
                        }

                        if !self.is_candidate_interesting(&candidate) {
                            metrics().increment_counter("buy_attempts_filtered");
                            debug!(mint=%candidate.mint, program=%candidate.program, "Candidate filtered out");
                            continue;
                        }
                        
                        // Create pipeline context for correlation tracking
                        let ctx = PipelineContext::new("buy_engine");
                        ctx.logger.log_candidate_processed(&candidate.mint.to_string(), &candidate.program, true);
                        
                        info!(mint=%candidate.mint, program=%candidate.program, correlation_id=ctx.correlation_id, "Attempting BUY for candidate");
                        metrics().increment_counter("buy_attempts_total");

                        let buy_timer = Timer::new("buy_latency_seconds");
                        match self.try_buy(candidate.clone(), ctx.clone()).await {
                            Ok(sig) => {
                                buy_timer.finish();
                                let latency_ms = std::time::Instant::now().elapsed().as_millis() as u64;
                                
                                metrics().increment_counter("buy_success_total");
                                ctx.logger.log_buy_success(&candidate.mint.to_string(), &sig.to_string(), latency_ms);
                                
                                // Update scoreboard
                                endpoint_server().update_scoreboard(&candidate.mint.to_string(), &candidate.program, true, latency_ms).await;
                                
                                info!(mint=%candidate.mint, sig=%sig, correlation_id=ctx.correlation_id, "BUY success, entering PassiveToken mode");

                                let exec_price = self.get_execution_price_mock(&candidate).await;

                                {
                                    let mut st = self.app_state.lock().await;
                                    st.mode = Mode::PassiveToken(candidate.mint);
                                    st.active_token = Some(candidate.clone());
                                    st.last_buy_price = Some(exec_price);
                                    st.holdings_percent = 1.0;
                                }

                                info!(mint=%candidate.mint, price=%exec_price, "Recorded buy price and entered PassiveToken");
                            }
                            Err(e) => {
                                buy_timer.finish();
                                let latency_ms = std::time::Instant::now().elapsed().as_millis() as u64;
                                
                                metrics().increment_counter("buy_failure_total");
                                ctx.logger.log_buy_failure(&candidate.mint.to_string(), &e.to_string(), latency_ms);
                                
                                // Update scoreboard with failure
                                endpoint_server().update_scoreboard(&candidate.mint.to_string(), &candidate.program, false, latency_ms).await;
                                
                                warn!(error=%e, correlation_id=ctx.correlation_id, "BUY attempt failed; staying in Sniffing");
                            }
                        }
                    }
                    Ok(None) => {
                        warn!("Candidate channel closed; BuyEngine exiting");
                        break;
                    }
                    Err(_) => {
                        continue;
                    }
                }
            } else {
                match timeout(Duration::from_millis(500), self.candidate_rx.recv()).await {
                    Ok(Some(c)) => {
                        debug!(mint=%c.mint, "Passive mode: ignoring candidate");
                    }
                    Ok(None) => {
                        warn!("Candidate channel closed; BuyEngine exiting");
                        break;
                    }
                    Err(_) => {
                        sleep(Duration::from_millis(50)).await;
                    }
                }
            }
        }
        info!("BuyEngine stopped");
    }

    pub async fn sell(&self, percent: f64) -> Result<()> {
        let ctx = PipelineContext::new("buy_engine_sell");

        // Validate holdings percentage for overflow protection
        let pct = match validator().validate_holdings_percent(percent.clamp(0.0, 1.0)) {
            Ok(validated_pct) => validated_pct,
            Err(e) => {
                ctx.logger.error("Invalid sell percentage", serde_json::json!({"error": e, "percent": percent}));
                return Err(anyhow!("Invalid sell percentage: {}", e));
            }
        };

        let (mode, candidate_opt, current_pct) = {
            let st = self.app_state.lock().await;
            (st.mode.clone(), st.active_token.clone(), st.holdings_percent)
        };

        let mint = match mode {
            Mode::PassiveToken(m) => m,
            Mode::Sniffing => {
                ctx.logger.warn("Sell requested in Sniffing mode; ignoring", serde_json::json!({"action": "sell_rejected"}));
                warn!(correlation_id=ctx.correlation_id, "Sell requested in Sniffing mode; ignoring");
                return Err(anyhow!("not in PassiveToken mode"));
            }
        };

        let _candidate = candidate_opt.ok_or_else(|| anyhow!("no active token in AppState"))?;
        
        // Validate the new holdings calculation
        let new_holdings = match validator().validate_holdings_percent((current_pct * (1.0 - pct)).max(0.0)) {
            Ok(validated_holdings) => validated_holdings,
            Err(e) => {
                ctx.logger.error("Holdings calculation overflow", serde_json::json!({"error": e, "current": current_pct, "sell": pct}));
                return Err(anyhow!("Holdings calculation error: {}", e));
            }
        };

        ctx.logger.log_sell_operation(&mint.to_string(), pct, new_holdings);
        info!(mint=%mint, sell_percent=pct, correlation_id=ctx.correlation_id, "Composing SELL transaction");

        let sell_tx = self.create_sell_transaction(&mint, pct).await?;

        match self.rpc.send_on_many_rpc(vec![sell_tx]).await {
            Ok(sig) => {
                // Check for duplicate signatures
                let sig_str = sig.to_string();
                if !validator().check_duplicate_signature(&sig_str) {
                    warn!(mint=%mint, sig=%sig, correlation_id=ctx.correlation_id, "Duplicate signature detected for SELL");
                    metrics().increment_counter("duplicate_signatures_detected");
                }
                
                info!(mint=%mint, sig=%sig, correlation_id=ctx.correlation_id, "SELL broadcasted");
                let mut st = self.app_state.lock().await;
                st.holdings_percent = new_holdings;
                if st.holdings_percent <= f64::EPSILON {
                    info!(mint=%mint, correlation_id=ctx.correlation_id, "Sold 100%; returning to Sniffing mode");
                    st.mode = Mode::Sniffing;
                    st.active_token = None;
                    st.last_buy_price = None;
                }
                Ok(())
            }
            Err(e) => {
                error!(mint=%mint, error=%e, correlation_id=ctx.correlation_id, "SELL failed to broadcast");
                Err(e)
            }
        }
    }

    fn is_candidate_interesting(&self, c: &PremintCandidate) -> bool {
        c.program == "pump.fun"
    }

    async fn get_execution_price_mock(&self, _candidate: &PremintCandidate) -> f64 {
        1.0_f64
    }

    async fn try_buy(&self, candidate: PremintCandidate, ctx: PipelineContext) -> Result<Signature> {
        let mut acquired_indices: Vec<usize> = Vec::new();
        let mut txs: Vec<VersionedTransaction> = Vec::new();

        // Get recent blockhash once for all transactions
        let recent_blockhash = match &self.tx_builder {
            Some(builder) => {
                // Try to get fresh blockhash, but don't fail if network is unavailable
                match tokio::time::timeout(Duration::from_millis(2000), async {
                    builder.rpc_client().get_latest_blockhash().await
                }).await {
                    Ok(Ok(hash)) => Some(hash),
                    _ => None, // Use None to let tx_builder handle it
                }
            }
            None => None,
        };

        for _ in 0..self.config.nonce_count {
            match self.nonce_manager.acquire_nonce().await {
                Ok((_nonce_pubkey, idx)) => {
                    ctx.logger.log_nonce_operation("acquire", Some(idx), true);
                    acquired_indices.push(idx);
                    let tx = self.create_buy_transaction(&candidate, recent_blockhash).await?;
                    txs.push(tx);
                }
                Err(e) => {
                    ctx.logger.log_nonce_operation("acquire_failed", None, false);
                    warn!(error=%e, correlation_id=ctx.correlation_id, "Failed to acquire nonce; proceeding with fewer");
                    break;
                }
            }
        }

        if txs.is_empty() {
            for idx in acquired_indices.drain(..) {
                ctx.logger.log_nonce_operation("release", Some(idx), true);
                self.nonce_manager.release_nonce(idx);
            }
            return Err(anyhow!("no transactions prepared (no nonces acquired)"));
        }

        ctx.logger.log_buy_attempt(&candidate.mint.to_string(), txs.len());
        
        let res = self
            .rpc
            .send_on_many_rpc(txs)
            .await
            .context("broadcast BUY failed");

        for idx in acquired_indices {
            ctx.logger.log_nonce_operation("release", Some(idx), true);
            self.nonce_manager.release_nonce(idx);
        }

        res
    }

    async fn create_buy_transaction(
        &self,
        candidate: &PremintCandidate,
        recent_blockhash: Option<solana_sdk::hash::Hash>,
    ) -> Result<VersionedTransaction> {
        match &self.tx_builder {
            Some(builder) => {
                let config = TransactionConfig::default();
                builder.build_buy_transaction(candidate, &config, recent_blockhash).await
            }
            None => {
                // Fallback to placeholder for testing/mock mode
                Ok(Self::create_placeholder_tx(&candidate.mint, "buy"))
            }
        }
    }

    async fn create_sell_transaction(
        &self,
        mint: &Pubkey,
        sell_percent: f64,
    ) -> Result<VersionedTransaction> {
        match &self.tx_builder {
            Some(builder) => {
                let config = TransactionConfig::default();
                builder.build_sell_transaction(mint, sell_percent, &config, None).await
            }
            None => {
                // Fallback to placeholder for testing/mock mode
                Ok(Self::create_placeholder_tx(mint, "sell"))
            }
        }
    }

    fn create_placeholder_tx(_token_mint: &Pubkey, _action: &str) -> VersionedTransaction {
        let from = Pubkey::new_unique();
        let to = Pubkey::new_unique();
        let ix = system_instruction::transfer(&from, &to, 1);
        let msg = Message::new(&[ix], None);
        let tx = Transaction::new_unsigned(msg);
        VersionedTransaction::from(tx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::pin::Pin;
    use tokio::sync::mpsc;

    #[derive(Debug)]
    struct AlwaysOkBroadcaster;
    impl RpcBroadcaster for AlwaysOkBroadcaster {
        fn send_on_many_rpc<'a>(
            &'a self,
            _txs: Vec<VersionedTransaction>,
        ) -> Pin<Box<dyn Future<Output = Result<Signature>> + Send + 'a>> {
            Box::pin(async { Ok(Signature::from([7u8; 64])) })
        }
    }

    #[tokio::test]
    async fn buy_enters_passive_and_sell_returns_to_sniffing() {
        let (tx, rx): (mpsc::Sender<PremintCandidate>, mpsc::Receiver<PremintCandidate>) =
            mpsc::channel(8);

        let app_state = Arc::new(Mutex::new(AppState {
            mode: Mode::Sniffing,
            active_token: None,
            last_buy_price: None,
            holdings_percent: 0.0,
        }));

        let mut engine = BuyEngine {
            rpc: Arc::new(AlwaysOkBroadcaster),
            nonce_manager: Arc::new(NonceManager::new(2)),
            candidate_rx: rx,
            app_state: app_state.clone(),
            config: Config {
                nonce_count: 1,
                ..Config::default()
            },
            tx_builder: None, // No transaction builder for tests
        };

        let candidate = PremintCandidate {
            mint: Pubkey::new_unique(),
            creator: Pubkey::new_unique(),
            program: "pump.fun".to_string(),
            slot: 0,
            timestamp: 0,
        };
        tx.send(candidate).await.unwrap();
        drop(tx);

        engine.run().await;

        {
            let st = app_state.lock().await;
            match st.mode {
                Mode::PassiveToken(_) => {}
                _ => panic!("Expected PassiveToken mode after buy"),
            }
            assert_eq!(st.holdings_percent, 1.0);
            assert!(st.last_buy_price.is_some());
            assert!(st.active_token.is_some());
        }

        engine.sell(1.0).await.expect("sell should succeed");
        let st = app_state.lock().await;
        assert!(st.is_sniffing());
        assert!(st.active_token.is_none());
        assert!(st.last_buy_price.is_none());
    }
}