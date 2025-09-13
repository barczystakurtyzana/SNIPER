//! Core logic for auto-buy and one-token state machine.
//!
//! Responsibilities:
//! - Consume candidates from an mpsc receiver while in Sniffing mode.
//! - Filter candidates by simple heuristics (e.g., program == "pump.fun").
//! - Acquire up to N nonces, build N distinct transactions (skeleton), and broadcast via RpcBroadcaster.
//! - On first success, switch to PassiveToken mode (one-token mode) and hold until sold.
//! - Provide a sell(percent) API that reduces holdings and returns to Sniffing when 100% sold.

use std::{sync::{Arc, atomic::{AtomicBool, AtomicU32, Ordering}}, time::{Duration, Instant}};

use anyhow::{anyhow, Context, Result};
use solana_sdk::{
    pubkey::Pubkey,
    signature::Signature,
    transaction::VersionedTransaction,
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

/// Exponential backoff state for failure handling
#[derive(Debug)]
struct BackoffState {
    consecutive_failures: AtomicU32,
    last_failure: Mutex<Option<Instant>>,
    base_delay_ms: u64,
    max_delay_ms: u64,
    backoff_multiplier: f64,
}

impl BackoffState {
    fn new() -> Self {
        Self {
            consecutive_failures: AtomicU32::new(0),
            last_failure: Mutex::new(None),
            base_delay_ms: 100,
            max_delay_ms: 10_000,
            backoff_multiplier: 2.0,
        }
    }

    async fn record_failure(&self) {
        let failures = self.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;
        let mut last_failure = self.last_failure.lock().await;
        *last_failure = Some(Instant::now());
        debug!("BackoffState: recorded failure #{}", failures);
    }

    async fn record_success(&self) {
        let prev_failures = self.consecutive_failures.swap(0, Ordering::Relaxed);
        if prev_failures > 0 {
            info!("BackoffState: success after {} failures, resetting backoff", prev_failures);
        }
        let mut last_failure = self.last_failure.lock().await;
        *last_failure = None;
    }

    async fn should_backoff(&self) -> Option<Duration> {
        let failures = self.consecutive_failures.load(Ordering::Relaxed);
        if failures == 0 {
            return None;
        }

        let delay_ms = (self.base_delay_ms as f64 * self.backoff_multiplier.powi((failures - 1) as i32))
            .min(self.max_delay_ms as f64) as u64;
        
        Some(Duration::from_millis(delay_ms))
    }

    fn get_failure_count(&self) -> u32 {
        self.consecutive_failures.load(Ordering::Relaxed)
    }
}

pub struct BuyEngine {
    pub rpc: Arc<dyn RpcBroadcaster>,
    pub nonce_manager: Arc<NonceManager>,
    pub candidate_rx: CandidateReceiver,
    pub app_state: Arc<Mutex<AppState>>,
    pub config: Config,
    pub tx_builder: Option<TransactionBuilder>,
    backoff_state: BackoffState,
    pending_buy: Arc<AtomicBool>,
}

impl BuyEngine {
    pub fn new(
        rpc: Arc<dyn RpcBroadcaster>,
        nonce_manager: Arc<NonceManager>,
        candidate_rx: CandidateReceiver,
        app_state: Arc<Mutex<AppState>>,
        config: Config,
        tx_builder: Option<TransactionBuilder>,
    ) -> Self {
        Self {
            rpc,
            nonce_manager,
            candidate_rx,
            app_state,
            config,
            tx_builder,
            backoff_state: BackoffState::new(),
            pending_buy: Arc::new(AtomicBool::new(false)),
        }
    }

    pub async fn run(&mut self) {
        info!("BuyEngine started");
        loop {
            let sniffing = {
                let st = self.app_state.lock().await;
                st.is_sniffing()
            };

            if sniffing {
                // Check if we should backoff due to recent failures
                if let Some(backoff_duration) = self.backoff_state.should_backoff().await {
                    let failure_count = self.backoff_state.get_failure_count();
                    warn!("BuyEngine: backing off for {:?} after {} consecutive failures", 
                          backoff_duration, failure_count);
                    sleep(backoff_duration).await;
                    continue;
                }

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
                                
                                StructuredLogger::log_buy_success(
                                    &correlation_id,
                                    &candidate.mint.to_string(),
                                    &sig.to_string(),
                                    exec_price,
                                    latency_ms,
                                );
                                
                                info!(mint=%candidate.mint, sig=%sig, "BUY success, entering PassiveToken mode");
                                self.backoff_state.record_success().await;

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

        // Check if there's a pending buy operation
        if self.pending_buy.load(Ordering::Relaxed) {
            warn!("Sell requested while buy is pending; rejecting to avoid race condition");
            return Err(anyhow!("buy operation in progress"));
        }

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

        match self.rpc.send_on_many_rpc(vec![sell_tx], None).await {
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

    /// Protected buy operation with atomic guards and proper lease management
    async fn try_buy_with_guards(&self, candidate: PremintCandidate, correlation_id: CorrelationId) -> Result<Signature> {
        // Set pending flag atomically
        if self.pending_buy.compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed).is_err() {
            return Err(anyhow!("buy operation already in progress"));
        }

        // Ensure we clear the pending flag on exit
        let _guard = scopeguard::guard((), |_| {
            self.pending_buy.store(false, Ordering::Relaxed);
        });

        // Call the actual buy logic
        self.try_buy(candidate, PipelineContext::default()).await
    }

    async fn try_buy(&self, candidate: PremintCandidate, ctx: PipelineContext) -> Result<Signature> {
        let mut acquired_indices: Vec<usize> = Vec::new();

        let mut txs: Vec<VersionedTransaction> = Vec::new();

        // Get recent blockhash once for all transactions
        let recent_blockhash = self.get_recent_blockhash().await;

        for _ in 0..self.config.nonce_count {
            match self.nonce_manager.acquire_nonce().await {

                Ok((_nonce_pubkey, idx)) => {
                    ctx.logger.log_nonce_operation("acquire", Some(idx), true);
                    acquired_indices.push(idx);

                    let tx = self.create_buy_transaction(&candidate, recent_blockhash).await?;
                    txs.push(tx);

                    acquired_leases.push(lease);

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
            .send_on_many_rpc(txs, Some(correlation_id))
            .await
            .context("broadcast BUY failed");



        for idx in acquired_indices {

            ctx.logger.log_nonce_operation("release", Some(idx), true);
            self.nonce_manager.release_nonce(idx);

        }

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
                #[cfg(any(test, feature = "mock-mode"))]
                {
                    Ok(Self::create_placeholder_tx(&candidate.mint, "buy"))
                }
                #[cfg(not(any(test, feature = "mock-mode")))]
                {
                    Err(anyhow!("No transaction builder available in production mode"))
                }
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
                #[cfg(any(test, feature = "mock-mode"))]
                {
                    Ok(Self::create_placeholder_tx(mint, "sell"))
                }
                #[cfg(not(any(test, feature = "mock-mode")))]
                {
                    Err(anyhow!("No transaction builder available in production mode"))
                }
            }
        }
    }

    #[cfg(any(test, feature = "mock-mode"))]
    fn create_placeholder_tx(_token_mint: &Pubkey, _action: &str) -> VersionedTransaction {
        use solana_sdk::{message::Message, system_instruction, transaction::Transaction};
        
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
            _correlation_id: Option<CorrelationId>,
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

        let mut engine = BuyEngine::new(
            Arc::new(AlwaysOkBroadcaster),
            Arc::new(NonceManager::new(2)),
            rx,
            app_state.clone(),
            Config {
                nonce_count: 1,
                ..Config::default()
            },
            None, // No transaction builder for tests
        );

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

    #[tokio::test]
    async fn test_backoff_behavior() {
        let (tx, rx): (mpsc::Sender<PremintCandidate>, mpsc::Receiver<PremintCandidate>) =
            mpsc::channel(8);

        let app_state = Arc::new(Mutex::new(AppState {
            mode: Mode::Sniffing,
            active_token: None,
            last_buy_price: None,
            holdings_percent: 0.0,
        }));

        #[derive(Debug)]
        struct FailingBroadcaster;
        impl RpcBroadcaster for FailingBroadcaster {
            fn send_on_many_rpc<'a>(
                &'a self,
                _txs: Vec<VersionedTransaction>,
                _correlation_id: Option<CorrelationId>,
            ) -> Pin<Box<dyn Future<Output = Result<Signature>> + Send + 'a>> {
                Box::pin(async { Err(anyhow!("simulated failure")) })
            }
        }

        let engine = BuyEngine::new(
            Arc::new(FailingBroadcaster),
            Arc::new(NonceManager::new(2)),
            rx,
            app_state.clone(),
            Config {
                nonce_count: 1,
                ..Config::default()
            },
            None,
        );

        // Test backoff state
        assert_eq!(engine.backoff_state.get_failure_count(), 0);
        
        engine.backoff_state.record_failure().await;
        assert_eq!(engine.backoff_state.get_failure_count(), 1);
        
        let backoff_duration = engine.backoff_state.should_backoff().await;
        assert!(backoff_duration.is_some());
        assert!(backoff_duration.unwrap().as_millis() >= 100);
        
        engine.backoff_state.record_success().await;
        assert_eq!(engine.backoff_state.get_failure_count(), 0);
        
        let no_backoff = engine.backoff_state.should_backoff().await;
        assert!(no_backoff.is_none());
    }

    #[tokio::test]
    async fn test_atomic_buy_protection() {
        let (tx, rx): (mpsc::Sender<PremintCandidate>, mpsc::Receiver<PremintCandidate>) =
            mpsc::channel(8);

        let app_state = Arc::new(Mutex::new(AppState {
            mode: Mode::Sniffing,
            active_token: None,
            last_buy_price: None,
            holdings_percent: 0.0,
        }));

        let engine = BuyEngine::new(
            Arc::new(AlwaysOkBroadcaster),
            Arc::new(NonceManager::new(2)),
            rx,
            app_state.clone(),
            Config {
                nonce_count: 1,
                ..Config::default()
            },
            None,
        );

        let candidate = PremintCandidate {
            mint: Pubkey::new_unique(),
            creator: Pubkey::new_unique(),
            program: "pump.fun".to_string(),
            slot: 0,
            timestamp: 0,
        };

        // First buy should succeed
        let correlation_id = CorrelationId::new();
        let result1 = engine.try_buy_with_guards(candidate.clone(), correlation_id).await;
        assert!(result1.is_ok());

        // Immediate second buy should fail due to pending flag
        engine.pending_buy.store(true, Ordering::Relaxed);
        let correlation_id2 = CorrelationId::new();
        let result2 = engine.try_buy_with_guards(candidate, correlation_id2).await;
        assert!(result2.is_err());
        assert!(result2.unwrap_err().to_string().contains("already in progress"));
    }

    #[tokio::test]
    async fn test_sell_buy_race_protection() {
        let (_tx, rx): (mpsc::Sender<PremintCandidate>, mpsc::Receiver<PremintCandidate>) =
            mpsc::channel(8);

        let app_state = Arc::new(Mutex::new(AppState {
            mode: Mode::PassiveToken(Pubkey::new_unique()),
            active_token: Some(PremintCandidate {
                mint: Pubkey::new_unique(),
                creator: Pubkey::new_unique(),
                program: "pump.fun".to_string(),
                slot: 0,
                timestamp: 0,
            }),
            last_buy_price: Some(1.0),
            holdings_percent: 1.0,
        }));

        let engine = BuyEngine::new(
            Arc::new(AlwaysOkBroadcaster),
            Arc::new(NonceManager::new(2)),
            rx,
            app_state.clone(),
            Config::default(),
            None,
        );

        // Simulate pending buy
        engine.pending_buy.store(true, Ordering::Relaxed);

        // Sell should fail due to pending buy
        let result = engine.sell(0.5).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("buy operation in progress"));
    }

    #[tokio::test]
    async fn test_nonce_lease_raii_behavior() {
        let (_tx, rx): (mpsc::Sender<PremintCandidate>, mpsc::Receiver<PremintCandidate>) =
            mpsc::channel(8);

        let app_state = Arc::new(Mutex::new(AppState {
            mode: Mode::Sniffing,
            active_token: None,
            last_buy_price: None,
            holdings_percent: 0.0,
        }));

        let nonce_manager = Arc::new(NonceManager::new(2));

        let engine = BuyEngine::new(
            Arc::new(AlwaysOkBroadcaster),
            Arc::clone(&nonce_manager),
            rx,
            app_state.clone(),
            Config {
                nonce_count: 2,
                ..Config::default()
            },
            None,
        );

        // All permits should be available initially
        assert_eq!(nonce_manager.available_permits(), 2);

        let candidate = PremintCandidate {
            mint: Pubkey::new_unique(),
            creator: Pubkey::new_unique(),
            program: "pump.fun".to_string(),
            slot: 0,
            timestamp: 0,
        };

        // Perform buy operation - should acquire and release nonces automatically
        let correlation_id = CorrelationId::new();
        let result = engine.try_buy_with_guards(candidate, correlation_id).await;
        assert!(result.is_ok());

        // Give time for async cleanup
        tokio::time::sleep(Duration::from_millis(50)).await;

        // All permits should be available again after RAII cleanup
        assert_eq!(nonce_manager.available_permits(), 2);
    }
}