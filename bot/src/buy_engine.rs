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
use crate::nonce_manager::NonceManager;
use crate::rpc_manager::RpcBroadcaster;
use crate::types::{AppState, CandidateReceiver, Mode, PremintCandidate};

#[derive(Debug)]
pub struct BuyEngine {
    pub rpc: Arc<dyn RpcBroadcaster>,
    pub nonce_manager: Arc<NonceManager>,
    pub candidate_rx: CandidateReceiver,
    pub app_state: Arc<Mutex<AppState>>,
    pub config: Config,
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
                        if !self.is_candidate_interesting(&candidate) {
                            debug!(mint=%candidate.mint, program=%candidate.program, "Candidate filtered out");
                            continue;
                        }
                        info!(mint=%candidate.mint, program=%candidate.program, "Attempting BUY for candidate");

                        match self.try_buy(candidate.clone()).await {
                            Ok(sig) => {
                                info!(mint=%candidate.mint, sig=%sig, "BUY success, entering PassiveToken mode");

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
                                warn!(error=%e, "BUY attempt failed; staying in Sniffing");
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
        let pct = percent.clamp(0.0, 1.0);

        let (mode, candidate_opt, current_pct) = {
            let st = self.app_state.lock().await;
            (st.mode.clone(), st.active_token.clone(), st.holdings_percent)
        };

        let mint = match mode {
            Mode::PassiveToken(m) => m,
            Mode::Sniffing => {
                warn!("Sell requested in Sniffing mode; ignoring");
                return Err(anyhow!("not in PassiveToken mode"));
            }
        };

        let candidate = candidate_opt.ok_or_else(|| anyhow!("no active token in AppState"))?;
        let new_holdings = (current_pct * (1.0 - pct)).max(0.0);

        info!(mint=%mint, sell_percent=pct, "Composing SELL transaction");

        let sell_tx = Self::create_placeholder_tx(&candidate.mint, "sell");

        match self.rpc.send_on_many_rpc(vec![sell_tx]).await {
            Ok(sig) => {
                info!(mint=%mint, sig=%sig, "SELL broadcasted");
                let mut st = self.app_state.lock().await;
                st.holdings_percent = new_holdings;
                if st.holdings_percent <= f64::EPSILON {
                    info!(mint=%mint, "Sold 100%; returning to Sniffing mode");
                    st.mode = Mode::Sniffing;
                    st.active_token = None;
                    st.last_buy_price = None;
                }
                Ok(())
            }
            Err(e) => {
                error!(mint=%mint, error=%e, "SELL failed to broadcast");
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

    async fn try_buy(&self, candidate: PremintCandidate) -> Result<Signature> {
        let mut acquired_indices: Vec<usize> = Vec::new();
        let mut txs: Vec<VersionedTransaction> = Vec::new();

        for _ in 0..self.config.nonce_count {
            match self.nonce_manager.acquire_nonce().await {
                Ok((_nonce_pubkey, idx)) => {
                    acquired_indices.push(idx);
                    let tx = Self::create_placeholder_tx(&candidate.mint, "buy");
                    txs.push(tx);
                }
                Err(e) => {
                    warn!(error=%e, "Failed to acquire nonce; proceeding with fewer");
                    break;
                }
            }
        }

        if txs.is_empty() {
            for idx in acquired_indices.drain(..) {
                self.nonce_manager.release_nonce(idx);
            }
            return Err(anyhow!("no transactions prepared (no nonces acquired)"));
        }

        let res = self
            .rpc
            .send_on_many_rpc(txs)
            .await
            .context("broadcast BUY failed");

        for idx in acquired_indices {
            self.nonce_manager.release_nonce(idx);
        }

        res
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

    struct AlwaysOkBroadcaster;
    impl RpcBroadcaster for AlwaysOkBroadcaster {
        fn send_on_many_rpc<'a>(
            &'a self,
            _txs: Vec<VersionedTransaction>,
        ) -> Pin<Box<dyn Future<Output = Result<Signature>> + Send + 'a>> {
            Box::pin(async { Ok(Signature::new(&[7u8; 64])) })
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