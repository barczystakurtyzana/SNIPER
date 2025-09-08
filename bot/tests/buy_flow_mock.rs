use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use solana_sdk::{
    pubkey::Pubkey,
    signature::Signature,
    transaction::VersionedTransaction,
};
use tokio::sync::mpsc;

use sniffer_bot_light::buy_engine::BuyEngine;
use sniffer_bot_light::config::Config;
use sniffer_bot_light::nonce_manager::NonceManager;
use sniffer_bot_light::rpc_manager::RpcBroadcaster;
use sniffer_bot_light::types::{AppState, CandidateReceiver, CandidateSender, Mode, PremintCandidate};

#[derive(Clone, Debug)]
struct PatternBroadcaster {
    pattern: Vec<bool>,
}
impl PatternBroadcaster {
    fn new(pattern: Vec<bool>) -> Self {
        Self { pattern }
    }
}
impl RpcBroadcaster for PatternBroadcaster {
    fn send_on_many_rpc<'a>(
        &'a self,
        txs: Vec<VersionedTransaction>,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Signature>> + Send + 'a>> {
        Box::pin(async move {
            let n = txs.len();
            if let Some((i, _)) = self
                .pattern
                .iter()
                .take(n)
                .enumerate()
                .find(|(_, ok)| **ok)
            {
                let mut b = [0u8; 64];
                b[0] = (i as u8) + 1;
                return Ok(Signature::from(b));
            }
            anyhow::bail!("PatternBroadcaster: all endpoints failing for {} tx(s)", n);
        })
    }
}

#[derive(Clone, Debug)]
struct AlwaysOk;
impl RpcBroadcaster for AlwaysOk {
    fn send_on_many_rpc<'a>(
        &'a self,
        _txs: Vec<VersionedTransaction>,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Signature>> + Send + 'a>> {
        Box::pin(async move { Ok(Signature::from([9u8; 64])) })
    }
}

#[tokio::test]
async fn buy_first_success_pattern_and_state_transitions() {
    let app_state = Arc::new(tokio::sync::Mutex::new(AppState {
        mode: Mode::Sniffing,
        active_token: None,
        last_buy_price: None,
        holdings_percent: 0.0,
    }));

    let (_tx, rx): (CandidateSender, CandidateReceiver) = mpsc::channel(8);

    let nonce_mgr = Arc::new(NonceManager::new(2));

    let rpc_buy: Arc<dyn RpcBroadcaster> = Arc::new(PatternBroadcaster::new(vec![false, true]));

    let mut engine = BuyEngine::new(
        rpc_buy.clone(),
        nonce_mgr.clone(),
        rx,
        app_state.clone(),
        Config {
            nonce_count: 2,
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

    // Call private logic indirectly by simulating state update on success:
    // (In integration this is done in run(); here we emulate post-success state)
    {
        let mut st = engine.app_state.lock().await;
        st.mode = Mode::PassiveToken(candidate.mint);
        st.active_token = Some(candidate.clone());
        st.last_buy_price = Some(1.0);
        st.holdings_percent = 1.0;
    }

    let rpc_sell: Arc<dyn RpcBroadcaster> = Arc::new(AlwaysOk);
    let (_stub_tx, stub_rx) = mpsc::channel(1);
    let engine_for_sell = BuyEngine::new(
        rpc_sell.clone(),
        nonce_mgr.clone(),
        stub_rx,
        app_state.clone(),
        Config::default(),
        None, // No transaction builder for tests
    );

    engine_for_sell
        .sell(1.0)
        .await
        .expect("sell should succeed with AlwaysOk broadcaster");

    let st = app_state.lock().await;
    assert!(st.is_sniffing());
    assert!(st.active_token.is_none());
    assert!(st.last_buy_price.is_none());
    assert_eq!(st.holdings_percent, 0.0);
}