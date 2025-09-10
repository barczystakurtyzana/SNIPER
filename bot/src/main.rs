//! Application entry: wires sniffer (mock/real), buy engine, and GUI together.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, Mutex};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use sniffer_bot_light::buy_engine::BuyEngine;
use sniffer_bot_light::config::{Config, SnifferMode};
use sniffer_bot_light::gui::{launch_gui, GuiEvent, GuiEventSender};
use sniffer_bot_light::nonce_manager::NonceManager;
use sniffer_bot_light::rpc_manager::{RpcBroadcaster, RpcManager};
use sniffer_bot_light::sniffer;
use sniffer_bot_light::sniffer::runner::SnifferRunner;
use sniffer_bot_light::tx_builder::TransactionBuilder;
use sniffer_bot_light::types::{AppState, CandidateReceiver, CandidateSender, Mode, ProgramLogEvent};
use sniffer_bot_light::wallet::WalletManager;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .init();

    let cfg = Config::load();
    info!("Loaded config: {:?}", cfg);

    let app_state = Arc::new(Mutex::new(AppState {
        mode: Mode::Sniffing,
        active_token: None,
        last_buy_price: None,
        holdings_percent: 0.0,
    }));

    let (cand_tx, cand_rx): (CandidateSender, CandidateReceiver) = mpsc::channel(1024);
    let (raw_tx, _raw_rx): (mpsc::Sender<ProgramLogEvent>, mpsc::Receiver<ProgramLogEvent>) =
        mpsc::channel(256);
    let (gui_tx, mut gui_rx): (GuiEventSender, mpsc::Receiver<GuiEvent>) = mpsc::channel(64);


    let prod = Arc::new(RpcManager::new_with_config(cfg.rpc_endpoints.clone(), cfg.clone()));
    let rpc: Arc<dyn RpcBroadcaster> = prod.clone();
    let nonce_manager = Arc::new(NonceManager::new(cfg.nonce_count));

    // Setup wallet and transaction builder if keypair is configured
    let tx_builder = if let Some(keypair_path) = &cfg.keypair_path {
        match WalletManager::from_file(keypair_path) {
            Ok(wallet) => {
                let primary_endpoint = cfg.rpc_endpoints.first()
                    .unwrap_or(&"https://api.devnet.solana.com".to_string()).clone();
                Some(TransactionBuilder::new(wallet, &primary_endpoint))
            }
            Err(e) => {
                error!("Failed to load wallet from {}: {}", keypair_path, e);
                info!("Continuing without transaction builder - will use placeholder transactions");
                None
            }
        }
    } else {
        info!("No keypair configured, using placeholder transactions for testing");
        None
    };

    let engine_state = app_state.clone();
    let mut engine = BuyEngine::new(
        rpc.clone(),
        nonce_manager.clone(),
        cand_rx,
        engine_state,
        cfg.clone(),
        tx_builder,
    );

    let sniffer_handle = match cfg.sniffer_mode {
        SnifferMode::Mock => {
            info!("Starting MOCK sniffer");
            sniffer::run_mock_sniffer(cand_tx.clone())
        }
        SnifferMode::Real => {
            info!("Starting REAL sniffer runner (WSS + HTTP fallback)");
            let runner = SnifferRunner::new(cfg.clone());
            tokio::spawn(async move {
                runner.run(cand_tx.clone(), Some(raw_tx)).await;
            })
        }
    };

    let engine_app_state = app_state.clone();
    let rpc_for_sell: Arc<dyn RpcBroadcaster> = rpc.clone();
    let nonce_for_sell = nonce_manager.clone();
    let cfg_for_sell = cfg.clone();
    let sell_task = tokio::spawn(async move {
        struct SellHandle {
            rpc: Arc<dyn RpcBroadcaster>,
            state: Arc<Mutex<AppState>>,
            nonce: Arc<NonceManager>,
            cfg: Config,
        }
        impl SellHandle {
            async fn sell(&self, percent: f64) -> anyhow::Result<()> {
                let (_tx, rx) = mpsc::channel(1);
                let engine = BuyEngine::new(
                    self.rpc.clone(),
                    self.nonce.clone(),
                    rx,
                    self.state.clone(),
                    self.cfg.clone(),
                    None, // No transaction builder needed for sell-only handle
                );
                engine.sell(percent).await?;
                Ok(())
            }
        }
        let handle = SellHandle {
            rpc: rpc_for_sell.clone(),
            state: engine_app_state.clone(),
            nonce: nonce_for_sell.clone(),
            cfg: cfg_for_sell.clone(),
        };
        while let Some(ev) = gui_rx.recv().await {
            match ev {
                GuiEvent::SellPercent(p) => {
                    if let Err(e) = handle.sell(p).await {
                        error!(percent=p, error=%e, "Sell failed");
                    }
                }
            }
        }
    });

    let engine_task = tokio::spawn(async move {
        engine.run().await;
    });

    launch_gui(
        "Sniffer Bot (GUI)",
        app_state.clone(),
        gui_tx.clone(),
        Duration::from_millis(cfg.gui_update_interval_ms),
    )?;

    sniffer_handle.abort();
    engine_task.abort();
    sell_task.abort();

    Ok(())
}