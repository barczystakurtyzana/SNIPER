//! Application entry: wires sniffer (mock/real), buy engine, and GUI together.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, Mutex};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use crate::buy_engine::BuyEngine;
use crate::config::{Config, SnifferMode};
use crate::gui::{launch_gui, GuiEvent, GuiEventSender};
use crate::nonce_manager::NonceManager;
use crate::rpc_manager::{RpcBroadcaster, RpcManager};
use crate::sniffer::{run_mock_sniffer};
use crate::sniffer::runner::SnifferRunner;
use crate::types::{AppState, CandidateReceiver, CandidateSender, Mode, ProgramLogEvent};

mod buy_engine;
mod config;
mod gui;
mod nonce_manager;
mod rpc_manager;
mod sniffer;
mod time_utils;
mod types;

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

    let prod = Arc::new(RpcManager::new(cfg.rpc_endpoints.clone()));
    let rpc: Arc<dyn RpcBroadcaster> = prod.clone();
    let nonce_manager = Arc::new(NonceManager::new(cfg.nonce_count));

    let engine_state = app_state.clone();
    let mut engine = BuyEngine {
        rpc: rpc.clone(),
        nonce_manager: nonce_manager.clone(),
        candidate_rx: cand_rx,
        app_state: engine_state,
        config: cfg.clone(),
    };

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
                let engine = BuyEngine {
                    rpc: self.rpc.clone(),
                    nonce_manager: self.nonce.clone(),
                    candidate_rx: rx,
                    app_state: self.state.clone(),
                    config: self.cfg.clone(),
                };
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