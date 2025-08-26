use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use eframe::egui::{self, Key};
use eframe::{App, Frame};
use tokio::sync::{mpsc::Sender, Mutex};
use tracing::info;

use crate::types::{AppState, Mode, PremintCandidate};

#[derive(Clone, Debug)]
pub enum GuiEvent {
    SellPercent(f64),
}
pub type GuiEventSender = Sender<GuiEvent>;

pub fn launch_gui(
    title: &str,
    app_state: Arc<Mutex<AppState>>,
    gui_tx: GuiEventSender,
    refresh: Duration,
) -> Result<()> {
    let native_options = eframe::NativeOptions::default();
    let app = BotApp::new(app_state, gui_tx, refresh);
    eframe::run_native(title, native_options, Box::new(|_| Box::new(app)))?;
    Ok(())
}

struct BotApp {
    app_state: Arc<Mutex<AppState>>,
    gui_tx: GuiEventSender,
    refresh: Duration,
}

impl BotApp {
    fn new(app_state: Arc<Mutex<AppState>>, gui_tx: GuiEventSender, refresh: Duration) -> Self {
        Self {
            app_state,
            gui_tx,
            refresh,
        }
    }

    fn draw_state(&self, ui: &mut egui::Ui, st: &AppState) {
        ui.heading("Sniffer Bot");
        match &st.mode {
            Mode::Sniffing => {
                ui.label("Mode: Sniffing");
            }
            Mode::PassiveToken(mint) => {
                ui.label(format!("Mode: PassiveToken ({mint})"));
            }
        }
        if let Some(tok) = st.active_token.as_ref() {
            ui.label(format!("Active mint: {}", tok.mint));
            if let Some(price) = st.last_buy_price {
                ui.label(format!("Last buy price (mock): {:.4}", price));
            }
            ui.label(format!("Holdings: {:.0}%", st.holdings_percent * 100.0));
        } else {
            ui.label("No active token");
        }

        ui.separator();
        ui.horizontal(|ui| {
            if ui.button("Sell 25% (W)").clicked() {
                let _ = self.gui_tx.try_send(GuiEvent::SellPercent(0.25));
            }
            if ui.button("Sell 50% (Q)").clicked() {
                let _ = self.gui_tx.try_send(GuiEvent::SellPercent(0.50));
            }
            if ui.button("Sell 100% (S)").clicked() {
                let _ = self.gui_tx.try_send(GuiEvent::SellPercent(1.0));
            }
        });
        ui.label("Shortcuts: W=25%, Q=50%, S=100%");
    }
}

impl App for BotApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut Frame) {
        ctx.input(|i| {
            if i.key_pressed(Key::W) {
                let _ = self.gui_tx.try_send(GuiEvent::SellPercent(0.25));
            }
            if i.key_pressed(Key::Q) {
                let _ = self.gui_tx.try_send(GuiEvent::SellPercent(0.50));
            }
            if i.key_pressed(Key::S) {
                let _ = self.gui_tx.try_send(GuiEvent::SellPercent(1.0));
            }
        });

        egui::CentralPanel::default().show(ctx, |ui| {
            let st = self.app_state.blocking_lock().clone();
            self.draw_state(ui, &st);
        });

        ctx.request_repaint_after(self.refresh);
    }

    fn on_close_event(&mut self) -> bool {
        info!("GUI closed");
        true
    }
}