use std::sync::Arc;
use tokio::{
    sync::mpsc::Sender,
    time::{self, Duration},
};
use tracing::{debug, warn};

use crate::config::Config;
use crate::sniffer::http_source::HttpSource;
use crate::sniffer::source::CandidateSource;
use crate::sniffer::wss_source::WssSource;
use crate::types::{PremintCandidate, ProgramLogEvent};

/// Orchestrator that prefers WSS and falls back to HTTP poller on WSS silence/unhealth.
/// - Starts WSS first
/// - If WSS is silent longer than cfg.wss_max_silent_ms and fallback is enabled (and not required),
///   it starts HTTP poller
/// - When WSS recovers, it stops HTTP and returns to WSS-only
pub struct SnifferRunner {
    cfg: Config,
}

impl SnifferRunner {
    pub fn new(cfg: Config) -> Self {
        Self { cfg }
    }

    pub async fn run(
        &self,
        cand_tx: Sender<PremintCandidate>,
        raw_log_tx: Option<Sender<ProgramLogEvent>>,
    ) {
        let wss = Arc::new(WssSource::new(self.cfg.clone()));
        let http = Arc::new(HttpSource::new(self.cfg.clone()));

        // start WSS
        {
            let wss_cloned = wss.clone();
            let cand_tx_wss = cand_tx.clone();
            let raw_log_tx_wss = raw_log_tx.clone();
            tokio::spawn(async move {
                wss_cloned.run(cand_tx_wss, raw_log_tx_wss).await;
            });
        }

        // watchdog loop
        let check_every = Duration::from_millis(self.cfg.wss_heartbeat_ms.max(200));
        loop {
            time::sleep(check_every).await;

            let wss_ok = wss.is_healthy();
            debug!(target:"sniffer", wss_ok, "Runner watchdog tick");

            if wss_ok {
                if http.is_healthy() {
                    // stop HTTP fallback
                    http.request_stop();
                }
                continue;
            }

            if self.cfg.wss_required {
                warn!(target: "sniffer", "WSS required & unhealthy → waiting for reconnect (no fallback).");
                continue;
            }

            if self.cfg.http_fallback_enabled && !http.is_healthy() {
                let http_cloned = http.clone();
                let cand_tx_http = cand_tx.clone();
                let raw_log_tx_http = raw_log_tx.clone();
                warn!(target:"sniffer", "Switch: WSS -> HTTP (fallback starting)");
                tokio::spawn(async move {
                    http_cloned.run(cand_tx_http, raw_log_tx_http).await;
                });
            }
        }
    }
}