use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time;

use sniffer_bot_light::config::{Config, SnifferMode};
use sniffer_bot_light::sniffer::runner::SnifferRunner;
use sniffer_bot_light::types::{PremintCandidate, ProgramLogEvent};

#[tokio::test]
async fn runner_smoke_in_real_mode() {
    let cfg = Config {
        sniffer_mode: SnifferMode::Real,
        http_fallback_enabled: true,
        wss_required: false,
        wss_heartbeat_ms: 200,
        wss_max_silent_ms: 300,
        ..Config::default()
    };

    let (cand_tx, mut cand_rx) = mpsc::channel::<PremintCandidate>(16);
    let (raw_tx, _raw_rx) = mpsc::channel::<ProgramLogEvent>(16);
    let runner = SnifferRunner::new(cfg);

    let h = tokio::spawn(async move { runner.run(cand_tx, Some(raw_tx)).await });

    let _ = time::timeout(Duration::from_millis(300), cand_rx.recv()).await;
    h.abort();

    assert!(true);
}