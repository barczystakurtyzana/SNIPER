//! Sniffer entrypoint coordinating Mock or Real (WSS + HTTP fallback) sources.

pub mod real;
pub mod source;
pub mod wss_source;
pub mod http_source;
pub mod runner;

use crate::config::{Config, SnifferMode};
use crate::sniffer::runner::SnifferRunner;
use crate::types::CandidateSender;
use crate::types::PremintCandidate;
use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::{task::JoinHandle, time};
use tracing::{debug, info, warn};

use solana_sdk::{pubkey::Pubkey, signature::{Keypair, Signer}};

/// TTL window for a mint. Within this duration, repeated occurrences of the same mint are ignored.
const CANDIDATE_TTL: Duration = Duration::from_secs(5);
/// Minimal spacing between emitted candidates (debounce).
const DEBOUNCE_DELAY: Duration = Duration::from_millis(300);
/// Maximum allowed age of a candidate based on its timestamp.
const MAX_CANDIDATE_AGE: Duration = Duration::from_secs(5);

/// Start the sniffer in the given mode.
/// Returns a JoinHandle that can be aborted to stop the sniffer.
pub async fn run_sniffer(
    mode: SnifferMode,
    sender: CandidateSender,
    config: &Config,
) -> JoinHandle<()> {
    match mode {
        SnifferMode::Mock => run_mock_sniffer(sender),
        SnifferMode::Real => {
            let runner = SnifferRunner::new(config.clone());
            tokio::spawn(async move {
                runner.run(sender, None).await;
            })
        }
    }
}

/// Mock sniffer: emits a fabricated PremintCandidate with TTL/debounce/age filtering.
pub fn run_mock_sniffer(sender: CandidateSender) -> JoinHandle<()> {
    tokio::spawn(async move {
        info!(
            "Starting MOCK sniffer with TTL={:?}, debounce={:?}, max_age={:?}",
            CANDIDATE_TTL, DEBOUNCE_DELAY, MAX_CANDIDATE_AGE
        );

        let mut seen: HashMap<Pubkey, Instant> = HashMap::new();
        let mut last_emit: Instant = Instant::now() - DEBOUNCE_DELAY;

        let mut ticker = time::interval(Duration::from_millis(500));
        let mut burst_left: u8 = 0;

        loop {
            ticker.tick().await;

            if burst_left == 0 && fastrand::f32() < 0.1 {
                burst_left = 3;
            }
            if burst_left > 0 {
                burst_left -= 1;
                time::sleep(Duration::from_millis(75)).await;
            }

            let mint = Keypair::new().pubkey();
            let creator = Keypair::new().pubkey();
            let now_secs = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();

            let candidate = PremintCandidate {
                mint,
                creator,
                program: "pump.fun".to_string(),
                slot: 0,
                timestamp: now_secs,
                instruction_summary: Some("Mock candidate".to_string()),
                is_jito_bundle: None,
            };

            let now = Instant::now();

            seen.retain(|_, seen_at| now.duration_since(*seen_at) < CANDIDATE_TTL);

            let candidate_age = Duration::from_secs(0);
            if candidate_age > MAX_CANDIDATE_AGE {
                debug!(mint=%candidate.mint, age=?candidate_age, "Dropping candidate: too old");
                continue;
            }

            if let Some(seen_at) = seen.get(&candidate.mint) {
                if now.duration_since(*seen_at) < CANDIDATE_TTL {
                    debug!(mint=%candidate.mint, "Skipping due to TTL window");
                    continue;
                }
            }

            if now.duration_since(last_emit) < DEBOUNCE_DELAY {
                debug!(mint=%candidate.mint, "Skipping due to debounce");
                continue;
            }

            seen.insert(candidate.mint, now);
            last_emit = now;

            info!(
                target: "sniffer.mock",
                mint = %candidate.mint,
                creator = %candidate.creator,
                program = %candidate.program,
                ts = candidate.timestamp,
                "Emitting mock PremintCandidate"
            );

            if let Err(e) = sender.send(candidate).await {
                warn!(error = %e, "Receiver dropped; stopping mock sniffer");
                break;
            }
        }

        debug!("Mock sniffer task exited");
    })
}