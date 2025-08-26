use async_trait::async_trait;
use solana_sdk::pubkey::Pubkey;
use tokio::sync::mpsc::Sender;

use crate::types::{PremintCandidate, ProgramLogEvent};

#[async_trait]
pub trait CandidateSource: Send + Sync {
    async fn run(
        &self,
        cand_tx: Sender<PremintCandidate>,
        raw_log_tx: Option<Sender<ProgramLogEvent>>,
    );

    fn is_healthy(&self) -> bool;

    fn request_stop(&self);
}

// Pump.fun program (constant)
pub const PUMP_FUN_PROGRAM: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
pub fn pump_fun_program_pk() -> Pubkey {
    PUMP_FUN_PROGRAM
        .parse::<Pubkey>()
        .expect("invalid pump.fun program id")
}