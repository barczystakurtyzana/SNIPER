use solana_sdk::pubkey::Pubkey;
use solana_sdk::transaction::VersionedTransaction;
use solana_sdk::message::{v0, VersionedMessage};
use solana_sdk::instruction::Instruction;
use solana_sdk::hash::Hash;
use tokio::sync::mpsc;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PremintCandidate {
    pub mint: Pubkey,
    pub creator: Pubkey,
    pub program: String,
    pub slot: u64,
    pub timestamp: u64,
    pub instruction_summary: Option<String>,
    pub is_jito_bundle: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuantumCandidateGui {
    pub mint: Pubkey,
    pub score: u8,
    pub reason: String,
    pub feature_scores: HashMap<String, f64>,
    pub timestamp: u64,
}

pub type CandidateSender = mpsc::Sender<PremintCandidate>;
pub type CandidateReceiver = mpsc::Receiver<PremintCandidate>;

#[derive(Debug, Clone)]
pub enum Mode {
    Sniffing,
    PassiveToken(Pubkey),
    QuantumManual,
}

#[derive(Debug, Clone)]
pub struct AppState {
    pub mode: Mode,
    pub active_token: Option<PremintCandidate>,
    pub last_buy_price: Option<f64>,
    pub holdings_percent: f64,
    pub quantum_suggestions: Vec<QuantumCandidateGui>,
}

impl AppState {
    pub fn is_sniffing(&self) -> bool {
        matches!(self.mode, Mode::Sniffing)
    }
}

#[derive(Clone, Debug)]
pub struct ProgramLogEvent {
    pub slot: u64,
    pub signature: String,
    pub program: String,
    pub logs: Vec<String>,
    pub ts_ms: u64,
}

/// Token profiles for MarketMaker simulation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenProfile {
    /// A promising token that generates growing interest with small swap transactions
    Gem,
    /// A token that will perform a rug pull after a random duration (1-3 minutes)
    RugPull,
    /// A token with minimal activity that gets removed from active tracking
    Trash,
}

/// State of a token being managed by MarketMaker
#[derive(Debug, Clone)]
pub struct TokenState {
    pub mint: Pubkey,
    pub profile: TokenProfile,
    pub created_at: std::time::Instant,
    pub activity_count: u32,
    pub is_active: bool,
}

/// Helper function to create a simple versioned transaction for testing
pub fn create_versioned_transaction(
    instructions: Vec<Instruction>,
    payer: &Pubkey,
    blockhash: Hash,
    _priority_fee: u64,
) -> VersionedTransaction {
    let message = v0::Message::try_compile(payer, &instructions, &[], blockhash)
        .expect("Failed to compile message");
    
    VersionedTransaction {
        signatures: vec![solana_sdk::signature::Signature::default()],
        message: VersionedMessage::V0(message),
    }
}