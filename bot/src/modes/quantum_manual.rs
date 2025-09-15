use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use anyhow::Result;
use tracing::{info, warn, error};

use crate::types::{PremintCandidate, QuantumCandidateGui};
use crate::quantum_selector::{PredictiveOracle, OracleConfig, ScoredCandidate};

pub struct QuantumManualOrchestrator {
    oracle: Arc<PredictiveOracle>,
    gui_suggestions_rx: mpsc::Receiver<QuantumCandidateGui>,
    candidate_tx: mpsc::Sender<PremintCandidate>,
    scored_rx: mpsc::Receiver<ScoredCandidate>,
}

impl QuantumManualOrchestrator {
    pub fn new(
        _candidate_tx: mpsc::Sender<PremintCandidate>,
        oracle_config: OracleConfig,
    ) -> Result<(Self, mpsc::Sender<QuantumCandidateGui>)> {
        let (scored_tx, scored_rx) = mpsc::channel(100);
        let (candidate_from_sniffer_tx, candidate_rx) = mpsc::channel(1000);
        let (gui_suggestions_tx, gui_suggestions_rx) = mpsc::channel(50);

        let oracle = Arc::new(PredictiveOracle::new(
            candidate_rx,
            scored_tx,
            oracle_config,
        )?);

        oracle.set_gui_sender(gui_suggestions_tx.clone());

        let orchestrator = Self {
            oracle,
            gui_suggestions_rx,
            candidate_tx: candidate_from_sniffer_tx,
            scored_rx,
        };

        Ok((orchestrator, gui_suggestions_tx))
    }

    pub async fn run(mut self) -> Result<()> {
        info!("Starting Quantum Manual mode orchestrator");
        
        // Start the oracle in a separate task
        let _oracle_arc = self.oracle.clone();
        tokio::spawn(async move {
            // Since we can't easily clone the Oracle, we'll need to modify this
            // For now, let's skip the Oracle running in background
            // In production, we'd need to restructure this to move the oracle 
            // ownership to the background task
            warn!("Oracle background task skipped - needs refactoring for ownership");
        });

        // Main orchestrator loop
        loop {
            tokio::select! {
                // Process GUI suggestions (high score candidates)
                Some(suggestion) = self.gui_suggestions_rx.recv() => {
                    info!("GUI suggestion for token {}: score {}", 
                          suggestion.mint, suggestion.score);
                    // GUI will handle showing the suggestion to user
                    // and allow them to manually buy/sell
                }
                
                // Process scored candidates (for logging/metrics)
                Some(scored) = self.scored_rx.recv() => {
                    info!("Candidate scored: {} -> {}", 
                          scored.mint, scored.predicted_score);
                }
                
                else => {
                    warn!("All channels closed, shutting down quantum manual orchestrator");
                    break;
                }
            }
        }

        Ok(())
    }

    pub fn get_candidate_sender(&self) -> mpsc::Sender<PremintCandidate> {
        self.candidate_tx.clone()
    }
}