use anyhow::{anyhow, Result};
use solana_sdk::pubkey::Pubkey;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};

/// Lightweight nonce/index manager:
/// - Provides at most `capacity` parallel "nonce slots"
/// - acquire_nonce() returns (dummy_pubkey, index), to be released via release_nonce(index)
#[derive(Debug)]
pub struct NonceManager {
    capacity: usize,
    sem: Arc<Semaphore>,
    free: Arc<Mutex<VecDeque<usize>>>,
}

impl NonceManager {
    pub fn new(capacity: usize) -> Self {
        let free = (0..capacity).collect::<VecDeque<_>>();
        Self {
            capacity,
            sem: Arc::new(Semaphore::new(capacity)),
            free: Arc::new(Mutex::new(free)),
        }
    }

    pub async fn acquire_nonce(&self) -> Result<(Pubkey, usize)> {
        // Acquire semaphore first
        let permit = self
            .sem
            .acquire()
            .await
            .map_err(|_| anyhow!("semaphore closed"))?;
        // Keep permit alive for the lifetime of the slot; we drop it in release_nonce.
        // To keep it simple, we don't store the permit; rather, we add one new permit back on release.
        drop(permit);

        let mut guard = self.free.lock().await;
        if let Some(idx) = guard.pop_front() {
            Ok((Pubkey::new_unique(), idx))
        } else {
            // Shouldn't happen with semaphore protection
            Err(anyhow!("no free nonce index"))
        }
    }

    pub fn release_nonce(&self, idx: usize) {
        // Remove the async spawn overhead by using blocking operations
        // This assumes the calling context can handle potential blocking
        if let Ok(mut guard) = self.free.try_lock() {
            guard.push_back(idx);
            self.sem.add_permits(1);
        } else {
            // Fallback to async spawn only if we can't get immediate lock
            let free = self.free.clone();
            let sem = self.sem.clone();
            tokio::spawn(async move {
                free.lock().await.push_back(idx);
                sem.add_permits(1);
            });
        }
    }
}