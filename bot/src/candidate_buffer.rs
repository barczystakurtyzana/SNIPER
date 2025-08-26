//! Candidate buffer with TTL and de-duplication.
//!
//! Stores premint candidates keyed by mint Pubkey, prevents duplicates, and expires old entries.
//! Provides simple selection policy for "best" candidate: the oldest (earliest inserted/seen).
//!
//! Typical usage (shared):
//! let buf = new_shared(Duration::from_secs(30), 1024);
//! {
//!     let mut guard = buf.lock().await;
//!     guard.push(candidate).await;
//!     let best = guard.pop_best().await;
//! }
//!
//! Notes:
//! - De-duplication is by candidate.mint.
//! - TTL is enforced on push/pop via cleanup, but callers can also call cleanup() periodically.
//! - If the buffer is full on push, the oldest entry is evicted to make room.

use crate::types::PremintCandidate;
use solana_sdk::pubkey::Pubkey;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

/// In-memory candidate buffer.
#[derive(Debug)]
pub struct CandidateBuffer {
    /// Map by mint pubkey; value holds the candidate and the insertion Instant.
    pub map: HashMap<Pubkey, (PremintCandidate, Instant)>,
    /// Time-to-live for each entry.
    pub ttl: Duration,
    /// Maximum number of entries to store; oldest will be evicted when full.
    pub max_size: usize,
}

impl CandidateBuffer {
    /// Create a new buffer with given TTL and capacity.
    pub fn new(ttl: Duration, max_size: usize) -> Self {
        Self {
            map: HashMap::new(),
            ttl,
            max_size,
        }
    }

    /// Insert a candidate if not present and not expired.
    /// Returns true when inserted, false when duplicate or ignored.
    pub fn push(&mut self, c: PremintCandidate) -> bool {
        // Clean expired entries first.
        let _ = self.cleanup();

        if self.map.contains_key(&c.mint) {
            return false;
        }

        // Enforce capacity by evicting the oldest if at capacity.
        if self.map.len() >= self.max_size && self.max_size > 0 {
            if let Some((oldest_key, _)) = self
                .map
                .iter()
                .min_by_key(|(_, (_cand, seen_at))| *seen_at)
                .map(|(k, v)| (*k, v.1))
            {
                self.map.remove(&oldest_key);
            }
        }

        self.map.insert(c.mint, (c, Instant::now()));
        true
    }

    /// Pop the "best" candidate (oldest by insertion time).
    /// Returns None if empty after cleanup or no item is eligible.
    pub fn pop_best(&mut self) -> Option<PremintCandidate> {
        // Remove expired first.
        let _ = self.cleanup();

        // Pick the oldest entry.
        let oldest_key = self
            .map
            .iter()
            .min_by_key(|(_, (_cand, seen_at))| *seen_at)
            .map(|(k, _)| *k)?;

        self.map.remove(&oldest_key).map(|(cand, _)| cand)
    }

    /// Remove expired entries according to TTL.
    /// Returns the number of removed entries.
    pub fn cleanup(&mut self) -> usize {
        if self.ttl.is_zero() {
            // If TTL is zero, expire everything immediately.
            let removed = self.map.len();
            self.map.clear();
            return removed;
        }
        let now = Instant::now();
        let before = self.map.len();
        self.map
            .retain(|_, (_cand, seen_at)| now.duration_since(*seen_at) < self.ttl);
        before.saturating_sub(self.map.len())
    }
}

/// Shared buffer wrapper for concurrent access.
pub type SharedCandidateBuffer = Arc<Mutex<CandidateBuffer>>;

/// Helper to create a shared CandidateBuffer wrapped in Arc<Mutex<...>>.
pub fn new_shared(ttl: Duration, max_size: usize) -> SharedCandidateBuffer {
    Arc::new(Mutex::new(CandidateBuffer::new(ttl, max_size)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PremintCandidate;
    use std::time::Duration;
    use tokio::time::{sleep, Duration as TokioDuration};

    fn fixed_pubkey(byte: u8) -> Pubkey {
        let mut b = [0u8; 32];
        b.fill(byte);
        Pubkey::new_from_array(b)
    }

    fn mk_candidate(byte: u8, ts: u64) -> PremintCandidate {
        PremintCandidate {
            mint: fixed_pubkey(byte),
            creator: fixed_pubkey(byte.wrapping_add(1)),
            program: "mock".to_string(),
            slot: 1,
            timestamp: ts,
        }
    }

    #[test]
    fn push_and_dedup() {
        let mut buf = CandidateBuffer::new(Duration::from_secs(30), 10);
        let c1 = mk_candidate(1, 1);
        let c1_dup = mk_candidate(1, 2);

        assert!(buf.push(c1));
        assert!(!buf.push(c1_dup), "duplicate mint should be ignored");
        assert_eq!(buf.map.len(), 1);
    }

    #[tokio::test]
    async fn ttl_cleanup_and_pop() {
        let mut buf = CandidateBuffer::new(Duration::from_millis(50), 10);
        let c = mk_candidate(2, 1);
        assert!(buf.push(c));
        assert_eq!(buf.map.len(), 1);

        sleep(TokioDuration::from_millis(60)).await;
        let removed = buf.cleanup();
        assert_eq!(removed, 1);
        assert!(buf.pop_best().is_none(), "should be empty after expiry");
    }

    #[tokio::test]
    async fn pop_best_oldest() {
        let mut buf = CandidateBuffer::new(Duration::from_secs(10), 10);
        let c1 = mk_candidate(10, 111);
        let c2 = mk_candidate(11, 222);

        assert!(buf.push(c1.clone()));
        // Ensure different insertion instants
        sleep(TokioDuration::from_millis(5)).await;
        assert!(buf.push(c2.clone()));

        // Oldest should be c1
        let popped1 = buf.pop_best().unwrap();
        assert_eq!(popped1.mint, c1.mint);

        // Next should be c2
        let popped2 = buf.pop_best().unwrap();
        assert_eq!(popped2.mint, c2.mint);

        assert!(buf.pop_best().is_none());
    }

    #[tokio::test]
    async fn evicts_oldest_when_full() {
        let mut buf = CandidateBuffer::new(Duration::from_secs(30), 2);
        let c1 = mk_candidate(1, 1);
        let c2 = mk_candidate(2, 2);
        let c3 = mk_candidate(3, 3);

        assert!(buf.push(c1.clone()));
        sleep(TokioDuration::from_millis(2)).await;
        assert!(buf.push(c2.clone()));
        sleep(TokioDuration::from_millis(2)).await;
        // Now capacity full; pushing c3 should evict the oldest (c1)
        assert!(buf.push(c3.clone()));

        assert!(!buf.map.contains_key(&c1.mint), "oldest should be evicted");
        assert!(buf.map.contains_key(&c2.mint));
        assert!(buf.map.contains_key(&c3.mint));
    }
}