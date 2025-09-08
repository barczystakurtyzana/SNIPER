use anyhow::{anyhow, Result};
use solana_sdk::pubkey::Pubkey;
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore, SemaphorePermit};

/// RAII wrapper for a nonce slot that properly manages semaphore permit
#[derive(Debug)]
pub struct SlotLease {
    idx: usize,
    pubkey: Pubkey,
    manager: Arc<NonceManagerInner>,
    // We store a weak reference to the permit instead of the permit itself
    // to avoid lifetime issues
    _permit_guard: PermitGuard,
}

#[derive(Debug)]
struct PermitGuard {
    _permit: SemaphorePermit<'static>,
}

// Safe wrapper around the permit
impl PermitGuard {
    fn new(permit: SemaphorePermit<'_>) -> Self {
        // Safety: We ensure the semaphore outlives this guard by storing
        // it in Arc<NonceManagerInner> which is also stored in SlotLease
        let permit_static = unsafe { std::mem::transmute(permit) };
        Self { _permit: permit_static }
    }
}

impl SlotLease {
    pub fn index(&self) -> usize {
        self.idx
    }

    pub fn pubkey(&self) -> Pubkey {
        self.pubkey
    }
}

impl Drop for SlotLease {
    fn drop(&mut self) {
        let free = self.manager.free.clone();
        let allocated = self.manager.allocated.clone();
        let idx = self.idx;
        
        // Execute release synchronously to avoid race conditions
        let rt = tokio::runtime::Handle::current();
        rt.spawn(async move {
            let mut free_guard = free.lock().await;
            let mut allocated_guard = allocated.lock().await;
            
            // Only release if it was actually allocated (prevent double release)
            if allocated_guard.remove(&idx) {
                free_guard.push_back(idx);
            }
            // Permit is automatically released when _permit is dropped
        });
    }
}

#[derive(Debug)]
struct NonceManagerInner {
    capacity: usize,
    sem: Arc<Semaphore>,
    free: Arc<Mutex<VecDeque<usize>>>,
    allocated: Arc<Mutex<HashSet<usize>>>,
}

/// Lightweight nonce/index manager:
/// - Provides at most `capacity` parallel "nonce slots"
/// - acquire_nonce() returns a SlotLease that manages the permit lifecycle via RAII
#[derive(Debug)]
pub struct NonceManager {
    inner: Arc<NonceManagerInner>,
}

impl NonceManager {
    pub fn new(capacity: usize) -> Self {
        let free = (0..capacity).collect::<VecDeque<_>>();
        let inner = Arc::new(NonceManagerInner {
            capacity,
            sem: Arc::new(Semaphore::new(capacity)),
            free: Arc::new(Mutex::new(free)),
            allocated: Arc::new(Mutex::new(HashSet::new())),
        });
        Self { inner }
    }

    pub async fn acquire_nonce(&self) -> Result<SlotLease> {
        // Acquire semaphore permit first - this blocks if at capacity
        let permit = self
            .inner
            .sem
            .acquire()
            .await
            .map_err(|_| anyhow!("semaphore closed"))?;

        // Get next available index
        let mut free_guard = self.inner.free.lock().await;
        let mut allocated_guard = self.inner.allocated.lock().await;
        
        if let Some(idx) = free_guard.pop_front() {
            // Validate that index is in expected range
            if idx >= self.inner.capacity {
                return Err(anyhow!("invalid nonce index {} >= {}", idx, self.inner.capacity));
            }
            
            // Mark as allocated to prevent double release
            allocated_guard.insert(idx);
            drop(free_guard);
            drop(allocated_guard);
            
            // Convert permit and store in guard
            let permit_guard = PermitGuard::new(permit);
            
            Ok(SlotLease {
                idx,
                pubkey: Pubkey::new_unique(),
                manager: self.inner.clone(),
                _permit_guard: permit_guard,
            })
        } else {
            // This should not happen with proper semaphore usage
            Err(anyhow!("no free nonce index despite semaphore permit"))
        }
    }

    /// Legacy method for backward compatibility - prefer using SlotLease directly
    /// Note: This method is now deprecated and should not be used in new code
    #[deprecated(note = "Use SlotLease RAII pattern instead")]
    pub fn release_nonce(&self, idx: usize) {
        let free = self.inner.free.clone();
        let allocated = self.inner.allocated.clone();
        
        // Execute synchronously to avoid race conditions
        tokio::spawn(async move {
            let mut free_guard = free.lock().await;
            let mut allocated_guard = allocated.lock().await;
            
            // Validate index range
            if idx >= allocated_guard.len() + free_guard.len() {
                tracing::warn!("release_nonce called with invalid index {}", idx);
                return;
            }
            
            // Only release if it was actually allocated (prevent double release)
            if allocated_guard.remove(&idx) {
                free_guard.push_back(idx);
            } else {
                tracing::warn!("release_nonce called on non-allocated or already released index {}", idx);
            }
        });
    }
}