use anyhow::{anyhow, Result};
use solana_sdk::pubkey::Pubkey;

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::pin::Pin;
use std::future::Future;
use std::time::Duration;
use tokio::sync::{Mutex, Semaphore};


/// RAII lease for index slots that automatically releases on drop
pub struct IndexLease {
    index: usize,
    manager: Arc<dyn SlotManager>,
}

impl IndexLease {
    fn new(index: usize, manager: Arc<dyn SlotManager>) -> Self {
        Self { index, manager }
    }
    

    pub fn index(&self) -> usize {
        self.index
    }

}

impl Drop for IndexLease {
    fn drop(&mut self) {
        // Release the index when the lease is dropped
        let manager = Arc::clone(&self.manager);
        let index = self.index;
        tokio::spawn(async move {
            let _ = manager.release_index(index).await;
        });
    }
}

/// Abstract trait for slot/index management systems
pub trait SlotManager: Send + Sync + std::fmt::Debug {
    /// Acquire an index slot, returns a lease that auto-releases on drop
    fn acquire_index(&self) -> Pin<Box<dyn Future<Output = Result<IndexLease>> + Send + '_>>;
    
    /// Release an index slot manually (also done automatically via Drop)
    fn release_index(&self, index: usize) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;
    
    /// Get a dummy pubkey for the given index (for compatibility)
    fn get_pubkey_for_index(&self, index: usize) -> Pubkey;
}

#[derive(Debug)]
struct NonceManagerInner {
    capacity: usize,
    sem: Arc<Semaphore>,
    free: Arc<Mutex<VecDeque<usize>>>,
    allocated: Arc<Mutex<HashSet<usize>>>,
}

/// Lightweight index slot manager:
/// - Provides at most `capacity` parallel index slots
/// - acquire_index() returns IndexLease that auto-releases on drop
/// - For backward compatibility, also provides the old nonce-style API

#[derive(Debug)]
pub struct IndexSlotManager {
    capacity: usize,
    sem: Arc<Semaphore>,

    inner: Arc<NonceManagerInner>,
}

// Type alias for backward compatibility
pub type NonceManager = IndexSlotManager;

impl IndexSlotManager {
    pub fn new(capacity: usize) -> Self {
        let free = (0..capacity).collect::<VecDeque<_>>();
        let sem = Arc::new(Semaphore::new(capacity));
        let inner = Arc::new(NonceManagerInner {
            capacity,
            sem: sem.clone(),
            free: Arc::new(Mutex::new(free)),
            allocated: Arc::new(Mutex::new(HashSet::new())),
        });
        Self { 
            capacity,
            sem,
            inner 
        }
    }



    /// Legacy API - acquire nonce returns (dummy_pubkey, index)
    pub async fn acquire_nonce(&self) -> Result<(Pubkey, usize)> {
        // Acquire semaphore first


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
            
            Ok(IndexLease {
                index: idx,
                manager: self.inner.clone(),
            })
        } else {
            // This should not happen with proper semaphore usage
            Err(anyhow!("no free nonce index despite semaphore permit"))
        }
    }


    pub fn release_nonce(&self, idx: usize) {
        // Remove the async spawn overhead by using blocking operations
        // This assumes the calling context can handle potential blocking
        if let Ok(mut guard) = self.inner.free.try_lock() {
            guard.push_back(idx);
            self.inner.sem.add_permits(1);
        } else {
            // Fallback to async spawn only if we can't get immediate lock
            let free = self.inner.free.clone();
            let sem = self.inner.sem.clone();
            tokio::spawn(async move {
                free.lock().await.push_back(idx);
                sem.add_permits(1);
            });
        }
    }
}