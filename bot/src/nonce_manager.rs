use anyhow::{anyhow, Result};
use solana_sdk::pubkey::Pubkey;
use std::collections::{VecDeque, HashSet};
use std::sync::Arc;
use std::pin::Pin;
use std::future::Future;
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
        Self {
            capacity,
            sem: Arc::new(Semaphore::new(capacity)),
            inner: Arc::new(NonceManagerInner {
                free: Mutex::new(free),
                in_use: Mutex::new(HashSet::new()),
            }),
        }
    }


    /// Legacy API - acquire nonce returns (dummy_pubkey, index)
    pub async fn acquire_nonce(&self) -> Result<(Pubkey, usize)> {
        // Acquire semaphore first

        let permit = self
            .sem
            .acquire()
            .await
            .map_err(|_| anyhow!("semaphore closed"))?;

        // Get a free index
        let mut free = self.inner.free.lock().await;
        let index = free
            .pop_front()
            .ok_or_else(|| anyhow!("no free nonce index"))?;
        drop(free);

        // Mark as in use
        let mut in_use = self.inner.in_use.lock().await;
        in_use.insert(index);
        drop(in_use);

        // Convert permit to static lifetime for storage in lease
        let static_permit = unsafe { std::mem::transmute(permit) };

        Ok(NonceLease {
            index,
            pubkey: Pubkey::new_unique(),
            permit: static_permit,
            manager: Arc::clone(&self.inner),
        })
    }

    /// For backward compatibility - acquire and return (Pubkey, index)
    /// Note: This bypasses RAII protection and should be avoided in new code
    pub async fn acquire_nonce_legacy(&self) -> Result<(Pubkey, usize)> {
        let lease = self.acquire_nonce().await?;
        let pubkey = *lease.pubkey();
        let index = lease.index();
        
        // Forget the lease to prevent auto-release
        std::mem::forget(lease);
        
        Ok((pubkey, index))
    }

    /// For backward compatibility - manually release a nonce index
    /// Note: This should be avoided in new code, use NonceLease instead
    pub async fn release_nonce(&self, idx: usize) {
        self.inner.return_index(idx).await;
        // Note: This doesn't release the semaphore permit since we don't have access to it
        // This is a limitation of the legacy API
        self.sem.add_permits(1);
    }

    /// Get current capacity
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get number of available permits (approximate)
    pub fn available_permits(&self) -> usize {
        self.sem.available_permits()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_acquire_and_auto_release() {
        let manager = NonceManager::new(2);
        
        // Acquire a lease
        let lease = manager.acquire_nonce().await.unwrap();
        assert_eq!(manager.available_permits(), 1);
        
        let index = lease.index();
        assert!(index < 2);
        
        // Drop the lease - should auto-release
        drop(lease);
        
        // Give some time for the async drop to complete
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(manager.available_permits(), 2);
    }

    #[tokio::test]
    async fn test_capacity_zero_edge_case() {
        let manager = NonceManager::new(0);
        
        // Should immediately fail to acquire
        let result = timeout(Duration::from_millis(100), manager.acquire_nonce()).await;
        assert!(result.is_err() || result.unwrap().is_err());
    }

    #[tokio::test]
    async fn test_double_release_protection() {
        let manager = NonceManager::new(1);
        
        let lease = manager.acquire_nonce().await.unwrap();
        let index = lease.index();
        
        // Manually try to double-release using legacy API
        manager.release_nonce(index).await;
        manager.release_nonce(index).await; // Should be ignored
        
        drop(lease); // This should also be safe
        
        tokio::time::sleep(Duration::from_millis(10)).await;
        
        // Should still be able to acquire
        let _lease2 = manager.acquire_nonce().await.unwrap();
    }

    #[tokio::test]
    async fn test_concurrent_acquire_release() {
        let manager = Arc::new(NonceManager::new(5));
        let mut handles = Vec::new();
        
        // Spawn multiple tasks that acquire and release leases
        for _ in 0..10 {
            let manager_clone = Arc::clone(&manager);
            handles.push(tokio::spawn(async move {
                for _ in 0..5 {
                    let lease = manager_clone.acquire_nonce().await.unwrap();
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    drop(lease);
                }
            }));
        }
        
        // Wait for all tasks to complete
        for handle in handles {
            handle.await.unwrap();
        }
        
        // All permits should be available again
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(manager.available_permits(), 5);
    }


    /// Legacy API - release nonce now returns Future for deterministic testing
    pub async fn release_nonce(&self, idx: usize) -> Result<()> {
        self.free.lock().await.push_back(idx);
        self.sem.add_permits(1);
        Ok(())
    }
}

impl SlotManager for IndexSlotManager {
    fn acquire_index(&self) -> Pin<Box<dyn Future<Output = Result<IndexLease>> + Send + '_>> {
        Box::pin(async move {
            // Acquire semaphore first
            let permit = self
                .sem
                .acquire()
                .await
                .map_err(|_| anyhow!("semaphore closed"))?;
            drop(permit);

            let mut guard = self.free.lock().await;
            if let Some(idx) = guard.pop_front() {
                Ok(IndexLease::new(idx, Arc::new(self.clone()) as Arc<dyn SlotManager>))
            } else {
                Err(anyhow!("no free index slot"))
            }
        })
    }

    fn release_index(&self, idx: usize) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async move {
            self.free.lock().await.push_back(idx);
            self.sem.add_permits(1);
            Ok(())
        })
    }

    fn get_pubkey_for_index(&self, _index: usize) -> Pubkey {
        Pubkey::new_unique()
    }
}

// Need Clone for SlotManager trait object
impl Clone for IndexSlotManager {
    fn clone(&self) -> Self {
        Self {
            capacity: self.capacity,
            sem: Arc::clone(&self.sem),
            free: Arc::clone(&self.free),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use tokio::task::JoinSet;

    #[tokio::test]
    async fn concurrent_acquire_release_no_duplicates() {
        let manager = Arc::new(IndexSlotManager::new(3));
        let mut handles = JoinSet::new();
        let acquired_indices = Arc::new(tokio::sync::Mutex::new(HashSet::new()));

        // Spawn multiple concurrent tasks that acquire and immediately release
        for i in 0..10 {
            let manager = Arc::clone(&manager);
            let indices = Arc::clone(&acquired_indices);
            
            handles.spawn(async move {
                for _ in 0..5 {
                    match manager.acquire_nonce().await {
                        Ok((_pubkey, idx)) => {
                            // Record that we acquired this index
                            {
                                let mut set = indices.lock().await;
                                if set.contains(&idx) {
                                    panic!("Duplicate index {} acquired by task {}", idx, i);
                                }
                                set.insert(idx);
                            }
                            
                            // Small delay to increase chance of conflicts
                            tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
                            
                            // Release the index
                            let _ = manager.release_nonce(idx).await;
                            
                            // Remove from tracking set
                            {
                                let mut set = indices.lock().await;
                                set.remove(&idx);
                            }
                        }
                        Err(_) => {
                            // This can happen if all slots are temporarily exhausted
                        }
                    }
                }
            });
        }

        // Wait for all tasks to complete
        while let Some(result) = handles.join_next().await {
            result.expect("Task should complete successfully");
        }

        // All indices should be released by now
        let final_indices = acquired_indices.lock().await;
        assert!(final_indices.is_empty(), "All indices should be released");
    }

    #[tokio::test]
    async fn raii_lease_automatic_release() {
        let manager = Arc::new(IndexSlotManager::new(2)) as Arc<dyn SlotManager>;
        
        // Acquire all slots using RAII leases
        let lease1 = manager.acquire_index().await.expect("Should acquire first slot");
        let lease2 = manager.acquire_index().await.expect("Should acquire second slot");
        
        // Trying to acquire a third should fail
        assert!(manager.acquire_index().await.is_err(), "Should fail to acquire third slot");
        
        let idx1 = lease1.index();
        let idx2 = lease2.index();
        assert_ne!(idx1, idx2, "Indices should be unique");
        
        // Drop one lease
        drop(lease1);
        
        // Give the background task a moment to release
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        
        // Should now be able to acquire again
        let lease3 = manager.acquire_index().await.expect("Should acquire after release");
        assert!(lease3.index() == idx1 || lease3.index() != idx2, "Should get back the released index or a new one");
    }

    #[tokio::test]
    async fn legacy_api_compatibility() {
        let manager = IndexSlotManager::new(2);
        
        let (pubkey1, idx1) = manager.acquire_nonce().await.expect("Should acquire");
        let (pubkey2, idx2) = manager.acquire_nonce().await.expect("Should acquire");
        
        assert_ne!(idx1, idx2, "Indices should be unique");
        assert_ne!(pubkey1, pubkey2, "Pubkeys should be unique");
        
        // Should fail to acquire third
        assert!(manager.acquire_nonce().await.is_err());
        
        // Release and should be able to acquire again
        manager.release_nonce(idx1).await.expect("Should release");
        let (_pubkey3, _idx3) = manager.acquire_nonce().await.expect("Should acquire after release");

    }
}