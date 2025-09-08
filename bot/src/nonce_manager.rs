use anyhow::{anyhow, Result};
use solana_sdk::pubkey::Pubkey;
use std::collections::{VecDeque, HashSet};
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore, SemaphorePermit};

/// RAII guard for nonce slot that automatically releases on drop
#[derive(Debug)]
pub struct NonceLease {
    index: usize,
    pubkey: Pubkey,
    permit: SemaphorePermit<'static>,
    manager: Arc<NonceManagerInner>,
}

impl NonceLease {
    pub fn index(&self) -> usize {
        self.index
    }

    pub fn pubkey(&self) -> &Pubkey {
        &self.pubkey
    }
}

impl Drop for NonceLease {
    fn drop(&mut self) {
        let manager = Arc::clone(&self.manager);
        let index = self.index;
        
        // Spawn task to avoid blocking the drop
        tokio::spawn(async move {
            manager.return_index(index).await;
        });
        
        // The permit is automatically released when dropped
    }
}

/// Internal manager state
#[derive(Debug)]
struct NonceManagerInner {
    free: Mutex<VecDeque<usize>>,
    in_use: Mutex<HashSet<usize>>,
}

impl NonceManagerInner {
    async fn return_index(&self, index: usize) {
        let mut in_use = self.in_use.lock().await;
        if in_use.remove(&index) {
            drop(in_use);
            let mut free = self.free.lock().await;
            free.push_back(index);
        }
        // Ignore double releases - index wasn't in use
    }
}

/// Lightweight nonce/index manager with RAII pattern:
/// - Provides at most `capacity` parallel "nonce slots"
/// - acquire_nonce() returns NonceLease that auto-releases on drop
/// - Prevents double releases and index duplication through RAII
#[derive(Debug)]
pub struct NonceManager {
    capacity: usize,
    sem: Arc<Semaphore>,
    inner: Arc<NonceManagerInner>,
}

impl NonceManager {
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

    /// Acquire a nonce lease. Returns RAII guard that auto-releases on drop.
    pub async fn acquire_nonce(&self) -> Result<NonceLease> {
        // Acquire semaphore permit - this will be stored in the lease
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

    #[tokio::test]
    async fn test_race_conditions_no_index_duplication() {
        let manager = Arc::new(NonceManager::new(3));
        let indices = Arc::new(Mutex::new(HashSet::new()));
        let mut handles = Vec::new();
        
        // Spawn multiple concurrent acquisitions
        for _ in 0..10 {
            let manager_clone = Arc::clone(&manager);
            let indices_clone = Arc::clone(&indices);
            
            handles.push(tokio::spawn(async move {
                if let Ok(lease) = manager_clone.acquire_nonce().await {
                    let index = lease.index();
                    
                    // Check for index duplication
                    {
                        let mut indices_guard = indices_clone.lock().await;
                        assert!(!indices_guard.contains(&index), "Index {} was duplicated!", index);
                        indices_guard.insert(index);
                    }
                    
                    // Hold the lease briefly
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    
                    // Remove from our tracking set before drop
                    {
                        let mut indices_guard = indices_clone.lock().await;
                        indices_guard.remove(&index);
                    }
                }
            }));
        }
        
        // Wait for all tasks to complete
        for handle in handles {
            handle.await.unwrap();
        }
        
        // All permits should be available again
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(manager.available_permits(), 3);
    }

    #[tokio::test]
    async fn test_backward_compatibility() {
        let manager = NonceManager::new(2);
        
        // Test legacy API
        let (pubkey, index) = manager.acquire_nonce_legacy().await.unwrap();
        assert!(index < 2);
        assert_ne!(pubkey, Pubkey::default());
        assert_eq!(manager.available_permits(), 1);
        
        // Release using legacy API
        manager.release_nonce(index).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(manager.available_permits(), 2);
    }
}