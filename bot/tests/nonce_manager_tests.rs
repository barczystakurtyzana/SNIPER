use sniffer_bot_light::nonce_manager::NonceManager;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

#[tokio::test]
async fn test_semaphore_proper_usage() {
    let capacity = 3;
    let manager = Arc::new(NonceManager::new(capacity));
    
    // Should be able to acquire exactly `capacity` leases
    let mut leases = Vec::new();
    for _ in 0..capacity {
        let lease = manager.acquire_nonce().await.expect("should acquire nonce");
        leases.push(lease);
    }
    
    // Next acquisition should timeout (blocked by semaphore)
    let manager_clone = manager.clone();
    let result = timeout(Duration::from_millis(100), async move {
        manager_clone.acquire_nonce().await
    }).await;
    
    assert!(result.is_err(), "should timeout when at capacity");
    
    // Release one lease and should be able to acquire again
    drop(leases.pop()); // This should release the permit via RAII
    
    // Give some time for the async release to complete
    tokio::time::sleep(Duration::from_millis(50)).await;
    
    let new_lease = timeout(Duration::from_millis(100), manager.acquire_nonce()).await
        .expect("should not timeout")
        .expect("should acquire after release");
    
    assert!(new_lease.index() < capacity);
}

#[tokio::test]
async fn test_no_permit_inflation() {
    let capacity = 2;
    let manager = Arc::new(NonceManager::new(capacity));
    
    // Acquire and release many times - should not inflate permits
    for _ in 0..10 {
        let lease = manager.acquire_nonce().await.expect("should acquire");
        drop(lease); // Release via RAII
        tokio::time::sleep(Duration::from_millis(10)).await; // Let async release complete
    }
    
    // Should still be limited to capacity
    let mut leases = Vec::new();
    for _ in 0..capacity {
        let lease = manager.acquire_nonce().await.expect("should acquire nonce");
        leases.push(lease);
    }
    
    // Next should still block
    let manager_clone = manager.clone();
    let result = timeout(Duration::from_millis(100), async move {
        manager_clone.acquire_nonce().await
    }).await;
    
    assert!(result.is_err(), "should still be limited to capacity after many cycles");
}

#[tokio::test]
async fn test_unique_indices() {
    let capacity = 5;
    let manager = Arc::new(NonceManager::new(capacity));
    
    let mut leases = Vec::new();
    let mut indices = HashSet::new();
    
    // Acquire all leases
    for _ in 0..capacity {
        let lease = manager.acquire_nonce().await.expect("should acquire nonce");
        let idx = lease.index();
        
        assert!(idx < capacity, "index should be within capacity");
        assert!(indices.insert(idx), "index should be unique");
        
        leases.push(lease);
    }
    
    // All indices should be unique
    assert_eq!(indices.len(), capacity);
}

#[tokio::test]
async fn test_concurrent_acquire_release() {
    let capacity = 3;
    let manager = Arc::new(NonceManager::new(capacity));
    
    let mut handles = Vec::new();
    
    // Spawn multiple tasks that acquire and release
    for _ in 0..10 {
        let manager_clone = manager.clone();
        let handle = tokio::spawn(async move {
            let lease = manager_clone.acquire_nonce().await?;
            tokio::time::sleep(Duration::from_millis(50)).await;
            drop(lease); // Release via RAII
            Ok::<_, anyhow::Error>(())
        });
        handles.push(handle);
    }
    
    // All should complete successfully
    for handle in handles {
        handle.await.expect("task should complete").expect("should not error");
    }
}

#[tokio::test] 
async fn test_deprecated_release_nonce_validation() {
    let capacity = 3;
    let manager = NonceManager::new(capacity);
    
    // Using deprecated API - should not panic on invalid indices
    #[allow(deprecated)]
    {
        manager.release_nonce(999); // Invalid high index
        manager.release_nonce(0); // Valid but not allocated
    }
    
    // Should still work normally
    let _lease = manager.acquire_nonce().await.expect("should still work");
}