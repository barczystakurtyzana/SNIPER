use sniffer_bot_light::{
    metrics::metrics,
    security::validator,
    endpoints::endpoint_server,
    structured_logging::PipelineContext,
    types::PremintCandidate,
};
use solana_sdk::pubkey::Pubkey;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_telemetry_integration() {
    // Test metrics collection
    metrics().increment_counter("test_integration_counter");
    metrics().set_gauge("test_buffer_size", 100);
    
    let snapshot = metrics().export_metrics();
    assert!(snapshot.counters.contains_key("test_integration_counter"));
    assert!(snapshot.gauges.contains_key("test_buffer_size"));
    assert_eq!(snapshot.gauges["test_buffer_size"], 100);

    println!("✅ Metrics integration test passed");
}

#[tokio::test]
async fn test_structured_logging_integration() {
    let ctx = PipelineContext::new("test_component");
    let correlation_id = ctx.correlation_id;
    
    // Test structured logging
    ctx.logger.info("Test message", serde_json::json!({"key": "value"}));
    ctx.logger.log_buy_attempt("test_mint", 3);
    
    // Create child context with same correlation ID
    let child_ctx = ctx.child("child_component");
    assert_eq!(child_ctx.correlation_id, correlation_id);
    
    println!("✅ Structured logging integration test passed");
}

#[test]
fn test_security_validation_integration() {
    let candidate = PremintCandidate {
        mint: Pubkey::new_unique(),
        creator: Pubkey::new_unique(),
        program: "pump.fun".to_string(),
        slot: 12345,
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    };

    let validation = validator().validate_candidate(&candidate);
    assert!(validation.is_valid());

    // Test rate limiting
    let mint = Pubkey::new_unique();
    assert!(validator().check_mint_rate_limit(&mint, 60, 3));
    assert!(validator().check_mint_rate_limit(&mint, 60, 3));
    assert!(validator().check_mint_rate_limit(&mint, 60, 3));
    assert!(!validator().check_mint_rate_limit(&mint, 60, 3)); // Should be blocked

    println!("✅ Security validation integration test passed");
}

#[tokio::test]
async fn test_endpoint_server_integration() {
    let server = endpoint_server();
    
    // Update scoreboard
    server.update_scoreboard("test_mint", "pump.fun", true, 150).await;
    server.update_scoreboard("test_mint", "pump.fun", false, 200).await;
    
    // Get responses
    let health = server.get_health_response();
    assert!(health.contains("healthy"));
    
    let status = server.get_status_response().await;
    assert!(status.contains("metrics"));
    
    let scoreboard = server.get_scoreboard_response(Some(10)).await;
    assert!(scoreboard.contains("test_mint"));
    
    let metrics_response = server.get_metrics_response();
    assert!(!metrics_response.is_empty());

    println!("✅ Endpoint server integration test passed");
}

#[tokio::test]
async fn test_performance_optimizations() {
    use sniffer_bot_light::candidate_buffer::CandidateBuffer;
    
    // Test optimized candidate buffer performance
    let mut buffer = CandidateBuffer::new(Duration::from_secs(30), 1000);
    
    let start = std::time::Instant::now();
    
    // Insert many candidates
    for i in 0..1000 {
        let candidate = PremintCandidate {
            mint: Pubkey::new_unique(),
            creator: Pubkey::new_unique(),
            program: "pump.fun".to_string(),
            slot: i as u64,
            timestamp: i as u64,
        };
        buffer.push(candidate);
    }
    
    let insert_duration = start.elapsed();
    
    let start = std::time::Instant::now();
    
    // Pop all candidates (should be O(1) per operation now)
    let mut count = 0;
    while buffer.pop_best().is_some() {
        count += 1;
    }
    
    let pop_duration = start.elapsed();
    
    assert_eq!(count, 1000);
    
    // Performance should be reasonable
    assert!(insert_duration.as_millis() < 1000); // Less than 1 second
    assert!(pop_duration.as_millis() < 100); // Much faster pops due to O(1) optimization
    
    println!("✅ Performance optimization test passed");
    println!("   Insert time: {}ms for 1000 items", insert_duration.as_millis());
    println!("   Pop time: {}ms for 1000 items", pop_duration.as_millis());
}