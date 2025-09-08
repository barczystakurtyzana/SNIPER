use sniffer_bot_light::{
    metrics::metrics,
    endpoints::endpoint_server,
    security::validator,
    types::PremintCandidate,
};
use solana_sdk::pubkey::Pubkey;
use std::time::Duration;

#[tokio::main]
async fn main() {
    println!("🚀 SNIPER Bot Telemetry Demo");
    println!("=============================");

    // Initialize some mock data
    println!("\n📊 Simulating bot activity...");
    
    // Simulate some buy attempts
    for i in 0..10 {
        if i % 3 == 0 {
            metrics().increment_counter("buy_success_total");
            endpoint_server().update_scoreboard(&format!("mint_{}", i), "pump.fun", true, 100 + i * 10).await;
        } else {
            metrics().increment_counter("buy_failure_total");
            endpoint_server().update_scoreboard(&format!("mint_{}", i), "pump.fun", false, 200 + i * 10).await;
        }
        metrics().increment_counter("buy_attempts_total");
    }
    
    // Set some gauges
    metrics().set_gauge("candidate_buffer_size", 42);
    metrics().set_gauge("nonce_leases_in_flight", 3);
    
    // Record some latencies
    for latency in [50, 75, 100, 150, 200] {
        metrics().record_histogram("buy_latency_seconds", Duration::from_millis(latency));
    }

    // Test security features
    println!("🔒 Testing security features...");
    let test_candidate = PremintCandidate {
        mint: Pubkey::new_unique(),
        creator: Pubkey::new_unique(),
        program: "pump.fun".to_string(),
        slot: 12345,
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    };
    
    let validation = validator().validate_candidate(&test_candidate);
    println!("   Candidate validation: {}", if validation.is_valid() { "✅ PASSED" } else { "❌ FAILED" });
    
    // Display endpoint responses
    println!("\n📋 Endpoint Responses:");
    println!("======================");
    
    // Health endpoint
    println!("\n🏥 Health Endpoint:");
    let health = endpoint_server().get_health_response();
    println!("{}", health);
    
    // Metrics endpoint (Prometheus format)
    println!("\n📊 Metrics Endpoint (Prometheus format):");
    let metrics_response = endpoint_server().get_metrics_response();
    println!("{}", metrics_response);
    
    // Status endpoint (JSON)
    println!("\n📈 Status Endpoint (JSON):");
    let status = endpoint_server().get_status_response().await;
    println!("{}", status);
    
    // Scoreboard endpoint
    println!("\n🏆 Scoreboard Endpoint (Top 5):");
    let scoreboard = endpoint_server().get_scoreboard_response(Some(5)).await;
    println!("{}", scoreboard);
    
    println!("\n✅ Demo completed successfully!");
    println!("\nKey improvements implemented:");
    println!("• ⚡ Optimized CandidateBuffer from O(n) to O(1) operations");
    println!("• 📊 Added comprehensive metrics collection");
    println!("• 🔄 Implemented structured JSON logging with correlation IDs");
    println!("• 🔒 Added security validation and rate limiting");
    println!("• 🌐 Created endpoint server for metrics and monitoring");
    println!("• 💾 Added RPC client connection pooling");
    println!("• 🛡️  Enhanced overflow protection and duplicate detection");
}