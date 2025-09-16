use crate::types::PremintCandidate;
use solana_sdk::pubkey::Pubkey;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Validation and security checks for candidates and operations
#[derive(Debug, Default)]
pub struct SecurityValidator {
    /// Track seen signatures to detect duplicates
    seen_signatures: Arc<Mutex<HashSet<String>>>,
    /// Rate limiting per mint to prevent spam
    mint_rate_limiter: Arc<Mutex<HashMap<Pubkey, Vec<Instant>>>>,
    /// Last seen slot for monotonic validation
    last_slot: Arc<Mutex<u64>>,
}

impl SecurityValidator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Validate a candidate for security issues
    pub fn validate_candidate(&self, candidate: &PremintCandidate) -> ValidationResult {
        let mut issues = Vec::new();

        // Pubkey sanity check
        if candidate.mint == Pubkey::default() {
            issues.push("Invalid mint pubkey: default/zero pubkey".to_string());
        }

        if candidate.creator == Pubkey::default() {
            issues.push("Invalid creator pubkey: default/zero pubkey".to_string());
        }

        // Slot monotonic validation (slots should generally increase)
        {
            let mut last_slot = self.last_slot.lock().unwrap();
            if candidate.slot < *last_slot && *last_slot > 0 {
                // Allow some backwards tolerance for network reordering
                if *last_slot - candidate.slot > 10 {
                    issues.push(format!(
                        "Slot significantly backwards: current {} vs last {}",
                        candidate.slot, *last_slot
                    ));
                }
            } else {
                *last_slot = candidate.slot.max(*last_slot);
            }
        }

        // Program validation
        if candidate.program.is_empty() {
            issues.push("Empty program name".to_string());
        }

        // Timestamp sanity check (not too far in past or future)
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        if candidate.timestamp > 0 {
            let time_diff = if candidate.timestamp > now {
                candidate.timestamp - now
            } else {
                now - candidate.timestamp
            };

            // Allow 5 minutes tolerance
            if time_diff > 300 {
                issues.push(format!(
                    "Timestamp too far from current time: {} vs {}",
                    candidate.timestamp, now
                ));
            }
        }

        ValidationResult { issues }
    }

    /// Check if a mint is being spammed (rate limiting)
    pub fn check_mint_rate_limit(&self, mint: &Pubkey, window_secs: u64, max_per_window: usize) -> bool {
        let mut rate_limiter = self.mint_rate_limiter.lock().unwrap();
        let now = Instant::now();
        let window_duration = Duration::from_secs(window_secs);

        let timestamps = rate_limiter.entry(*mint).or_insert_with(Vec::new);

        // Remove old timestamps outside the window
        timestamps.retain(|&timestamp| now.duration_since(timestamp) < window_duration);

        // Check if we're within limits
        if timestamps.len() >= max_per_window {
            return false; // Rate limit exceeded
        }

        // Record this request
        timestamps.push(now);
        true
    }

    /// Check for duplicate signature attempts
    pub fn check_duplicate_signature(&self, signature: &str) -> bool {
        let mut seen = self.seen_signatures.lock().unwrap();
        if seen.contains(signature) {
            return false; // Duplicate detected
        }
        seen.insert(signature.to_string());

        // Prevent unbounded growth by cleaning old signatures periodically
        if seen.len() > 10000 {
            // Keep only last 5000 signatures
            let mut sigs: Vec<_> = seen.iter().cloned().collect();
            sigs.sort();
            seen.clear();
            for sig in sigs.into_iter().skip(5000) {
                seen.insert(sig);
            }
        }

        true
    }

    /// Validate holdings percentage for overflow protection
    pub fn validate_holdings_percent(&self, percent: f64) -> Result<f64, String> {
        if !percent.is_finite() {
            return Err("Holdings percent is not finite".to_string());
        }

        if percent < 0.0 {
            return Err("Holdings percent cannot be negative".to_string());
        }

        if percent > 1.0 {
            return Err("Holdings percent cannot exceed 100%".to_string());
        }

        Ok(percent)
    }

    /// Validate lamports amount for overflow protection
    pub fn validate_lamports(&self, amount: u64) -> Result<u64, String> {
        // Check for reasonable bounds to prevent overflow in calculations
        const MAX_REASONABLE_LAMPORTS: u64 = 1_000_000_000_000_000; // 1M SOL in lamports

        if amount > MAX_REASONABLE_LAMPORTS {
            return Err(format!(
                "Lamports amount {} exceeds reasonable maximum {}",
                amount, MAX_REASONABLE_LAMPORTS
            ));
        }

        Ok(amount)
    }

    /// Clear old data periodically for memory management
    pub fn cleanup_old_data(&self) {
        let now = Instant::now();
        
        // Clean rate limiter data older than 1 hour
        {
            let mut rate_limiter = self.mint_rate_limiter.lock().unwrap();
            rate_limiter.retain(|_mint, timestamps| {
                timestamps.retain(|&timestamp| now.duration_since(timestamp) < Duration::from_secs(3600));
                !timestamps.is_empty()
            });
        }

        // Limit signature cache size
        {
            let mut seen = self.seen_signatures.lock().unwrap();
            if seen.len() > 10000 {
                let to_remove = seen.len() - 5000;
                let mut sigs: Vec<_> = seen.iter().cloned().collect();
                sigs.sort();
                for sig in sigs.into_iter().take(to_remove) {
                    seen.remove(&sig);
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct ValidationResult {
    pub issues: Vec<String>,
}

impl ValidationResult {
    pub fn is_valid(&self) -> bool {
        self.issues.is_empty()
    }

    pub fn has_critical_issues(&self) -> bool {
        // Consider all issues critical for now
        !self.issues.is_empty()
    }
}

/// Global security validator instance
static GLOBAL_VALIDATOR: std::sync::OnceLock<SecurityValidator> = std::sync::OnceLock::new();

/// Get global security validator
pub fn validator() -> &'static SecurityValidator {
    GLOBAL_VALIDATOR.get_or_init(SecurityValidator::new)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_candidate_validation() {
        let validator = SecurityValidator::new();
        
        let valid_candidate = PremintCandidate {
            mint: Pubkey::new_unique(),
            creator: Pubkey::new_unique(),
            program: "pump.fun".to_string(),
            slot: 1000,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            instruction_summary: Some("Test instruction".to_string()),
            is_jito_bundle: Some(false),
        };

        let result = validator.validate_candidate(&valid_candidate);
        assert!(result.is_valid());

        let invalid_candidate = PremintCandidate {
            mint: Pubkey::default(),
            creator: Pubkey::default(),
            program: "".to_string(),
            slot: 0,
            timestamp: 0,
            instruction_summary: None,
            is_jito_bundle: None,
        };

        let result = validator.validate_candidate(&invalid_candidate);
        assert!(!result.is_valid());
        assert!(result.issues.len() >= 2); // Should have mint and creator issues
    }

    #[test]
    fn test_rate_limiting() {
        let validator = SecurityValidator::new();
        let mint = Pubkey::new_unique();

        // Should allow first few requests
        assert!(validator.check_mint_rate_limit(&mint, 60, 3));
        assert!(validator.check_mint_rate_limit(&mint, 60, 3));
        assert!(validator.check_mint_rate_limit(&mint, 60, 3));

        // Should block 4th request
        assert!(!validator.check_mint_rate_limit(&mint, 60, 3));
    }

    #[test]
    fn test_duplicate_signature_detection() {
        let validator = SecurityValidator::new();

        assert!(validator.check_duplicate_signature("sig1"));
        assert!(!validator.check_duplicate_signature("sig1")); // Duplicate
        assert!(validator.check_duplicate_signature("sig2")); // New signature
    }

    #[test]
    fn test_holdings_validation() {
        let validator = SecurityValidator::new();

        assert!(validator.validate_holdings_percent(0.5).is_ok());
        assert!(validator.validate_holdings_percent(1.0).is_ok());
        assert!(validator.validate_holdings_percent(0.0).is_ok());

        assert!(validator.validate_holdings_percent(-0.1).is_err());
        assert!(validator.validate_holdings_percent(1.1).is_err());
        assert!(validator.validate_holdings_percent(f64::INFINITY).is_err());
        assert!(validator.validate_holdings_percent(f64::NAN).is_err());
    }
}