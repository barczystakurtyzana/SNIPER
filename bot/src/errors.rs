//! Central error taxonomy for the Sniper Bot
//! 
//! This module provides a standardized error classification system
//! that can be easily mapped to metrics, logs, and monitoring systems.

use thiserror::Error;

/// High-level error categories for metrics and monitoring
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCategory {
    /// Network/RPC related errors
    Network,
    /// Configuration errors
    Configuration,
    /// Resource exhaustion (nonces, memory, etc.)
    Resource,
    /// Transaction building/signing errors
    Transaction,
    /// Data validation errors
    Validation,
    /// Internal system errors
    System,
}

impl ErrorCategory {
    /// Get Prometheus metric label for this category
    pub fn metric_label(&self) -> &'static str {
        match self {
            ErrorCategory::Network => "network",
            ErrorCategory::Configuration => "configuration",
            ErrorCategory::Resource => "resource",
            ErrorCategory::Transaction => "transaction",
            ErrorCategory::Validation => "validation",
            ErrorCategory::System => "system",
        }
    }
}

/// Standardized error types with context and categorization
#[derive(Error, Debug)]
pub enum SniperError {
    #[error("Network error: {message}")]
    Network { message: String, source: Option<anyhow::Error> },
    
    #[error("Configuration error: {message}")]
    Configuration { message: String },
    
    #[error("Resource exhausted: {resource_type}")]
    ResourceExhausted { resource_type: String },
    
    #[error("Transaction error: {message}")]
    Transaction { message: String, source: Option<anyhow::Error> },
    
    #[error("Validation error: {field}: {message}")]
    Validation { field: String, message: String },
    
    #[error("System error: {message}")]
    System { message: String, source: Option<anyhow::Error> },
}

impl SniperError {
    /// Get the error category for metrics/classification
    pub fn category(&self) -> ErrorCategory {
        match self {
            SniperError::Network { .. } => ErrorCategory::Network,
            SniperError::Configuration { .. } => ErrorCategory::Configuration,
            SniperError::ResourceExhausted { .. } => ErrorCategory::Resource,
            SniperError::Transaction { .. } => ErrorCategory::Transaction,
            SniperError::Validation { .. } => ErrorCategory::Validation,
            SniperError::System { .. } => ErrorCategory::System,
        }
    }
    
    /// Create a network error with context
    pub fn network<S: Into<String>>(message: S) -> Self {
        Self::Network {
            message: message.into(),
            source: None,
        }
    }
    
    /// Create a network error with source
    pub fn network_with_source<S: Into<String>>(message: S, source: anyhow::Error) -> Self {
        Self::Network {
            message: message.into(),
            source: Some(source),
        }
    }
    
    /// Create a configuration error
    pub fn config<S: Into<String>>(message: S) -> Self {
        Self::Configuration {
            message: message.into(),
        }
    }
    
    /// Create a resource exhaustion error
    pub fn resource_exhausted<S: Into<String>>(resource_type: S) -> Self {
        Self::ResourceExhausted {
            resource_type: resource_type.into(),
        }
    }
    
    /// Create a transaction error with context
    pub fn transaction<S: Into<String>>(message: S) -> Self {
        Self::Transaction {
            message: message.into(),
            source: None,
        }
    }
    
    /// Create a transaction error with source
    pub fn transaction_with_source<S: Into<String>>(message: S, source: anyhow::Error) -> Self {
        Self::Transaction {
            message: message.into(),
            source: Some(source),
        }
    }
    
    /// Create a validation error
    pub fn validation<F: Into<String>, M: Into<String>>(field: F, message: M) -> Self {
        Self::Validation {
            field: field.into(),
            message: message.into(),
        }
    }
    
    /// Create a system error with context
    pub fn system<S: Into<String>>(message: S) -> Self {
        Self::System {
            message: message.into(),
            source: None,
        }
    }
    
    /// Create a system error with source
    pub fn system_with_source<S: Into<String>>(message: S, source: anyhow::Error) -> Self {
        Self::System {
            message: message.into(),
            source: Some(source),
        }
    }
}

/// Extension trait to easily categorize and convert anyhow errors
pub trait ErrorContext {
    /// Add network error context
    fn network_context<S: Into<String>>(self, message: S) -> SniperError;
    
    /// Add transaction error context
    fn transaction_context<S: Into<String>>(self, message: S) -> SniperError;
    
    /// Add system error context
    fn system_context<S: Into<String>>(self, message: S) -> SniperError;
}

impl ErrorContext for anyhow::Error {
    fn network_context<S: Into<String>>(self, message: S) -> SniperError {
        SniperError::network_with_source(message, self)
    }
    
    fn transaction_context<S: Into<String>>(self, message: S) -> SniperError {
        SniperError::transaction_with_source(message, self)
    }
    
    fn system_context<S: Into<String>>(self, message: S) -> SniperError {
        SniperError::system_with_source(message, self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;

    #[test]
    fn error_categorization() {
        let net_err = SniperError::network("RPC timeout");
        assert_eq!(net_err.category(), ErrorCategory::Network);
        assert_eq!(net_err.category().metric_label(), "network");
        
        let config_err = SniperError::config("Invalid nonce count");
        assert_eq!(config_err.category(), ErrorCategory::Configuration);
        
        let resource_err = SniperError::resource_exhausted("nonce_slots");
        assert_eq!(resource_err.category(), ErrorCategory::Resource);
    }

    #[test]
    fn error_context_extension() {
        let base_error = anyhow!("Connection failed");
        let categorized = base_error.network_context("Failed to connect to RPC");
        
        assert_eq!(categorized.category(), ErrorCategory::Network);
        assert!(categorized.to_string().contains("Network error"));
        assert!(categorized.to_string().contains("Failed to connect to RPC"));
    }
}