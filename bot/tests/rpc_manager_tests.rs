use sniffer_bot_light::rpc_manager::{RpcBroadcaster, RpcManager, RpcErrorType, classify_rpc_error};
use anyhow::{anyhow, Result};
use solana_client::client_error::{ClientError, ClientErrorKind};
use solana_client::rpc_request::{RpcError, RpcResponseErrorData, RpcRequest};
use solana_sdk::{
    signature::Signature,
    transaction::VersionedTransaction,
    pubkey::Pubkey,
    message::{Message, VersionedMessage},
    system_instruction,
};
use std::future::Future;
use std::pin::Pin;

#[tokio::test]
async fn test_classify_already_processed_error() {
    let rpc_error = RpcError::RpcResponseError {
        code: -32002,
        message: "Transaction was already processed".to_string(),
        data: RpcResponseErrorData::Empty,
    };
    let client_error = ClientError::new_with_request(
        ClientErrorKind::RpcError(rpc_error), 
        RpcRequest::SendTransaction
    );
    
    let error_type = classify_rpc_error(&client_error);
    assert_eq!(error_type, RpcErrorType::AlreadyProcessed);
}

#[tokio::test]
async fn test_classify_duplicate_signature_error() {
    let rpc_error = RpcError::RpcResponseError {
        code: -32002,
        message: "Duplicate signature detected".to_string(),
        data: RpcResponseErrorData::Empty,
    };
    let client_error = ClientError::new_with_request(
        ClientErrorKind::RpcError(rpc_error), 
        RpcRequest::SendTransaction
    );
    
    let error_type = classify_rpc_error(&client_error);
    assert_eq!(error_type, RpcErrorType::DuplicateSignature);
}

#[tokio::test]
async fn test_classify_blockhash_not_found_error() {
    let rpc_error = RpcError::RpcResponseError {
        code: -32002,
        message: "Blockhash not found".to_string(),
        data: RpcResponseErrorData::Empty,
    };
    let client_error = ClientError::new_with_request(
        ClientErrorKind::RpcError(rpc_error), 
        RpcRequest::SendTransaction
    );
    
    let error_type = classify_rpc_error(&client_error);
    assert_eq!(error_type, RpcErrorType::BlockhashNotFound);
}

#[tokio::test]
async fn test_classify_rate_limited_error() {
    let rpc_error = RpcError::RpcResponseError {
        code: -32002,
        message: "Rate limit exceeded".to_string(),
        data: RpcResponseErrorData::Empty,
    };
    let client_error = ClientError::new_with_request(
        ClientErrorKind::RpcError(rpc_error), 
        RpcRequest::SendTransaction
    );
    
    let error_type = classify_rpc_error(&client_error);
    assert_eq!(error_type, RpcErrorType::RateLimited);
}

#[tokio::test]
async fn test_classify_generic_error() {
    let rpc_error = RpcError::RpcResponseError {
        code: -32002,
        message: "Some unknown error".to_string(),
        data: RpcResponseErrorData::Empty,
    };
    let client_error = ClientError::new_with_request(
        ClientErrorKind::RpcError(rpc_error), 
        RpcRequest::SendTransaction
    );
    
    let error_type = classify_rpc_error(&client_error);
    assert_eq!(error_type, RpcErrorType::Other("Some unknown error".to_string()));
}

#[tokio::test]
async fn test_rpc_manager_construction() {
    // Test that the RpcManager can be constructed with multiple endpoints
    let manager = RpcManager::new(vec![
        "http://endpoint1".to_string(),
        "http://endpoint2".to_string(), 
    ]);
    
    assert_eq!(manager.endpoints.len(), 2);
}

#[derive(Clone, Debug)]
struct MockSuccessBroadcaster;

impl RpcBroadcaster for MockSuccessBroadcaster {
    fn send_on_many_rpc<'a>(
        &'a self,
        _txs: Vec<VersionedTransaction>,
    ) -> Pin<Box<dyn Future<Output = Result<Signature>> + Send + 'a>> {
        Box::pin(async move {
            Ok(Signature::default())
        })
    }
}

#[tokio::test]
async fn test_rpc_broadcaster_trait_usage() {
    let broadcaster = MockSuccessBroadcaster;
    let tx = create_dummy_transaction();
    
    let result = broadcaster.send_on_many_rpc(vec![tx]).await;
    assert!(result.is_ok(), "Mock broadcaster should succeed");
}

fn create_dummy_transaction() -> VersionedTransaction {
    let from = Pubkey::new_unique();
    let to = Pubkey::new_unique();
    let instruction = system_instruction::transfer(&from, &to, 1000000);
    
    let message = Message::new(&[instruction], Some(&from));
    let versioned_message = VersionedMessage::Legacy(message);
    
    VersionedTransaction {
        message: versioned_message,
        signatures: vec![Signature::default()],
    }
}