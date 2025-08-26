//! REAL sniffer utilities: stricter pump.fun-like heuristics and metadata backfill.

use regex::Regex;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::RpcTransactionConfig;
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_transaction_status::UiTransactionEncoding;
use std::str::FromStr;

/// Extract potential mint and creator from logs using pump.fun-like patterns.
/// Returns (maybe_mint, maybe_creator, all_pubkeys_seen)
pub fn parse_pump_logs(logs: &[String]) -> (Option<Pubkey>, Option<Pubkey>, Vec<Pubkey>) {
    let re = Regex::new(r"[1-9A-HJ-NP-Za-km-z]{32,44}").unwrap();
    let mut keys = Vec::new();

    for line in logs {
        for m in re.find_iter(line) {
            if let Ok(pk) = Pubkey::from_str(m.as_str()) {
                keys.push(pk);
            }
        }
    }

    let mut maybe_mint: Option<Pubkey> = None;
    let mut maybe_creator: Option<Pubkey> = None;

    for line in logs {
        let lower = line.to_lowercase();

        if lower.contains("initialize") && lower.contains("mint") {
            if let Some(pk) = first_key_in_line(&re, line) {
                maybe_mint = Some(pk);
            }
        }
        if lower.contains("create") && lower.contains("mint") {
            if let Some(pk) = first_key_in_line(&re, line) {
                maybe_mint = Some(pk);
            }
        }
        if lower.contains("metadata") && lower.contains("creator") {
            if let Some(pk) = first_key_in_line(&re, line) {
                maybe_creator = Some(pk);
            }
        }
        if lower.contains("authority") && lower.contains("assign") {
            if let Some(pk) = first_key_in_line(&re, line) {
                maybe_creator = Some(pk);
            }
        }
    }

    (maybe_mint, maybe_creator, keys)
}

fn first_key_in_line(re: &Regex, line: &str) -> Option<Pubkey> {
    re.find_iter(line)
        .filter_map(|m| Pubkey::from_str(m.as_str()).ok())
        .next()
}

/// Fetch metadata via RPC getTransaction and attempt to backfill mint/creator.
pub async fn fetch_meta_from_rpc(
    rpc_http_url: &str,
    sig: &str,
    commitment: &str,
) -> anyhow::Result<(Option<Pubkey>, Option<Pubkey>)> {
    let client = RpcClient::new(rpc_http_url.to_string());

    let commitment_cfg = match commitment.to_ascii_lowercase().as_str() {
        "processed" => CommitmentConfig {
            commitment: CommitmentLevel::Processed,
        },
        "finalized" => CommitmentConfig {
            commitment: CommitmentLevel::Finalized,
        },
        _ => CommitmentConfig {
            commitment: CommitmentLevel::Confirmed,
        },
    };

    let tx = client
        .get_transaction_with_config(
            &sig.parse::<Signature>()?,
            RpcTransactionConfig {
                encoding: Some(UiTransactionEncoding::Json),
                commitment: Some(commitment_cfg),
                max_supported_transaction_version: Some(0),
            },
        )
        .await?;

    let mut mint: Option<Pubkey> = None;
    let mut creator: Option<Pubkey> = None;

    if let Some(meta) = tx.transaction.meta {
        if let Some(logs) = meta.log_messages {
            let (m, c, _) = parse_pump_logs(&logs);
            if m.is_some() {
                mint = m;
            }
            if c.is_some() {
                creator = c;
            }
        }

        if mint.is_none() {
            if let Some(bal) = meta.post_token_balances.as_ref().and_then(|v| v.get(0)) {
                if let Some(m_str) = &bal.mint {
                    if let Ok(pk) = Pubkey::from_str(m_str) {
                        mint = Some(pk);
                    }
                }
            }
        }
    }

    Ok((mint, creator))
}