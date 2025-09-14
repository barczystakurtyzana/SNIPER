# TransactionBuilder — overview

This document explains the production-ready TransactionBuilder used by the bot.

## Key features
- Multi-RPC rotation with exponential backoff
- Recent blockhash cache (TTL = 15s)
- Correct signature vector sizing when `sign=false` (uses `message.header().num_required_signatures`)
- External API parsing (`parse_external_api_response`) with:
  - program allowlist (`allowed_programs`)
  - strict signer validation (reject any `is_signer=true` for a key different than the wallet pubkey)
- HTTP integrations:
  - PumpPortal / Moralis fallback with MEMO
  - LetsBonk (HTTP) with optional API key
  - Optional `pumpfun` crate (feature-guarded)
- NonceManager integration to coordinate parallel builds
- Jito bundle candidate wrapper

## Configuration (TransactionConfig)
- `priority_fee_lamports` (u64): micro-lamports per CU
- `compute_unit_limit` (u32): CU limit
- `buy_amount_lamports` (u64): lamports to buy
- `slippage_percent` (f64 0..=100)
- `rpc_endpoints` (Vec<String>): rotation list
- `rpc_retry_attempts` (usize)
- `rpc_timeout_ms` (u64)
- `pumpportal_url` / `pumpportal_api_key`
- `letsbonk_api_url` / `letsbonk_api_key`
- `jito_bundle_enabled` (bool)
- `signer_keypair_index` (Option<usize>)
- `nonce_count` (usize)
- `allowed_programs` (Vec<Pubkey>)

Validation ensures:
- `buy_amount_lamports > 0`
- `0.0 <= slippage_percent <= 100.0`
- `rpc_endpoints` non-empty
- `nonce_count > 0`

## External APIs — response shape
Preferred JSON:
```json
{
  "program_id": "So11111111111111111111111111111111111111112",
  "data": "BASE64_ENCODED_INSTRUCTION_DATA",
  "accounts": [
    { "pubkey": "WalletPubkey...", "is_signer": false, "is_writable": false }
  ]
}
```
- `program_id` must be a valid Pubkey (and on allowlist if provided)
- `data` is base64-decoded and limited to 4KB
- `accounts` entries are validated:
  - any `is_signer=true` pubkey must equal the current wallet pubkey

Legacy (fallback):
```json
{ "instruction_b64": "BASE64" }
```
- Will be wrapped into an SPL MEMO instruction.

## Jito bundle
`prepare_jito_bundle` packages a set of VersionedTransaction plus constraints (max cost, target slot).

## Notes
- Blockhash TTL balances freshness and RPC pressure.
- If the `pumpfun` feature is disabled or an HTTP provider fails, a MEMO placeholder is used (safe default).