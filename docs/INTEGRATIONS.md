# Integrations and API format

## PumpPortal / Moralis (HTTP)
- Request includes mint, amount/price, slippage, payer
- Response (preferred):
  - `program_id` — string (Pubkey)
  - `data` — base64 bytes for instruction
  - `accounts` — list of `{ pubkey, is_signer, is_writable }`

## LetsBonk (HTTP)
- Similar structure; optional header for API key:
  - `X-API-KEY: ...`

## Signer validation
- Any account with `is_signer=true` must match the wallet pubkey of the running bot. Otherwise instruction is rejected.

## Program allowlist
- If `allowed_programs` is non-empty, only instructions with `program_id` present in the list are accepted.

## Legacy fallback
- If API returns only `instruction_b64`, the builder uses an SPL MEMO instruction to keep the flow robust and observable.