# ASTROpay Rust Backend

This service is the beginning of the backend migration out of Next.js route handlers.

What it currently owns:

- merchant registration, login, logout, and cookie-backed sessions
- invoice creation, listing, detail lookup, and status lookup
- Horizon-backed reconciliation for pending invoices
- webhook-driven payment marking (`/api/webhooks/stellar`)
- a Rust migration runner that reuses the existing SQL migrations

What is intentionally not faked yet:

- buyer XDR generation/submission for checkout
- merchant settlement cron

Those routes return `501 Not Implemented` in the Rust service until the Stellar transaction logic is ported properly.

## Merchant registration and wallet keys

`POST /api/auth/register` trims `stellar_public_key` and `settlement_public_key` before storage. Registration returns **409 Conflict** if either incoming key already appears on **any** existing merchant in **either** column (same rule for both keys so a key cannot be “shared” as someone else’s business wallet or settlement wallet). That prevents ambiguous ownership of payouts and identity.

Apply migration `002_merchant_wallet_key_indexes.sql` (via `cargo run --bin migrate`) so those lookups stay fast as the merchant table grows.

**Verify:** `cargo test wallet_conflict` (pure logic). With Postgres running, register a merchant, then register again with the same stellar or settlement key (or swap roles) and expect **409** with the conflict message.

## Run locally

```bash
cd rust-backend
cargo run --bin migrate
cargo run
```

The service reads env vars from:

- `rust-backend/.env.local`
- `rust-backend/.env`
- `../usdc-payment-link-tool/.env.local`
- `../usdc-payment-link-tool/.env`
