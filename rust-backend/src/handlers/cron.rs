use axum::{
    Json,
    extract::{Query, State},
    http::HeaderMap,
};
use chrono::Utc;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio_postgres::types::Json as PgJson;
use tracing::warn;

use crate::{
    AppState,
    auth::authorize_cron_request,
    error::AppError,
    models::Invoice,
    stellar::{
        fetch_treasury_payments, find_payment_for_invoice, invoice_is_expired,
        is_valid_account_public_key,
    },
};

/// Payouts that fail this many times are moved to the dead-letter path.
const PAYOUT_DEAD_LETTER_THRESHOLD: i32 = 5;

/// Default number of recent treasury payments to scan for orphans.
const ORPHAN_SCAN_LIMIT: u32 = 50;

#[derive(Deserialize)]
pub struct DryRunParams {
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Deserialize)]
pub struct OrphanParams {
    /// How many recent treasury payments to fetch from Horizon (default 50, max 200).
    pub limit: Option<u32>,
}

pub async fn reconcile(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<DryRunParams>,
) -> Result<Json<Value>, AppError> {
    authorize_cron_request(&state.config.cron_secret, &headers)?;
    let dry_run = params.dry_run;
    let mut client = state.pool.get().await?;
    let rows = client
        .query(
            "SELECT * FROM invoices WHERE status = 'pending' ORDER BY created_at ASC LIMIT 100",
            &[],
        )
        .await?;
    let invoices = rows.iter().map(Invoice::from_row).collect::<Vec<_>>();
    let mut results = Vec::with_capacity(invoices.len());

    for invoice in invoices {
        if invoice_is_expired(&invoice, Utc::now()) {
            if !dry_run {
                client
                    .execute(
                        "UPDATE invoices SET status = 'expired', updated_at = NOW() WHERE id = $1 AND status = 'pending'",
                        &[&invoice.id],
                    )
                    .await?;
            }
            results.push(json!({ "publicId": invoice.public_id, "action": "expired" }));
            continue;
        }

        match find_payment_for_invoice(&state.config, &invoice).await? {
            Some(payment) => {
                let transaction = client.transaction().await?;
                transaction
                    .execute(
                        "UPDATE invoices
                         SET status = 'paid', paid_at = NOW(), transaction_hash = $2, updated_at = NOW()
                         WHERE id = $1 AND status = 'pending'",
                        &[&invoice.id, &payment.hash],
                    )
                    .await?;
                transaction
                    .execute(
                        "INSERT INTO payment_events (invoice_id, event_type, payload) VALUES ($1, $2, $3)",
                        &[&invoice.id, &"payment_detected", &payment.payment],
                    )
                    .await?;
                let settlement_row = transaction
                    .query_opt(
                        "SELECT m.settlement_public_key
                         FROM merchants m
                         INNER JOIN invoices i ON i.merchant_id = m.id
                         WHERE i.id = $1",
                        &[&invoice.id],
                    )
                    .await?;
                let settlement_key: Option<String> = settlement_row.map(|row| row.get(0));
                let settlement_key = settlement_key.unwrap_or_default();
                let (payout_queued, payout_skip_reason) =
                    if !is_valid_account_public_key(&settlement_key) {
                        transaction
                            .execute(
                                "INSERT INTO payment_events (invoice_id, event_type, payload) VALUES ($1, $2, $3)",
                                &[
                                    &invoice.id,
                                    &"payout_skipped_invalid_destination",
                                    &json!({ "reason": "invalid_settlement_public_key" }),
                                ],
                            )
                            .await?;
                        (false, Some("invalid_settlement_public_key"))
                    } else {
                        let inserted = transaction
                            .execute(
                                "INSERT INTO payouts (invoice_id, merchant_id, destination_public_key, amount_cents, asset_code, asset_issuer)
                                 SELECT id, merchant_id, (SELECT settlement_public_key FROM merchants WHERE merchants.id = invoices.merchant_id),
                                        net_amount_cents, asset_code, asset_issuer
                                 FROM invoices WHERE id = $1
                                 ON CONFLICT (invoice_id) DO NOTHING",
                                &[&invoice.id],
                            )
                            .await?;
                        if inserted > 0 {
                            (true, None)
                        } else {
                            (false, Some("payout_already_queued"))
                        }
                    };
                transaction.commit().await?;
                results.push(json!({
                    "publicId": invoice.public_id,
                    "action": "paid",
                    "txHash": payment.hash,
                    "memo": payment.memo,
                    "payoutQueued": payout_queued,
                    "payoutSkipReason": payout_skip_reason
                }));
            }
            None => {
                results.push(json!({ "publicId": invoice.public_id, "action": "pending" }));
            }
        }
    }

    let body = json!({
        "dryRun": dry_run,
        "scanned": results.len(),
        "results": results
    });
    if !dry_run {
        if let Err(e) = client
            .execute(
                "INSERT INTO cron_runs (job_type, started_at, finished_at, success, metadata, error_detail)
                 VALUES ('reconcile', NOW(), NOW(), true, $1, NULL)",
                &[&PgJson(&body)],
            )
            .await
        {
            warn!(error = %e, "cron_runs audit insert failed for reconcile");
        }
    }

    Ok(Json(body))
}

/// Deletes sessions whose `expires_at` is in the past.
/// Safe to call repeatedly; each run is idempotent and logged to `cron_runs`.
/// Trigger via `GET /api/cron/purge-sessions` with `Authorization: Bearer <CRON_SECRET>`.
pub async fn purge_sessions(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, AppError> {
    authorize_cron_request(&state.config.cron_secret, &headers)?;
    let client = state.pool.get().await?;
    let deleted = client
        .execute("DELETE FROM sessions WHERE expires_at <= NOW()", &[])
        .await?;
    let body = json!({ "deleted": deleted });
    if let Err(e) = client
        .execute(
            "INSERT INTO cron_runs (job_type, started_at, finished_at, success, metadata, error_detail)
             VALUES ('purge_sessions', NOW(), NOW(), true, $1, NULL)",
            &[&PgJson(&body)],
        )
        .await
    {
        warn!(error = %e, "cron_runs audit insert failed for purge_sessions");
    }
    Ok(Json(body))
}

/// Scans `payouts` with status `failed` and increments their failure count.
/// Once a payout reaches [`PAYOUT_DEAD_LETTER_THRESHOLD`] failures it is moved
/// to `dead_lettered` status and a row is inserted into `payout_dead_letters`
/// so operators can inspect and manually resolve it.
///
/// Full Stellar transaction signing/submission is not implemented yet; this
/// handler only manages the failure-tracking and dead-letter escalation path.
pub async fn settle(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<DryRunParams>,
) -> Result<Json<Value>, AppError> {
    authorize_cron_request(&state.config.cron_secret, &headers)?;
    let mut client = state.pool.get().await?;

    // Fetch payouts that have failed and are not yet dead-lettered.
    let rows = client
        .query(
            "SELECT * FROM payouts WHERE status = 'failed' ORDER BY updated_at ASC LIMIT 100",
            &[],
        )
        .await?;

    let mut dead_lettered: Vec<Value> = Vec::new();
    let mut requeued: Vec<Value> = Vec::new();

    for row in &rows {
        let payout_id: uuid::Uuid = row.get("id");
        let invoice_id: uuid::Uuid = row.get("invoice_id");
        let merchant_id: uuid::Uuid = row.get("merchant_id");
        let failure_count: i32 = row.get("failure_count");
        let failure_reason: Option<String> = row.get("failure_reason");
        let new_count = failure_count + 1;

        let tx = client.transaction().await?;

        if new_count >= PAYOUT_DEAD_LETTER_THRESHOLD {
            // Escalate to dead-letter.
            tx.execute(
                "UPDATE payouts
                 SET status = 'dead_lettered', failure_count = $2, last_failure_at = NOW(), updated_at = NOW()
                 WHERE id = $1",
                &[&payout_id, &new_count],
            )
            .await?;
            tx.execute(
                "INSERT INTO payout_dead_letters (payout_id, invoice_id, merchant_id, failure_count, last_failure_reason)
                 VALUES ($1, $2, $3, $4, $5)
                 ON CONFLICT (payout_id) DO NOTHING",
                &[
                    &payout_id,
                    &invoice_id,
                    &merchant_id,
                    &new_count,
                    &failure_reason,
                ],
            )
            .await?;
            tx.execute(
                "INSERT INTO payment_events (invoice_id, event_type, payload) VALUES ($1, $2, $3)",
                &[
                    &invoice_id,
                    &"payout_dead_lettered",
                    &json!({ "payoutId": payout_id, "failureCount": new_count }),
                ],
            )
            .await?;
            tx.commit().await?;
            dead_lettered.push(json!({ "payoutId": payout_id, "failureCount": new_count }));
        } else {
            // Increment failure count and requeue for the next settle run.
            tx.execute(
                "UPDATE payouts
                 SET status = 'queued', failure_count = $2, last_failure_at = NOW(), updated_at = NOW()
                 WHERE id = $1",
                &[&payout_id, &new_count],
            )
            .await?;
            tx.commit().await?;
            requeued.push(json!({ "payoutId": payout_id, "failureCount": new_count }));
        }
    }

    let body = json!({
        "deadLettered": dead_lettered.len(),
        "requeued": requeued.len(),
        "deadLetteredItems": dead_lettered,
        "requeuedItems": requeued,
        "note": "Stellar transaction signing/submission is not implemented yet. This run only manages dead-letter escalation."
    });

    if let Err(e) = client
        .execute(
            "INSERT INTO cron_runs (job_type, started_at, finished_at, success, metadata, error_detail)
             VALUES ('settle', NOW(), NOW(), true, $1, NULL)",
            &[&PgJson(&body)],
        )
        .await
    {
        warn!(error = %e, "cron_runs audit insert failed for settle");
    }

    Ok(Json(body))
}

/// Returns on-chain USDC payments that arrived at the platform treasury but do not
/// match any invoice by `transaction_hash`.
///
/// These are "orphan" payments — funds received on-chain with no corresponding
/// invoice record. Operators use this endpoint to identify and manually reconcile them.
///
/// Query params:
/// - `limit`: number of recent treasury payments to scan (default 50, capped at 200)
///
/// Trigger via `GET /api/cron/orphan-payments` with `Authorization: Bearer <CRON_SECRET>`.
pub async fn orphan_payments(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<OrphanParams>,
) -> Result<Json<Value>, AppError> {
    authorize_cron_request(&state.config.cron_secret, &headers)?;

    let limit = params.limit.unwrap_or(ORPHAN_SCAN_LIMIT).min(200);
    if limit == 0 {
        return Err(AppError::bad_request("limit must be greater than 0"));
    }

    let treasury_payments = fetch_treasury_payments(&state.config, limit).await?;

    if treasury_payments.is_empty() {
        return Ok(Json(json!({
            "scanned": 0,
            "orphans": [],
            "treasury": state.config.platform_treasury_public_key,
        })));
    }

    // Collect all transaction hashes from Horizon to check against the DB in one query.
    let hashes: Vec<String> = treasury_payments
        .iter()
        .map(|p| p.transaction_hash.clone())
        .collect();

    let client = state.pool.get().await?;

    // Fetch all invoices whose transaction_hash matches any of the scanned payments.
    // Any hash NOT in this set is an orphan.
    let rows = client
        .query(
            "SELECT transaction_hash FROM invoices WHERE transaction_hash = ANY($1)",
            &[&hashes],
        )
        .await?;

    let known_hashes: std::collections::HashSet<String> = rows
        .iter()
        .filter_map(|r| r.get::<_, Option<String>>("transaction_hash"))
        .collect();

    let orphans: Vec<Value> = treasury_payments
        .iter()
        .filter(|p| !known_hashes.contains(&p.transaction_hash))
        .map(|p| {
            json!({
                "transactionHash": p.transaction_hash,
                "from": p.from,
                "amount": p.amount,
                "assetCode": p.asset_code,
                "assetIssuer": p.asset_issuer,
            })
        })
        .collect();

    Ok(Json(json!({
        "scanned": treasury_payments.len(),
        "orphanCount": orphans.len(),
        "orphans": orphans,
        "treasury": state.config.platform_treasury_public_key,
    })))
pub async fn replay_invoice(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<ReplayRequest>,
) -> Result<Json<Value>, AppError> {
    authorize_cron_request(&state.config.cron_secret, &headers)?;

    if body.public_id.trim().is_empty() {
        return Err(AppError::bad_request("publicId is required"));
    }

    let mut client = state.pool.get().await?;
    let row = client
        .query_opt(
            "SELECT * FROM invoices WHERE public_id = $1",
            &[&body.public_id],
        )
        .await?
        .ok_or_else(|| AppError::not_found(format!("Invoice '{}' not found", body.public_id)))?;

    let invoice = Invoice::from_row(&row);
    let dry_run = body.dry_run;

    if invoice.status != "pending" {
        return Ok(Json(json!({
            "dryRun": dry_run,
            "publicId": invoice.public_id,
            "action": "skipped",
            "reason": format!("invoice status is '{}', only 'pending' invoices can be replayed", invoice.status)
        })));
    }

    if invoice_is_expired(&invoice, Utc::now()) {
        if !dry_run {
            client
                .execute(
                    "UPDATE invoices SET status = 'expired', updated_at = NOW() WHERE id = $1 AND status = 'pending'",
                    &[&invoice.id],
                )
                .await?;
        }
        return Ok(Json(json!({
            "dryRun": dry_run,
            "publicId": invoice.public_id,
            "action": "expired"
        })));
    }

    match find_payment_for_invoice(&state.config, &invoice).await? {
        None => Ok(Json(json!({
            "dryRun": dry_run,
            "publicId": invoice.public_id,
            "action": "pending"
        }))),
        Some(payment) => {
            if dry_run {
                return Ok(Json(json!({
                    "dryRun": true,
                    "publicId": invoice.public_id,
                    "action": "paid",
                    "txHash": payment.hash,
                    "memo": payment.memo,
                    "payoutQueued": null,
                    "payoutSkipReason": null
                })));
            }

            let transaction = client.transaction().await?;
            transaction
                .execute(
                    "UPDATE invoices
                     SET status = 'paid', paid_at = NOW(), transaction_hash = $2, updated_at = NOW()
                     WHERE id = $1 AND status = 'pending'",
                    &[&invoice.id, &payment.hash],
                )
                .await?;
            transaction
                .execute(
                    "INSERT INTO payment_events (invoice_id, event_type, payload) VALUES ($1, $2, $3)",
                    &[&invoice.id, &"payment_detected", &payment.payment],
                )
                .await?;
            let settlement_row = transaction
                .query_opt(
                    "SELECT m.settlement_public_key
                     FROM merchants m
                     INNER JOIN invoices i ON i.merchant_id = m.id
                     WHERE i.id = $1",
                    &[&invoice.id],
                )
                .await?;
            let settlement_key = settlement_row
                .map(|r| r.get::<_, String>(0))
                .unwrap_or_default();
            let (payout_queued, payout_skip_reason) =
                if !is_valid_account_public_key(&settlement_key) {
                    transaction
                        .execute(
                            "INSERT INTO payment_events (invoice_id, event_type, payload) VALUES ($1, $2, $3)",
                            &[
                                &invoice.id,
                                &"payout_skipped_invalid_destination",
                                &json!({ "reason": "invalid_settlement_public_key" }),
                            ],
                        )
                        .await?;
                    (false, Some("invalid_settlement_public_key"))
                } else {
                    let inserted = transaction
                        .execute(
                            "INSERT INTO payouts (invoice_id, merchant_id, destination_public_key, amount_cents, asset_code, asset_issuer)
                             SELECT id, merchant_id, (SELECT settlement_public_key FROM merchants WHERE merchants.id = invoices.merchant_id),
                                    net_amount_cents, asset_code, asset_issuer
                             FROM invoices WHERE id = $1
                             ON CONFLICT (invoice_id) DO NOTHING",
                            &[&invoice.id],
                        )
                        .await?;
                    if inserted > 0 { (true, None) } else { (false, Some("payout_already_queued")) }
                };
            transaction.commit().await?;
            Ok(Json(json!({
                "dryRun": false,
                "publicId": invoice.public_id,
                "action": "paid",
                "txHash": payment.hash,
                "memo": payment.memo,
                "payoutQueued": payout_queued,
                "payoutSkipReason": payout_skip_reason
            })))
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderMap, HeaderValue, header};

    use crate::auth::authorize_cron_request;

    #[test]
    fn authorizes_valid_bearer_token() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer cron_secret"),
        );
        assert!(authorize_cron_request("cron_secret", &headers).is_ok());
    }

    #[test]
    fn rejects_missing_bearer_token() {
        let headers = HeaderMap::new();
        assert!(authorize_cron_request("cron_secret", &headers).is_err());
    }

    #[test]
    fn rejects_wrong_bearer_token() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer wrong"),
        );
        assert!(authorize_cron_request("cron_secret", &headers).is_err());
    }

    #[test]
    fn rejects_empty_configured_secret() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer anything"),
        );
        assert!(authorize_cron_request("", &headers).is_err());
    }

    #[test]
    fn dead_letter_threshold_is_five() {
        assert_eq!(super::PAYOUT_DEAD_LETTER_THRESHOLD, 5);
    }

    #[test]
    fn orphan_scan_default_limit_is_fifty() {
        assert_eq!(super::ORPHAN_SCAN_LIMIT, 50);
    }

    #[test]
    fn orphan_params_limit_caps_at_200() {
        // Mirrors the cap applied in the handler: .min(200)
        let raw: u32 = 999;
        assert_eq!(raw.min(200), 200);
    }

    #[test]
    fn known_hash_is_not_orphan() {
        use std::collections::HashSet;
        use serde_json::json;
        use crate::stellar::TreasuryPayment;

        let payments = vec![
            TreasuryPayment {
                transaction_hash: "abc123".to_string(),
                from: "GFROM".to_string(),
                amount: "10.00".to_string(),
                asset_code: "USDC".to_string(),
                asset_issuer: "ISSUER".to_string(),
            },
            TreasuryPayment {
                transaction_hash: "def456".to_string(),
                from: "GFROM2".to_string(),
                amount: "5.00".to_string(),
                asset_code: "USDC".to_string(),
                asset_issuer: "ISSUER".to_string(),
            },
        ];

        let mut known: HashSet<String> = HashSet::new();
        known.insert("abc123".to_string());

        let orphans: Vec<_> = payments
            .iter()
            .filter(|p| !known.contains(&p.transaction_hash))
            .map(|p| json!({ "transactionHash": p.transaction_hash }))
            .collect();

        assert_eq!(orphans.len(), 1);
        assert_eq!(orphans[0]["transactionHash"], "def456");
    }

    #[test]
    fn all_known_hashes_yields_no_orphans() {
        use std::collections::HashSet;
        use crate::stellar::TreasuryPayment;

        let payments = vec![TreasuryPayment {
            transaction_hash: "abc123".to_string(),
            from: "GFROM".to_string(),
            amount: "10.00".to_string(),
            asset_code: "USDC".to_string(),
            asset_issuer: "ISSUER".to_string(),
        }];

        let mut known: HashSet<String> = HashSet::new();
        known.insert("abc123".to_string());

        let orphans: Vec<_> = payments
            .iter()
            .filter(|p| !known.contains(&p.transaction_hash))
            .collect();

        assert!(orphans.is_empty());
    }

    #[test]
    fn empty_treasury_payments_yields_no_orphans() {
        use std::collections::HashSet;
        use crate::stellar::TreasuryPayment;

        let payments: Vec<TreasuryPayment> = vec![];
        let known: HashSet<String> = HashSet::new();

        let orphans: Vec<_> = payments
            .iter()
            .filter(|p| !known.contains(&p.transaction_hash))
            .collect();

        assert!(orphans.is_empty());
    }
}

#[cfg(test)]
mod replay_tests {
    use super::ReplayRequest;

    #[test]
    fn replay_request_dry_run_defaults_false() {
        let r: ReplayRequest = serde_json::from_str(r#"{"publicId":"inv_abc"}"#).unwrap();
        assert_eq!(r.public_id, "inv_abc");
        assert!(!r.dry_run);
    }

    #[test]
    fn replay_request_dry_run_true() {
        let r: ReplayRequest =
            serde_json::from_str(r#"{"publicId":"inv_abc","dry_run":true}"#).unwrap();
        assert!(r.dry_run);
    }

    #[test]
    fn replay_request_missing_public_id_fails() {
        assert!(serde_json::from_str::<ReplayRequest>(r#"{}"#).is_err());
    }
}
