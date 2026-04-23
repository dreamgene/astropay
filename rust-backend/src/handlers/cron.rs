use axum::{
    Json,
    extract::{Path, State},
    http::HeaderMap,
};
use chrono::Utc;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio_postgres::types::Json as PgJson;
use tracing::warn;
use uuid::Uuid;

use crate::{
    AppState,
    auth::authorize_cron_request,
    error::AppError,
    models::Invoice,
    stellar::{
        TransactionStatus, confirm_transaction, find_payment_for_invoice,
        invoice_is_expired, is_valid_account_public_key,
    },
};

/// Payouts that fail this many times are moved to the dead-letter path.
const PAYOUT_DEAD_LETTER_THRESHOLD: i32 = 5;

#[derive(Deserialize)]
pub struct DryRunParams {
    #[serde(default)]
    pub dry_run: bool,
}

pub async fn reconcile(
    State(state): State<AppState>,
    headers: HeaderMap,
    axum::extract::Query(params): axum::extract::Query<DryRunParams>,
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

        // Issue #167: treat HorizonUnavailable as a transient skip — do NOT
        // flip the invoice to failed.
        match find_payment_for_invoice(&state.config, &invoice).await {
            Err(AppError::HorizonUnavailable) => {
                warn!(
                    public_id = %invoice.public_id,
                    "Horizon unavailable during reconcile — skipping invoice to avoid false failure"
                );
                results.push(json!({ "publicId": invoice.public_id, "action": "skipped_horizon_unavailable" }));
                continue;
            }
            Err(e) => return Err(e),
            Ok(None) => {
                results.push(json!({ "publicId": invoice.public_id, "action": "pending" }));
            }
            Ok(Some(payment)) => {
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
                        if inserted > 0 { (true, None) } else { (false, Some("payout_already_queued")) }
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
        }
    }

    // Issue #158: confirm any payouts that are in 'submitted' state by querying
    // Horizon for their transaction hash.
    let submitted_rows = client
        .query(
            "SELECT id, transaction_hash FROM payouts WHERE status = 'submitted' AND transaction_hash IS NOT NULL LIMIT 100",
            &[],
        )
        .await?;

    let mut confirmed: Vec<Value> = Vec::new();
    let mut chain_failed: Vec<Value> = Vec::new();

    for row in &submitted_rows {
        let payout_id: Uuid = row.get("id");
        let tx_hash: String = row.get("transaction_hash");

        match confirm_transaction(&state.config, &tx_hash).await {
            Err(AppError::HorizonUnavailable) => {
                // Issue #167: skip — do not mark as failed during an outage.
                warn!(payout_id = %payout_id, "Horizon unavailable during payout confirmation — leaving as submitted");
            }
            Err(e) => return Err(e),
            Ok(TransactionStatus::Success) => {
                if !dry_run {
                    client
                        .execute(
                            "UPDATE payouts SET status = 'confirmed', updated_at = NOW() WHERE id = $1 AND status = 'submitted'",
                            &[&payout_id],
                        )
                        .await?;
                }
                confirmed.push(json!({ "payoutId": payout_id, "txHash": tx_hash }));
            }
            Ok(TransactionStatus::Failed) => {
                if !dry_run {
                    client
                        .execute(
                            "UPDATE payouts SET status = 'failed', failure_reason = 'chain_rejected', updated_at = NOW() WHERE id = $1 AND status = 'submitted'",
                            &[&payout_id],
                        )
                        .await?;
                }
                chain_failed.push(json!({ "payoutId": payout_id, "txHash": tx_hash }));
            }
            Ok(TransactionStatus::NotFound) => {
                // Still pending on-chain — leave as submitted.
            }
        }
    }

    let body = json!({
        "dryRun": dry_run,
        "scanned": results.len(),
        "results": results,
        "payoutsConfirmed": confirmed.len(),
        "payoutsChainFailed": chain_failed.len(),
        "confirmedItems": confirmed,
        "chainFailedItems": chain_failed,
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
/// to `dead_lettered` status.
pub async fn settle(
    State(state): State<AppState>,
    headers: HeaderMap,
    axum::extract::Query(params): axum::extract::Query<DryRunParams>,
) -> Result<Json<Value>, AppError> {
    authorize_cron_request(&state.config.cron_secret, &headers)?;
    let mut client = state.pool.get().await?;

    let rows = client
        .query(
            "SELECT * FROM payouts WHERE status = 'failed' ORDER BY updated_at ASC LIMIT 100",
            &[],
        )
        .await?;

    let mut dead_lettered: Vec<Value> = Vec::new();
    let mut requeued: Vec<Value> = Vec::new();

    for row in &rows {
        let payout_id: Uuid = row.get("id");
        let invoice_id: Uuid = row.get("invoice_id");
        let merchant_id: Uuid = row.get("merchant_id");
        let failure_count: i32 = row.get("failure_count");
        let failure_reason: Option<String> = row.get("failure_reason");
        let new_count = failure_count + 1;

        let tx = client.transaction().await?;

        if new_count >= PAYOUT_DEAD_LETTER_THRESHOLD {
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
                &[&payout_id, &invoice_id, &merchant_id, &new_count, &failure_reason],
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

/// Issue #178 — Manual replay endpoint for a specific payout settlement.
///
/// `POST /api/cron/payouts/:payout_id/replay`
///
/// Resets a single payout from `dead_lettered` or `failed` back to `queued`
/// so the next settle run will attempt it again. The action is audited in
/// `payment_events` for full operator traceability.
///
/// Requires the same `Authorization: Bearer <CRON_SECRET>` header as other
/// cron endpoints so only operators with the secret can trigger replays.
pub async fn replay_payout(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(payout_id): Path<Uuid>,
) -> Result<Json<Value>, AppError> {
    authorize_cron_request(&state.config.cron_secret, &headers)?;
    let mut client = state.pool.get().await?;

    // Load the payout — must exist and be in a replayable state.
    let row = client
        .query_opt(
            "SELECT id, invoice_id, merchant_id, status, failure_count FROM payouts WHERE id = $1",
            &[&payout_id],
        )
        .await?
        .ok_or_else(|| AppError::not_found(format!("payout {payout_id} not found")))?;

    let status: String = row.get("status");
    let invoice_id: Uuid = row.get("invoice_id");
    let failure_count: i32 = row.get("failure_count");

    if status != "dead_lettered" && status != "failed" {
        return Err(AppError::bad_request(format!(
            "payout {payout_id} has status '{status}'; only dead_lettered or failed payouts can be replayed"
        )));
    }

    let tx = client.transaction().await?;

    // Reset the payout to queued with zeroed failure count so it gets a clean attempt.
    tx.execute(
        "UPDATE payouts
         SET status = 'queued', failure_count = 0, failure_reason = NULL,
             last_failure_at = NULL, updated_at = NOW()
         WHERE id = $1",
        &[&payout_id],
    )
    .await?;

    // Audit the replay in payment_events.
    tx.execute(
        "INSERT INTO payment_events (invoice_id, event_type, payload) VALUES ($1, $2, $3)",
        &[
            &invoice_id,
            &"payout_replayed",
            &json!({
                "payoutId": payout_id,
                "previousStatus": status,
                "previousFailureCount": failure_count,
                "replayedAt": Utc::now().to_rfc3339(),
            }),
        ],
    )
    .await?;

    tx.commit().await?;

    Ok(Json(json!({
        "payoutId": payout_id,
        "previousStatus": status,
        "newStatus": "queued",
        "message": "Payout has been reset to queued and will be retried on the next settle run."
    })))
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
}
