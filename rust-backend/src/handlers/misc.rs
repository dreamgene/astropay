use axum::{Json, extract::State, http::HeaderMap};
use serde_json::{Value, json};

use crate::{
    AppState,
    auth::authorize_cron_request,
    error::AppError,
    models::StellarWebhookRequest,
    money_state::{
        INVOICE_ALREADY_TRANSITIONED_REASON, InvoicePaidOutcome, mark_invoice_paid_and_queue_payout,
    },
};

pub async fn health() -> Json<Value> {
    Json(json!({ "ok": true, "service": "astropay-rust-backend" }))
}

pub async fn stellar_webhook(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<StellarWebhookRequest>,
) -> Result<Json<Value>, AppError> {
    authorize_cron_request(&state.config.cron_secret, &headers)?;
    if payload.public_id.is_empty() || payload.transaction_hash.is_empty() {
        return Err(AppError::bad_request(
            "publicId and transactionHash are required",
        ));
    }

    let mut client = state.pool.get().await?;
    let row = client
        .query_opt(
            "SELECT id, status FROM invoices WHERE public_id = $1",
            &[&payload.public_id],
        )
        .await?;
    let Some(row) = row else {
        return Err(AppError::not_found("Invoice not found"));
    };

    let invoice_id: uuid::Uuid = row.get("id");
    let mut status: String = row.get("status");
    let mut payout_queued: Option<bool> = None;
    let mut payout_skip_reason: Option<&'static str> = None;
    if status == "pending" {
        let transaction = client.transaction().await?;
        let outcome = mark_invoice_paid_and_queue_payout(
            &transaction,
            invoice_id,
            &payload.transaction_hash,
            &payload.rest,
        )
        .await?;
        transaction.commit().await?;
        match outcome {
            InvoicePaidOutcome::Applied {
                payout_queued: queued,
                payout_skip_reason: skip,
            } => {
                status = "paid".to_string();
                payout_queued = Some(queued);
                payout_skip_reason = skip.map(|reason| reason.as_str());
            }
            InvoicePaidOutcome::AlreadyTransitioned => {
                let refreshed = client
                    .query_one("SELECT status FROM invoices WHERE id = $1", &[&invoice_id])
                    .await?;
                status = refreshed.get("status");
                payout_queued = Some(false);
                payout_skip_reason = Some(INVOICE_ALREADY_TRANSITIONED_REASON);
            }
        }
    }

    Ok(Json(json!({
        "received": true,
        "invoiceId": invoice_id,
        "status": status,
        "payoutQueued": payout_queued,
        "payoutSkipReason": payout_skip_reason
    })))
}
