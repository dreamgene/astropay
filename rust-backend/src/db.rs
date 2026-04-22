//! PostgreSQL connection pool.
//!
//! **Cron audit** — table `cron_runs` (see migration `004_cron_runs.sql`) stores one row per
//! reconcile/settle HTTP run with JSONB `metadata` matching the response summary. Application
//! code should not fail the cron HTTP response if an audit insert fails; log and continue.

use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod, Runtime};
use tokio_postgres::Config as PgConfig;

use crate::config::Config;

pub fn create_pool(config: &Config) -> anyhow::Result<Pool> {
    let pg = config.database_url.parse::<PgConfig>()?;
    let manager_config = ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    };
    let manager = Manager::from_config(pg, tokio_postgres::NoTls, manager_config);
    Ok(Pool::builder(manager)
        .runtime(Runtime::Tokio1)
        .max_size(16)
        .build()?)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    #[test]
    fn cron_runs_migration_defines_audit_table() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../usdc-payment-link-tool/migrations/004_cron_runs.sql");
        let sql = std::fs::read_to_string(path).expect("read 004_cron_runs.sql");
        assert!(sql.contains("CREATE TABLE cron_runs"));
        assert!(sql.contains("job_type"));
        assert!(sql.contains("metadata JSONB"));
        assert!(sql.contains("cron_runs_job_type_started_at_idx"));
    }
}
