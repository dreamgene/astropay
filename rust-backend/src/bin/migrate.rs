use std::{fs, path::PathBuf};

use dotenvy::from_filename;
use tokio_postgres::NoTls;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    for path in [
        ".env.local",
        ".env",
        "../usdc-payment-link-tool/.env.local",
        "../usdc-payment-link-tool/.env",
    ] {
        let _ = from_filename(path);
    }

    let database_url = std::env::var("DATABASE_URL")?;
    let (mut client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("postgres connection error: {error}");
        }
    });

    client
        .execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations (
               id TEXT PRIMARY KEY,
               applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
             )",
            &[],
        )
        .await?;

    let migrations_dir = PathBuf::from("../usdc-payment-link-tool/migrations");
    if !migrations_dir.is_dir() {
        anyhow::bail!(
            "migrations directory not found: {} (run from rust-backend/)",
            migrations_dir.display()
        );
    }

    let mut files = fs::read_dir(&migrations_dir)
        .map_err(|e| anyhow::anyhow!("read_dir {}: {e}", migrations_dir.display()))?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("sql"))
        .collect::<Vec<_>>();
    files.sort();

    for file in files {
        let name = file
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default()
            .to_string();
        let exists = client
            .query_opt("SELECT 1 FROM schema_migrations WHERE id = $1", &[&name])
            .await?;
        if exists.is_some() {
            continue;
        }

        let sql = fs::read_to_string(&file)?;
        let transaction = client.transaction().await?;
        transaction
            .batch_execute(&sql)
            .await
            .map_err(|e| anyhow::anyhow!("migration {name} failed: {e}"))?;
        transaction
            .execute("INSERT INTO schema_migrations (id) VALUES ($1)", &[&name])
            .await?;
        transaction.commit().await?;
        println!("Applied {name}");
    }

    Ok(())
}
