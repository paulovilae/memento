use crate::migrations;
use std::str::FromStr;
use std::time::Duration;

pub async fn init_db() -> anyhow::Result<sqlx::PgPool> {
    let db_url = std::env::var("MEMENTO_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .unwrap_or_else(|_| {
            "postgresql://imaginos:imaginos_secure_2026@localhost:5432/os_core_db".to_string()
        });

    let connect_options =
        sqlx::postgres::PgConnectOptions::from_str(&db_url)?.application_name("memento-core");
    let pool = sqlx::postgres::PgPoolOptions::new()
        .min_connections(1)
        .max_connections(3)
        .acquire_timeout(Duration::from_secs(5))
        .idle_timeout(Duration::from_secs(60))
        .max_lifetime(Duration::from_secs(60 * 10))
        .connect_with(connect_options)
        .await?;

    migrations::run_all(&pool).await?;

    Ok(pool)
}
