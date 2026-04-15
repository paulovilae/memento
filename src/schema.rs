use crate::migrations;

pub async fn init_db() -> anyhow::Result<sqlx::PgPool> {
    let db_url = std::env::var("MEMENTO_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .unwrap_or_else(|_| {
            "postgresql://imaginos:imaginos_secure_2026@localhost:5432/os_core_db".to_string()
        });
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect(&db_url)
        .await?;

    migrations::run_all(&pool).await?;

    Ok(pool)
}
