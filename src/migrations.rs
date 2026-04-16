use crate::document_index;

async fn ensure_pg_column(
    pool: &sqlx::PgPool,
    table: &str,
    column: &str,
    column_definition: &str,
) -> anyhow::Result<()> {
    let query = format!(
        "SELECT column_name FROM information_schema.columns WHERE table_name='{}' AND column_name='{}'",
        table, column
    );
    let rows = sqlx::query(&query).fetch_all(pool).await?;
    if rows.is_empty() {
        let sql = format!("ALTER TABLE {table} ADD COLUMN {column} {column_definition}");
        sqlx::query(&sql).execute(pool).await?;
    }
    Ok(())
}

async fn ensure_pg_index(
    pool: &sqlx::PgPool,
    index_name: &str,
    table: &str,
    columns_sql: &str,
) -> anyhow::Result<()> {
    let exists = sqlx::query_scalar::<_, i64>(
        "SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = $1 LIMIT 1",
    )
    .bind(index_name)
    .fetch_optional(pool)
    .await?
    .is_some();

    if !exists {
        let sql = format!("CREATE INDEX {index_name} ON {table} {columns_sql}");
        sqlx::query(&sql).execute(pool).await?;
    }

    Ok(())
}

async fn ensure_pg_expression_index(
    pool: &sqlx::PgPool,
    index_name: &str,
    table: &str,
    expression_sql: &str,
) -> anyhow::Result<()> {
    let exists = sqlx::query_scalar::<_, i64>(
        "SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = $1 LIMIT 1",
    )
    .bind(index_name)
    .fetch_optional(pool)
    .await?
    .is_some();

    if !exists {
        let sql = format!("CREATE INDEX {index_name} ON {table} {expression_sql}");
        sqlx::query(&sql).execute(pool).await?;
    }

    Ok(())
}

async fn ensure_migrations_table(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        "#,
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn run_migration(
    pool: &sqlx::PgPool,
    version: i32,
    name: &str,
    apply: impl std::future::Future<Output = anyhow::Result<()>>,
) -> anyhow::Result<()> {
    let exists = sqlx::query_scalar::<_, i32>(
        "SELECT version FROM schema_migrations WHERE version = $1 LIMIT 1",
    )
    .bind(version)
    .fetch_optional(pool)
    .await?
    .is_some();

    if exists {
        return Ok(());
    }

    apply.await?;

    sqlx::query("INSERT INTO schema_migrations (version, name) VALUES ($1, $2)")
        .bind(version)
        .bind(name)
        .execute(pool)
        .await?;

    Ok(())
}

async fn migration_1_core_memory(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS memento_memory (
            id SERIAL PRIMARY KEY,
            chat_id TEXT NOT NULL,
            role TEXT NOT NULL,
            content TEXT NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        "#,
    )
    .execute(pool)
    .await?;
    ensure_pg_index(
        pool,
        "idx_memento_memory_chat_timestamp",
        "memento_memory",
        "(chat_id, timestamp DESC)",
    )
    .await?;
    Ok(())
}

async fn migration_2_adaptive_memory(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS adaptive_memory_profiles (
            chat_id TEXT PRIMARY KEY,
            recent_limit INTEGER NOT NULL DEFAULT 4,
            candidate_limit INTEGER NOT NULL DEFAULT 18,
            max_message_chars INTEGER NOT NULL DEFAULT 900,
            max_total_chars INTEGER NOT NULL DEFAULT 2600,
            recency_weight REAL NOT NULL DEFAULT 0.35,
            overlap_weight REAL NOT NULL DEFAULT 0.55,
            assistant_weight REAL NOT NULL DEFAULT 0.10,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS adaptive_memory_feedback (
            id SERIAL PRIMARY KEY,
            chat_id TEXT NOT NULL,
            signal TEXT NOT NULL,
            observed_chars INTEGER,
            query TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        "#,
    )
    .execute(pool)
    .await?;

    ensure_pg_index(
        pool,
        "idx_adaptive_memory_feedback_chat_timestamp",
        "adaptive_memory_feedback",
        "(chat_id, timestamp DESC)",
    )
    .await?;
    Ok(())
}

async fn migration_3_bayesian(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS bayesian_interactions (
            id SERIAL PRIMARY KEY,
            session_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            domain TEXT NOT NULL,
            round INTEGER NOT NULL,
            options_json TEXT NOT NULL,
            choice_index INTEGER NOT NULL,
            prior_json TEXT,
            posterior_json TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS user_priors (
            user_id TEXT NOT NULL,
            domain TEXT NOT NULL,
            prior_json TEXT NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (user_id, domain)
        )
        "#,
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn migration_4_scoped_memory(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS scoped_memory (
            id SERIAL PRIMARY KEY,
            user_id TEXT NOT NULL,
            tenant_id TEXT NOT NULL DEFAULT 'default',
            app_id TEXT NOT NULL DEFAULT 'os',
            expert_id TEXT NOT NULL DEFAULT 'ava',
            session_id TEXT NOT NULL DEFAULT '',
            device_id TEXT NOT NULL DEFAULT 'server',
            scope TEXT NOT NULL DEFAULT 'personal',
            source TEXT NOT NULL DEFAULT 'chat',
            content TEXT NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        "#,
    )
    .execute(pool)
    .await?;

    ensure_pg_column(
        pool,
        "scoped_memory",
        "memory_type",
        "TEXT NOT NULL DEFAULT 'event'",
    )
    .await?;
    ensure_pg_column(pool, "scoped_memory", "content_json", "TEXT").await?;
    ensure_pg_column(pool, "scoped_memory", "confidence", "REAL").await?;
    ensure_pg_column(pool, "scoped_memory", "provenance_refs", "TEXT").await?;
    ensure_pg_column(pool, "scoped_memory", "derivation_method", "TEXT").await?;
    ensure_pg_column(
        pool,
        "scoped_memory",
        "status",
        "TEXT NOT NULL DEFAULT 'active'",
    )
    .await?;
    ensure_pg_column(pool, "scoped_memory", "expires_at", "TIMESTAMP").await?;
    ensure_pg_column(pool, "scoped_memory", "wing", "TEXT NOT NULL DEFAULT ''").await?;
    ensure_pg_column(pool, "scoped_memory", "hall", "TEXT NOT NULL DEFAULT ''").await?;
    ensure_pg_column(pool, "scoped_memory", "room", "TEXT NOT NULL DEFAULT ''").await?;
    ensure_pg_column(
        pool,
        "scoped_memory",
        "entry_title",
        "TEXT NOT NULL DEFAULT ''",
    )
    .await?;
    ensure_pg_column(pool, "scoped_memory", "tags_json", "TEXT").await?;
    ensure_pg_column(
        pool,
        "scoped_memory",
        "usage_count",
        "INTEGER NOT NULL DEFAULT 0",
    )
    .await?;
    ensure_pg_column(pool, "scoped_memory", "last_used_at", "TIMESTAMP").await?;
    ensure_pg_column(pool, "scoped_memory", "promoted_from", "TEXT").await?;

    ensure_pg_index(
        pool,
        "idx_scoped_memory_primary_lookup",
        "scoped_memory",
        "(user_id, app_id, scope, memory_type, timestamp DESC)",
    )
    .await?;
    ensure_pg_index(
        pool,
        "idx_scoped_memory_palace_lookup",
        "scoped_memory",
        "(tenant_id, app_id, wing, hall, room, timestamp DESC)",
    )
    .await?;
    ensure_pg_index(
        pool,
        "idx_scoped_memory_session_lookup",
        "scoped_memory",
        "(session_id, timestamp DESC)",
    )
    .await?;
    ensure_pg_index(
        pool,
        "idx_scoped_memory_status_lookup",
        "scoped_memory",
        "(status, expires_at, timestamp DESC)",
    )
    .await?;
    ensure_pg_index(
        pool,
        "idx_scoped_memory_usage_lookup",
        "scoped_memory",
        "(app_id, usage_count DESC, last_used_at DESC)",
    )
    .await?;
    ensure_pg_index(
        pool,
        "idx_scoped_memory_retrieval_lookup",
        "scoped_memory",
        "(app_id, status, memory_type, timestamp DESC)",
    )
    .await?;
    ensure_pg_expression_index(
        pool,
        "idx_scoped_memory_fts_lookup",
        "scoped_memory",
        "USING GIN (to_tsvector('simple', coalesce(entry_title, '') || ' ' || coalesce(content, '') || ' ' || coalesce(memory_type, '') || ' ' || coalesce(tags_json, '')))",
    )
    .await?;
    Ok(())
}

async fn migration_5_audit_and_bio(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS audit_log (
            id SERIAL PRIMARY KEY,
            actor TEXT NOT NULL,
            expert_identity TEXT NOT NULL,
            capability_used TEXT NOT NULL,
            sensitive_action TEXT,
            target_app TEXT,
            target_page TEXT,
            mutation_description TEXT NOT NULL,
            tenant_id TEXT,
            session_id TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        "#,
    )
    .execute(pool)
    .await?;

    let langs = ["", "_es", "_fr", "_it"];
    for ext in &langs {
        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS paulo_bio_experience{} (
                id SERIAL PRIMARY KEY,
                slug TEXT UNIQUE NOT NULL,
                title TEXT NOT NULL,
                company TEXT NOT NULL,
                duration TEXT NOT NULL,
                tag TEXT NOT NULL,
                summary TEXT NOT NULL,
                sort_order INTEGER DEFAULT 0
            )",
            ext
        ))
        .execute(pool)
        .await?;

        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS paulo_bio_education{} (
                id SERIAL PRIMARY KEY,
                slug TEXT UNIQUE NOT NULL,
                degree TEXT NOT NULL,
                institution TEXT NOT NULL,
                duration TEXT NOT NULL,
                tag TEXT NOT NULL,
                summary TEXT,
                sort_order INTEGER DEFAULT 0
            )",
            ext
        ))
        .execute(pool)
        .await?;

        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS paulo_bio_skills{} (
                id SERIAL PRIMARY KEY,
                category TEXT NOT NULL,
                name TEXT NOT NULL,
                level TEXT DEFAULT 'expert'
            )",
            ext
        ))
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn migration_6_document_index(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    document_index::init_tables(pool).await
}

async fn migration_7_audit_chain(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    sqlx::query("ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS payload_json JSONB")
        .execute(pool)
        .await?;
    sqlx::query("ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS prev_entry_hash TEXT")
        .execute(pool)
        .await?;
    sqlx::query("ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS entry_hash TEXT")
        .execute(pool)
        .await?;
    sqlx::query(
        "ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS signature_verified BOOLEAN DEFAULT false",
    )
    .execute(pool)
    .await?;
    sqlx::query("ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS retention_until TIMESTAMP")
        .execute(pool)
        .await?;
    sqlx::query(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_audit_log_entry_hash ON audit_log(entry_hash)",
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_audit_log_retention_until ON audit_log(retention_until)",
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn run_all(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    ensure_migrations_table(pool).await?;

    run_migration(pool, 1, "core_memory", migration_1_core_memory(pool)).await?;
    run_migration(
        pool,
        2,
        "adaptive_memory",
        migration_2_adaptive_memory(pool),
    )
    .await?;
    run_migration(pool, 3, "bayesian_memory", migration_3_bayesian(pool)).await?;
    run_migration(pool, 4, "scoped_memory", migration_4_scoped_memory(pool)).await?;
    run_migration(pool, 5, "audit_and_bio", migration_5_audit_and_bio(pool)).await?;
    run_migration(pool, 6, "document_index", migration_6_document_index(pool)).await?;
    run_migration(pool, 7, "audit_chain", migration_7_audit_chain(pool)).await?;

    Ok(())
}
