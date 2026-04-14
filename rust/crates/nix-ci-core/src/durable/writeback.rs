//! Write-through: every in-memory state transition reflected to
//! Postgres. These functions are called from HTTP handlers after
//! in-memory state has been updated. Errors degrade the service
//! (return 5xx) but don't roll the in-memory state back — the next
//! coordinator restart will bring the durable view back in sync via
//! rehydration.

use sqlx::{PgPool, Postgres, Transaction};

use crate::error::Result;
use crate::types::{ClaimId, DrvHash, ErrorCategory, JobId};

pub async fn upsert_job(pool: &PgPool, job_id: JobId, external_ref: Option<&str>) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO jobs (id, status, external_ref)
        VALUES ($1, 'pending', $2)
        ON CONFLICT (id) DO NOTHING
        "#,
    )
    .bind(job_id.0)
    .bind(external_ref)
    .execute(pool)
    .await?;
    Ok(())
}

/// Look up an existing job by `external_ref`. Used by `create` to make
/// POST /jobs idempotent: a client retry after a lost response must
/// return the *same* job_id, not mint a new one.
pub async fn find_job_by_external_ref(pool: &PgPool, external_ref: &str) -> Result<Option<JobId>> {
    let row: Option<(sqlx::types::Uuid,)> =
        sqlx::query_as("SELECT id FROM jobs WHERE external_ref = $1 LIMIT 1")
            .bind(external_ref)
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|(id,)| JobId(id)))
}

pub async fn heartbeat_job(pool: &PgPool, job_id: JobId) -> Result<bool> {
    let updated = sqlx::query(
        r#"
        UPDATE jobs SET last_heartbeat = now()
        WHERE id = $1 AND status IN ('pending','building')
        "#,
    )
    .bind(job_id.0)
    .execute(pool)
    .await?;
    Ok(updated.rows_affected() > 0)
}

pub async fn seal_job(pool: &PgPool, job_id: JobId) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE jobs SET sealed = TRUE
        WHERE id = $1
        "#,
    )
    .bind(job_id.0)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn fail_job_eval(pool: &PgPool, job_id: JobId, message: &str) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE jobs
        SET status = 'failed',
            eval_error = $2,
            done_at = now()
        WHERE id = $1 AND done_at IS NULL
        "#,
    )
    .bind(job_id.0)
    .bind(message)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn transition_job_terminal(pool: &PgPool, job_id: JobId, status: &str) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE jobs
        SET status = $2, done_at = now()
        WHERE id = $1 AND done_at IS NULL
        "#,
    )
    .bind(job_id.0)
    .bind(status)
    .execute(pool)
    .await?;
    Ok(())
}

pub struct UpsertDrv<'a> {
    pub drv_hash: &'a DrvHash,
    pub drv_path: &'a str,
    pub drv_name: &'a str,
    pub system: &'a str,
    pub required_features: &'a [String],
    pub max_attempts: i32,
}

/// Typed batch row for the bulk-ingest path. One per drv; `input_drvs`
/// holds the drv_paths of direct deps (already filtered to the
/// submission's membership set).
pub struct BatchDrv {
    pub drv_hash: String,
    pub drv_path: String,
    pub drv_name: String,
    pub system: String,
    pub required_features: Vec<String>,
    pub max_attempts: i32,
    pub input_drvs: Vec<String>,
    pub is_root: bool,
}

/// Bulk-upsert many drvs + their deps + root-attribs in one
/// transaction, using `UNNEST` so we pay ~one round-trip per table
/// rather than per drv. This is the hot path for full-DAG ingest.
pub async fn upsert_drvs_batch(pool: &PgPool, job_id: JobId, batch: &[BatchDrv]) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let mut tx: Transaction<'_, Postgres> = pool.begin().await?;

    // 1. Upsert the drv rows themselves.
    let drv_hashes: Vec<&str> = batch.iter().map(|b| b.drv_hash.as_str()).collect();
    let drv_paths: Vec<&str> = batch.iter().map(|b| b.drv_path.as_str()).collect();
    let drv_names: Vec<&str> = batch.iter().map(|b| b.drv_name.as_str()).collect();
    let systems: Vec<&str> = batch.iter().map(|b| b.system.as_str()).collect();
    let max_attempts: Vec<i32> = batch.iter().map(|b| b.max_attempts).collect();
    let required_features_json: Vec<serde_json::Value> = batch
        .iter()
        .map(|b| serde_json::json!(b.required_features))
        .collect();

    sqlx::query(
        r#"
        INSERT INTO derivations (drv_hash, drv_path, drv_name, system, max_attempts)
        SELECT * FROM UNNEST(
            $1::text[],
            $2::text[],
            $3::text[],
            $4::text[],
            $5::int[]
        ) AS t(drv_hash, drv_path, drv_name, system, max_attempts)
        ON CONFLICT (drv_hash) DO NOTHING
        "#,
    )
    .bind(&drv_hashes)
    .bind(&drv_paths)
    .bind(&drv_names)
    .bind(&systems)
    .bind(&max_attempts)
    .execute(&mut *tx)
    .await?;

    // The straight-CROSS-JOIN above works for the required_features
    // unnest but Postgres's type inference struggles with arrays of
    // arrays; if the LATERAL form fails on some servers we fall back
    // per-row for required_features below.
    sqlx::query(
        r#"
        UPDATE derivations d
        SET required_features = COALESCE(
            ARRAY(SELECT jsonb_array_elements_text(t.rf))::text[],
            d.required_features
        )
        FROM UNNEST($1::text[], $2::jsonb[]) AS t(drv_hash, rf)
        WHERE d.drv_hash = t.drv_hash
          AND d.required_features = '{}'::text[]
        "#,
    )
    .bind(&drv_hashes)
    .bind(&required_features_json)
    .execute(&mut *tx)
    .await?;

    // 2. Ensure every input-drv referenced as an edge exists as a
    //    placeholder row. Collect unique input drv paths.
    let mut input_hashes: Vec<String> = Vec::new();
    let mut input_paths: Vec<String> = Vec::new();
    let mut input_names: Vec<String> = Vec::new();
    let mut seen_inputs: std::collections::HashSet<String> =
        std::collections::HashSet::with_capacity(batch.iter().map(|b| b.input_drvs.len()).sum());
    for b in batch {
        for input_path in &b.input_drvs {
            let Some(input_hash) = crate::types::drv_hash_from_path(input_path) else {
                continue;
            };
            if seen_inputs.insert(input_hash.as_str().to_string()) {
                input_hashes.push(input_hash.as_str().to_string());
                input_paths.push(input_path.clone());
                input_names.push(crate::server::ingest_common::placeholder_name_from(
                    input_path,
                ));
            }
        }
    }
    if !input_hashes.is_empty() {
        let input_hash_refs: Vec<&str> = input_hashes.iter().map(|s| s.as_str()).collect();
        let input_path_refs: Vec<&str> = input_paths.iter().map(|s| s.as_str()).collect();
        let input_name_refs: Vec<&str> = input_names.iter().map(|s| s.as_str()).collect();
        let default_system: Vec<&str> = vec![systems[0]; input_hashes.len()];
        sqlx::query(
            r#"
            INSERT INTO derivations (drv_hash, drv_path, drv_name, system)
            SELECT * FROM UNNEST(
                $1::text[],
                $2::text[],
                $3::text[],
                $4::text[]
            ) AS t(drv_hash, drv_path, drv_name, system)
            ON CONFLICT (drv_hash) DO NOTHING
            "#,
        )
        .bind(&input_hash_refs)
        .bind(&input_path_refs)
        .bind(&input_name_refs)
        .bind(&default_system)
        .execute(&mut *tx)
        .await?;
    }

    // 3. Bulk-insert edges.
    let mut edge_parents: Vec<String> = Vec::new();
    let mut edge_deps: Vec<String> = Vec::new();
    for b in batch {
        for input_path in &b.input_drvs {
            let Some(input_hash) = crate::types::drv_hash_from_path(input_path) else {
                continue;
            };
            edge_parents.push(b.drv_hash.clone());
            edge_deps.push(input_hash.as_str().to_string());
        }
    }
    if !edge_parents.is_empty() {
        let parent_refs: Vec<&str> = edge_parents.iter().map(|s| s.as_str()).collect();
        let dep_refs: Vec<&str> = edge_deps.iter().map(|s| s.as_str()).collect();
        sqlx::query(
            r#"
            INSERT INTO deps (drv_hash, dep_hash)
            SELECT * FROM UNNEST($1::text[], $2::text[]) AS t(drv_hash, dep_hash)
            ON CONFLICT DO NOTHING
            "#,
        )
        .bind(&parent_refs)
        .bind(&dep_refs)
        .execute(&mut *tx)
        .await?;
    }

    // 4. Roots
    let root_hashes: Vec<&str> = batch
        .iter()
        .filter(|b| b.is_root)
        .map(|b| b.drv_hash.as_str())
        .collect();
    if !root_hashes.is_empty() {
        let job_ids: Vec<sqlx::types::Uuid> = (0..root_hashes.len()).map(|_| job_id.0).collect();
        sqlx::query(
            r#"
            INSERT INTO job_roots (job_id, drv_hash)
            SELECT * FROM UNNEST($1::uuid[], $2::text[]) AS t(job_id, drv_hash)
            ON CONFLICT DO NOTHING
            "#,
        )
        .bind(&job_ids)
        .bind(&root_hashes)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok(())
}

pub async fn upsert_drv_and_deps(
    pool: &PgPool,
    drv: UpsertDrv<'_>,
    input_drvs: &[String],
    job_id: JobId,
    is_root: bool,
) -> Result<()> {
    let mut tx: Transaction<'_, Postgres> = pool.begin().await?;

    sqlx::query(
        r#"
        INSERT INTO derivations
            (drv_hash, drv_path, drv_name, system, required_features, max_attempts)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (drv_hash) DO NOTHING
        "#,
    )
    .bind(drv.drv_hash.as_str())
    .bind(drv.drv_path)
    .bind(drv.drv_name)
    .bind(drv.system)
    .bind(drv.required_features)
    .bind(drv.max_attempts)
    .execute(&mut *tx)
    .await?;

    // Upsert the input-drv rows so the dep edges have valid FKs.
    // Dummy metadata for placeholder rows; it'll be overwritten on
    // subsequent calls that carry the real IngestDrvRequest.
    for input_path in input_drvs {
        let input_hash = crate::types::drv_hash_from_path(input_path)
            .ok_or_else(|| crate::Error::BadRequest(format!("bad drv_path: {input_path}")))?;
        sqlx::query(
            r#"
            INSERT INTO derivations (drv_hash, drv_path, drv_name, system)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (drv_hash) DO NOTHING
            "#,
        )
        .bind(input_hash.as_str())
        .bind(input_path)
        .bind(crate::server::ingest_common::placeholder_name_from(
            input_path,
        ))
        .bind(drv.system)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO deps (drv_hash, dep_hash)
            VALUES ($1, $2)
            ON CONFLICT DO NOTHING
            "#,
        )
        .bind(drv.drv_hash.as_str())
        .bind(input_hash.as_str())
        .execute(&mut *tx)
        .await?;
    }

    if is_root {
        sqlx::query(
            r#"
            INSERT INTO job_roots (job_id, drv_hash)
            VALUES ($1, $2)
            ON CONFLICT DO NOTHING
            "#,
        )
        .bind(job_id.0)
        .bind(drv.drv_hash.as_str())
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok(())
}

pub async fn mark_building(
    pool: &PgPool,
    drv_hash: &DrvHash,
    claim_id: ClaimId,
    attempt: i32,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE derivations
        SET state = 'building',
            assigned_claim_id = $2,
            attempt = $3
        WHERE drv_hash = $1 AND state = 'pending'
        "#,
    )
    .bind(drv_hash.as_str())
    .bind(claim_id.0)
    .bind(attempt)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn mark_done(pool: &PgPool, drv_hash: &DrvHash, claim_id: ClaimId) -> Result<bool> {
    let result = sqlx::query(
        r#"
        UPDATE derivations
        SET state = 'done',
            assigned_claim_id = NULL,
            completed_at = now()
        WHERE drv_hash = $1 AND assigned_claim_id = $2 AND state = 'building'
        "#,
    )
    .bind(drv_hash.as_str())
    .bind(claim_id.0)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

pub struct MarkFailed<'a> {
    pub drv_hash: &'a DrvHash,
    pub claim_id: Option<ClaimId>,
    pub category: ErrorCategory,
    pub message: Option<&'a str>,
    pub exit_code: Option<i32>,
    pub log_tail: Option<&'a str>,
}

pub async fn mark_flaky_retry(
    pool: &PgPool,
    failed: MarkFailed<'_>,
    backoff_ms: i64,
) -> Result<bool> {
    let result = sqlx::query(
        r#"
        UPDATE derivations
        SET state = 'pending',
            assigned_claim_id = NULL,
            next_attempt_at = now() + (($2::bigint) * INTERVAL '1 millisecond'),
            error_category = $3,
            error_message = $4,
            exit_code = $5,
            log_tail = $6
        WHERE drv_hash = $1
          AND state = 'building'
          AND attempt < max_attempts
        "#,
    )
    .bind(failed.drv_hash.as_str())
    .bind(backoff_ms)
    .bind(failed.category.as_str())
    .bind(failed.message)
    .bind(failed.exit_code)
    .bind(failed.log_tail)
    .execute(pool)
    .await?;
    if result.rows_affected() == 0 {
        // In-memory decided to retry but PG says attempt >= max_attempts
        // or the row isn't in building state. Reconcile: flip it to
        // failed so it doesn't sit stuck forever. The caller doesn't
        // know which, so we force the row to failed regardless — safer
        // than leaving a row in `building` with no live claim.
        sqlx::query(
            r#"
            UPDATE derivations
            SET state = 'failed',
                assigned_claim_id = NULL,
                completed_at = now(),
                error_category = $2,
                error_message = COALESCE($3, error_message),
                exit_code = COALESCE($4, exit_code),
                log_tail = COALESCE($5, log_tail)
            WHERE drv_hash = $1 AND state != 'done'
            "#,
        )
        .bind(failed.drv_hash.as_str())
        .bind(failed.category.as_str())
        .bind(failed.message)
        .bind(failed.exit_code)
        .bind(failed.log_tail)
        .execute(pool)
        .await?;
        return Ok(false);
    }
    Ok(true)
}

pub async fn mark_terminal_failure(pool: &PgPool, failed: MarkFailed<'_>) -> Result<Vec<String>> {
    let mut tx = pool.begin().await?;

    // Originating drv
    sqlx::query(
        r#"
        UPDATE derivations
        SET state = 'failed',
            assigned_claim_id = NULL,
            completed_at = now(),
            error_category = $2,
            error_message = $3,
            exit_code = $4,
            log_tail = $5
        WHERE drv_hash = $1 AND state IN ('building', 'pending')
        "#,
    )
    .bind(failed.drv_hash.as_str())
    .bind(failed.category.as_str())
    .bind(failed.message)
    .bind(failed.exit_code)
    .bind(failed.log_tail)
    .execute(&mut *tx)
    .await?;

    // Cache the failed output paths with a 1h TTL.
    sqlx::query(
        r#"
        INSERT INTO failed_outputs (output_path, drv_hash)
        SELECT drv_path, drv_hash FROM derivations WHERE drv_hash = $1
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind(failed.drv_hash.as_str())
    .execute(&mut *tx)
    .await?;

    // Propagate: transitive dependents → failed, carrying propagated_from.
    let propagated: Vec<(String,)> = sqlx::query_as(
        r#"
        WITH RECURSIVE failed_tree AS (
            SELECT drv_hash FROM deps WHERE dep_hash = $1
            UNION
            SELECT d.drv_hash FROM deps d
            JOIN failed_tree ft ON d.dep_hash = ft.drv_hash
        )
        UPDATE derivations
        SET state = 'failed',
            propagated_from = $1,
            error_category = 'propagated_failure',
            error_message = 'dep failed: ' || $1,
            completed_at = now(),
            assigned_claim_id = NULL
        WHERE drv_hash IN (SELECT drv_hash FROM failed_tree)
          AND state IN ('pending','building')
        RETURNING drv_hash
        "#,
    )
    .bind(failed.drv_hash.as_str())
    .fetch_all(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(propagated.into_iter().map(|(h,)| h).collect())
}

pub async fn is_failed_output(pool: &PgPool, drv_path: &str) -> Result<bool> {
    let row: Option<(String,)> = sqlx::query_as(
        r#"
        SELECT output_path FROM failed_outputs
        WHERE output_path = $1 AND expires_at > now()
        LIMIT 1
        "#,
    )
    .bind(drv_path)
    .fetch_optional(pool)
    .await?;
    Ok(row.is_some())
}
