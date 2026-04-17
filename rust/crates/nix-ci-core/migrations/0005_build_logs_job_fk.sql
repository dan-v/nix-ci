-- Tie build_logs to jobs with an ON DELETE CASCADE foreign key so a
-- job wipe (retention cleanup or manual delete) can't leave orphan
-- log rows. Previously cleanup.rs handled this via app-level DELETE,
-- which is correct today but fragile if a future code path forgets
-- the log-side delete. A DB-level FK turns "orphan log cleanup" from
-- "don't forget" into "physically impossible".
--
-- Safe to run on a live database: the FK is NOT VALID to avoid a
-- full scan while holding an ACCESS EXCLUSIVE lock on build_logs;
-- VALIDATE CONSTRAINT afterwards takes a ROW SHARE lock and scans
-- only the rows that aren't already known-good. A coordinator with
-- existing logs can redeploy without downtime.
ALTER TABLE build_logs
    ADD CONSTRAINT build_logs_job_fk
    FOREIGN KEY (job_id)
    REFERENCES jobs (id)
    ON DELETE CASCADE
    NOT VALID;

ALTER TABLE build_logs VALIDATE CONSTRAINT build_logs_job_fk;
