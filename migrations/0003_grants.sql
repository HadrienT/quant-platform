-- Exactly the privileges of the contract (docs/contract.md §3). Nobody is ever
-- granted UPDATE, DELETE or TRUNCATE.
--
-- The writer needs INSERT only: the sink inserts with a target-less
-- `ON CONFLICT DO NOTHING` (the primary key is the only unique constraint, so it
-- is equivalent). `ON CONFLICT (cols)` would additionally demand SELECT — see ADR-011.

REVOKE ALL ON SCHEMA audit FROM PUBLIC;
GRANT USAGE ON SCHEMA audit TO audit_writer, audit_reader;

GRANT INSERT ON audit.events TO audit_writer;
GRANT SELECT ON ALL TABLES IN SCHEMA audit TO audit_reader;

-- Later objects created by the owner (partitions, views): readable by the
-- reader, never by the writer beyond audit.events.
ALTER DEFAULT PRIVILEGES IN SCHEMA audit GRANT SELECT ON TABLES TO audit_reader;

-- Functions are executable by PUBLIC by default; maintenance is the owner's job.
ALTER DEFAULT PRIVILEGES IN SCHEMA audit REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA audit FROM PUBLIC;
