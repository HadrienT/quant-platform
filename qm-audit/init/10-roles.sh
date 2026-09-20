#!/usr/bin/env bash
# Runs ONCE, on the first start of an empty data directory (Postgres image
# convention), as the bootstrap superuser `qm_admin`.
#
# Creates the three login roles of the contract (docs/contract.md §3) with the
# passwords from the environment, hands the database to `audit_owner`, and locks
# the superuser to the local socket. Grants on audit.* live in the migrations.
set -euo pipefail

: "${AUDIT_DB_OWNER_PASSWORD:?}" "${AUDIT_DB_WRITER_PASSWORD:?}" "${AUDIT_DB_READER_PASSWORD:?}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  -v owner_pw="$AUDIT_DB_OWNER_PASSWORD" \
  -v writer_pw="$AUDIT_DB_WRITER_PASSWORD" \
  -v reader_pw="$AUDIT_DB_READER_PASSWORD" \
  -v db="$POSTGRES_DB" <<'SQL'
-- No role is a superuser: the privilege system is what makes the trail immutable.
CREATE ROLE audit_owner  LOGIN PASSWORD :'owner_pw'  NOSUPERUSER NOCREATEDB NOCREATEROLE;
CREATE ROLE audit_writer LOGIN PASSWORD :'writer_pw' NOSUPERUSER NOCREATEDB NOCREATEROLE;
CREATE ROLE audit_reader LOGIN PASSWORD :'reader_pw' NOSUPERUSER NOCREATEDB NOCREATEROLE;

-- The owner runs the migrations, so it owns the database (and thus the schema it creates).
ALTER DATABASE :"db" OWNER TO audit_owner;
REVOKE ALL ON DATABASE :"db" FROM PUBLIC;
GRANT CONNECT ON DATABASE :"db" TO audit_writer, audit_reader;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
SQL

# The bootstrap superuser is for `docker compose exec` (local socket, no password)
# only: refuse it over TCP so a leaked network path never reaches a superuser.
sed -i '1i host all qm_admin all reject' "$PGDATA/pg_hba.conf"
