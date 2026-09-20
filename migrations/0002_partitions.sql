-- Monthly partition maintenance. Retention is a whole-partition operation: detach
-- then drop, never a row-by-row DELETE.

CREATE FUNCTION audit.partition_name(month date) RETURNS text
LANGUAGE sql IMMUTABLE AS $$
    SELECT 'events_y' || to_char(month, 'YYYY') || 'm' || to_char(month, 'MM')
$$;

-- Creates the missing monthly partitions from `months_back` before the current
-- month to `months_ahead` after it. Returns how many it created.
--
-- Why months_back: the sink replays from Kafka (up to 90 days) after a rebuild.
-- Those events are dated in past months; without their partitions they would all
-- land in the DEFAULT partition, and Postgres then REFUSES to create a partition
-- whose range already holds rows in DEFAULT.
CREATE FUNCTION audit.ensure_partitions(months_back int DEFAULT 3, months_ahead int DEFAULT 3)
RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    first_of_month date := date_trunc('month', now() AT TIME ZONE 'UTC')::date;
    m              date;
    part           text;
    created        int := 0;
BEGIN
    FOR i IN -months_back .. months_ahead LOOP
        m := (first_of_month + make_interval(months => i))::date;
        part := audit.partition_name(m);
        IF to_regclass(format('audit.%I', part)) IS NOT NULL THEN
            CONTINUE;
        END IF;
        BEGIN
            EXECUTE format(
                'CREATE TABLE audit.%I PARTITION OF audit.events FOR VALUES FROM (%L) TO (%L)',
                part,
                m::text || ' 00:00:00+00',
                (m + interval '1 month')::date::text || ' 00:00:00+00');
            created := created + 1;
        EXCEPTION WHEN check_violation THEN
            RAISE WARNING
                'cannot create % : the DEFAULT partition already holds events of that month (see v_default_partition_events); needs a manual, reviewed repair',
                part;
        END;
    END LOOP;
    RETURN created;
END;
$$;

-- Detaches then drops the monthly partitions that lie entirely beyond the
-- retention window (a partition is kept while ANY of its days is younger than
-- `retention_months`). Returns the names it dropped (or would drop, dry_run).
-- The DEFAULT partition is never dropped.
CREATE FUNCTION audit.drop_expired_partitions(retention_months int DEFAULT 12, dry_run boolean DEFAULT false)
RETURNS text[]
LANGUAGE plpgsql AS $$
DECLARE
    cutoff  timestamptz := now() - make_interval(months => retention_months);
    rec     record;
    m       date;
    dropped text[] := '{}';
BEGIN
    FOR rec IN
        SELECT c.relname
        FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhrelid
        WHERE i.inhparent = 'audit.events'::regclass
          AND c.relname ~ '^events_y[0-9]{4}m[0-9]{2}$'
        ORDER BY c.relname
    LOOP
        m := to_date(substring(rec.relname FROM 9 FOR 4) || '-' || substring(rec.relname FROM 14 FOR 2) || '-01', 'YYYY-MM-DD');
        IF ((m + interval '1 month')::date::text || ' 00:00:00+00')::timestamptz <= cutoff THEN
            IF NOT dry_run THEN
                EXECUTE format('ALTER TABLE audit.events DETACH PARTITION audit.%I', rec.relname);
                EXECUTE format('DROP TABLE audit.%I', rec.relname);
            END IF;
            dropped := dropped || rec.relname::text;
        END IF;
    END LOOP;
    RETURN dropped;
END;
$$;
