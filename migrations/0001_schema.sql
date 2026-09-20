-- The audit trail: one append-only, monthly-partitioned table (contract §3).
CREATE TABLE audit.events (
    event_id     uuid        NOT NULL,
    type         text        NOT NULL,
    version      int         NOT NULL,
    occurred_at  timestamptz NOT NULL,
    request_id   text,
    trace_id     text,
    username     text,
    producer     jsonb       NOT NULL,
    payload      jsonb       NOT NULL,
    src_topic    text        NOT NULL,
    src_part     int         NOT NULL,
    src_offset   bigint      NOT NULL,
    PRIMARY KEY (event_id, occurred_at)
) PARTITION BY RANGE (occurred_at);

COMMENT ON TABLE audit.events IS
    'Append-only audit trail. Rows are never updated or deleted; retention detaches and drops whole monthly partitions.';

-- Without a DEFAULT partition, an event dated in a month that has no partition
-- would make the INSERT fail and BLOCK the sink. A wrongly-dated event must not
-- stop the audit: it lands here, and shows in v_default_partition_events.
CREATE TABLE audit.events_default PARTITION OF audit.events DEFAULT;

CREATE INDEX events_type_time_idx ON audit.events (type, occurred_at DESC);
CREATE INDEX events_trace_idx     ON audit.events (trace_id)   WHERE trace_id IS NOT NULL;
CREATE INDEX events_request_idx   ON audit.events (request_id) WHERE request_id IS NOT NULL;

-- Defence in depth: privileges already give nobody UPDATE/DELETE, this makes the
-- owner (and a superuser who forgets) fail too. Row triggers on a partitioned
-- table apply to every partition, including future ones. TRUNCATE is left alone
-- on purpose: it is how the rebuild-from-Kafka drill empties the table.
CREATE FUNCTION audit.forbid_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'audit.events is append-only: % is forbidden', TG_OP
        USING ERRCODE = 'insufficient_privilege';
END;
$$;

CREATE TRIGGER events_append_only
    BEFORE UPDATE OR DELETE ON audit.events
    FOR EACH ROW EXECUTE FUNCTION audit.forbid_mutation();
