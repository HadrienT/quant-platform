-- Tamper evidence (WP 02, optional task 9). Per (src_topic, src_part), every row stores
--   chain_hash = sha256(chain_prev || its own content)
-- where chain_prev is the chain_hash of the previous row of the same Kafka partition.
-- Altering a row, or removing/reordering one in the middle, breaks the chain from that
-- point on; audit.verify_chain() finds where.
--
-- It lives in the DATABASE (a trigger), not in the sink: the sink stays unchanged, keeps
-- no state, and a re-delivered event (skipped by ON CONFLICT) cannot advance the chain.
-- What it does NOT detect: rows removed at the very END of a partition's history, and the
-- oldest rows leaving through retention (verification trusts the first surviving row's
-- recorded chain_prev). It proves consistency, not completeness. Pre-existing rows keep
-- NULL chain columns and are ignored.

ALTER TABLE audit.events
    ADD COLUMN chain_prev bytea,
    ADD COLUMN chain_hash bytea;

CREATE INDEX events_chain_lookup_idx ON audit.events (src_topic, src_part, src_offset DESC);

-- The hash of one row, from its previous hash and its content. Deterministic: jsonb's
-- text form is canonical (sorted keys, normalised spacing).
CREATE FUNCTION audit.chain_hash_of(
    prev bytea, event_id uuid, type text, version int, occurred_at timestamptz,
    request_id text, trace_id text, username text, producer jsonb, payload jsonb,
    src_topic text, src_part int, src_offset bigint
) RETURNS bytea
LANGUAGE sql IMMUTABLE AS $$
    SELECT sha256(prev || convert_to(
        format('%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s',
               event_id, type, version,
               to_char(occurred_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US'),
               coalesce(request_id, ''), coalesce(trace_id, ''), coalesce(username, ''),
               producer::text, payload::text, src_topic || '/' || src_part, src_offset),
        'UTF8'))
$$;

-- SECURITY DEFINER: the writer has INSERT only, yet the trigger must read the previous
-- row. It runs as the owner and touches nothing but the row being inserted.
CREATE FUNCTION audit.chain_link() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, audit AS $$
DECLARE
    prev bytea;
BEGIN
    SELECT e.chain_hash INTO prev
    FROM audit.events e
    WHERE e.src_topic = NEW.src_topic AND e.src_part = NEW.src_part
      AND e.src_offset < NEW.src_offset AND e.chain_hash IS NOT NULL
    ORDER BY e.src_offset DESC
    LIMIT 1;

    NEW.chain_prev := coalesce(prev, '\x0000000000000000000000000000000000000000000000000000000000000000'::bytea);
    NEW.chain_hash := audit.chain_hash_of(
        NEW.chain_prev, NEW.event_id, NEW.type, NEW.version, NEW.occurred_at,
        NEW.request_id, NEW.trace_id, NEW.username, NEW.producer, NEW.payload,
        NEW.src_topic, NEW.src_part, NEW.src_offset);
    RETURN NEW;
END;
$$;

CREATE TRIGGER events_chain
    BEFORE INSERT ON audit.events
    FOR EACH ROW EXECUTE FUNCTION audit.chain_link();

-- Recomputes every chained row and reports, per Kafka partition, the FIRST offset where the
-- chain does not hold (NULL broken_at = intact). Two things are checked per row:
--   content   : the stored hash equals sha256(stored prev || content)  -> row not altered
--   linkage   : the stored prev equals the previous row's stored hash  -> nothing removed/reordered
CREATE FUNCTION audit.verify_chain(only_topic text DEFAULT NULL)
RETURNS TABLE (src_topic text, src_part int, rows_checked bigint, broken_at bigint, reason text)
-- SECURITY DEFINER: the reader may call this function but not its helper chain_hash_of
-- (functions are not executable by PUBLIC, 0003); it reads only what the reader can already read.
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, audit AS $$
    WITH r AS (
        SELECT e.src_topic, e.src_part, e.src_offset, e.chain_prev, e.chain_hash,
               audit.chain_hash_of(e.chain_prev, e.event_id, e.type, e.version, e.occurred_at,
                                   e.request_id, e.trace_id, e.username, e.producer, e.payload,
                                   e.src_topic, e.src_part, e.src_offset) AS recomputed,
               lag(e.chain_hash) OVER w AS previous_hash,
               row_number() OVER w      AS n
        FROM audit.events e
        WHERE e.chain_hash IS NOT NULL AND (only_topic IS NULL OR e.src_topic = only_topic)
        WINDOW w AS (PARTITION BY e.src_topic, e.src_part ORDER BY e.src_offset)
    ), flagged AS (
        SELECT r.*,
               CASE WHEN r.recomputed <> r.chain_hash THEN 'row content was altered'
                    WHEN r.n > 1 AND r.previous_hash <> r.chain_prev THEN 'a row before this one was removed or altered'
               END AS problem
        FROM r
    )
    SELECT f.src_topic, f.src_part, count(*),
           min(f.src_offset) FILTER (WHERE f.problem IS NOT NULL),
           (array_agg(f.problem ORDER BY f.src_offset) FILTER (WHERE f.problem IS NOT NULL))[1]
    FROM flagged f
    GROUP BY f.src_topic, f.src_part
    ORDER BY f.src_topic, f.src_part
$$;

GRANT EXECUTE ON FUNCTION audit.verify_chain(text) TO audit_reader;
