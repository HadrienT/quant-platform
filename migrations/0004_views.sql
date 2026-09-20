-- Views for the usual audit questions. They read payload fields that belong to the
-- producer; the assumed shapes are listed in ADR-011 and must be confirmed by
-- quant-modeling. The reader role can select from them (default privileges).

-- Every fallback to a live source or a default, per day and kind. The end-of-work
-- measure of quant-modeling's "no more live fallbacks" goal is: this view is EMPTY
-- over a week of normal traffic.
CREATE VIEW audit.v_fallbacks_daily AS
SELECT date_trunc('day', occurred_at AT TIME ZONE 'UTC')::date AS day,
       payload ->> 'kind'                                     AS kind,
       count(*)                                               AS events
FROM audit.events
WHERE type = 'data.fallback'
GROUP BY 1, 2;

-- Failed logins per hashed IP (the IP itself is never stored: HMAC at emission).
CREATE VIEW audit.v_login_failures_by_hash AS
SELECT payload ->> 'ip_hash'        AS ip_hash,
       count(*)                     AS failures,
       count(DISTINCT username)     AS distinct_usernames,
       min(occurred_at)             AS first_seen,
       max(occurred_at)             AS last_seen
FROM audit.events
WHERE type = 'auth.login_failed'
GROUP BY 1;

-- The 100 slowest valuations, with what is needed to open the trace.
CREATE VIEW audit.v_slowest_valuations AS
SELECT event_id,
       occurred_at,
       trace_id,
       payload ->> 'product'                 AS product,
       payload #>> '{engine,name}'           AS engine,
       payload #>> '{model,name}'            AS model,
       (payload #>> '{timing,duration_ms}')::numeric AS duration_ms
FROM audit.events
WHERE type = 'pricing.valuation'
  AND payload #>> '{timing,duration_ms}' ~ '^[0-9]+(\.[0-9]+)?$'
ORDER BY duration_ms DESC
LIMIT 100;

-- Quality of the market inputs behind each valuation, per day:
--   unobserved : at least one input is `default` or `proxied`
--   stale      : no default/proxied input, but at least one is `stale`
--   observed   : every input is `observed`
CREATE VIEW audit.v_valuations_by_status AS
WITH per_valuation AS (
    SELECT event_id,
           occurred_at,
           CASE
               WHEN EXISTS (SELECT 1 FROM jsonb_array_elements(coalesce(payload -> 'market_inputs', '[]'::jsonb)) i
                            WHERE i ->> 'status' IN ('default', 'proxied')) THEN 'unobserved'
               WHEN EXISTS (SELECT 1 FROM jsonb_array_elements(coalesce(payload -> 'market_inputs', '[]'::jsonb)) i
                            WHERE i ->> 'status' = 'stale') THEN 'stale'
               ELSE 'observed'
           END AS status
    FROM audit.events
    WHERE type = 'pricing.valuation'
)
SELECT date_trunc('day', occurred_at AT TIME ZONE 'UTC')::date AS day,
       status,
       count(*) AS valuations
FROM per_valuation
GROUP BY 1, 2;

-- Events that fell in the DEFAULT partition (dated in a month without a partition).
-- Should be empty; if not, the maintenance job needs a look (docs/RUNBOOK.md).
CREATE VIEW audit.v_default_partition_events AS
SELECT event_id, type, occurred_at, src_topic, src_part, src_offset
FROM audit.events_default;
