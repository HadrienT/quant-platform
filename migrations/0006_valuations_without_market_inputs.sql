-- v_valuations_by_status gains a fourth status, `no_market_inputs` (issue #1).
--
-- The producer records as `market_inputs` only the data a pricing READ from the
-- market database (quant-modeling ADR-011 §4); a pricing on parameters the user
-- typed in has an empty list. 0004 classified those `observed` (no default,
-- proxied or stale input among none), which made nearly every valuation look
-- priced on observed data. Same columns, so CREATE OR REPLACE keeps the grants.
CREATE OR REPLACE VIEW audit.v_valuations_by_status AS
WITH per_valuation AS (
    SELECT event_id,
           occurred_at,
           CASE
               WHEN jsonb_typeof(payload -> 'market_inputs') IS DISTINCT FROM 'array'
                    OR jsonb_array_length(payload -> 'market_inputs') = 0 THEN 'no_market_inputs'
               WHEN EXISTS (SELECT 1 FROM jsonb_array_elements(payload -> 'market_inputs') i
                            WHERE i ->> 'status' IN ('default', 'proxied')) THEN 'unobserved'
               WHEN EXISTS (SELECT 1 FROM jsonb_array_elements(payload -> 'market_inputs') i
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
