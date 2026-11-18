-- Demo data quality history for orders_enrichment: a week of normal micro-batches, then in the last
-- 30 minutes an upstream producer starts sending `currency_code` (lower case) instead of `currency`.
-- Idempotent: clears the demo pipeline's rows first.
DELETE FROM dq.check_result WHERE pipeline = 'orders_enrichment';
DELETE FROM dq.quarantine_row WHERE pipeline = 'orders_enrichment';
DELETE FROM ops.pipeline_control WHERE pipeline = 'orders_enrichment';

-- one micro-batch every 5 minutes for 7 days, 2,000 rows each
WITH batches AS (
    SELECT row_number() OVER (ORDER BY ts) AS batch_id, ts
    FROM generate_series(now() - interval '7 days', now(), interval '5 minutes') AS ts
), rules(rule, normal_failed) AS (
    VALUES ('order_id_present', 0), ('amount_positive', 3), ('currency_known', 2), ('amount_within_limit', 1)
)
INSERT INTO dq.check_result (pipeline, batch_id, rule, checked_rows, failed_rows, checked_at)
SELECT 'orders_enrichment', b.batch_id, r.rule, 2000,
       CASE WHEN r.rule = 'currency_known' AND b.ts > now() - interval '30 minutes'
            THEN 360 + (b.batch_id % 7) * 5          -- ~18% of rows suddenly fail
            ELSE r.normal_failed + (b.batch_id % 3)  -- ~0.1-0.25%, normal noise
       END,
       b.ts
FROM batches b CROSS JOIN rules r;

-- normal quarantined rows from the past week (old schema: `currency` field)
INSERT INTO dq.quarantine_row (pipeline, batch_id, rule, payload, quarantined_at)
SELECT 'orders_enrichment', g, 'amount_positive',
       jsonb_build_object('order_id', 'o-' || (900000 + g), 'customer_id', 'c-' || (g % 500),
                          'amount', -1 * (g % 40), 'currency', 'USD', 'order_ts', now() - g * interval '20 minutes'),
       now() - g * interval '20 minutes'
FROM generate_series(1, 60) AS g;

-- the new failures: producer v2 renamed currency -> currency_code and lower-cased it
INSERT INTO dq.quarantine_row (pipeline, batch_id, rule, payload, quarantined_at)
SELECT 'orders_enrichment', 2000 + g, 'currency_known',
       jsonb_build_object('order_id', 'o-' || (990000 + g), 'customer_id', 'c-' || (g % 300),
                          'amount', 25 + (g % 90), 'currency', NULL,
                          'currency_code', (ARRAY['usd', 'eur', 'gbp', 'inr'])[1 + g % 4],
                          'producer_version', 'orders-api/2.0.0', 'order_ts', now() - g * interval '20 seconds'),
       now() - g * interval '20 seconds'
FROM generate_series(1, 80) AS g;
