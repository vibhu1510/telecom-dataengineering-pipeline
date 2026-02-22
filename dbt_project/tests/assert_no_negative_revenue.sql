-- Singular test: revenue must never be negative in any fact table.
-- Returns rows that violate this rule — test passes when zero rows returned.
SELECT
    'fact_calls'           AS table_name,
    call_id                AS record_id,
    revenue_usd            AS bad_value,
    'negative_revenue'     AS violation_type
FROM {{ ref('fact_calls') }}
WHERE revenue_usd < 0

UNION ALL

SELECT
    'agg_customer_monthly_summary',
    CONCAT(customer_id, '_', year_month),
    total_monthly_revenue_usd,
    'negative_revenue'
FROM {{ ref('agg_customer_monthly_summary') }}
WHERE total_monthly_revenue_usd < 0
