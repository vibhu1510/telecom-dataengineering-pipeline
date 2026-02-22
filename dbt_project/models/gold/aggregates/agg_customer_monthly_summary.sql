{{/*
  Gold: agg_customer_monthly_summary
  ====================================
  Monthly usage and revenue rollup per customer.
  Powers churn prediction features and executive revenue dashboards.

  Grain: one row per (customer_id, year_month).
*/}}

{{
  config(
    materialized = 'incremental',
    unique_key = ['customer_id', 'year_month'],
    incremental_strategy = 'merge',
  )
}}

WITH calls AS (
    SELECT
        customer_key,
        DATE_FORMAT(call_date, '%Y-%m')                    AS year_month,
        YEAR(call_date)                                    AS year,
        MONTH(call_date)                                   AS month_num,
        COUNT(*)                                           AS total_calls,
        SUM(successful_call_count)                         AS successful_calls,
        SUM(call_duration_seconds)                         AS total_call_seconds,
        SUM(roaming_call_count)                            AS roaming_calls,
        SUM(revenue_usd)                                   AS call_revenue_usd
    FROM {{ ref('fact_calls') }}
    {% if is_incremental() %}
    WHERE call_date >= DATE_ADD('month', -2, DATE_TRUNC('month', CURRENT_DATE))
    {% endif %}
    GROUP BY 1, 2, 3, 4
),

customers AS (
    SELECT
        customer_key,
        customer_id,
        plan_code,
        plan_tier,
        plan_monthly_fee_usd,
        value_segment,
        tenure_segment,
        churn_risk,
        state,
        number_of_lines
    FROM {{ ref('dim_customers') }}
    WHERE is_current = TRUE
),

final AS (
    SELECT
        c.customer_id,
        c.plan_code,
        c.plan_tier,
        c.plan_monthly_fee_usd,
        c.value_segment,
        c.tenure_segment,
        c.churn_risk,
        c.state,
        c.number_of_lines,

        ca.year_month,
        ca.year,
        ca.month_num,

        -- Usage metrics
        COALESCE(ca.total_calls, 0)                        AS total_calls,
        COALESCE(ca.successful_calls, 0)                   AS successful_calls,
        COALESCE(ca.total_call_seconds, 0)                 AS total_call_seconds,
        ROUND(COALESCE(ca.total_call_seconds, 0) / 60.0, 1) AS total_call_minutes,
        COALESCE(ca.roaming_calls, 0)                      AS roaming_calls,

        -- Revenue
        CAST(COALESCE(ca.call_revenue_usd, 0) AS DECIMAL(14,4)) AS usage_revenue_usd,
        CAST(c.plan_monthly_fee_usd AS DECIMAL(10,2))      AS subscription_revenue_usd,
        CAST(
            COALESCE(ca.call_revenue_usd, 0) + c.plan_monthly_fee_usd
            AS DECIMAL(14,4)
        )                                                  AS total_monthly_revenue_usd,

        -- Derived engagement metrics
        CASE
            WHEN COALESCE(ca.total_calls, 0) = 0         THEN 'Inactive'
            WHEN COALESCE(ca.total_calls, 0) < 10        THEN 'Low Usage'
            WHEN COALESCE(ca.total_calls, 0) < 50        THEN 'Medium Usage'
            ELSE                                               'Heavy Usage'
        END                                                AS usage_segment,

        -- Roaming ratio
        CASE
            WHEN COALESCE(ca.total_calls, 0) > 0
            THEN CAST(ca.roaming_calls AS DECIMAL) / ca.total_calls
            ELSE 0
        END                                                AS roaming_ratio,

        CURRENT_TIMESTAMP                                  AS _gold_processed_at
    FROM customers c
    LEFT JOIN calls ca ON c.customer_key = ca.customer_key
    WHERE ca.year_month IS NOT NULL
)

SELECT * FROM final
