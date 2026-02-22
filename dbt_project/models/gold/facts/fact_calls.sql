{{/*
  Gold: fact_calls — Central fact table for voice call events.
  ============================================================
  Star schema design: surrogate keys to all dimensions, additive measures.

  Grain: one row per call attempt (including failed calls).

  Consumers:
    - Revenue reporting dashboard
    - Network utilization analysis
    - Customer usage reports
    - Call quality monitoring
*/}}

{{
  config(
    unique_key = 'call_id',
    partition_by = {'field': 'call_date_key', 'data_type': 'int', 'granularity': 'day'},
    cluster_by = ['tower_key', 'call_status']
  )
}}

WITH calls AS (
    SELECT * FROM {{ ref('silver_calls') }}
    {% if is_incremental() %}
    WHERE _silver_processed_at >= CAST(
        DATE_ADD('day', -{{ var('incremental_lookback_days') }}, CURRENT_DATE)
        AS TIMESTAMP
    )
    {% endif %}
),

customers AS (
    SELECT customer_id, customer_key, plan_tier, value_segment, tenure_segment
    FROM {{ ref('dim_customers') }}
    WHERE is_current = TRUE
),

towers AS (
    SELECT tower_id, tower_key, region, capacity_tier
    FROM {{ ref('dim_towers') }}
),

dates AS (
    SELECT full_date, date_key
    FROM {{ ref('dim_date') }}
),

joined AS (
    SELECT
        -- Surrogate key for the fact row
        {{ dbt_utils.generate_surrogate_key(['c.call_id']) }} AS call_fact_key,

        -- Degenerate dimension (natural key kept in fact for drilling)
        c.call_id,

        -- Foreign keys to dimensions
        COALESCE(cust.customer_key, 'UNKNOWN')             AS customer_key,
        COALESCE(t.tower_key,       'UNKNOWN')             AS tower_key,
        COALESCE(d.date_key,        -1)                    AS call_date_key,

        -- Caller dimension bridge (for many-to-many: caller and called)
        c.caller_phone,
        c.called_phone,

        -- Degenerate dimensions (low-cardinality, stored directly in fact)
        c.call_status,
        c.plan_type,
        c.is_roaming,
        c.is_successful,
        c.is_zero_duration,

        -- Additive measures
        CAST(c.call_duration_seconds AS BIGINT)            AS call_duration_seconds,
        CAST(c.call_duration_minutes AS DECIMAL(10,2))     AS call_duration_minutes,
        CAST(c.revenue_usd AS DECIMAL(12,4))               AS revenue_usd,

        -- Semi-additive (can be summed within a day, but not across days)
        1                                                  AS call_count,          -- for COUNT
        CASE WHEN c.is_successful THEN 1 ELSE 0 END        AS successful_call_count,
        CASE WHEN NOT c.is_successful THEN 1 ELSE 0 END    AS failed_call_count,
        CASE WHEN c.is_roaming THEN 1 ELSE 0 END           AS roaming_call_count,

        -- Timestamps (for row-level time drill-down)
        c.call_start_ts,
        c.call_end_ts,
        c.call_date,

        -- Lineage
        CURRENT_TIMESTAMP                                  AS _gold_processed_at

    FROM calls c
    LEFT JOIN customers cust ON c.customer_id = cust.customer_id
    LEFT JOIN towers t        ON c.tower_id    = t.tower_id
    LEFT JOIN dates d         ON c.call_date   = d.full_date
)

SELECT * FROM joined
