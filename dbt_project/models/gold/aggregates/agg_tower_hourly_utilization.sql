{{/*
  Gold: agg_tower_hourly_utilization
  ====================================
  Pre-aggregated tower-level utilization metrics by hour.
  Powers the network ops real-time dashboard.

  Grain: one row per (tower_id, hour).

  Refreshed every 30 minutes via Airflow incremental run.
  Trino reads this directly; Superset connects here.
*/}}

{{
  config(
    materialized = 'incremental',
    unique_key = ['tower_id', 'measurement_hour'],
    incremental_strategy = 'merge',
  )
}}

WITH probe_data AS (
    SELECT *
    FROM {{ ref('silver_network_probes') }}
    {% if is_incremental() %}
    WHERE measurement_ts >= CAST(
        DATE_ADD('hour', -{{ var('incremental_lookback_days') * 24 }}, CURRENT_TIMESTAMP)
        AS TIMESTAMP
    )
    {% endif %}
),

call_data AS (
    SELECT
        tower_id,
        DATE_TRUNC('hour', call_start_ts) AS call_hour,
        COUNT(*)                           AS total_calls,
        SUM(successful_call_count)         AS successful_calls,
        SUM(failed_call_count)             AS failed_calls,
        SUM(call_duration_seconds)         AS total_call_seconds,
        SUM(revenue_usd)                   AS total_revenue_usd
    FROM {{ ref('fact_calls') }}
    {% if is_incremental() %}
    WHERE call_start_ts >= CAST(
        DATE_ADD('hour', -{{ var('incremental_lookback_days') * 24 }}, CURRENT_TIMESTAMP)
        AS TIMESTAMP
    )
    {% endif %}
    GROUP BY 1, 2
),

probe_agg AS (
    SELECT
        tower_id,
        measurement_hour,
        region,
        -- Average KPIs over the hour (multiple probes per tower per hour)
        AVG(load_pct)                      AS avg_load_pct,
        MAX(load_pct)                      AS peak_load_pct,
        AVG(active_connections)            AS avg_active_connections,
        AVG(avg_signal_strength_dbm)       AS avg_signal_strength_dbm,
        AVG(avg_sinr_db)                   AS avg_sinr_db,
        AVG(call_setup_success_rate_pct)   AS avg_call_setup_success_rate_pct,
        AVG(call_drop_rate_pct)            AS avg_call_drop_rate_pct,
        AVG(avg_throughput_mbps)           AS avg_throughput_mbps,
        AVG(packet_loss_pct)               AS avg_packet_loss_pct,
        AVG(avg_latency_ms)                AS avg_latency_ms,
        AVG(temperature_celsius)           AS avg_temperature_celsius,
        MAX(temperature_celsius)           AS max_temperature_celsius,
        -- Health status: worst status observed in the hour
        MAX(CASE load_status
            WHEN 'CRITICAL' THEN 4
            WHEN 'WARNING'  THEN 3
            WHEN 'ELEVATED' THEN 2
            ELSE                 1
        END)                               AS max_load_status_rank,
        -- Alert count: how many probe readings triggered an alert
        SUM(CASE WHEN alert_threshold_breached THEN 1 ELSE 0 END) AS alert_count,
        COUNT(*)                           AS probe_reading_count
    FROM probe_data
    GROUP BY tower_id, measurement_hour, region
),

final AS (
    SELECT
        p.tower_id,
        p.measurement_hour,
        p.region,
        CAST(p.measurement_hour AS DATE)                   AS measurement_date,
        HOUR(p.measurement_hour)                           AS hour_of_day,

        -- Probe-based metrics
        CAST(p.avg_load_pct AS DECIMAL(6,2))               AS avg_load_pct,
        CAST(p.peak_load_pct AS DECIMAL(6,2))              AS peak_load_pct,
        CAST(p.avg_active_connections AS INTEGER)          AS avg_active_connections,
        CAST(p.avg_signal_strength_dbm AS DECIMAL(8,2))    AS avg_signal_strength_dbm,
        CAST(p.avg_sinr_db AS DECIMAL(8,2))                AS avg_sinr_db,
        CAST(p.avg_call_setup_success_rate_pct AS DECIMAL(6,3)) AS avg_call_setup_success_rate_pct,
        CAST(p.avg_call_drop_rate_pct AS DECIMAL(6,3))     AS avg_call_drop_rate_pct,
        CAST(p.avg_throughput_mbps AS DECIMAL(10,2))       AS avg_throughput_mbps,
        CAST(p.avg_packet_loss_pct AS DECIMAL(6,3))        AS avg_packet_loss_pct,
        CAST(p.avg_latency_ms AS DECIMAL(8,1))             AS avg_latency_ms,
        CAST(p.avg_temperature_celsius AS DECIMAL(6,2))    AS avg_temperature_celsius,
        CAST(p.max_temperature_celsius AS DECIMAL(6,2))    AS max_temperature_celsius,

        -- Health classification (convert back from rank)
        CASE p.max_load_status_rank
            WHEN 4 THEN 'CRITICAL'
            WHEN 3 THEN 'WARNING'
            WHEN 2 THEN 'ELEVATED'
            ELSE        'NORMAL'
        END                                                AS worst_load_status,
        p.alert_count,
        p.probe_reading_count,

        -- Call-based metrics (may be NULL if no calls from that tower in that hour)
        COALESCE(c.total_calls, 0)                         AS total_calls,
        COALESCE(c.successful_calls, 0)                    AS successful_calls,
        COALESCE(c.failed_calls, 0)                        AS failed_calls,
        COALESCE(c.total_call_seconds, 0)                  AS total_call_seconds,
        CAST(COALESCE(c.total_revenue_usd, 0) AS DECIMAL(14,4)) AS total_revenue_usd,

        -- Derived KPIs
        CASE
            WHEN c.total_calls > 0
            THEN CAST(c.successful_calls AS DECIMAL) / c.total_calls * 100
            ELSE NULL
        END                                                AS call_success_rate_pct,

        CURRENT_TIMESTAMP                                  AS _gold_processed_at

    FROM probe_agg p
    LEFT JOIN call_data c
        ON  p.tower_id = c.tower_id
        AND p.measurement_hour = c.call_hour
)

SELECT * FROM final
