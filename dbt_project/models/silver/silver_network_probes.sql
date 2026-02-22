{{/*
  Silver: Cleansed network probe / tower KPI records.
  Adds derived health indicators and anomaly flags used by the
  network ops dashboard and real-time alerting.
*/}}

{{
  config(
    unique_key = 'probe_id',
    partition_by = {'field': 'measurement_date', 'data_type': 'date', 'granularity': 'day'},
  )
}}

WITH raw AS (
    SELECT *
    FROM iceberg.bronze.network_probe_events
    {% if is_incremental() %}
    WHERE _bronze_ingested_at >= CAST(
        DATE_ADD('day', -{{ var('incremental_lookback_days') }}, CURRENT_DATE)
        AS TIMESTAMP
    )
    {% endif %}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY probe_id ORDER BY _bronze_ingested_at DESC) AS _rn
    FROM raw
    WHERE probe_id IS NOT NULL AND tower_id IS NOT NULL
),

enriched AS (
    SELECT
        probe_id,
        tower_id,
        region,
        CAST(measurement_timestamp AS TIMESTAMP)           AS measurement_ts,
        CAST(measurement_timestamp AS DATE)                AS measurement_date,
        DATE_TRUNC('hour', CAST(measurement_timestamp AS TIMESTAMP)) AS measurement_hour,

        -- Capacity metrics
        CAST(load_pct AS DECIMAL(6,2))                     AS load_pct,
        CAST(active_connections AS INTEGER)                AS active_connections,
        CAST(capacity_erlangs AS INTEGER)                  AS capacity_erlangs,

        -- Signal quality
        CAST(avg_signal_strength_dbm AS DECIMAL(8,2))      AS avg_signal_strength_dbm,
        CAST(avg_sinr_db AS DECIMAL(8,2))                  AS avg_sinr_db,

        -- Performance KPIs
        CAST(call_setup_success_rate_pct AS DECIMAL(6,3))  AS call_setup_success_rate_pct,
        CAST(call_drop_rate_pct AS DECIMAL(6,3))           AS call_drop_rate_pct,
        CAST(handoff_success_rate_pct AS DECIMAL(6,3))     AS handoff_success_rate_pct,
        CAST(avg_throughput_mbps AS DECIMAL(10,2))         AS avg_throughput_mbps,
        CAST(packet_loss_pct AS DECIMAL(6,3))              AS packet_loss_pct,
        CAST(avg_latency_ms AS DECIMAL(8,1))               AS avg_latency_ms,

        -- Hardware
        CAST(power_output_watts AS DECIMAL(8,2))           AS power_output_watts,
        CAST(temperature_celsius AS DECIMAL(6,2))          AS temperature_celsius,
        software_version,

        -- Derived health classification
        CASE
            WHEN load_pct >= 90                              THEN 'CRITICAL'
            WHEN load_pct >= {{ var('tower_load_alert_pct') }} THEN 'WARNING'
            WHEN load_pct >= 60                              THEN 'ELEVATED'
            ELSE                                                  'NORMAL'
        END                                                AS load_status,

        CASE
            WHEN call_drop_rate_pct >= {{ var('call_drop_rate_alert_pct') }} THEN 'DEGRADED'
            WHEN call_drop_rate_pct >= 1.0                   THEN 'WARNING'
            ELSE                                                  'HEALTHY'
        END                                                AS call_quality_status,

        CAST(alert_threshold_breached AS BOOLEAN)          AS alert_threshold_breached,

        -- Anomaly flag: temperature too high
        CAST(temperature_celsius > 70 AS BOOLEAN)          AS is_overheating,

        CURRENT_TIMESTAMP                                  AS _silver_processed_at
    FROM deduplicated
    WHERE _rn = 1
)

SELECT * FROM enriched
