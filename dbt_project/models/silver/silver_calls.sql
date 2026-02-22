{{/*
  Silver: Cleansed and conformed call event records.

  Transformations applied:
    1. Cast string timestamps to proper TIMESTAMP type
    2. Decode numeric result codes to human-readable status
    3. Filter impossible/corrupted records (negative duration, null keys)
    4. Standardize column names to snake_case business-friendly convention
    5. Add derived columns (call_date, is_roaming, call_duration_minutes)
    6. Deduplicate (Bronze may have duplicates from at-least-once Kafka delivery)
    7. Add data lineage metadata (_silver_processed_at, _source)

  Incremental strategy: MERGE on event_id so reruns are idempotent.
*/}}

{{
  config(
    unique_key = 'call_id',
    partition_by = {
      'field': 'call_date',
      'data_type': 'date',
      'granularity': 'day'
    },
    cluster_by = ['tower_id', 'call_status']
  )
}}

WITH raw AS (
    SELECT *
    FROM {{ ref('bronze_call_events') }}
    {% if is_incremental() %}
    -- Only process records from the last N days (handles late arrivals)
    WHERE _bronze_ingested_at >= CAST(
        DATE_ADD('day', -{{ var('incremental_lookback_days') }}, CURRENT_DATE)
        AS TIMESTAMP
    )
    {% endif %}
),

deduplicated AS (
    -- Row_number deduplication: keep the latest record per event_id
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY _bronze_ingested_at DESC
        ) AS _rn
    FROM raw
    WHERE
        -- Hard filter: drop records with obviously corrupt data
        event_id IS NOT NULL
        AND caller_msisdn IS NOT NULL
        AND tower_id IS NOT NULL
        AND duration_seconds >= 0   -- negative duration is impossible
),

cleaned AS (
    SELECT
        -- Primary key
        event_id                                           AS call_id,

        -- Timestamps (properly cast)
        CAST(call_start_timestamp AS TIMESTAMP)            AS call_start_ts,
        CAST(call_end_timestamp   AS TIMESTAMP)            AS call_end_ts,
        CAST(call_start_timestamp AS DATE)                 AS call_date,

        -- Participants
        caller_msisdn                                      AS caller_phone,
        called_msisdn                                      AS called_phone,
        caller_customer_id                                 AS customer_id,

        -- Network context
        tower_id,
        tower_city,
        tower_state,
        tower_lat                                          AS tower_latitude,
        tower_lon                                          AS tower_longitude,

        -- Call metrics
        duration_seconds                                   AS call_duration_seconds,
        ROUND(duration_seconds / 60.0, 2)                  AS call_duration_minutes,

        -- Decoded status (human-readable)
        CASE call_result_code
            WHEN '0'  THEN 'SUCCESS'
            WHEN '17' THEN 'USER_BUSY'
            WHEN '21' THEN 'CALL_REJECTED'
            WHEN '31' THEN 'NETWORK_FAILURE'
            WHEN '38' THEN 'NETWORK_OUT_OF_ORDER'
            WHEN '41' THEN 'TEMPORARY_FAILURE'
            ELSE           'UNKNOWN_' || COALESCE(call_result_code, 'NULL')
        END                                                AS call_status,

        call_result_code                                   AS raw_result_code,

        -- Plan
        plan_type,
        CAST(roaming AS BOOLEAN)                           AS is_roaming,

        -- Revenue
        CAST(revenue_usd AS DECIMAL(12, 4))                AS revenue_usd,

        -- Derived flags
        CASE WHEN call_result_code = '0' THEN TRUE ELSE FALSE END  AS is_successful,
        CASE WHEN duration_seconds = 0   THEN TRUE ELSE FALSE END  AS is_zero_duration,

        -- Data lineage
        'bronze_call_events'                               AS _source_table,
        CURRENT_TIMESTAMP                                  AS _silver_processed_at,
        _kafka_offset,
        _kafka_partition,
        year,
        month,
        day
    FROM deduplicated
    WHERE _rn = 1
)

SELECT * FROM cleaned
