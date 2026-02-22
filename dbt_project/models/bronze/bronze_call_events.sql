{{/*
  Bronze view over raw call_events Iceberg table.
  This is a thin alias — no transformations, preserving immutability.
  Analysts can always query back to exactly what arrived from the network.
*/}}

SELECT
    event_id,
    event_type,
    caller_msisdn,
    called_msisdn,
    caller_customer_id,
    tower_id,
    tower_city,
    tower_state,
    tower_lat,
    tower_lon,
    call_start_timestamp,
    call_end_timestamp,
    duration_seconds,
    call_result_code,
    call_result_desc,
    plan_type,
    roaming,
    revenue_usd,
    ingested_at,
    -- Kafka provenance metadata
    _kafka_topic,
    _kafka_partition,
    _kafka_offset,
    _kafka_timestamp,
    _bronze_ingested_at,
    -- Partition columns
    year,
    month,
    day
FROM iceberg.bronze.call_events
