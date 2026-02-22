-- Tower load percentage must always be between 0 and 100.
-- A reading outside this range indicates a sensor fault or ingestion bug.
SELECT
    probe_id,
    tower_id,
    measurement_ts,
    load_pct
FROM {{ ref('silver_network_probes') }}
WHERE load_pct < 0
   OR load_pct > 100
