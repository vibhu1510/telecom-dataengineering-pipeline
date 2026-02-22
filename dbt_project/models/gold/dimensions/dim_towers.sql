{{/*
  Gold: dim_towers — Cell tower reference dimension.
  Enriched with geographic hierarchy and capacity tiering.
  SCD Type 1 (overwrite) — tower location/specs rarely change and history
  is not analytically meaningful.
*/}}

{{
  config(materialized='table')
}}

WITH tower_source AS (
    -- In production this would come from a network inventory system.
    -- Here we seed from static reference data.
    SELECT * FROM {{ ref('towers_seed') }}
),

enriched AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['tower_id']) }} AS tower_key,

        tower_id,
        city,
        state,

        -- Geographic hierarchy
        CASE state
            WHEN 'WA' THEN 'Northwest'  WHEN 'OR' THEN 'Northwest'
            WHEN 'CA' THEN 'Southwest'  WHEN 'AZ' THEN 'Southwest'
            WHEN 'NV' THEN 'Southwest'  WHEN 'TX' THEN 'South'
            WHEN 'FL' THEN 'Southeast'  WHEN 'GA' THEN 'Southeast'
            WHEN 'NY' THEN 'Northeast'  WHEN 'MA' THEN 'Northeast'
            WHEN 'IL' THEN 'Midwest'    WHEN 'OH' THEN 'Midwest'    WHEN 'MI' THEN 'Midwest'
            WHEN 'CO' THEN 'Mountain'   WHEN 'UT' THEN 'Mountain'
            ELSE            'Other'
        END                                                AS region,

        CAST(latitude AS DECIMAL(9,6))                     AS latitude,
        CAST(longitude AS DECIMAL(9,6))                    AS longitude,
        tower_type,  -- macro, small_cell, mmwave

        CAST(capacity_erlangs AS INTEGER)                  AS capacity_erlangs,
        CASE
            WHEN capacity_erlangs >= 1000 THEN 'Tier1_High'
            WHEN capacity_erlangs >= 500  THEN 'Tier2_Medium'
            ELSE                               'Tier3_Low'
        END                                                AS capacity_tier,

        radio_technology,  -- 4G_LTE, 5G_NR, 5G_MMWAVE
        install_date,
        last_maintenance_date,
        is_active,

        CURRENT_TIMESTAMP                                  AS _gold_processed_at
    FROM tower_source
)

SELECT * FROM enriched
