{{/*
  Silver: Conformed customer dimension source.
  Reads from CRM Bronze, applies SCD Type 2 logic tracked via _record_hash.
  One record per customer — current state only (history tracked in dim_customers).
*/}}

{{
  config(
    unique_key = 'customer_id',
  )
}}

WITH raw AS (
    SELECT *
    FROM iceberg.bronze.crm_customers
    {% if is_incremental() %}
    WHERE _ingested_at >= CAST(
        DATE_ADD('day', -{{ var('incremental_lookback_days') }}, CURRENT_DATE)
        AS TIMESTAMP
    )
    {% endif %}
),

latest_per_customer AS (
    -- If the same customer_id appears multiple times (e.g. from overlapping batch runs),
    -- keep only the most recently updated record.
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY CAST(updated_at AS TIMESTAMP) DESC
        ) AS _rn
    FROM raw
    WHERE customer_id IS NOT NULL
),

cleaned AS (
    SELECT
        customer_id,
        account_number,

        -- PII: we store raw in Silver; masking is applied in Gold or via column security
        first_name,
        last_name,
        email,
        phone_number,

        -- Address
        billing_address_street,
        billing_address_city,
        billing_address_state                              AS state,
        billing_address_zip,

        -- Plan
        plan_code,
        CAST(plan_monthly_fee_usd AS DECIMAL(10, 2))       AS plan_monthly_fee_usd,
        CAST(contract_start_date AS DATE)                  AS contract_start_date,
        CAST(contract_end_date   AS DATE)                  AS contract_end_date,

        -- Derived: tenure in months
        DATEDIFF('month',
            CAST(contract_start_date AS DATE),
            CURRENT_DATE
        )                                                  AS tenure_months,

        -- Status
        CAST(is_active AS BOOLEAN)                         AS is_active,
        CAST(number_of_lines AS INTEGER)                   AS number_of_lines,

        -- Device
        device_type,
        device_imei,

        -- Segmentation
        acquisition_channel,
        CAST(lifetime_value_usd AS DECIMAL(12, 2))         AS lifetime_value_usd,
        churn_risk,
        CAST(nps_score AS INTEGER)                         AS nps_score,

        -- Lineage
        CAST(created_at AS TIMESTAMP)                      AS crm_created_at,
        CAST(updated_at AS TIMESTAMP)                      AS crm_updated_at,
        _source_system,
        _record_hash,
        CURRENT_TIMESTAMP                                  AS _silver_processed_at

    FROM latest_per_customer
    WHERE _rn = 1
)

SELECT * FROM cleaned
