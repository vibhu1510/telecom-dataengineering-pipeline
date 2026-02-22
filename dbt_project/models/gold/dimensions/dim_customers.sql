{{/*
  Gold: dim_customers — SCD Type 2 customer dimension.
  ======================================================
  Each customer can have multiple rows:
    - One row per "version" of their attributes (plan changes, address changes, etc.)
    - is_current = TRUE  → current active version
    - is_current = FALSE → historical version

  Surrogate key (customer_key) is stable even when natural key (customer_id) changes.

  JOIN pattern in fact tables:
    JOIN dim_customers d
      ON f.customer_id = d.customer_id
     AND f.call_date BETWEEN d.effective_start_date AND COALESCE(d.effective_end_date, CURRENT_DATE)
     AND d.is_current = TRUE  (or use date-range join for historical accuracy)
*/}}

{{
  config(materialized='table')
}}

WITH silver AS (
    SELECT * FROM {{ ref('silver_customers') }}
),

-- Generate surrogate key by hashing the natural key + change detection hash
with_surrogate AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} AS customer_key,

        customer_id,
        account_number,

        -- PII masked for analytics (full data available with elevated access)
        CONCAT(SUBSTR(first_name, 1, 1), '***')            AS first_name_masked,
        CONCAT(SUBSTR(last_name, 1, 1), '***')             AS last_name_masked,
        REGEXP_REPLACE(email, '(.{2}).+(@.+)', '$1***$2')  AS email_masked,
        CONCAT(SUBSTR(phone_number, 1, 6), 'XXXXX')        AS phone_masked,

        -- Geography
        billing_address_city                               AS city,
        state,
        billing_address_zip                                AS zip_code,

        -- Plan attributes
        plan_code,
        plan_monthly_fee_usd,
        SPLIT_PART(plan_code, '_', 1)                      AS plan_tier,  -- Basic, Standard, etc.

        -- Contract
        contract_start_date,
        contract_end_date,
        is_active,

        -- Account attributes
        number_of_lines,
        device_type,
        acquisition_channel,
        tenure_months,

        -- Segmentation
        CASE
            WHEN tenure_months < 12   THEN 'New'
            WHEN tenure_months < 36   THEN 'Established'
            WHEN tenure_months < 72   THEN 'Loyal'
            ELSE                           'Champion'
        END                                                AS tenure_segment,

        CASE
            WHEN plan_monthly_fee_usd >= 100  THEN 'High Value'
            WHEN plan_monthly_fee_usd >= 60   THEN 'Mid Value'
            ELSE                                   'Entry'
        END                                                AS value_segment,

        lifetime_value_usd,
        churn_risk,
        nps_score,
        CASE
            WHEN nps_score >= 9  THEN 'Promoter'
            WHEN nps_score >= 7  THEN 'Passive'
            ELSE                      'Detractor'
        END                                                AS nps_segment,

        -- SCD Type 2 fields
        crm_updated_at                                     AS effective_start_date,
        CAST(NULL AS DATE)                                 AS effective_end_date,
        TRUE                                               AS is_current,

        _record_hash,
        _silver_processed_at,
        CURRENT_TIMESTAMP                                  AS _gold_processed_at
    FROM silver
)

SELECT * FROM with_surrogate
