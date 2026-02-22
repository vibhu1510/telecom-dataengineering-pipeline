{{/*
  Gold: dim_date — Date dimension with fiscal calendar support.
  Covers 2020-01-01 through 2030-12-31.
  Used for all time-based aggregations in fact tables.
*/}}

{{
  config(materialized='table')
}}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart = "day",
        start_date = "cast('2020-01-01' as date)",
        end_date = "cast('2031-01-01' as date)"
    ) }}
),

enriched AS (
    SELECT
        -- Surrogate key (integer YYYYMMDD — compact and sortable)
        CAST(DATE_FORMAT(date_day, '%Y%m%d') AS INTEGER)   AS date_key,
        date_day                                           AS full_date,

        -- Calendar hierarchy
        DAY(date_day)                                      AS day_of_month,
        DAYOFWEEK(date_day)                                AS day_of_week_num,    -- 1=Sunday
        DATE_FORMAT(date_day, '%A')                        AS day_of_week_name,
        DAYOFYEAR(date_day)                                AS day_of_year,
        WEEKOFYEAR(date_day)                               AS week_of_year,
        MONTH(date_day)                                    AS month_num,
        DATE_FORMAT(date_day, '%B')                        AS month_name,
        DATE_FORMAT(date_day, '%b')                        AS month_abbr,
        QUARTER(date_day)                                  AS quarter_num,
        CONCAT('Q', QUARTER(date_day))                     AS quarter_name,
        YEAR(date_day)                                     AS calendar_year,
        DATE_FORMAT(date_day, '%Y-%m')                     AS year_month,

        -- Flags
        CAST(DAYOFWEEK(date_day) IN (1, 7) AS BOOLEAN)    AS is_weekend,
        CAST(DAYOFWEEK(date_day) NOT IN (1, 7) AS BOOLEAN) AS is_weekday,

        -- US Federal Holidays (simplified)
        CAST(
            (MONTH(date_day) = 1  AND DAY(date_day) = 1)   OR  -- New Year's Day
            (MONTH(date_day) = 7  AND DAY(date_day) = 4)   OR  -- Independence Day
            (MONTH(date_day) = 11 AND DAY(date_day) = 11)  OR  -- Veterans Day
            (MONTH(date_day) = 12 AND DAY(date_day) = 25)      -- Christmas
        AS BOOLEAN)                                        AS is_federal_holiday,

        -- Fiscal calendar (assumes fiscal year = calendar year, Q1 starts Jan)
        -- Adjust CASE logic if the company has a non-standard fiscal calendar
        QUARTER(date_day)                                  AS fiscal_quarter_num,
        CONCAT('FY', YEAR(date_day), '-Q', QUARTER(date_day)) AS fiscal_quarter_label,
        YEAR(date_day)                                     AS fiscal_year,

        -- Relative to today (useful for "last 30 days" type filters)
        DATEDIFF('day', date_day, CURRENT_DATE)            AS days_ago,
        DATEDIFF('week', date_day, CURRENT_DATE)           AS weeks_ago,
        DATEDIFF('month', date_day, CURRENT_DATE)          AS months_ago

    FROM date_spine
)

SELECT * FROM enriched
ORDER BY full_date
