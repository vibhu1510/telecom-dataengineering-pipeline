-- Row count anomaly detection: today's fact_calls row count should not drop
-- more than 50% compared to the 7-day average.
-- Returns a row (causing test failure) if an anomaly is detected.
WITH daily_counts AS (
    SELECT
        call_date,
        COUNT(*) AS daily_rows
    FROM {{ ref('fact_calls') }}
    WHERE call_date >= DATE_ADD('day', -8, CURRENT_DATE)
    GROUP BY call_date
),

baseline AS (
    SELECT AVG(daily_rows) AS avg_7day_rows
    FROM daily_counts
    WHERE call_date < CURRENT_DATE
      AND call_date >= DATE_ADD('day', -7, CURRENT_DATE)
),

today AS (
    SELECT daily_rows AS today_rows
    FROM daily_counts
    WHERE call_date = CURRENT_DATE
)

SELECT
    today_rows,
    avg_7day_rows,
    ROUND(today_rows / avg_7day_rows * 100, 1) AS pct_of_avg,
    'row_count_anomaly: today is <50% of 7-day average' AS violation
FROM today, baseline
WHERE avg_7day_rows > 0
  AND today_rows < avg_7day_rows * 0.5
