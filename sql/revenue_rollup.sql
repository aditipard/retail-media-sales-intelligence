-- Revenue vs. target by week, with running total and variance
SELECT
    week_start,
    quarter,
    week_num,
    revenue,
    target,
    ROUND(revenue - target, 2)                          AS variance,
    ROUND((revenue / target - 1) * 100, 1)              AS variance_pct,
    ROUND(SUM(revenue) OVER (
        PARTITION BY quarter ORDER BY week_num
    ), 2)                                               AS cumulative_revenue,
    ROUND(AVG(revenue) OVER (
        ORDER BY week_num ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    ), 2)                                               AS rolling_4wk_avg
FROM read_csv_auto('data/raw/weekly_revenue.csv')
ORDER BY week_num;