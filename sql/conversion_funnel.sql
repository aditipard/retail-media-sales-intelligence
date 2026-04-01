--Campaign funnel: impressions → clicks → redemptions by vertical; ROAS and CTR benchmarks per vertical per quarter
SELECT
    CASE
        WHEN CAST(week_start AS DATE) < '2024-04-01' THEN 'Q1'
        WHEN CAST(week_start AS DATE) < '2024-07-01' THEN 'Q2'
        WHEN CAST(week_start AS DATE) < '2024-10-01' THEN 'Q3'
        ELSE 'Q4'
    END                                         AS quarter,
    vertical,
    SUM(impressions)                            AS total_impressions,
    SUM(clicks)                                 AS total_clicks,
    SUM(redemptions)                            AS total_redemptions,
    ROUND(AVG(ctr) * 100, 2)                   AS avg_ctr_pct,
    ROUND(AVG(redemption_rate) * 100, 2)       AS avg_redemption_rate_pct,
    ROUND(SUM(spend), 2)                        AS total_spend,
    ROUND(SUM(revenue_driven), 2)              AS total_revenue_driven,
    ROUND(AVG(roas), 2)                         AS avg_roas
FROM read_csv_auto('data/raw/campaigns.csv')
GROUP BY quarter, vertical
ORDER BY quarter, avg_roas DESC;