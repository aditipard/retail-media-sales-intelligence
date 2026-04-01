-- Deal count, total value and avg days in stage per rep and shows pipeline health and where deals are getting stuck
SELECT
    rep_name,
    stage,
    COUNT(*)                            AS deal_count,
    ROUND(AVG(deal_value), 2)           AS avg_deal_value,
    ROUND(SUM(weighted_value), 2)       AS total_weighted_pipeline,
    ROUND(AVG(days_in_stage), 1)        AS avg_days_in_stage,
    ROUND(AVG(close_probability), 2)    AS avg_close_prob
FROM read_csv_auto('data/raw/pipeline.csv')
GROUP BY rep_name, stage
ORDER BY rep_name, avg_close_prob DESC;