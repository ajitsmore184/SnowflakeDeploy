---------------------------------------------------------------------
-- 9. ANALYTICS ON THE STAR SCHEMA / MARTS
----------------------------------------------------------------------
-- Weekday vs weekend, by user type
SELECT is_weekend, usertype, SUM(trips) AS trips, ROUND(AVG(avg_minutes),1) AS avg_min
FROM mart.dt_daily_trips
GROUP BY ALL ORDER BY 1,2;

-- Top 10 stations in each month
SELECT * FROM mart.dt_station_rank WHERE rnk <= 10 ORDER BY year, month, rnk;

-- Classic star-join: busiest days
SELECT d.full_date, d.day_name, COUNT(*) AS trips
FROM core.fact_trips f JOIN core.dim_date d ON d.date_key = f.date_key
GROUP BY ALL ORDER BY trips DESC LIMIT 10;