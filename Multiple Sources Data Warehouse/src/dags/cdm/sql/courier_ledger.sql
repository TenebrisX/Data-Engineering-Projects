-- delivery_data CTE
WITH delivery_data AS (
    SELECT
        cd.courier_id::int AS courier_id,
        AVG(rate) AS rate_avg,
        SUM(cd.tip_sum) AS courier_tips_sum
    FROM
        dds.c_deliveries cd
    GROUP BY
        courier_id
),
-- order_data CTE
order_data AS (
    SELECT
        do2.courier_id,
        COUNT(DISTINCT do2.id) AS orders_count,
        SUM(fps.total_sum) AS orders_total_sum,
        SUM(fps.total_sum) * 0.25 AS order_processing_fee,
        do2.timestamp_id AS timestamp_id
    FROM
        dds.dm_orders do2
    JOIN
        dds.fct_product_sales fps ON fps.order_id = do2.id
    GROUP BY
        courier_id, do2.timestamp_id
),
-- date_data CTE
date_data AS (
    SELECT
        dt.id AS id,
        dt."year" AS "settlement_year",
        dt."month" AS "settlement_month"
    FROM
        dds.dm_timestamps dt
    WHERE
        dt.ts >= DATE_TRUNC('day', CURRENT_DATE) - INTERVAL '1 month'
)
SELECT
    cc.id AS courier_id,
    cc.courier_name AS courier_name,
    dt.settlement_year,
    dt.settlement_month,
    od.orders_count,
    od.orders_total_sum,
    dd.rate_avg,
    od.order_processing_fee,
    CASE
        WHEN dd.rate_avg < 4 THEN
            GREATEST(od.orders_total_sum * 0.05, 100)
        WHEN dd.rate_avg >= 4 AND dd.rate_avg < 4.5 THEN
            GREATEST(od.orders_total_sum * 0.07, 150)
        WHEN dd.rate_avg >= 4.5 AND dd.rate_avg < 4.9 THEN
            GREATEST(od.orders_total_sum * 0.08, 175)
        WHEN dd.rate_avg >= 4.9 THEN
            GREATEST(od.orders_total_sum * 0.1, 200)
    END AS courier_order_sum,
    dd.courier_tips_sum,
    (CASE
        WHEN dd.rate_avg < 4 THEN
            GREATEST(od.orders_total_sum * 0.05, 100)
        WHEN dd.rate_avg >= 4 AND dd.rate_avg < 4.5 THEN
            GREATEST(od.orders_total_sum * 0.07, 150)
        WHEN dd.rate_avg >= 4.5 AND dd.rate_avg < 4.9 THEN
            GREATEST(od.orders_total_sum * 0.08, 175)
        WHEN dd.rate_avg >= 4.9 THEN
            GREATEST(od.orders_total_sum * 0.1, 200)
    END) + dd.courier_tips_sum * 0.95 AS total_payment
FROM
    dds.c_couriers cc
JOIN
    delivery_data dd ON dd.courier_id = cc.id
JOIN
    order_data od ON cc.id = od.courier_id
JOIN
    date_data dt ON dt.id = od.timestamp_id
WHERE
    cc.id > %(threshold)s
LIMIT %(limit)s;
