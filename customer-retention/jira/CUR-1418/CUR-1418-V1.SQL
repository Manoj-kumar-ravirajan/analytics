with transaction_query AS
    (SELECT entity_id orderid,
          concat(owner_id, '-', order_unique_id) unique_id,
          subtype,
          sum(CASE WHEN cast(offer_type AS varchar) != 'scratchCardOffer' THEN cast(amount AS INT) END) custCashBackAmount
   FROM
     (SELECT * FROM
        (SELECT id,
                yyyymmdd,
                coin_wallet_amount,
                transaction_type,
                subtype,
                entity_id,
                owner_id,
                order_unique_id,
                coin_wallet_changes,
                coin_wallet_offer_type,
                subsub_type
         FROM payments.transactions_snapshot
         WHERE yyyymmdd BETWEEN date_format({{start_date}}, '%Y%m%d') AND date_format({{endDate}}, '%Y%m%d')
           AND owner_type = 'customer'
           AND transaction_status = 'done'
           AND cardinality(coin_id) != 0 )
      CROSS JOIN unnest(coin_wallet_offer_type, coin_wallet_amount) AS tbl1(offer_type, amount)
      )
    WHERE transaction_type = 'credit'
        AND subtype IN ('locationOffer', 'rideOffer', 'customerCashBack', 'scratchCard')
        AND cast(offer_type AS varchar) IN ('locationOffer', 'rideOffer', 'scratchCardOffer')
    GROUP BY 1, 2, 3
    ),

orders_1 AS
(SELECT week(cast(order_date AS date)) week_number,
        date_format(date_trunc('week', cast(order_date AS date)), '%Y-%m-%d') AS WeekStartDate,
        order_date,
        order_id order_id,
        concat(customer_id, '-', unique_id) unique_id,
        city_name,
        offer_details_offer_id,
        offer_details_offer_code,
        offer_details_offer_type,
        cast(discount AS INT) discount_amount,
        sub_total AS subtotal,
        customer_id,
        distance_final_distance distance
FROM "orders"."order_logs_snapshot"
WHERE yyyymmdd BETWEEN date_format({{start_date}}, '%Y%m%d') AND date_format({{endDate}}, '%Y%m%d')
    AND order_status = 'dropped'
    AND service_obj_service_name = 'Link'
    AND (spd_fraud_flag != true OR spd_fraud_flag IS NULL)
    AND {{city_name}}
),

orders_cb AS
    (SELECT 
        a.*,
        coalesce(b.custCashBackAmount,0) custCashBackAmount
    FROM orders_1 a
    LEFT JOIN transaction_query b on (a.order_id = b.orderid OR a.unique_id = b.unique_id)
    ),

v1 AS
(
    SELECT *
    FROM
    (
        SELECT 
            day,
            customer_rf_segment,
            CASE
            WHEN customer_rf_segment LIKE '%HH%' THEN '02. HH'
            WHEN customer_rf_segment LIKE '%NEW%' THEN '02. HH'
            WHEN customer_rf_segment LIKE '%GOLD%' THEN '05. Gold'
            WHEN customer_rf_segment LIKE '%SILVER%' THEN '04. Silver'
            WHEN customer_rf_segment LIKE '%PLATINUM%' THEN '06. Platinum'
            WHEN customer_rf_segment LIKE '%PRIME%' THEN '03. Prime'
            WHEN customer_rf_segment LIKE '%ELITE%' THEN '07. Elite'
            WHEN customer_rf_segment LIKE '%INACTIVE%' THEN '09. Inactive'
            WHEN customer_rf_segment LIKE '%DORMANT%' THEN '08. Dormant'
            END AS finalSegment,
            customerid, geo_city, service_name,
            ROW_NUMBER() OVER(PARTITION BY customerid, day ORDER BY recency ASC) AS row
        FROM "datasets"."customer_retention_immutable"
        where
            day >= {{start_date}} - interval '1' day 
            AND day <= {{endDate}}
            --AND day_of_week(cast(day as date)) = 7
            AND service_name = 'Link'
            AND geo_city IN (SELECT DISTINCT city_name FROM orders_1)
    )
    WHERE
        ROW = 1 
),
orders_seg AS
(
SELECT  
        a.order_date, 
        b.finalSegment,
        a.city_name,
        
FROM orders_cb AS a
LEFT JOIN v1 b ON date(a.order_date) = b.day AND a.city_name = b.geo_city
)
SELECT 
        *
FROM orders_seg