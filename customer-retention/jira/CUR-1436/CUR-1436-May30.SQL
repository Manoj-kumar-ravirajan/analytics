with coin_credited AS
    (SELECT entity_id orderid,
          concat(owner_id, '-', order_unique_id) unique_id,
          --,SUM(CASE WHEN cast(offer_type AS varchar) = 'scratchCardOffer' THEN cast(amount AS INT) END) scratchcard_amount,
          SUM(CASE WHEN cast(offer_type AS varchar) != 'scratchCardOffer' THEN cast(amount AS INT) END) custCashBackAmount
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
    GROUP BY 1, 2
    ),
coin_expired AS
    (SELECT entity_id orderid,
          concat(owner_id, '-', order_unique_id) unique_id,
          --,SUM(CASE WHEN cast(offer_type AS varchar) = 'scratchCardOffer' THEN cast(amount AS INT) END) scratchcard_amount,
          SUM(CASE WHEN cast(offer_type AS varchar) != 'scratchCardOffer' THEN cast(amount AS INT) END) custexpiryAmount
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
         WHERE --yyyymmdd BETWEEN date_format({{start_date}}, '%Y%m%d') AND date_format({{endDate}}, '%Y%m%d')
           yyyymmdd BETWEEN date_format({{start_date}}, '%Y%m%d') AND date_format({{endDate}}+ interval '21' day, '%Y%m%d')
           
           AND owner_type = 'customer'
           AND transaction_status = 'done'
           AND cardinality(coin_id) != 0 )
      CROSS JOIN unnest(coin_wallet_offer_type, coin_wallet_amount) AS tbl1(offer_type, amount)
      )
    WHERE transaction_type = 'debit' AND subtype = 'coinExpired'
        AND CAST(offer_type AS varchar) IN ('locationOffer', 'rideOffer', 'scratchCardOffer')
    GROUP BY 1, 2
    ),
transaction_query AS
(   SELECT 
        a.orderid,
        a.unique_id,
        coalesce(a.custCashBackAmount,0) custCashBackAmount,
        coalesce(b.custexpiryAmount,0) custexpiryAmount
    FROM coin_credited a
    LEFT JOIN coin_expired b on a.orderid = b.orderid 
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
        AND service_obj_service_name = {{service}}
        AND (spd_fraud_flag != true OR spd_fraud_flag IS NULL)
        AND {{city_name}}
        AND {{offer_details_offer_type}}
        AND {{offer_details_offer_code}}
    ),
orders_2 AS
    (SELECT 
          order_date,
          city_name AS cityname,
          COUNT(order_id) net_city_rides
          
    FROM "orders"."order_logs_snapshot"
    WHERE yyyymmdd BETWEEN date_format({{start_date}}, '%Y%m%d') AND date_format({{endDate}}, '%Y%m%d')
        AND order_status = 'dropped'
        AND service_obj_service_name = {{service}}
        AND (spd_fraud_flag != true OR spd_fraud_flag IS NULL)
        AND {{city_name}}
    GROUP BY 1,2
    ),    
orders_cb AS
    (SELECT a.*,
            c.net_city_rides,
          coalesce(b.custCashBackAmount, 0) AS custCashBackAmount,
          coalesce(b.custexpiryAmount, 0) AS custexpiryAmount,
          coalesce(b.custCashBackAmount - b.custexpiryAmount, 0) AS coin_utilization
    FROM orders_1 a
    LEFT JOIN orders_2 c ON a.order_date = c.order_date AND a.city_name = c.cityname   
    LEFT JOIN transaction_query b on a.order_id = b.orderid
    ),
offer_details as 
    (select 
       yyyymmdd, 
        startdate, endDate, 
        userselectors, 
        id, 
        offer_name,
        description
    from 
    (
    select 
        yyyymmdd, 
        startdate, endDate, 
        userselectors, 
        id, 
        NAME AS offer_name,
        description,
        row_number() over(partition by id order by yyyymmdd desc) offer_rnk
    from 
        raw.kafka_offers_immutable 
    where
        yyyymmdd between date_format({{start_date}} - interval '14' day, '%Y%m%d') and date_format({{endDate}}, '%Y%m%d')
    )
    where offer_rnk = 1
)
SELECT  order_date, 
        city_name,
        case when offer_details_offer_type = 'subscription' then '' else offer_details_offer_id end offer_id,
        b.offer_name,
        description,
        offer_details_offer_type AS offer_type,
        coalesce(offer_details_offer_code, 'No coupon') AS coupon_code,
        SUM(discount_amount) AS discount_amount,        
        SUM(custCashBackAmount) AS total_coin_credited,
        SUM(custexpiryAmount) AS total_coin_expired,
        SUM(coin_utilization) AS coin_utilization,
        net_city_rides,        
        COUNT(order_id) AS offer_rides,
        COUNT(order_id)*100/net_city_rides AS offer_rides_percentage,        
        COUNT(CASE WHEN discount_amount != 0 THEN order_id END) discounted_rides,
        SUM(subtotal) subtotal,
        SUM(discount_amount)+SUM(coin_utilization) AS burn,
        TRY((SUM(discount_amount)/SUM(subtotal))*100) disc_Percentage,
        TRY(((SUM(discount_amount)+SUM(coin_utilization))/SUM(subtotal))*100) burn_Percentage,
        coalesce(TRY((SUM(discount_amount)/count(CASE WHEN discount_amount != 0 THEN order_id END))), 0) DPR,
        coalesce(TRY(((SUM(discount_amount)+SUM(coin_utilization))/count(CASE WHEN discount_amount != 0 THEN order_id END))), 0) BPR
        
FROM orders_cb a
LEFT JOIN offer_details b ON a.offer_details_offer_id = b.id
--WHERE  offer_name is not null
GROUP BY 1, 2, 3, 4, 5, 6,7, net_city_rides
ORDER BY 1,2, 13 desc