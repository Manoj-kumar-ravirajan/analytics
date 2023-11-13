with transaction_query AS
    (
    SELECT entity_id orderid,
          concat(owner_id, '-', order_unique_id) unique_id,
          subtype,
          sum(CASE WHEN cast(offer_type AS varchar) != 'scratchCardOffer' THEN cast(amount AS INT) END) custCashBackAmount
   FROM
     (SELECT * FROM
        (SELECT id, yyyymmdd, coin_wallet_amount, transaction_type,
                subtype, entity_id, owner_id,
                order_unique_id, coin_wallet_changes,
                coin_wallet_offer_type, subsub_type
         FROM payments.transactions_snapshot
         WHERE yyyymmdd BETWEEN date_format({{start_date}}, '%Y%m%d') AND date_format({{endDate}}, '%Y%m%d')
            --yyyymmdd BETWEEN date_format(date('{start_date}'),'%Y%m%d') AND date_format(date('{endDate}'),'%Y%m%d')
           AND owner_type = 'customer'
           AND transaction_status = 'done'
           AND cardinality(coin_id) != 0 )
      CROSS JOIN unnest(coin_wallet_offer_type, coin_wallet_amount) AS tbl1(offer_type, amount)
      )
    WHERE transaction_type = 'credit'

    GROUP BY 1, 2, 3
    ),

orders_1 AS
(   SELECT --week(cast(order_date AS date)) week_number,
        --date_format(date_trunc('week', cast(order_date AS date)), '%Y-%m-%d') AS WeekStartDate,
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
        --yyyymmdd BETWEEN date_format(date('{start_date}'),'%Y%m%d') AND date_format(date('{endDate}'),'%Y%m%d')
        AND order_status = 'dropped'
        AND service_obj_service_name = 'Link'
        AND (spd_fraud_flag != true OR spd_fraud_flag IS NULL)
        --AND city_name = '{city_name}'
        AND {{city_name}}
        AND offer_details_offer_type IN ('location','location_coupon', 'retention', 'retention_coupon','rides' ,'rides_coupon', 'subscription')
),
orders_cb AS
(   SELECT 
        a.*,
        coalesce(b.custCashBackAmount,0) custCashBackAmount
    FROM orders_1 a
    LEFT JOIN transaction_query b on (a.order_id = b.orderid) --OR a.unique_id = b.unique_id)
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
            customerid, 
            geo_city, 
            --service_name,
            ROW_NUMBER() OVER(PARTITION BY customerid, day ORDER BY recency ASC) AS row
        FROM "datasets"."customer_retention_immutable"
        where
            day >= {{start_date}} AND day <= {{endDate}}
            --cast(day as date)  >= date('{start_date}') AND cast(day as date) <= date('{endDate}')
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
        a.city_name,
        b.finalSegment,
        --SUM(discount_amount) AS total_discount,
        --SUM(custCashBackAmount) AS total_coin_credited,

        (SUM(discount_amount) + SUM(custCashBackAmount)) AS Total_Burn,
        --location_coupon
        COUNT(DISTINCT CASE WHEN offer_details_offer_type in ('location','location_coupon') THEN customer_id END) AS net_cust_with_loc_coupon,
        COUNT(CASE WHEN offer_details_offer_type in ('location','location_coupon') THEN order_id END) loc_rides_loc_coupon,
        SUM(CASE WHEN offer_details_offer_type in ('location','location_coupon') THEN discount_amount END) total_dis_with_loc_coupon,
        SUM(CASE WHEN offer_details_offer_type in ('location','location_coupon') THEN custCashBackAmount END) total_coins_credited_with_loc_coupon,
        SUM(CASE WHEN offer_details_offer_type in ('location','location_coupon') THEN subtotal END) subtotal_with_loc_coupon,

        --retention_coupon
        COUNT(DISTINCT CASE WHEN offer_details_offer_type in ('retention','retention_coupon') THEN customer_id END) AS net_cust_with_ret_coupon,
        COUNT(CASE WHEN offer_details_offer_type in ('retention','retention_coupon') THEN order_id END) loc_rides_ret_coupon,
        SUM(CASE WHEN offer_details_offer_type in ('retention','retention_coupon') THEN discount_amount END) total_dis_with_ret_coupon,
        SUM(CASE WHEN offer_details_offer_type in ('retention','retention_coupon') THEN custCashBackAmount END) total_coins_credited_with_ret_coupon,
        SUM(CASE WHEN offer_details_offer_type in ('retention','retention_coupon') THEN subtotal END) subtotal_with_ret_coupon,
        
        --rides_coupon
        COUNT(DISTINCT CASE WHEN offer_details_offer_type in ('rides','rides_coupon') THEN customer_id END) AS net_cust_with_rid_coupon,
        COUNT(CASE WHEN offer_details_offer_type in ('rides','rides_coupon') THEN order_id END) loc_rides_rid_coupon,
        SUM(CASE WHEN offer_details_offer_type in ('rides','rides_coupon') THEN discount_amount END) total_dis_with_rid_coupon,
        SUM(CASE WHEN offer_details_offer_type in ('rides','rides_coupon') THEN custCashBackAmount END) total_coins_credited_with_rid_coupon,
        SUM(CASE WHEN offer_details_offer_type in ('rides','rides_coupon') THEN subtotal END) subtotal_with_rid_coupon,
        
        --subscription
        COUNT( DISTINCT CASE WHEN offer_details_offer_type = 'subscription' THEN customer_id END) AS net_cust_with_subs,
        COUNT(CASE WHEN offer_details_offer_type = 'subscription' THEN order_id END) loc_rides_subs,
        SUM(CASE WHEN offer_details_offer_type = 'subscription' THEN discount_amount END) total_dis_with_subs,
        SUM(CASE WHEN offer_details_offer_type = 'subscription' THEN custCashBackAmount END) total_coins_credited_with_subs,
        SUM(CASE WHEN offer_details_offer_type = 'subscription' THEN subtotal END) subtotal_with_subs

FROM orders_cb AS a
LEFT JOIN v1 b ON a.customer_id = b.customerid AND date(a.order_date) = b.day AND a.city_name = b.geo_city
GROUP BY 1,2,3
)
SELECT 
        order_date,
        finalSegment,
        city_name,
        --location_discounts_percentage_without_coin
        COALESCE( (total_dis_with_loc_coupon / (subtotal_with_loc_coupon/loc_rides_loc_coupon)), 0) AS loc_dis_percent_wo_coin_loc_coupon,
        COALESCE( (total_dis_with_ret_coupon/(subtotal_with_ret_coupon/loc_rides_ret_coupon)), 0) AS loc_dis_percent_wo_coin_ret_coupon,
        COALESCE( (total_dis_with_rid_coupon/(subtotal_with_rid_coupon/loc_rides_rid_coupon)), 0) AS loc_dis_percent_wo_coin_rid_coupon,
        COALESCE( (total_dis_with_subs/(subtotal_with_subs/loc_rides_subs)), 0) AS loc_dis_percent_wo_coin_subs,
        --location_discounts_percentage_with_coin
        COALESCE( ((total_dis_with_loc_coupon + total_coins_credited_with_loc_coupon) / (subtotal_with_loc_coupon/loc_rides_loc_coupon)), 0) AS loc_dis_percent_with_coin_loc_coupon,
        COALESCE( ((total_dis_with_ret_coupon + total_coins_credited_with_ret_coupon) / (subtotal_with_ret_coupon/loc_rides_ret_coupon)), 0) AS loc_dis_percent_with_coin_ret_coupon,
        COALESCE( ((total_dis_with_rid_coupon + total_coins_credited_with_rid_coupon) / (subtotal_with_rid_coupon/loc_rides_rid_coupon)), 0) AS loc_dis_percent_with_coin_rid_coupon,
        COALESCE( ((total_dis_with_subs + total_coins_credited_with_subs) / (subtotal_with_subs/loc_rides_subs)), 0) AS loc_dis_percent_with_coin_subs,
        --location_dpr_wo_coin
        COALESCE( (total_dis_with_loc_coupon / loc_rides_loc_coupon), 0) AS loc_dpr_wo_coin_loc_coupon,
        COALESCE( (total_dis_with_ret_coupon / loc_rides_ret_coupon), 0) AS loc_dpr_wo_coin_ret_coupon,
        COALESCE( (total_dis_with_rid_coupon / loc_rides_rid_coupon), 0) AS loc_dpr_wo_coin_rid_coupon,
        COALESCE( (total_dis_with_subs / loc_rides_subs), 0) AS loc_dpr_wo_coin_subs,
        --location_dpr_with_coin
        COALESCE( ((total_dis_with_loc_coupon + total_coins_credited_with_loc_coupon) / loc_rides_loc_coupon), 0) AS loc_dpr_with_coin_loc_coupon,
        COALESCE( ((total_dis_with_ret_coupon + total_coins_credited_with_ret_coupon) / loc_rides_ret_coupon), 0) AS loc_dpr_with_coin_ret_coupon,
        COALESCE( ((total_dis_with_rid_coupon + total_coins_credited_with_rid_coupon) / loc_rides_rid_coupon), 0) AS loc_dpr_with_coin_rid_coupon,
        COALESCE( ((total_dis_with_subs + total_coins_credited_with_subs) / loc_rides_subs), 0) AS loc_dpr_with_coin_subs,
        --Location_Budget_Percentage
        COALESCE( ((total_dis_with_loc_coupon + total_coins_credited_with_loc_coupon) / (Total_Burn)), 0) AS loc_budget_percent_loc_coupon,
        COALESCE( ((total_dis_with_ret_coupon + total_coins_credited_with_ret_coupon) / (Total_Burn)), 0) AS loc_budget_percent_ret_coupon,
        COALESCE( ((total_dis_with_rid_coupon + total_coins_credited_with_rid_coupon) / (Total_Burn)), 0) AS loc_budget_percent_rid_coupon,
        COALESCE( ((total_dis_with_subs + total_coins_credited_with_subs) / (Total_Burn)), 0) AS loc_budget_percent_subs,
        --location_customers
        COALESCE( net_cust_with_loc_coupon, 0) AS net_cust_with_loc_coupon,
        COALESCE( net_cust_with_ret_coupon, 0) AS net_cust_with_ret_coupon,
        COALESCE( net_cust_with_rid_coupon, 0) AS net_cust_with_rid_coupon,
        COALESCE( net_cust_with_subs, 0) AS net_cust_with_subs,
        --location_rides
        COALESCE( loc_rides_loc_coupon, 0) AS loc_rides_loc_coupon,
        COALESCE( loc_rides_ret_coupon, 0) AS loc_rides_ret_coupon,
        COALESCE( loc_rides_rid_coupon, 0) AS loc_rides_rid_coupon,
        COALESCE( loc_rides_subs, 0) AS loc_rides_subs,
        --location_discounts
        COALESCE( total_dis_with_loc_coupon, 0) AS total_dis_with_loc_coupon,
        COALESCE( total_dis_with_ret_coupon, 0) AS total_dis_with_ret_coupon,
        COALESCE( total_dis_with_rid_coupon, 0) AS total_dis_with_rid_coupon,
        COALESCE( total_dis_with_subs, 0) AS total_dis_with_subs,
        --location_coins_credit
        COALESCE( total_coins_credited_with_loc_coupon, 0) AS total_coins_credited_with_loc_coupon,
        COALESCE( total_coins_credited_with_ret_coupon, 0) AS total_coins_credited_with_ret_coupon,
        COALESCE( total_coins_credited_with_rid_coupon, 0) AS total_coins_credited_with_rid_coupon,
        COALESCE( total_coins_credited_with_subs, 0) AS total_coins_credited_with_subs,
        --location_burn
        COALESCE( (total_dis_with_loc_coupon + total_coins_credited_with_loc_coupon), 0) AS loc_burn_loc_coupon,
        COALESCE( (total_dis_with_ret_coupon + total_coins_credited_with_ret_coupon), 0) AS loc_burn_ret_coupon,
        COALESCE( (total_dis_with_rid_coupon + total_coins_credited_with_rid_coupon), 0) AS loc_burn_rid_coupon,
        COALESCE( (total_dis_with_subs + total_coins_credited_with_subs), 0) AS loc_burn_subs

FROM orders_seg
ORDER BY 1,2,3