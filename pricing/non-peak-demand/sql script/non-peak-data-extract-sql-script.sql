WITH city_cluster_hex AS (

        SELECT
            cch.hex_id AS hex_id,
            cch.cluster AS cluster,
            cch.city
        FROM
            datasets.city_cluster_hex cch
        WHERE
            cch.resolution = 8
    ),

    fare_estimates AS (

        SELECT
            fe_ench.yyyymmdd AS yyyymmdd,
            DATE_FORMAT(DATE_TRUNC('week', DATE_PARSE(fe_ench.yyyymmdd, '%Y%m%d')), '%Y%m%d') week_start_date,
            DATE_FORMAT(DATE_PARSE(fe_ench.yyyymmdd, '%Y%m%d'), '%W') week_day,
            fe_ench.quarter_hour AS quarter_hour,
            SUBSTR(fe_ench.quarter_hour,1,2) AS hour,
            fe_ench.city AS city_name,
            fe_ench.service_name AS service_name,
            fe_ench.service_detail_id AS service_detail_id,
            fe_ench.pickup_location_hex_8 AS pickup_location_hex_8,
            fe_ench.drop_location_hex_8 AS drop_location_hex_8,
            COALESCE(fe_ench.price_increase_strategy,'surge_not_applied') AS surge_strategy,
            COALESCE(fe_ench.is_gradient_applied,false) AS gradient_applied,
            fe_ench.fare_estimate_id AS fare_estimate_id,
            fe_ench.user_id AS fe_customer_id,
            fe_ench.sub_total,
            fe_ench.final_amount,
            fe_ench.dynamic_surge AS dynamic_surge,
            fe_ench.dynamic_fare AS dynamic_fare,
            fe_ench.ride_distance AS ride_distance,
            fe_ench.offer_type AS offer_type,
            fe_ench.discount_type AS discount_type,
            fe_ench.discount_amount AS discount_amount,
            fe_ench.final_amount AS final_amount
    
        FROM
            pricing.fare_estimates_enriched fe_ench
        WHERE
            fe_ench.yyyymmdd >= '20230710'
            AND fe_ench.yyyymmdd <= '20230710'
            AND fe_ench.service_detail_id = '5bed473f1278885df4ea9d57'
                        
    ),
    
    rate_card AS (
        
        SELECT 
            city,
            yyyymmdd,
            fare_estimate_id,
            service_detail_id,
            ROUND(CAST(rate_card_amount AS DOUBLE),2) AS rate_card_amount
        FROM 
            experiments.iprice_cleaned_responses_v2 
        WHERE 
            yyyymmdd >= '20230710'
            AND yyyymmdd <= '20230710'
            AND service_detail_id = '5bed473f1278885df4ea9d57'
    ),

    orders AS (

        SELECT
            ols.yyyymmdd AS yyyymmdd,
            ols.service_detail_id AS service_detail_id,
            ols.customer_id AS gross_customer_id,
            ols.estimate_id AS fare_estimate_id,
            ols.order_status AS order_status,
            ols.order_id AS order_id,
            ols.spd_fraud_flag AS spd_fraud_flag,
            ols.distance_final_distance AS distance_final_distance,
            ols.rate_card_amount AS rate_card_amount,
            ols.accept_to_pickup_distance  AS accept_to_pickup_distance,
            ROW_NUMBER() OVER (PARTITION BY ols.order_id ORDER BY ols.updated_epoch DESC) AS row_number,
            CASE 
            WHEN ols.order_status IN ('dropped') AND ols.spd_fraud_flag != True THEN 'net_orders'
            WHEN ols.order_status IN ('customerCancelled') AND ols.cancel_reason IN ('order cancelled before rider accepted') THEN 'cobra'
            WHEN ols.order_status IN ('customerCancelled') AND ols.cancel_reason IN ('Order cancelled before rider was mapped') THEN 'cobrm'
            WHEN ols.order_status IN ('customerCancelled') AND ols.cancel_reason NOT IN ('order cancelled before rider accepted', 'Order cancelled before rider was mapped') THEN 'ocara'
            WHEN ols.order_status IN ('expired') AND length(ols.map_riders) < 28 THEN 'stockout'
            WHEN ols.order_status IN ('expired') AND length(ols.map_riders) >= 28 THEN 'expiry_mapped' 
            ELSE 'Other' 
            END AS order_state
            
        FROM
            orders.order_logs_snapshot ols
        WHERE
            ols.yyyymmdd >= '20230710' 
            AND ols.yyyymmdd <= '20230710'
            AND ols.service_detail_id = '5bed473f1278885df4ea9d57'
    ),
    
    mismatch AS (
        SELECT 
            nm.service_detail_id, 
            nm.yyyymmdd,
            nm.quarter_hour,
            nm.hex_id,
            nm.service_level,
            nm.rr_counts AS demand,
            nm.idle_captain_counts_probabilistic AS supply,
            (nm.rr_counts - nm.idle_captain_counts_probabilistic) AS mismatch
            
        FROM 
            datasets.marketplace_mismatch_realtime nm
            
        JOIN city_cluster_hex cch 
            ON cch.hex_id = nm.hex_id
            AND cch.city = 'Chennai'
    
        WHERE 
            yyyymmdd BETWEEN '20230710' AND '20230710'
            AND nm.service_detail_id  = '5bed473f1278885df4ea9d57'
    ),

    combined_df AS (

        SELECT 
            fe.yyyymmdd AS yyyymmdd,
            fe.week_start_date AS week_start_date,
            fe.week_day AS week_day,
            fe.quarter_hour AS quarter_hour,
            fe.hour AS hour,
            fe.city_name AS city_name,
            fe.service_name AS service_name,
            fe.service_detail_id AS service_detail_id,
            pic.cluster AS pickup_cluster,
            pic.hex_id AS pickup_hex_8,
            
            COALESCE(mm.demand, 0) AS demand,
            COALESCE(mm.supply, 0) AS supply,
            COALESCE(mm.mismatch, 0) AS mismatch,
            
            drp.cluster AS drop_cluster,
            drp.hex_id AS drop_hex_8,
            
            fe.surge_strategy AS surge_strategy,
            fe.dynamic_surge AS dynamic_surge,
            fe.dynamic_fare AS dynamic_fare,
            fe.sub_total AS sub_total,
            rc.rate_card_amount AS fe_rate_card_amount,
            ord.rate_card_amount AS ols_rate_card_amount,
            fe.ride_distance AS ride_distance,
            fe.discount_type AS discount_type,
            fe.discount_amount AS discount_amount,
            
            fe.fe_customer_id AS fe_customer_id,
            fe.fare_estimate_id AS fare_estimate_id,
            
            ord.gross_customer_id AS gross_customer_id,
            ord.order_id AS order_id,
            ord.order_status AS order_status,
            ord.spd_fraud_flag AS spd_fraud_flag,
            ord.distance_final_distance AS distance_final_distance,
            ord.order_state,
            ord.row_number,
            ord.accept_to_pickup_distance
            
        FROM
            fare_estimates fe
        
        LEFT JOIN
            city_cluster_hex pic
            ON fe.pickup_location_hex_8 = pic.hex_id
        
        LEFT JOIN 
            city_cluster_hex drp
            ON fe.drop_location_hex_8 = drp.hex_id
            
        LEFT JOIN
            rate_card rc
            ON fe.yyyymmdd = rc.yyyymmdd
            AND fe.city_name = rc.city
            AND fe.fare_estimate_id = rc.fare_estimate_id
            AND fe.service_detail_id = rc.service_detail_id
        
        LEFT JOIN
            orders ord
            ON fe.yyyymmdd = ord.yyyymmdd
            AND fe.service_detail_id = ord.service_detail_id
            AND fe.fare_estimate_id = ord.fare_estimate_id
            
        LEFT JOIN 
            mismatch mm 
            ON fe.yyyymmdd = mm.yyyymmdd
            AND fe.quarter_hour = mm.quarter_hour
            AND fe.service_detail_id = mm.service_detail_id
            AND fe.pickup_location_hex_8 = mm.hex_id
            
    )

SELECT 
    
    yyyymmdd,
    week_start_date,
    week_day,
    city_name,
    service_name,
    service_detail_id,
    quarter_hour,
    hour,
    pickup_cluster,
    pickup_hex_8,
    surge_strategy,
    demand,
    supply,
    mismatch,
    
    COUNT(DISTINCT fare_estimate_id) fe_count,
    COUNT(DISTINCT order_id) gross_orders,
    COUNT(DISTINCT CASE WHEN order_status = 'dropped' AND spd_fraud_flag != true THEN order_id END) net_orders,
    
    COUNT(DISTINCT CASE WHEN surge_strategy = 'surge_not_applied' OR surge_strategy IS NULL THEN fare_estimate_id END) surged_fe,
    COUNT(DISTINCT CASE WHEN order_status = 'dropped' AND spd_fraud_flag != true AND (surge_strategy = 'surge_not_applied' OR surge_strategy IS NULL) THEN order_id END) surged_net_orders,
    
    SUM(dynamic_surge) AS dynamic_surge,
    SUM(dynamic_fare) AS dynamic_fare,
    SUM(sub_total) AS sub_total,
    SUM(fe_rate_card_amount) AS fe_rate_card_amount,
    SUM(ols_rate_card_amount) AS ols_rate_card_amount,

    AVG(ride_distance) AS mean_ride_distance,
    AVG(distance_final_distance) AS mean_distance_final_distance,
    
    COUNT(DISTINCT CASE WHEN order_state IN ('cobra') AND row_number = 1 THEN order_id END) AS cobra,
    COUNT(DISTINCT CASE WHEN order_state IN ('ocara') AND row_number = 1 THEN order_id END) AS ocara,
    COUNT(DISTINCT CASE WHEN accept_to_pickup_distance > 0 THEN order_id END) AS accepted_orders,
    COUNT(DISTINCT CASE WHEN order_state IN ('cobrm') AND row_number = 1 THEN order_id END) AS cobrm,
    COUNT(DISTINCT CASE WHEN order_state IN ('stockout') AND row_number = 1 THEN order_id END) AS stockout,
    COUNT(DISTINCT CASE WHEN order_state IN ('expiry_mapped') AND row_number = 1 THEN order_id END) AS expiry_mapped
    

    
FROM
    combined_df cdf

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
ORDER BY 1,10,7