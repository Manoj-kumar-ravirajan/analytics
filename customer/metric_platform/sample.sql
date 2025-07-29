WITH city_cluster_hex AS (
    
        SELECT
            cch.hex_id AS hex_id,
            cch.cluster AS cluster
        FROM
            datasets.city_cluster_hex cch
        WHERE
            cch.resolution = 8
    ),
    
    additive_surge AS (
        
        SELECT
            srg_bkp.yyyymmdd AS yyyymmdd,
            srg_bkp.service_detail_id AS service_detail_id,
            srg_bkp.fare_estimate_id AS fare_estimate_id,
            srg_bkp.price_increase_strategy AS additive_strategy
        FROM
            pricing.iprice_customer_surge_breakup srg_bkp
        WHERE
            srg_bkp.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}''), '%Y%m%d') 
            AND srg_bkp.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}''), '%Y%m%d')
            AND srg_bkp.surge_category = 'additive'
    ),
    
    fare_estimates AS (

        SELECT
            'Route' AS geo_level,
            'Cluster' AS geo_value,
            fe_ench.epoch AS epoch,
            fe_ench.yyyymmdd AS yyyymmdd,
            fe_ench.city AS city_name,
            fe_ench.service_name AS service_name,
            fe_ench.service_detail_id AS service_detail_id,
            fe_ench.pickup_location_hex_8 AS pickup_location_hex_8,
            fe_ench.drop_location_hex_8 AS drop_location_hex_8,
            COALESCE(fe_ench.price_increase_strategy,'no_dynamic_surge') AS surge_strategy,
            COALESCE(fe_ench.is_gradient_applied,false) AS gradient_applied,
            fe_ench.fare_estimate_id AS fare_estimate_id

        FROM
            pricing_internal.fare_estimates_enriched_v3_1 fe_ench
        WHERE
            fe_ench.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d') 
            AND fe_ench.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
            AND fe_ench.service_name IN ('Link', 'Auto', 'Auto Pool', 'CabEconomy')
    ),
    
    orders AS (
    
        SELECT
            ols.yyyymmdd AS yyyymmdd,
            ols.service_detail_id AS service_detail_id,
            ols.service_obj_service_name AS service_name,
            ols.estimate_id AS fare_estimate_id,
            ols.order_status AS order_status,
            ols.order_id AS order_id,
            row_number() over (partition by ols.order_id order by ols.updated_epoch desc) as Rank_1,
            ols.accept_to_pickup_distance  AS accept_to_pickup_distance,
            ols.map_riders AS map_riders,
            ols.event_type,
            (case when ols.order_status in ('dropped') and ols.spd_fraud_flag != True then 'Net_Orders'
                when ols.order_status in ('customerCancelled') and ols.cancel_reason in ('order cancelled before rider accepted') then 'COBRA'
                when ols.order_status in ('customerCancelled') and ols.cancel_reason in ('Order cancelled before rider was mapped') then 'COBRM'
                when ols.order_status in ('customerCancelled') and ols.cancel_reason not in ('order cancelled before rider accepted', 'Order cancelled before rider was mapped') then 'OCARA'
                else 'Other'
            end) AS End_State
            
            
        FROM
            orders.order_logs_immutable AS ols
        WHERE
            ols.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d') 
            AND ols.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
            AND ols.service_obj_service_name IN ('Link', 'Auto', 'Auto Pool', 'CabEconomy')
    ),
    
    combined_df AS (
        
        SELECT 
            fe.geo_level AS geo_level,
            fe.geo_value AS geo_value,
            fe.epoch AS epoch,
            fe.yyyymmdd AS yyyymmdd,
            fe.city_name AS city_name,
            fe.service_name AS service_name,
            fe.service_detail_id AS service_detail_id,
            pic.cluster AS pickup_location,
            drp.cluster AS drop_location,
            fe.surge_strategy AS surge_strategy,
            fe.gradient_applied AS gradient_applied,
            COALESCE(add_srg.additive_strategy,'no_additive_surge') AS additive_strategy,
            fe.fare_estimate_id AS fare_estimate_id,
            ord.order_status AS order_status,
            ord.order_id AS order_id,
            ord.Rank_1 AS Rank_1,
            ord.End_State AS End_State,
            ord.accept_to_pickup_distance AS accept_to_pickup_distance,
            ord.map_riders,
            ord.event_type
        FROM
            fare_estimates AS fe
        LEFT JOIN
            city_cluster_hex AS pic
            ON fe.pickup_location_hex_8 = pic.hex_id
        LEFT JOIN 
            city_cluster_hex AS drp
            ON fe.drop_location_hex_8 = pic.hex_id
        LEFT JOIN
            additive_surge AS add_srg
            ON fe.yyyymmdd = add_srg.yyyymmdd
            AND fe.service_detail_id = add_srg.service_detail_id
            AND fe.fare_estimate_id = add_srg.fare_estimate_id
        LEFT JOIN
            orders AS ord
            ON fe.yyyymmdd = ord.yyyymmdd
            AND fe.service_detail_id = ord.service_detail_id
            AND fe.service_name = ord.service_name
            AND fe.fare_estimate_id = ord.fare_estimate_id
    )
    
SELECT 
    cdf.geo_level AS geo_level,
    cdf.geo_value AS geo_value,
    cdf.city_name AS city_name,
    cdf.service_name AS service_name,
    cdf.service_detail_id AS service_detail_id,
    cdf.pickup_location AS pickup_location,
    cdf.drop_location AS drop_location,
    cdf.surge_strategy AS surge_strategy,
    cdf.gradient_applied AS gradient_applied,
    cdf.additive_strategy AS additive_strategy,
    count(distinct case when length(cdf.map_riders) < 28 and event_type = 'expired' then order_id end) as stockout_test
    
FROM
    combined_df AS cdf
GROUP BY
    geo_level,
    geo_value,
    city_name,
    service_name,
    service_detail_id,
    pickup_location,
    drop_location,
    surge_strategy,
    gradient_applied,
    additive_strategy






    WITH iallocator_customer_segments AS ( 
    
    SELECT
        (WEEK(DATE_TRUNC('WEEK', '{{StartDate}}') - INTERVAL '8' DAY) + 1) AS week_no,
        DATE_FORMAT(DATE_TRUNC('WEEK', '{{StartDate}}') - INTERVAL '7' DAY, '%Y%m%d') AS week_start_date,
        segments.customer_id AS customer_id, 
        segments.taxi_lifetime_last_ride_city AS taxi_last_ride_city,
        segments.taxi_retention_segments AS taxi_retention_segments
    FROM 
        datasets.iallocator_customer_segments AS segments
    where 
        segments.run_date >= DATE_FORMAT(DATE_TRUNC('WEEK', '{{StartDate}}') - INTERVAL '8' DAY, '%Y-%m-%d')
        AND segments.run_date <= DATE_FORMAT(DATE_TRUNC('WEEK', '{{EndDate}}') - INTERVAL '8' DAY, '%Y-%m-%d')

        -- Always sunday

    )

    SELECT
        segments.week_no AS week_no,
        segments.week_start_date AS week_start_date,
        segments.taxi_last_ride_city AS taxi_last_ride_city,
        segments.taxi_retention_segments AS taxi_retention_segments,
        COUNT(DISTINCT segments.customer_id) AS rf_segment_base_customer
    FROM 
        iallocator_customer_segments segments
    GROUP BY 
        week_no,
        week_start_date,
        taxi_last_ride_city,
        taxi_retention_segments





WITH iallocator_customer_segments AS ( 
    
    SELECT
        (WEEK(DATE_TRUNC('WEEK', {{date}}) - INTERVAL '8' DAY) + 1) AS week_no,
        DATE_FORMAT(DATE_TRUNC('WEEK', {{date}}) - INTERVAL '7' DAY, '%Y%m%d') AS week_start_date,
        segments.customer_id AS customer_id, 
        segments.taxi_lifetime_last_ride_city AS taxi_last_ride_city,
        segments.taxi_retention_segments AS taxi_retention_segments
    FROM 
        datasets.iallocator_customer_segments AS segments
    where 
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', {{date}}) - INTERVAL '8' DAY, '%Y-%m-%d')
        AND segments.taxi_lifetime_last_ride_city = 'Bangalore'
    ),

    olf AS (

    SELECT 
        WEEK(DATE_PARSE(olf.yyyymmdd, '%Y%m%d')) AS week_no,
        DATE_FORMAT(DATE_TRUNC('WEEK', DATE_PARSE(olf.yyyymmdd, '%Y%m%d')), '%Y%m%d') AS week_start_date,
        olf.customer_id AS customer_id,
        olf.order_id AS order_id,
        olf.modified_order_status AS modified_order_status,
        olf.spd_fraud_flag AS spd_fraud_flag
    FROM 
        orders.order_logs_fact AS olf
    WHERE 
        olf.yyyymmdd >= DATE_FORMAT(DATE_TRUNC('WEEK', {{date}}) - INTERVAL '7' DAY, '%Y%m%d')
        AND olf.yyyymmdd <= DATE_FORMAT(DATE_TRUNC('WEEK', {{date}}) - INTERVAL '1' DAY, '%Y%m%d')
        AND olf.service_obj_service_name IN ('Auto','Auto Lite','Auto NCR','Auto Pool','AutoPremium','CityAuto','Link','Bike Lite','Bike Metro','Bike Pink','CabEconomy','CabPremium')
    ),

    merge AS (
    
    SELECT
        segments.week_no AS week_no,
        segments.week_start_date AS week_start_date,
        segments.taxi_last_ride_city AS taxi_last_ride_city,
        segments.taxi_retention_segments  AS taxi_retention_segments,
        olf.customer_id AS customer_id,
        olf.order_id AS order_id,
        olf.modified_order_status AS modified_order_status,
        olf.spd_fraud_flag AS spd_fraud_flag
    FROM 
        iallocator_customer_segments AS segments
    LEFT JOIN 
        olf AS olf
        ON segments.week_no = olf.week_no
        AND segments.week_start_date = olf.week_start_date
        AND segments.customer_id = olf.customer_id
    )
        
    SELECT
        merge.week_no AS week_no,
        merge.week_start_date AS week_start_date,
        merge.taxi_last_ride_city AS taxi_last_ride_city,
        merge.taxi_retention_segments AS taxi_retention_segments,
        COUNT(DISTINCT merge.customer_id) AS rf_segment_gross_customers
    FROM 
        merge AS merge
    GROUP BY 1,2,3,4
    ORDER BY 4