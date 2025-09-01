-- count_total_customer_segment_weekly
with customer_segments AS (
    SELECT 
        segments.run_date as run_date,
        segments.taxi_lifetime_last_ride_city as taxi_lifetime_last_ride_city,
        segments.customer_id AS customer_id, 
        segments.taxi_usecase_segment AS taxi_usecase_segment,
        segments.predicted_age AS predicted_age,
        segments.os AS os,
        segments.taxi_need_segment AS taxi_need_segment,
        segments.taxi_retention_segments AS taxi_retention_segment,
        CASE 
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) > 2 THEN 'TWO_PLUS'
        WHEN segments.taxi_lifetime_rr_city_list IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS multi_city_segment,
        segments.rr_last_90_days_service_tag AS cross_sell_segment,
        CASE 
        WHEN segments.geo_use_case_list IS NULL THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 2 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 4 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) > 2 THEN 'TWO_PLUS'
        ELSE 'UNKNOWN' END AS geo_usecase_segment,
        segments.rha_signal AS rha_signal,
        segments.quick_commerce_signal AS quick_commerce_signal,
        segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        CASE 
        WHEN segments.run_date = DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'FTU'
        WHEN segments.run_date > DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'RTU'
        WHEN segments.taxi_lifetime_first_rr_date IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS new_repeat_signal
        
      FROM 
        datasets.iallocator_customer_segments AS segments 
      WHERE
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '1' DAY, '%Y-%m-%d')
    )
    
    SELECT
        customer_segments.taxi_usecase_segment,
        customer_segments.predicted_age,
        customer_segments.os,
        customer_segments.taxi_need_segment,
        customer_segments.taxi_retention_segment,
        customer_segments.multi_city_segment,
        customer_segments.cross_sell_segment,
        customer_segments.geo_usecase_segment,
        customer_segments.rha_signal,
        customer_segments.quick_commerce_signal,
        customer_segments.vehicle_ownership_signal,
        customer_segments.new_repeat_signal,
        COUNT(DISTINCT customer_segments.customer_id) AS count_total_customer_segment_weekly
    FROM
        customer_segments AS customer_segments
    GROUP BY
        customer_segments.taxi_usecase_segment,
        customer_segments.predicted_age,
        customer_segments.os,
        customer_segments.taxi_need_segment,
        customer_segments.taxi_retention_segment,
        customer_segments.multi_city_segment,
        customer_segments.cross_sell_segment,
        customer_segments.geo_usecase_segment,
        customer_segments.rha_signal,
        customer_segments.quick_commerce_signal,
        customer_segments.vehicle_ownership_signal,
        customer_segments.new_repeat_signal


-- count_gross_customer_segment_weekly
-- count_gross_customer_segment_service_weekly
with customer_segments AS (
    SELECT 
        segments.customer_id AS customer_id, 
        segments.taxi_usecase_segment AS taxi_usecase_segment,
        segments.predicted_age AS predicted_age,
        segments.os AS os,
        segments.taxi_need_segment AS taxi_need_segment,
        segments.taxi_retention_segments AS taxi_retention_segment,
        CASE 
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) > 2 THEN 'TWO_PLUS'
        WHEN segments.taxi_lifetime_rr_city_list IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS multi_city_segment,
        segments.rr_last_90_days_service_tag AS cross_sell_segment,
        CASE 
        WHEN segments.geo_use_case_list IS NULL THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 2 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 4 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) > 2 THEN 'TWO_PLUS'
        ELSE 'UNKNOWN' END AS geo_usecase_segment,
        segments.rha_signal AS rha_signal,
        segments.quick_commerce_signal AS quick_commerce_signal,
        segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        CASE 
        WHEN segments.run_date = DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'FTU'
        WHEN segments.run_date > DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'RTU'
        WHEN segments.taxi_lifetime_first_rr_date IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS new_repeat_signal
        
      FROM 
        datasets.iallocator_customer_segments AS segments 
      WHERE
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '1' DAY, '%Y-%m-%d')
    ),
    
    order_logs AS (
    SELECT 
        olf.yyyymmdd AS yyyymmdd,
        olf.epoch AS epoch,
        olf.service_detail_id AS service_detail_id,
        olf.customer_id AS customer_id, 
        olf.order_id AS order_id,
        olf.city_name AS city_name
    FROM 
        orders.order_logs_fact AS olf 
    WHERE 
        olf.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d') 
        AND olf.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
    ),

    segment_order_merge AS (
    SELECT
        customer_segments.taxi_usecase_segment AS taxi_usecase_segment,
        customer_segments.predicted_age AS predicted_age,
        customer_segments.os AS os,
        customer_segments.taxi_need_segment AS taxi_need_segment,
        customer_segments.taxi_retention_segment AS taxi_retention_segment,
        customer_segments.multi_city_segment AS multi_city_segment,
        customer_segments.cross_sell_segment AS cross_sell_segment,
        customer_segments.geo_usecase_segment AS geo_usecase_segment,
        customer_segments.rha_signal AS rha_signal,
        customer_segments.quick_commerce_signal AS quick_commerce_signal,
        customer_segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        customer_segments.new_repeat_signal AS new_repeat_signal,
        order_logs.city_name AS city_name,
        order_logs.yyyymmdd AS yyyymmdd,
        order_logs.epoch AS epoch,
        order_logs.service_detail_id AS service_detail_id,
        order_logs.customer_id AS customer_id,
        order_logs.order_id AS order_id
    
    FROM
        customer_segments AS customer_segments
    LEFT JOIN 
        order_logs AS order_logs 
        ON customer_segments.customer_id = order_logs.customer_id
    )

    SELECT
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal,
        COUNT(DISTINCT segment_order_merge.customer_id) AS count_gross_customer_segment_service_weekly
    FROM
        segment_order_merge AS segment_order_merge
    GROUP BY
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal


-- count_net_customer_segment_weekly
-- count_net_customer_segment_service_weekly
with customer_segments AS (
    SELECT 
        segments.customer_id AS customer_id, 
        segments.taxi_usecase_segment AS taxi_usecase_segment,
        segments.predicted_age AS predicted_age,
        segments.os AS os,
        segments.taxi_need_segment AS taxi_need_segment,
        segments.taxi_retention_segments AS taxi_retention_segment,
        CASE 
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) > 2 THEN 'TWO_PLUS'
        WHEN segments.taxi_lifetime_rr_city_list IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS multi_city_segment,
        segments.rr_last_90_days_service_tag AS cross_sell_segment,
        CASE 
        WHEN segments.geo_use_case_list IS NULL THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 2 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 4 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) > 2 THEN 'TWO_PLUS'
        ELSE 'UNKNOWN' END AS geo_usecase_segment,
        segments.rha_signal AS rha_signal,
        segments.quick_commerce_signal AS quick_commerce_signal,
        segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        CASE 
        WHEN segments.run_date = DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'FTU'
        WHEN segments.run_date > DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'RTU'
        WHEN segments.taxi_lifetime_first_rr_date IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS new_repeat_signal
        
      FROM 
        datasets.iallocator_customer_segments AS segments 
      WHERE
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '1' DAY, '%Y-%m-%d')
    ),
    
    order_logs AS (
    SELECT 
        olf.yyyymmdd AS yyyymmdd,
        olf.epoch AS epoch,
        olf.service_detail_id AS service_detail_id,
        olf.customer_id AS customer_id, 
        olf.order_id AS order_id,
        olf.city_name AS city_name
    FROM 
        orders.order_logs_fact AS olf 
    WHERE 
        olf.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d') 
        AND olf.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
        AND olf.modified_order_status = 'dropped'
    ),

    segment_order_merge AS (
    SELECT
        customer_segments.taxi_usecase_segment AS taxi_usecase_segment,
        customer_segments.predicted_age AS predicted_age,
        customer_segments.os AS os,
        customer_segments.taxi_need_segment AS taxi_need_segment,
        customer_segments.taxi_retention_segment AS taxi_retention_segment,
        customer_segments.multi_city_segment AS multi_city_segment,
        customer_segments.cross_sell_segment AS cross_sell_segment,
        customer_segments.geo_usecase_segment AS geo_usecase_segment,
        customer_segments.rha_signal AS rha_signal,
        customer_segments.quick_commerce_signal AS quick_commerce_signal,
        customer_segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        customer_segments.new_repeat_signal AS new_repeat_signal,
        order_logs.city_name AS city_name,
        order_logs.yyyymmdd AS yyyymmdd,
        order_logs.epoch AS epoch,
        order_logs.service_detail_id AS service_detail_id,
        order_logs.customer_id AS customer_id,
        order_logs.order_id AS order_id
    
    FROM
        customer_segments AS customer_segments
    LEFT JOIN 
        order_logs AS order_logs 
        ON customer_segments.customer_id = order_logs.customer_id
    )

    SELECT
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal,
        COUNT(DISTINCT segment_order_merge.customer_id) AS count_net_customer_segment_service_weekly
    FROM
        segment_order_merge AS segment_order_merge
    GROUP BY
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal


-- count_rr_orders_segment_weekly
with customer_segments AS (
    SELECT 
        segments.customer_id AS customer_id, 
        segments.taxi_usecase_segment AS taxi_usecase_segment,
        segments.predicted_age AS predicted_age,
        segments.os AS os,
        segments.taxi_need_segment AS taxi_need_segment,
        segments.taxi_retention_segments AS taxi_retention_segment,
        CASE 
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) > 2 THEN 'TWO_PLUS'
        WHEN segments.taxi_lifetime_rr_city_list IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS multi_city_segment,
        segments.rr_last_90_days_service_tag AS cross_sell_segment,
        CASE 
        WHEN segments.geo_use_case_list IS NULL THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 2 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 4 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) > 2 THEN 'TWO_PLUS'
        ELSE 'UNKNOWN' END AS geo_usecase_segment,
        segments.rha_signal AS rha_signal,
        segments.quick_commerce_signal AS quick_commerce_signal,
        segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        CASE 
        WHEN segments.run_date = DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'FTU'
        WHEN segments.run_date > DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'RTU'
        WHEN segments.taxi_lifetime_first_rr_date IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS new_repeat_signal 
      FROM 
        datasets.iallocator_customer_segments AS segments 
      WHERE
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '1' DAY, '%Y-%m-%d')
    ),

    order_logs AS  (
    SELECT 
        olf.yyyymmdd AS yyyymmdd,
        olf.epoch AS epoch,
        olf.service_detail_id AS service_detail_id,
        olf.customer_id AS customer_id, 
        olf.order_id AS order_id,
        olf.city_name AS city_name
    FROM 
        orders.order_logs_fact AS olf 
    WHERE 
        olf.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d') 
        AND olf.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
    ),

    segment_order_merge AS (
    SELECT
        customer_segments.taxi_usecase_segment AS taxi_usecase_segment,
        customer_segments.predicted_age AS predicted_age,
        customer_segments.os AS os,
        customer_segments.taxi_need_segment AS taxi_need_segment,
        customer_segments.taxi_retention_segment AS taxi_retention_segment,
        customer_segments.multi_city_segment AS multi_city_segment,
        customer_segments.cross_sell_segment AS cross_sell_segment,
        customer_segments.geo_usecase_segment AS geo_usecase_segment,
        customer_segments.rha_signal AS rha_signal,
        customer_segments.quick_commerce_signal AS quick_commerce_signal,
        customer_segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        customer_segments.new_repeat_signal AS new_repeat_signal,
        order_logs.city_name AS city_name,
        order_logs.yyyymmdd AS yyyymmdd,
        order_logs.epoch AS epoch,
        order_logs.service_detail_id AS service_detail_id,
        order_logs.customer_id AS customer_id,
        order_logs.order_id AS order_id
    FROM
        customer_segments AS customer_segments
    LEFT JOIN 
        order_logs AS order_logs 
        ON customer_segments.customer_id = order_logs.customer_id
    )

    SELECT
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal,
        COUNT(DISTINCT segment_order_merge.order_id) AS count_rr_orders_segment_weekly
    FROM
        segment_order_merge AS segment_order_merge
    GROUP BY
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal


-- count_net_orders_segment_weekly
with customer_segments AS (
    SELECT 
        segments.customer_id AS customer_id, 
        segments.taxi_usecase_segment AS taxi_usecase_segment,
        segments.predicted_age AS predicted_age,
        segments.os AS os,
        segments.taxi_need_segment AS taxi_need_segment,
        segments.taxi_retention_segments AS taxi_retention_segment,
        CASE 
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) > 2 THEN 'TWO_PLUS'
        WHEN segments.taxi_lifetime_rr_city_list IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS multi_city_segment,
        segments.rr_last_90_days_service_tag AS cross_sell_segment,
        CASE 
        WHEN segments.geo_use_case_list IS NULL THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 2 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 4 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) > 2 THEN 'TWO_PLUS'
        ELSE 'UNKNOWN' END AS geo_usecase_segment,
        segments.rha_signal AS rha_signal,
        segments.quick_commerce_signal AS quick_commerce_signal,
        segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        CASE 
        WHEN segments.run_date = DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'FTU'
        WHEN segments.run_date > DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'RTU'
        WHEN segments.taxi_lifetime_first_rr_date IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS new_repeat_signal 
      FROM 
        datasets.iallocator_customer_segments AS segments 
      WHERE
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '1' DAY, '%Y-%m-%d')
    ),

    order_logs AS  (
    SELECT 
        olf.yyyymmdd AS yyyymmdd,
        olf.epoch AS epoch,
        olf.service_detail_id AS service_detail_id,
        olf.customer_id AS customer_id, 
        olf.order_id AS order_id,
        olf.city_name AS city_name
    FROM 
        orders.order_logs_fact AS olf 
    WHERE 
        olf.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d') 
        AND olf.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
        AND olf.modified_order_status = 'dropped'
    ),

    segment_order_merge AS (
    SELECT
        customer_segments.taxi_usecase_segment AS taxi_usecase_segment,
        customer_segments.predicted_age AS predicted_age,
        customer_segments.os AS os,
        customer_segments.taxi_need_segment AS taxi_need_segment,
        customer_segments.taxi_retention_segment AS taxi_retention_segment,
        customer_segments.multi_city_segment AS multi_city_segment,
        customer_segments.cross_sell_segment AS cross_sell_segment,
        customer_segments.geo_usecase_segment AS geo_usecase_segment,
        customer_segments.rha_signal AS rha_signal,
        customer_segments.quick_commerce_signal AS quick_commerce_signal,
        customer_segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        customer_segments.new_repeat_signal AS new_repeat_signal,
        order_logs.city_name AS city_name,
        order_logs.yyyymmdd AS yyyymmdd,
        order_logs.epoch AS epoch,
        order_logs.service_detail_id AS service_detail_id,
        order_logs.customer_id AS customer_id,
        order_logs.order_id AS order_id
    FROM
        customer_segments AS customer_segments
    LEFT JOIN 
        order_logs AS order_logs 
        ON customer_segments.customer_id = order_logs.customer_id
    )

    SELECT
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal,
        COUNT(DISTINCT segment_order_merge.order_id) AS count_net_orders_segment_weekly
    FROM
        segment_order_merge AS segment_order_merge
    GROUP BY
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal


-- count_cobrm_orders_segment_weekly
with customer_segments AS (
    SELECT 
        segments.customer_id AS customer_id, 
        segments.taxi_usecase_segment AS taxi_usecase_segment,
        segments.predicted_age AS predicted_age,
        segments.os AS os,
        segments.taxi_need_segment AS taxi_need_segment,
        segments.taxi_retention_segments AS taxi_retention_segment,
        CASE 
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) > 2 THEN 'TWO_PLUS'
        WHEN segments.taxi_lifetime_rr_city_list IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS multi_city_segment,
        segments.rr_last_90_days_service_tag AS cross_sell_segment,
        CASE 
        WHEN segments.geo_use_case_list IS NULL THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 2 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 4 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) > 2 THEN 'TWO_PLUS'
        ELSE 'UNKNOWN' END AS geo_usecase_segment,
        segments.rha_signal AS rha_signal,
        segments.quick_commerce_signal AS quick_commerce_signal,
        segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        CASE 
        WHEN segments.run_date = DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'FTU'
        WHEN segments.run_date > DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'RTU'
        WHEN segments.taxi_lifetime_first_rr_date IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS new_repeat_signal 
      FROM 
        datasets.iallocator_customer_segments AS segments 
      WHERE
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '1' DAY, '%Y-%m-%d')
    ),

    order_logs AS  (
    SELECT 
        olf.yyyymmdd AS yyyymmdd,
        olf.epoch AS epoch,
        olf.service_detail_id AS service_detail_id,
        olf.customer_id AS customer_id, 
        olf.order_id AS order_id,
        olf.city_name AS city_name
    FROM 
        orders.order_logs_fact AS olf 
    WHERE 
        olf.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d') 
        AND olf.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
        AND olf.modified_order_status = 'COBRM'
    ),

    segment_order_merge AS (
    SELECT
        customer_segments.taxi_usecase_segment AS taxi_usecase_segment,
        customer_segments.predicted_age AS predicted_age,
        customer_segments.os AS os,
        customer_segments.taxi_need_segment AS taxi_need_segment,
        customer_segments.taxi_retention_segment AS taxi_retention_segment,
        customer_segments.multi_city_segment AS multi_city_segment,
        customer_segments.cross_sell_segment AS cross_sell_segment,
        customer_segments.geo_usecase_segment AS geo_usecase_segment,
        customer_segments.rha_signal AS rha_signal,
        customer_segments.quick_commerce_signal AS quick_commerce_signal,
        customer_segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        customer_segments.new_repeat_signal AS new_repeat_signal,
        order_logs.city_name AS city_name,
        order_logs.yyyymmdd AS yyyymmdd,
        order_logs.epoch AS epoch,
        order_logs.service_detail_id AS service_detail_id,
        order_logs.customer_id AS customer_id,
        order_logs.order_id AS order_id
    FROM
        customer_segments AS customer_segments
    LEFT JOIN 
        order_logs AS order_logs 
        ON customer_segments.customer_id = order_logs.customer_id
    )

    SELECT
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal,
        COUNT(DISTINCT segment_order_merge.order_id) AS count_cobrm_orders_segment_weekly
    FROM
        segment_order_merge AS segment_order_merge
    GROUP BY
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal


-- count_cobra_order_segment_weekly
with customer_segments AS (
    SELECT 
        segments.customer_id AS customer_id, 
        segments.taxi_usecase_segment AS taxi_usecase_segment,
        segments.predicted_age AS predicted_age,
        segments.os AS os,
        segments.taxi_need_segment AS taxi_need_segment,
        segments.taxi_retention_segments AS taxi_retention_segment,
        CASE 
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) > 2 THEN 'TWO_PLUS'
        WHEN segments.taxi_lifetime_rr_city_list IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS multi_city_segment,
        segments.rr_last_90_days_service_tag AS cross_sell_segment,
        CASE 
        WHEN segments.geo_use_case_list IS NULL THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 2 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 4 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) > 2 THEN 'TWO_PLUS'
        ELSE 'UNKNOWN' END AS geo_usecase_segment,
        segments.rha_signal AS rha_signal,
        segments.quick_commerce_signal AS quick_commerce_signal,
        segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        CASE 
        WHEN segments.run_date = DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'FTU'
        WHEN segments.run_date > DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'RTU'
        WHEN segments.taxi_lifetime_first_rr_date IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS new_repeat_signal 
      FROM 
        datasets.iallocator_customer_segments AS segments 
      WHERE
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '1' DAY, '%Y-%m-%d')
    ),

    order_logs AS  (
    SELECT 
        olf.yyyymmdd AS yyyymmdd,
        olf.epoch AS epoch,
        olf.service_detail_id AS service_detail_id,
        olf.customer_id AS customer_id, 
        olf.order_id AS order_id,
        olf.city_name AS city_name
    FROM 
        orders.order_logs_fact AS olf 
    WHERE 
        olf.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d') 
        AND olf.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
        AND olf.modified_order_status = 'COBRA'
    ),

    segment_order_merge AS (
    SELECT
        customer_segments.taxi_usecase_segment AS taxi_usecase_segment,
        customer_segments.predicted_age AS predicted_age,
        customer_segments.os AS os,
        customer_segments.taxi_need_segment AS taxi_need_segment,
        customer_segments.taxi_retention_segment AS taxi_retention_segment,
        customer_segments.multi_city_segment AS multi_city_segment,
        customer_segments.cross_sell_segment AS cross_sell_segment,
        customer_segments.geo_usecase_segment AS geo_usecase_segment,
        customer_segments.rha_signal AS rha_signal,
        customer_segments.quick_commerce_signal AS quick_commerce_signal,
        customer_segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        customer_segments.new_repeat_signal AS new_repeat_signal,
        order_logs.city_name AS city_name,
        order_logs.yyyymmdd AS yyyymmdd,
        order_logs.epoch AS epoch,
        order_logs.service_detail_id AS service_detail_id,
        order_logs.customer_id AS customer_id,
        order_logs.order_id AS order_id
    FROM
        customer_segments AS customer_segments
    LEFT JOIN 
        order_logs AS order_logs 
        ON customer_segments.customer_id = order_logs.customer_id
    )

    SELECT
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal,
        COUNT(DISTINCT segment_order_merge.order_id) AS count_cobra_order_segment_weekly
    FROM
        segment_order_merge AS segment_order_merge
    GROUP BY
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal


-- count_ocara_orders_segment_weekly
with customer_segments AS (
    SELECT 
        segments.customer_id AS customer_id, 
        segments.taxi_usecase_segment AS taxi_usecase_segment,
        segments.predicted_age AS predicted_age,
        segments.os AS os,
        segments.taxi_need_segment AS taxi_need_segment,
        segments.taxi_retention_segments AS taxi_retention_segment,
        CASE 
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) > 2 THEN 'TWO_PLUS'
        WHEN segments.taxi_lifetime_rr_city_list IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS multi_city_segment,
        segments.rr_last_90_days_service_tag AS cross_sell_segment,
        CASE 
        WHEN segments.geo_use_case_list IS NULL THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 2 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 4 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) > 2 THEN 'TWO_PLUS'
        ELSE 'UNKNOWN' END AS geo_usecase_segment,
        segments.rha_signal AS rha_signal,
        segments.quick_commerce_signal AS quick_commerce_signal,
        segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        CASE 
        WHEN segments.run_date = DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'FTU'
        WHEN segments.run_date > DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'RTU'
        WHEN segments.taxi_lifetime_first_rr_date IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS new_repeat_signal 
      FROM 
        datasets.iallocator_customer_segments AS segments 
      WHERE
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '1' DAY, '%Y-%m-%d')
    ),

    order_logs AS  (
    SELECT 
        olf.yyyymmdd AS yyyymmdd,
        olf.epoch AS epoch,
        olf.service_detail_id AS service_detail_id,
        olf.customer_id AS customer_id, 
        olf.order_id AS order_id,
        olf.city_name AS city_name
    FROM 
        orders.order_logs_fact AS olf 
    WHERE 
        olf.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d') 
        AND olf.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
        AND olf.modified_order_status = 'OCARA'
    ),

    segment_order_merge AS (
    SELECT
        customer_segments.taxi_usecase_segment AS taxi_usecase_segment,
        customer_segments.predicted_age AS predicted_age,
        customer_segments.os AS os,
        customer_segments.taxi_need_segment AS taxi_need_segment,
        customer_segments.taxi_retention_segment AS taxi_retention_segment,
        customer_segments.multi_city_segment AS multi_city_segment,
        customer_segments.cross_sell_segment AS cross_sell_segment,
        customer_segments.geo_usecase_segment AS geo_usecase_segment,
        customer_segments.rha_signal AS rha_signal,
        customer_segments.quick_commerce_signal AS quick_commerce_signal,
        customer_segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        customer_segments.new_repeat_signal AS new_repeat_signal,
        order_logs.city_name AS city_name,
        order_logs.yyyymmdd AS yyyymmdd,
        order_logs.epoch AS epoch,
        order_logs.service_detail_id AS service_detail_id,
        order_logs.customer_id AS customer_id,
        order_logs.order_id AS order_id
    FROM
        customer_segments AS customer_segments
    LEFT JOIN 
        order_logs AS order_logs 
        ON customer_segments.customer_id = order_logs.customer_id
    )

    SELECT
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal,
        COUNT(DISTINCT segment_order_merge.order_id) AS count_ocara_orders_segment_weekly
    FROM
        segment_order_merge AS segment_order_merge
    GROUP BY
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal


-- count_expired_orders_segment_weekly
with customer_segments AS (
    SELECT 
        segments.customer_id AS customer_id, 
        segments.taxi_usecase_segment AS taxi_usecase_segment,
        segments.predicted_age AS predicted_age,
        segments.os AS os,
        segments.taxi_need_segment AS taxi_need_segment,
        segments.taxi_retention_segments AS taxi_retention_segment,
        CASE 
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) > 2 THEN 'TWO_PLUS'
        WHEN segments.taxi_lifetime_rr_city_list IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS multi_city_segment,
        segments.rr_last_90_days_service_tag AS cross_sell_segment,
        CASE 
        WHEN segments.geo_use_case_list IS NULL THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 2 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 4 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) > 2 THEN 'TWO_PLUS'
        ELSE 'UNKNOWN' END AS geo_usecase_segment,
        segments.rha_signal AS rha_signal,
        segments.quick_commerce_signal AS quick_commerce_signal,
        segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        CASE 
        WHEN segments.run_date = DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'FTU'
        WHEN segments.run_date > DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'RTU'
        WHEN segments.taxi_lifetime_first_rr_date IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS new_repeat_signal 
      FROM 
        datasets.iallocator_customer_segments AS segments 
      WHERE
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '1' DAY, '%Y-%m-%d')
    ),

    order_logs AS  (
    SELECT 
        olf.yyyymmdd AS yyyymmdd,
        olf.epoch AS epoch,
        olf.service_detail_id AS service_detail_id,
        olf.customer_id AS customer_id, 
        olf.order_id AS order_id,
        olf.city_name AS city_name
    FROM 
        orders.order_logs_fact AS olf 
    WHERE 
        olf.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d') 
        AND olf.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
        AND olf.modified_order_status = 'expired'
    ),

    segment_order_merge AS (
    SELECT
        customer_segments.taxi_usecase_segment AS taxi_usecase_segment,
        customer_segments.predicted_age AS predicted_age,
        customer_segments.os AS os,
        customer_segments.taxi_need_segment AS taxi_need_segment,
        customer_segments.taxi_retention_segment AS taxi_retention_segment,
        customer_segments.multi_city_segment AS multi_city_segment,
        customer_segments.cross_sell_segment AS cross_sell_segment,
        customer_segments.geo_usecase_segment AS geo_usecase_segment,
        customer_segments.rha_signal AS rha_signal,
        customer_segments.quick_commerce_signal AS quick_commerce_signal,
        customer_segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        customer_segments.new_repeat_signal AS new_repeat_signal,
        order_logs.city_name AS city_name,
        order_logs.yyyymmdd AS yyyymmdd,
        order_logs.epoch AS epoch,
        order_logs.service_detail_id AS service_detail_id,
        order_logs.customer_id AS customer_id,
        order_logs.order_id AS order_id
    FROM
        customer_segments AS customer_segments
    LEFT JOIN 
        order_logs AS order_logs 
        ON customer_segments.customer_id = order_logs.customer_id
    )

    SELECT
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal,
        COUNT(DISTINCT segment_order_merge.order_id) AS count_expired_orders_segment_weekly
    FROM
        segment_order_merge AS segment_order_merge
    GROUP BY
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal


-- count_aborted_orders_segment_weekly
with customer_segments AS (
    SELECT 
        segments.customer_id AS customer_id, 
        segments.taxi_usecase_segment AS taxi_usecase_segment,
        segments.predicted_age AS predicted_age,
        segments.os AS os,
        segments.taxi_need_segment AS taxi_need_segment,
        segments.taxi_retention_segments AS taxi_retention_segment,
        CASE 
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) > 2 THEN 'TWO_PLUS'
        WHEN segments.taxi_lifetime_rr_city_list IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS multi_city_segment,
        segments.rr_last_90_days_service_tag AS cross_sell_segment,
        CASE 
        WHEN segments.geo_use_case_list IS NULL THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 2 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 4 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) > 2 THEN 'TWO_PLUS'
        ELSE 'UNKNOWN' END AS geo_usecase_segment,
        segments.rha_signal AS rha_signal,
        segments.quick_commerce_signal AS quick_commerce_signal,
        segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        CASE 
        WHEN segments.run_date = DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'FTU'
        WHEN segments.run_date > DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'RTU'
        WHEN segments.taxi_lifetime_first_rr_date IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS new_repeat_signal 
      FROM 
        datasets.iallocator_customer_segments AS segments 
      WHERE
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '1' DAY, '%Y-%m-%d')
    ),

    order_logs AS  (
    SELECT 
        olf.yyyymmdd AS yyyymmdd,
        olf.epoch AS epoch,
        olf.service_detail_id AS service_detail_id,
        olf.customer_id AS customer_id, 
        olf.order_id AS order_id,
        olf.city_name AS city_name
    FROM 
        orders.order_logs_fact AS olf 
    WHERE 
        olf.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d') 
        AND olf.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
        AND olf.modified_order_status = 'aborted'
    ),

    segment_order_merge AS (
    SELECT
        customer_segments.taxi_usecase_segment AS taxi_usecase_segment,
        customer_segments.predicted_age AS predicted_age,
        customer_segments.os AS os,
        customer_segments.taxi_need_segment AS taxi_need_segment,
        customer_segments.taxi_retention_segment AS taxi_retention_segment,
        customer_segments.multi_city_segment AS multi_city_segment,
        customer_segments.cross_sell_segment AS cross_sell_segment,
        customer_segments.geo_usecase_segment AS geo_usecase_segment,
        customer_segments.rha_signal AS rha_signal,
        customer_segments.quick_commerce_signal AS quick_commerce_signal,
        customer_segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        customer_segments.new_repeat_signal AS new_repeat_signal,
        order_logs.city_name AS city_name,
        order_logs.yyyymmdd AS yyyymmdd,
        order_logs.epoch AS epoch,
        order_logs.service_detail_id AS service_detail_id,
        order_logs.customer_id AS customer_id,
        order_logs.order_id AS order_id
    FROM
        customer_segments AS customer_segments
    LEFT JOIN 
        order_logs AS order_logs 
        ON customer_segments.customer_id = order_logs.customer_id
    )

    SELECT
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal,
        COUNT(DISTINCT segment_order_merge.order_id) AS count_aborted_orders_segment_weekly
    FROM
        segment_order_merge AS segment_order_merge
    GROUP BY
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal


-- count_fe_events_segment_weekly
with customer_segments as (
    SELECT
        segments.customer_id AS customer_id, 
        segments.taxi_usecase_segment AS taxi_usecase_segment,
        segments.predicted_age AS predicted_age,
        segments.os AS os,
        segments.taxi_need_segment AS taxi_need_segment,
        segments.taxi_retention_segments AS taxi_retention_segment,
        CASE 
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) > 2 THEN 'TWO_PLUS'
        WHEN segments.taxi_lifetime_rr_city_list IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS multi_city_segment,
        segments.rr_last_90_days_service_tag AS cross_sell_segment,
        CASE 
        WHEN segments.geo_use_case_list IS NULL THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 2 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 4 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) > 2 THEN 'TWO_PLUS'
        ELSE 'UNKNOWN' END AS geo_usecase_segment,
        segments.rha_signal AS rha_signal,
        segments.quick_commerce_signal AS quick_commerce_signal,
        segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        CASE 
        WHEN segments.run_date = DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'FTU'
        WHEN segments.run_date > DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'RTU'
        WHEN segments.taxi_lifetime_first_rr_date IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS new_repeat_signal
        
    FROM
        datasets.iallocator_customer_segments AS segments
    WHERE 
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '1' DAY, '%Y-%m-%d')
    ),
    
    ct_fe_tbl as (
    SELECT
        ct_fe.yyyymmdd AS yyyymmdd,
        ct_fe.epoch as epoch,
        ct_fe.service_details_id as service_details_id,
        ct_fe.user_id AS customer_id, 
        ct_fe.fare_estimate_id AS fe_id,
        ct_fe.current_city as current_city
    FROM 
        canonical.clevertap_customer_fare_estimate AS ct_fe 
    WHERE 
        ct_fe.yyyymmdd >= DATE_FORMAT('{{StartDate}}', '%Y%m%d') 
        AND ct_fe.yyyymmdd <= DATE_FORMAT('{{EndDate}}', '%Y%m%d') 
    ),
    
    segment_fe_merge as (
    select
        customer_segments.taxi_usecase_segment AS taxi_usecase_segment,
        customer_segments.predicted_age AS predicted_age,
        customer_segments.os AS os,
        customer_segments.taxi_need_segment AS taxi_need_segment,
        customer_segments.taxi_retention_segment AS taxi_retention_segment,
        customer_segments.multi_city_segment AS multi_city_segment,
        customer_segments.cross_sell_segment AS cross_sell_segment,
        customer_segments.geo_usecase_segment AS geo_usecase_segment,
        customer_segments.rha_signal AS rha_signal,
        customer_segments.quick_commerce_signal AS quick_commerce_signal,
        customer_segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        customer_segments.new_repeat_signal AS new_repeat_signal,
        ct_fe_tbl.current_city as current_city,
        ct_fe_tbl.yyyymmdd as yyyymmdd,
        ct_fe_tbl.epoch as epoch,
        ct_fe_tbl.service_details_id as service_details_id,
        ct_fe_tbl.customer_id as customer_id,
        ct_fe_tbl.fe_id as fe_id
    from
        customer_segments as customer_segments
        left join ct_fe_tbl as ct_fe_tbl on customer_segments.customer_id = ct_fe_tbl.customer_id
    )
    select
        segment_fe_merge.taxi_usecase_segment,
        segment_fe_merge.predicted_age,
        segment_fe_merge.os,
        segment_fe_merge.taxi_need_segment,
        segment_fe_merge.taxi_retention_segment,
        segment_fe_merge.multi_city_segment,
        segment_fe_merge.cross_sell_segment,
        segment_fe_merge.geo_usecase_segment,
        segment_fe_merge.rha_signal,
        segment_fe_merge.quick_commerce_signal,
        segment_fe_merge.vehicle_ownership_signal,
        segment_fe_merge.new_repeat_signal,
        count(distinct segment_fe_merge.fe_id) as count_fe_events_segment_weekly
    from
        segment_fe_merge as segment_fe_merge
    group by
        segment_fe_merge.taxi_usecase_segment,
        segment_fe_merge.predicted_age,
        segment_fe_merge.os,
        segment_fe_merge.taxi_need_segment,
        segment_fe_merge.taxi_retention_segment,
        segment_fe_merge.multi_city_segment,
        segment_fe_merge.cross_sell_segment,
        segment_fe_merge.geo_usecase_segment,
        segment_fe_merge.rha_signal,
        segment_fe_merge.quick_commerce_signal,
        segment_fe_merge.vehicle_ownership_signal,
        segment_fe_merge.new_repeat_signal


-- count_fe_sessions_segment_weekly
with customer_segments as (
    SELECT
        segments.customer_id AS customer_id, 
        segments.taxi_usecase_segment AS taxi_usecase_segment,
        segments.predicted_age AS predicted_age,
        segments.os AS os,
        segments.taxi_need_segment AS taxi_need_segment,
        segments.taxi_retention_segments AS taxi_retention_segment,
        CASE 
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) > 2 THEN 'TWO_PLUS'
        WHEN segments.taxi_lifetime_rr_city_list IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS multi_city_segment,
        segments.rr_last_90_days_service_tag AS cross_sell_segment,
        CASE 
        WHEN segments.geo_use_case_list IS NULL THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 2 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 4 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) > 2 THEN 'TWO_PLUS'
        ELSE 'UNKNOWN' END AS geo_usecase_segment,
        segments.rha_signal AS rha_signal,
        segments.quick_commerce_signal AS quick_commerce_signal,
        segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        CASE 
        WHEN segments.run_date = DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'FTU'
        WHEN segments.run_date > DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'RTU'
        WHEN segments.taxi_lifetime_first_rr_date IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS new_repeat_signal
        
    FROM
        datasets.iallocator_customer_segments AS segments
    WHERE 
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '1' DAY, '%Y-%m-%d')
    ),
    
    ct_fe_tbl AS (
    SELECT 
        ct_fe.yyyymmdd AS yyyymmdd,
        ct_fe.epoch AS epoch,
        ct_fe.service_details_id AS service_details_id,
        ct_fe.user_id AS customer_id, 
        (cast(cast(ct_fe.ct_session_id AS decimal) AS varchar) || ' - ' || ct_fe.phone)  as fe_session,
        ct_fe.current_city AS current_city
    FROM 
        canonical.clevertap_customer_fare_estimate AS ct_fe 
    WHERE 
        ct_fe.yyyymmdd >= DATE_FORMAT('{{StartDate}}', '%Y%m%d') 
        AND ct_fe.yyyymmdd <= DATE_FORMAT('{{EndDate}}','%Y%m%d')
    ),
    
    segment_fe_merge AS (
    SELECT
        customer_segments.taxi_usecase_segment AS taxi_usecase_segment,
        customer_segments.predicted_age AS predicted_age,
        customer_segments.os AS os,
        customer_segments.taxi_need_segment AS taxi_need_segment,
        customer_segments.taxi_retention_segment AS taxi_retention_segment,
        customer_segments.multi_city_segment AS multi_city_segment,
        customer_segments.cross_sell_segment AS cross_sell_segment,
        customer_segments.geo_usecase_segment AS geo_usecase_segment,
        customer_segments.rha_signal AS rha_signal,
        customer_segments.quick_commerce_signal AS quick_commerce_signal,
        customer_segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        customer_segments.new_repeat_signal AS new_repeat_signal,
        ct_fe_tbl.current_city AS current_city,
        ct_fe_tbl.yyyymmdd AS yyyymmdd,
        ct_fe_tbl.epoch AS epoch,
        ct_fe_tbl.service_details_id AS service_details_id,
        ct_fe_tbl.customer_id AS customer_id,
        ct_fe_tbl.fe_session AS fe_session
    FROM
        customer_segments AS customer_segments
        LEFT JOIN ct_fe_tbl AS ct_fe_tbl ON customer_segments.customer_id = ct_fe_tbl.customer_id
    )
    SELECT
        segment_fe_merge.taxi_usecase_segment,
        segment_fe_merge.predicted_age,
        segment_fe_merge.os,
        segment_fe_merge.taxi_need_segment,
        segment_fe_merge.taxi_retention_segment,
        segment_fe_merge.multi_city_segment,
        segment_fe_merge.cross_sell_segment,
        segment_fe_merge.geo_usecase_segment,
        segment_fe_merge.rha_signal,
        segment_fe_merge.quick_commerce_signal,
        segment_fe_merge.vehicle_ownership_signal,
        segment_fe_merge.new_repeat_signal,
        count(distinct segment_fe_merge.fe_session) as count_fe_sessions_segment_weekly
    FROM
        segment_fe_merge as segment_fe_merge
    GROUP BY
        segment_fe_merge.taxi_usecase_segment,
        segment_fe_merge.predicted_age,
        segment_fe_merge.os,
        segment_fe_merge.taxi_need_segment,
        segment_fe_merge.taxi_retention_segment,
        segment_fe_merge.multi_city_segment,
        segment_fe_merge.cross_sell_segment,
        segment_fe_merge.geo_usecase_segment,
        segment_fe_merge.rha_signal,
        segment_fe_merge.quick_commerce_signal,
        segment_fe_merge.vehicle_ownership_signal,
        segment_fe_merge.new_repeat_signal


-- count_fe_customers_segment_weekly
with customer_segments as (
    SELECT
        segments.customer_id AS customer_id, 
        segments.taxi_usecase_segment AS taxi_usecase_segment,
        segments.predicted_age AS predicted_age,
        segments.os AS os,
        segments.taxi_need_segment AS taxi_need_segment,
        segments.taxi_retention_segments AS taxi_retention_segment,
        CASE 
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) > 2 THEN 'TWO_PLUS'
        WHEN segments.taxi_lifetime_rr_city_list IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS multi_city_segment,
        segments.rr_last_90_days_service_tag AS cross_sell_segment,
        CASE 
        WHEN segments.geo_use_case_list IS NULL THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 2 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 4 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) > 2 THEN 'TWO_PLUS'
        ELSE 'UNKNOWN' END AS geo_usecase_segment,
        segments.rha_signal AS rha_signal,
        segments.quick_commerce_signal AS quick_commerce_signal,
        segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        CASE 
        WHEN segments.run_date = DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'FTU'
        WHEN segments.run_date > DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'RTU'
        WHEN segments.taxi_lifetime_first_rr_date IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS new_repeat_signal
        
    FROM
        datasets.iallocator_customer_segments AS segments
    WHERE 
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '1' DAY, '%Y-%m-%d')
    ),
    
    ct_fe_tbl as (
    SELECT
        ct_fe.yyyymmdd AS yyyymmdd,
        ct_fe.epoch as epoch,
        ct_fe.service_details_id as service_details_id,
        ct_fe.user_id AS customer_id, 
        ct_fe.fare_estimate_id AS fe_id,
        ct_fe.current_city as current_city
    FROM 
        canonical.clevertap_customer_fare_estimate AS ct_fe 
    WHERE 
        ct_fe.yyyymmdd >= DATE_FORMAT('{{StartDate}}', '%Y%m%d') 
        AND ct_fe.yyyymmdd <= DATE_FORMAT('{{EndDate}}', '%Y%m%d') 
    ),
    
    segment_fe_merge as (
    select
        customer_segments.taxi_usecase_segment AS taxi_usecase_segment,
        customer_segments.predicted_age AS predicted_age,
        customer_segments.os AS os,
        customer_segments.taxi_need_segment AS taxi_need_segment,
        customer_segments.taxi_retention_segment AS taxi_retention_segment,
        customer_segments.multi_city_segment AS multi_city_segment,
        customer_segments.cross_sell_segment AS cross_sell_segment,
        customer_segments.geo_usecase_segment AS geo_usecase_segment,
        customer_segments.rha_signal AS rha_signal,
        customer_segments.quick_commerce_signal AS quick_commerce_signal,
        customer_segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        customer_segments.new_repeat_signal AS new_repeat_signal,
        ct_fe_tbl.current_city as current_city,
        ct_fe_tbl.yyyymmdd as yyyymmdd,
        ct_fe_tbl.epoch as epoch,
        ct_fe_tbl.service_details_id as service_details_id,
        ct_fe_tbl.customer_id as customer_id,
        ct_fe_tbl.fe_id as fe_id
    from
        customer_segments as customer_segments
        left join ct_fe_tbl as ct_fe_tbl on customer_segments.customer_id = ct_fe_tbl.customer_id
    )
    select
        segment_fe_merge.taxi_usecase_segment,
        segment_fe_merge.predicted_age,
        segment_fe_merge.os,
        segment_fe_merge.taxi_need_segment,
        segment_fe_merge.taxi_retention_segment,
        segment_fe_merge.multi_city_segment,
        segment_fe_merge.cross_sell_segment,
        segment_fe_merge.geo_usecase_segment,
        segment_fe_merge.rha_signal,
        segment_fe_merge.quick_commerce_signal,
        segment_fe_merge.vehicle_ownership_signal,
        segment_fe_merge.new_repeat_signal,
        count(distinct segment_fe_merge.customer_id) as count_fe_customers_segment_weekly
    from
        segment_fe_merge as segment_fe_merge
    group by
        segment_fe_merge.taxi_usecase_segment,
        segment_fe_merge.predicted_age,
        segment_fe_merge.os,
        segment_fe_merge.taxi_need_segment,
        segment_fe_merge.taxi_retention_segment,
        segment_fe_merge.multi_city_segment,
        segment_fe_merge.cross_sell_segment,
        segment_fe_merge.geo_usecase_segment,
        segment_fe_merge.rha_signal,
        segment_fe_merge.quick_commerce_signal,
        segment_fe_merge.vehicle_ownership_signal,
        segment_fe_merge.new_repeat_signal


-- sum_cobrm_ttc_secs_segment_weekly
with customer_segments AS (
    SELECT 
        segments.customer_id AS customer_id, 
        segments.taxi_usecase_segment AS taxi_usecase_segment,
        segments.predicted_age AS predicted_age,
        segments.os AS os,
        segments.taxi_need_segment AS taxi_need_segment,
        segments.taxi_retention_segments AS taxi_retention_segment,
        CASE 
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) > 2 THEN 'TWO_PLUS'
        WHEN segments.taxi_lifetime_rr_city_list IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS multi_city_segment,
        segments.rr_last_90_days_service_tag AS cross_sell_segment,
        CASE 
        WHEN segments.geo_use_case_list IS NULL THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 2 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 4 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) > 2 THEN 'TWO_PLUS'
        ELSE 'UNKNOWN' END AS geo_usecase_segment,
        segments.rha_signal AS rha_signal,
        segments.quick_commerce_signal AS quick_commerce_signal,
        segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        CASE 
        WHEN segments.run_date = DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'FTU'
        WHEN segments.run_date > DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'RTU'
        WHEN segments.taxi_lifetime_first_rr_date IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS new_repeat_signal
      FROM 
        datasets.iallocator_customer_segments AS segments 
      WHERE 
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '1' DAY, '%Y-%m-%d')
    ),
    
    order_logs AS (
    SELECT 
        olf.yyyymmdd AS yyyymmdd,
        olf.epoch AS epoch,
        olf.service_detail_id AS service_detail_id,
        olf.customer_id AS customer_id, 
        olf.order_id AS order_id,
        olf.city_name AS city_name,
        olf.customer_cancelled_epoch AS customer_cancelled_epoch,
        olf.order_requested_epoch AS order_requested_epoch
    FROM 
        orders.order_logs_fact AS olf 
    WHERE 
        olf.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d') 
        AND olf.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
        and olf.modified_order_status = 'COBRM'
    ),
    
    segment_order_merge AS (
    SELECT
        customer_segments.taxi_usecase_segment AS taxi_usecase_segment,
        customer_segments.predicted_age AS predicted_age,
        customer_segments.os AS os,
        customer_segments.taxi_need_segment AS taxi_need_segment,
        customer_segments.taxi_retention_segment AS taxi_retention_segment,
        customer_segments.multi_city_segment AS multi_city_segment,
        customer_segments.cross_sell_segment AS cross_sell_segment,
        customer_segments.geo_usecase_segment AS geo_usecase_segment,
        customer_segments.rha_signal AS rha_signal,
        customer_segments.quick_commerce_signal AS quick_commerce_signal,
        customer_segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        customer_segments.new_repeat_signal AS new_repeat_signal,
        order_logs.city_name AS city_name,
        order_logs.yyyymmdd AS yyyymmdd,
        order_logs.epoch AS epoch,
        order_logs.service_detail_id AS service_detail_id,
        order_logs.customer_id AS customer_id,
        order_logs.order_id AS order_id,
        order_logs.customer_cancelled_epoch AS customer_cancelled_epoch,
        order_logs.order_requested_epoch AS order_requested_epoch,
        ((CAST(order_logs.customer_cancelled_epoch AS BIGINT) - CAST(order_logs.order_requested_epoch AS BIGINT))/1000) AS cobrm_ttc_secs
    FROM
        customer_segments AS customer_segments
    LEFT JOIN
        order_logs AS order_logs 
        ON customer_segments.customer_id = order_logs.customer_id
    )

    SELECT
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal,
        SUM(segment_order_merge.cobrm_ttc_secs) AS sum_cobrm_ttc_secs_segment_weekly
    FROM
        segment_order_merge AS segment_order_merge
    group by
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal


-- perc_25_cobrm_ttc_secs_segment_weekly
with customer_segments AS (
    SELECT 
        segments.customer_id AS customer_id, 
        segments.taxi_usecase_segment AS taxi_usecase_segment,
        segments.predicted_age AS predicted_age,
        segments.os AS os,
        segments.taxi_need_segment AS taxi_need_segment,
        segments.taxi_retention_segments AS taxi_retention_segment,
        CASE 
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) > 2 THEN 'TWO_PLUS'
        WHEN segments.taxi_lifetime_rr_city_list IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS multi_city_segment,
        segments.rr_last_90_days_service_tag AS cross_sell_segment,
        CASE 
        WHEN segments.geo_use_case_list IS NULL THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 2 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 4 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) > 2 THEN 'TWO_PLUS'
        ELSE 'UNKNOWN' END AS geo_usecase_segment,
        segments.rha_signal AS rha_signal,
        segments.quick_commerce_signal AS quick_commerce_signal,
        segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        CASE 
        WHEN segments.run_date = DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'FTU'
        WHEN segments.run_date > DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'RTU'
        WHEN segments.taxi_lifetime_first_rr_date IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS new_repeat_signal
      FROM 
        datasets.iallocator_customer_segments AS segments 
      WHERE 
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '1' DAY, '%Y-%m-%d')
    ),
    
    order_logs AS (
    SELECT 
        olf.yyyymmdd AS yyyymmdd,
        olf.epoch AS epoch,
        olf.service_detail_id AS service_detail_id,
        olf.customer_id AS customer_id, 
        olf.order_id AS order_id,
        olf.city_name AS city_name,
        olf.customer_cancelled_epoch AS customer_cancelled_epoch,
        olf.order_requested_epoch AS order_requested_epoch
    FROM 
        orders.order_logs_fact AS olf 
    WHERE 
        olf.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d') 
        AND olf.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
        and olf.modified_order_status = 'COBRM'
    ),
    
    segment_order_merge AS (
    SELECT
        customer_segments.taxi_usecase_segment AS taxi_usecase_segment,
        customer_segments.predicted_age AS predicted_age,
        customer_segments.os AS os,
        customer_segments.taxi_need_segment AS taxi_need_segment,
        customer_segments.taxi_retention_segment AS taxi_retention_segment,
        customer_segments.multi_city_segment AS multi_city_segment,
        customer_segments.cross_sell_segment AS cross_sell_segment,
        customer_segments.geo_usecase_segment AS geo_usecase_segment,
        customer_segments.rha_signal AS rha_signal,
        customer_segments.quick_commerce_signal AS quick_commerce_signal,
        customer_segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        customer_segments.new_repeat_signal AS new_repeat_signal,
        order_logs.city_name AS city_name,
        order_logs.yyyymmdd AS yyyymmdd,
        order_logs.epoch AS epoch,
        order_logs.service_detail_id AS service_detail_id,
        order_logs.customer_id AS customer_id,
        order_logs.order_id AS order_id,
        order_logs.customer_cancelled_epoch AS customer_cancelled_epoch,
        order_logs.order_requested_epoch AS order_requested_epoch,
        ((CAST(order_logs.customer_cancelled_epoch AS BIGINT) - CAST(order_logs.order_requested_epoch AS BIGINT))/1000) AS cobrm_ttc_secs
    FROM
        customer_segments AS customer_segments
    LEFT JOIN
        order_logs AS order_logs 
        ON customer_segments.customer_id = order_logs.customer_id
    )

    SELECT
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal,
        PERCENTILE_APPROX(segment_order_merge.cobrm_ttc_secs , 0.25) AS perc_25_cobrm_ttc_secs_segment_weekly
    FROM
        segment_order_merge AS segment_order_merge
    group by
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal


-- perc_50_cobrm_ttc_secs_segment_weekly
with customer_segments AS (
    SELECT 
        segments.customer_id AS customer_id, 
        segments.taxi_usecase_segment AS taxi_usecase_segment,
        segments.predicted_age AS predicted_age,
        segments.os AS os,
        segments.taxi_need_segment AS taxi_need_segment,
        segments.taxi_retention_segments AS taxi_retention_segment,
        CASE 
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) > 2 THEN 'TWO_PLUS'
        WHEN segments.taxi_lifetime_rr_city_list IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS multi_city_segment,
        segments.rr_last_90_days_service_tag AS cross_sell_segment,
        CASE 
        WHEN segments.geo_use_case_list IS NULL THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 2 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 4 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) > 2 THEN 'TWO_PLUS'
        ELSE 'UNKNOWN' END AS geo_usecase_segment,
        segments.rha_signal AS rha_signal,
        segments.quick_commerce_signal AS quick_commerce_signal,
        segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        CASE 
        WHEN segments.run_date = DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'FTU'
        WHEN segments.run_date > DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'RTU'
        WHEN segments.taxi_lifetime_first_rr_date IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS new_repeat_signal
      FROM 
        datasets.iallocator_customer_segments AS segments 
      WHERE 
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '1' DAY, '%Y-%m-%d')
    ),
    
    order_logs AS (
    SELECT 
        olf.yyyymmdd AS yyyymmdd,
        olf.epoch AS epoch,
        olf.service_detail_id AS service_detail_id,
        olf.customer_id AS customer_id, 
        olf.order_id AS order_id,
        olf.city_name AS city_name,
        olf.customer_cancelled_epoch AS customer_cancelled_epoch,
        olf.order_requested_epoch AS order_requested_epoch
    FROM 
        orders.order_logs_fact AS olf 
    WHERE 
        olf.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d') 
        AND olf.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
        and olf.modified_order_status = 'COBRM'
    ),
    
    segment_order_merge AS (
    SELECT
        customer_segments.taxi_usecase_segment AS taxi_usecase_segment,
        customer_segments.predicted_age AS predicted_age,
        customer_segments.os AS os,
        customer_segments.taxi_need_segment AS taxi_need_segment,
        customer_segments.taxi_retention_segment AS taxi_retention_segment,
        customer_segments.multi_city_segment AS multi_city_segment,
        customer_segments.cross_sell_segment AS cross_sell_segment,
        customer_segments.geo_usecase_segment AS geo_usecase_segment,
        customer_segments.rha_signal AS rha_signal,
        customer_segments.quick_commerce_signal AS quick_commerce_signal,
        customer_segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        customer_segments.new_repeat_signal AS new_repeat_signal,
        order_logs.city_name AS city_name,
        order_logs.yyyymmdd AS yyyymmdd,
        order_logs.epoch AS epoch,
        order_logs.service_detail_id AS service_detail_id,
        order_logs.customer_id AS customer_id,
        order_logs.order_id AS order_id,
        order_logs.customer_cancelled_epoch AS customer_cancelled_epoch,
        order_logs.order_requested_epoch AS order_requested_epoch,
        ((CAST(order_logs.customer_cancelled_epoch AS BIGINT) - CAST(order_logs.order_requested_epoch AS BIGINT))/1000) AS cobrm_ttc_secs
    FROM
        customer_segments AS customer_segments
    LEFT JOIN
        order_logs AS order_logs 
        ON customer_segments.customer_id = order_logs.customer_id
    )

    SELECT
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal,
        PERCENTILE_APPROX(segment_order_merge.cobrm_ttc_secs , 0.50) AS perc_50_cobrm_ttc_secs_segment_weekly
    FROM
        segment_order_merge AS segment_order_merge
    group by
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal


-- perc_75_cobrm_ttc_secs_segment_weekly
with customer_segments AS (
    SELECT 
        segments.customer_id AS customer_id, 
        segments.taxi_usecase_segment AS taxi_usecase_segment,
        segments.predicted_age AS predicted_age,
        segments.os AS os,
        segments.taxi_need_segment AS taxi_need_segment,
        segments.taxi_retention_segments AS taxi_retention_segment,
        CASE 
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) > 2 THEN 'TWO_PLUS'
        WHEN segments.taxi_lifetime_rr_city_list IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS multi_city_segment,
        segments.rr_last_90_days_service_tag AS cross_sell_segment,
        CASE 
        WHEN segments.geo_use_case_list IS NULL THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 2 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 4 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) > 2 THEN 'TWO_PLUS'
        ELSE 'UNKNOWN' END AS geo_usecase_segment,
        segments.rha_signal AS rha_signal,
        segments.quick_commerce_signal AS quick_commerce_signal,
        segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        CASE 
        WHEN segments.run_date = DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'FTU'
        WHEN segments.run_date > DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'RTU'
        WHEN segments.taxi_lifetime_first_rr_date IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS new_repeat_signal
      FROM 
        datasets.iallocator_customer_segments AS segments 
      WHERE 
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '1' DAY, '%Y-%m-%d')
    ),
    
    order_logs AS (
    SELECT 
        olf.yyyymmdd AS yyyymmdd,
        olf.epoch AS epoch,
        olf.service_detail_id AS service_detail_id,
        olf.customer_id AS customer_id, 
        olf.order_id AS order_id,
        olf.city_name AS city_name,
        olf.customer_cancelled_epoch AS customer_cancelled_epoch,
        olf.order_requested_epoch AS order_requested_epoch
    FROM 
        orders.order_logs_fact AS olf 
    WHERE 
        olf.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d') 
        AND olf.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
        and olf.modified_order_status = 'COBRM'
    ),
    
    segment_order_merge AS (
    SELECT
        customer_segments.taxi_usecase_segment AS taxi_usecase_segment,
        customer_segments.predicted_age AS predicted_age,
        customer_segments.os AS os,
        customer_segments.taxi_need_segment AS taxi_need_segment,
        customer_segments.taxi_retention_segment AS taxi_retention_segment,
        customer_segments.multi_city_segment AS multi_city_segment,
        customer_segments.cross_sell_segment AS cross_sell_segment,
        customer_segments.geo_usecase_segment AS geo_usecase_segment,
        customer_segments.rha_signal AS rha_signal,
        customer_segments.quick_commerce_signal AS quick_commerce_signal,
        customer_segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        customer_segments.new_repeat_signal AS new_repeat_signal,
        order_logs.city_name AS city_name,
        order_logs.yyyymmdd AS yyyymmdd,
        order_logs.epoch AS epoch,
        order_logs.service_detail_id AS service_detail_id,
        order_logs.customer_id AS customer_id,
        order_logs.order_id AS order_id,
        order_logs.customer_cancelled_epoch AS customer_cancelled_epoch,
        order_logs.order_requested_epoch AS order_requested_epoch,
        ((CAST(order_logs.customer_cancelled_epoch AS BIGINT) - CAST(order_logs.order_requested_epoch AS BIGINT))/1000) AS cobrm_ttc_secs
    FROM
        customer_segments AS customer_segments
    LEFT JOIN
        order_logs AS order_logs 
        ON customer_segments.customer_id = order_logs.customer_id
    )

    SELECT
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal,
        PERCENTILE_APPROX(segment_order_merge.cobrm_ttc_secs , 0.75) AS perc_75_cobrm_ttc_secs_segment_weekly
    FROM
        segment_order_merge AS segment_order_merge
    group by
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal


-- perc_90_cobrm_ttc_secs_segment_weekly
with customer_segments AS (
    SELECT 
        segments.customer_id AS customer_id, 
        segments.taxi_usecase_segment AS taxi_usecase_segment,
        segments.predicted_age AS predicted_age,
        segments.os AS os,
        segments.taxi_need_segment AS taxi_need_segment,
        segments.taxi_retention_segments AS taxi_retention_segment,
        CASE 
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) > 2 THEN 'TWO_PLUS'
        WHEN segments.taxi_lifetime_rr_city_list IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS multi_city_segment,
        segments.rr_last_90_days_service_tag AS cross_sell_segment,
        CASE 
        WHEN segments.geo_use_case_list IS NULL THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 2 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 4 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) > 2 THEN 'TWO_PLUS'
        ELSE 'UNKNOWN' END AS geo_usecase_segment,
        segments.rha_signal AS rha_signal,
        segments.quick_commerce_signal AS quick_commerce_signal,
        segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        CASE 
        WHEN segments.run_date = DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'FTU'
        WHEN segments.run_date > DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'RTU'
        WHEN segments.taxi_lifetime_first_rr_date IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS new_repeat_signal
      FROM 
        datasets.iallocator_customer_segments AS segments 
      WHERE 
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '1' DAY, '%Y-%m-%d')
    ),
    
    order_logs AS (
    SELECT 
        olf.yyyymmdd AS yyyymmdd,
        olf.epoch AS epoch,
        olf.service_detail_id AS service_detail_id,
        olf.customer_id AS customer_id, 
        olf.order_id AS order_id,
        olf.city_name AS city_name,
        olf.customer_cancelled_epoch AS customer_cancelled_epoch,
        olf.order_requested_epoch AS order_requested_epoch
    FROM 
        orders.order_logs_fact AS olf 
    WHERE 
        olf.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d') 
        AND olf.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
        and olf.modified_order_status = 'COBRM'
    ),
    
    segment_order_merge AS (
    SELECT
        customer_segments.taxi_usecase_segment AS taxi_usecase_segment,
        customer_segments.predicted_age AS predicted_age,
        customer_segments.os AS os,
        customer_segments.taxi_need_segment AS taxi_need_segment,
        customer_segments.taxi_retention_segment AS taxi_retention_segment,
        customer_segments.multi_city_segment AS multi_city_segment,
        customer_segments.cross_sell_segment AS cross_sell_segment,
        customer_segments.geo_usecase_segment AS geo_usecase_segment,
        customer_segments.rha_signal AS rha_signal,
        customer_segments.quick_commerce_signal AS quick_commerce_signal,
        customer_segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        customer_segments.new_repeat_signal AS new_repeat_signal,
        order_logs.city_name AS city_name,
        order_logs.yyyymmdd AS yyyymmdd,
        order_logs.epoch AS epoch,
        order_logs.service_detail_id AS service_detail_id,
        order_logs.customer_id AS customer_id,
        order_logs.order_id AS order_id,
        order_logs.customer_cancelled_epoch AS customer_cancelled_epoch,
        order_logs.order_requested_epoch AS order_requested_epoch,
        ((CAST(order_logs.customer_cancelled_epoch AS BIGINT) - CAST(order_logs.order_requested_epoch AS BIGINT))/1000) AS cobrm_ttc_secs
    FROM
        customer_segments AS customer_segments
    LEFT JOIN
        order_logs AS order_logs 
        ON customer_segments.customer_id = order_logs.customer_id
    )

    SELECT
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal,
        PERCENTILE_APPROX(segment_order_merge.cobrm_ttc_secs , 0.90) AS perc_90_cobrm_ttc_secs_segment_weekly
    FROM
        segment_order_merge AS segment_order_merge
    group by
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal


-- perc_95_cobrm_ttc_secs_segment_weekly
with customer_segments AS (
    SELECT 
        segments.customer_id AS customer_id, 
        segments.taxi_usecase_segment AS taxi_usecase_segment,
        segments.predicted_age AS predicted_age,
        segments.os AS os,
        segments.taxi_need_segment AS taxi_need_segment,
        segments.taxi_retention_segments AS taxi_retention_segment,
        CASE 
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) > 2 THEN 'TWO_PLUS'
        WHEN segments.taxi_lifetime_rr_city_list IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS multi_city_segment,
        segments.rr_last_90_days_service_tag AS cross_sell_segment,
        CASE 
        WHEN segments.geo_use_case_list IS NULL THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 2 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 4 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) > 2 THEN 'TWO_PLUS'
        ELSE 'UNKNOWN' END AS geo_usecase_segment,
        segments.rha_signal AS rha_signal,
        segments.quick_commerce_signal AS quick_commerce_signal,
        segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        CASE 
        WHEN segments.run_date = DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'FTU'
        WHEN segments.run_date > DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'RTU'
        WHEN segments.taxi_lifetime_first_rr_date IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS new_repeat_signal
      FROM 
        datasets.iallocator_customer_segments AS segments 
      WHERE 
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '1' DAY, '%Y-%m-%d')
    ),
    
    order_logs AS (
    SELECT 
        olf.yyyymmdd AS yyyymmdd,
        olf.epoch AS epoch,
        olf.service_detail_id AS service_detail_id,
        olf.customer_id AS customer_id, 
        olf.order_id AS order_id,
        olf.city_name AS city_name,
        olf.customer_cancelled_epoch AS customer_cancelled_epoch,
        olf.order_requested_epoch AS order_requested_epoch
    FROM 
        orders.order_logs_fact AS olf 
    WHERE 
        olf.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d') 
        AND olf.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
        and olf.modified_order_status = 'COBRM'
    ),
    
    segment_order_merge AS (
    SELECT
        customer_segments.taxi_usecase_segment AS taxi_usecase_segment,
        customer_segments.predicted_age AS predicted_age,
        customer_segments.os AS os,
        customer_segments.taxi_need_segment AS taxi_need_segment,
        customer_segments.taxi_retention_segment AS taxi_retention_segment,
        customer_segments.multi_city_segment AS multi_city_segment,
        customer_segments.cross_sell_segment AS cross_sell_segment,
        customer_segments.geo_usecase_segment AS geo_usecase_segment,
        customer_segments.rha_signal AS rha_signal,
        customer_segments.quick_commerce_signal AS quick_commerce_signal,
        customer_segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        customer_segments.new_repeat_signal AS new_repeat_signal,
        order_logs.city_name AS city_name,
        order_logs.yyyymmdd AS yyyymmdd,
        order_logs.epoch AS epoch,
        order_logs.service_detail_id AS service_detail_id,
        order_logs.customer_id AS customer_id,
        order_logs.order_id AS order_id,
        order_logs.customer_cancelled_epoch AS customer_cancelled_epoch,
        order_logs.order_requested_epoch AS order_requested_epoch,
        ((CAST(order_logs.customer_cancelled_epoch AS BIGINT) - CAST(order_logs.order_requested_epoch AS BIGINT))/1000) AS cobrm_ttc_secs
    FROM
        customer_segments AS customer_segments
    LEFT JOIN
        order_logs AS order_logs 
        ON customer_segments.customer_id = order_logs.customer_id
    )

    SELECT
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal,
        PERCENTILE_APPROX(segment_order_merge.cobrm_ttc_secs , 0.95) AS perc_95_cobrm_ttc_secs_segment_weekly
    FROM
        segment_order_merge AS segment_order_merge
    group by
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal



-- sum_cobra_ttc_secs_segment_weekly
with customer_segments AS (
    SELECT 
        segments.customer_id AS customer_id, 
        segments.taxi_usecase_segment AS taxi_usecase_segment,
        segments.predicted_age AS predicted_age,
        segments.os AS os,
        segments.taxi_need_segment AS taxi_need_segment,
        segments.taxi_retention_segments AS taxi_retention_segment,
        CASE 
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) > 2 THEN 'TWO_PLUS'
        WHEN segments.taxi_lifetime_rr_city_list IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS multi_city_segment,
        segments.rr_last_90_days_service_tag AS cross_sell_segment,
        CASE 
        WHEN segments.geo_use_case_list IS NULL THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 2 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 4 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) > 2 THEN 'TWO_PLUS'
        ELSE 'UNKNOWN' END AS geo_usecase_segment,
        segments.rha_signal AS rha_signal,
        segments.quick_commerce_signal AS quick_commerce_signal,
        segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        CASE 
        WHEN segments.run_date = DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'FTU'
        WHEN segments.run_date > DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'RTU'
        WHEN segments.taxi_lifetime_first_rr_date IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS new_repeat_signal
      FROM 
        datasets.iallocator_customer_segments AS segments 
      WHERE 
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '1' DAY, '%Y-%m-%d')
    ),
    
    order_logs AS (
    SELECT 
        olf.yyyymmdd AS yyyymmdd,
        olf.epoch AS epoch,
        olf.service_detail_id AS service_detail_id,
        olf.customer_id AS customer_id, 
        olf.order_id AS order_id,
        olf.city_name AS city_name,
        olf.customer_cancelled_epoch AS customer_cancelled_epoch,
        olf.order_requested_epoch AS order_requested_epoch
    FROM 
        orders.order_logs_fact AS olf 
    WHERE 
        olf.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d') 
        AND olf.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
        and olf.modified_order_status = 'COBRA'
    ),
    
    segment_order_merge AS (
    SELECT
        customer_segments.taxi_usecase_segment AS taxi_usecase_segment,
        customer_segments.predicted_age AS predicted_age,
        customer_segments.os AS os,
        customer_segments.taxi_need_segment AS taxi_need_segment,
        customer_segments.taxi_retention_segment AS taxi_retention_segment,
        customer_segments.multi_city_segment AS multi_city_segment,
        customer_segments.cross_sell_segment AS cross_sell_segment,
        customer_segments.geo_usecase_segment AS geo_usecase_segment,
        customer_segments.rha_signal AS rha_signal,
        customer_segments.quick_commerce_signal AS quick_commerce_signal,
        customer_segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        customer_segments.new_repeat_signal AS new_repeat_signal,
        order_logs.city_name AS city_name,
        order_logs.yyyymmdd AS yyyymmdd,
        order_logs.epoch AS epoch,
        order_logs.service_detail_id AS service_detail_id,
        order_logs.customer_id AS customer_id,
        order_logs.order_id AS order_id,
        order_logs.customer_cancelled_epoch AS customer_cancelled_epoch,
        order_logs.order_requested_epoch AS order_requested_epoch,
        ((CAST(order_logs.customer_cancelled_epoch AS BIGINT) - CAST(order_logs.order_requested_epoch AS BIGINT))/1000) AS cobra_ttc_secs
    FROM
        customer_segments AS customer_segments
    LEFT JOIN
        order_logs AS order_logs 
        ON customer_segments.customer_id = order_logs.customer_id
    )

    SELECT
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal,
        SUM(segment_order_merge.cobra_ttc_secs) AS sum_cobra_ttc_secs_segment_weekly
    FROM
        segment_order_merge AS segment_order_merge
    group by
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal


-- perc_25_cobra_ttc_secs_segment_weekly
with customer_segments AS (
    SELECT 
        segments.customer_id AS customer_id, 
        segments.taxi_usecase_segment AS taxi_usecase_segment,
        segments.predicted_age AS predicted_age,
        segments.os AS os,
        segments.taxi_need_segment AS taxi_need_segment,
        segments.taxi_retention_segments AS taxi_retention_segment,
        CASE 
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) > 2 THEN 'TWO_PLUS'
        WHEN segments.taxi_lifetime_rr_city_list IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS multi_city_segment,
        segments.rr_last_90_days_service_tag AS cross_sell_segment,
        CASE 
        WHEN segments.geo_use_case_list IS NULL THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 2 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 4 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) > 2 THEN 'TWO_PLUS'
        ELSE 'UNKNOWN' END AS geo_usecase_segment,
        segments.rha_signal AS rha_signal,
        segments.quick_commerce_signal AS quick_commerce_signal,
        segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        CASE 
        WHEN segments.run_date = DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'FTU'
        WHEN segments.run_date > DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'RTU'
        WHEN segments.taxi_lifetime_first_rr_date IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS new_repeat_signal
      FROM 
        datasets.iallocator_customer_segments AS segments 
      WHERE 
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '1' DAY, '%Y-%m-%d')
    ),
    
    order_logs AS (
    SELECT 
        olf.yyyymmdd AS yyyymmdd,
        olf.epoch AS epoch,
        olf.service_detail_id AS service_detail_id,
        olf.customer_id AS customer_id, 
        olf.order_id AS order_id,
        olf.city_name AS city_name,
        olf.customer_cancelled_epoch AS customer_cancelled_epoch,
        olf.order_requested_epoch AS order_requested_epoch
    FROM 
        orders.order_logs_fact AS olf 
    WHERE 
        olf.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d') 
        AND olf.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
        and olf.modified_order_status = 'COBRA'
    ),
    
    segment_order_merge AS (
    SELECT
        customer_segments.taxi_usecase_segment AS taxi_usecase_segment,
        customer_segments.predicted_age AS predicted_age,
        customer_segments.os AS os,
        customer_segments.taxi_need_segment AS taxi_need_segment,
        customer_segments.taxi_retention_segment AS taxi_retention_segment,
        customer_segments.multi_city_segment AS multi_city_segment,
        customer_segments.cross_sell_segment AS cross_sell_segment,
        customer_segments.geo_usecase_segment AS geo_usecase_segment,
        customer_segments.rha_signal AS rha_signal,
        customer_segments.quick_commerce_signal AS quick_commerce_signal,
        customer_segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        customer_segments.new_repeat_signal AS new_repeat_signal,
        order_logs.city_name AS city_name,
        order_logs.yyyymmdd AS yyyymmdd,
        order_logs.epoch AS epoch,
        order_logs.service_detail_id AS service_detail_id,
        order_logs.customer_id AS customer_id,
        order_logs.order_id AS order_id,
        order_logs.customer_cancelled_epoch AS customer_cancelled_epoch,
        order_logs.order_requested_epoch AS order_requested_epoch,
        ((CAST(order_logs.customer_cancelled_epoch AS BIGINT) - CAST(order_logs.order_requested_epoch AS BIGINT))/1000) AS cobra_ttc_secs
    FROM
        customer_segments AS customer_segments
    LEFT JOIN
        order_logs AS order_logs 
        ON customer_segments.customer_id = order_logs.customer_id
    )

    SELECT
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal,
        PERCENTILE_APPROX(segment_order_merge.cobra_ttc_secs , 0.25) AS perc_25_cobra_ttc_secs_segment_weekly
    FROM
        segment_order_merge AS segment_order_merge
    group by
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal


-- perc_50_cobra_ttc_secs_segment_weekly
with customer_segments AS (
    SELECT 
        segments.customer_id AS customer_id, 
        segments.taxi_usecase_segment AS taxi_usecase_segment,
        segments.predicted_age AS predicted_age,
        segments.os AS os,
        segments.taxi_need_segment AS taxi_need_segment,
        segments.taxi_retention_segments AS taxi_retention_segment,
        CASE 
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) > 2 THEN 'TWO_PLUS'
        WHEN segments.taxi_lifetime_rr_city_list IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS multi_city_segment,
        segments.rr_last_90_days_service_tag AS cross_sell_segment,
        CASE 
        WHEN segments.geo_use_case_list IS NULL THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 2 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 4 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) > 2 THEN 'TWO_PLUS'
        ELSE 'UNKNOWN' END AS geo_usecase_segment,
        segments.rha_signal AS rha_signal,
        segments.quick_commerce_signal AS quick_commerce_signal,
        segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        CASE 
        WHEN segments.run_date = DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'FTU'
        WHEN segments.run_date > DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'RTU'
        WHEN segments.taxi_lifetime_first_rr_date IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS new_repeat_signal
      FROM 
        datasets.iallocator_customer_segments AS segments 
      WHERE 
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '1' DAY, '%Y-%m-%d')
    ),
    
    order_logs AS (
    SELECT 
        olf.yyyymmdd AS yyyymmdd,
        olf.epoch AS epoch,
        olf.service_detail_id AS service_detail_id,
        olf.customer_id AS customer_id, 
        olf.order_id AS order_id,
        olf.city_name AS city_name,
        olf.customer_cancelled_epoch AS customer_cancelled_epoch,
        olf.order_requested_epoch AS order_requested_epoch
    FROM 
        orders.order_logs_fact AS olf 
    WHERE 
        olf.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d') 
        AND olf.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
        and olf.modified_order_status = 'COBRA'
    ),
    
    segment_order_merge AS (
    SELECT
        customer_segments.taxi_usecase_segment AS taxi_usecase_segment,
        customer_segments.predicted_age AS predicted_age,
        customer_segments.os AS os,
        customer_segments.taxi_need_segment AS taxi_need_segment,
        customer_segments.taxi_retention_segment AS taxi_retention_segment,
        customer_segments.multi_city_segment AS multi_city_segment,
        customer_segments.cross_sell_segment AS cross_sell_segment,
        customer_segments.geo_usecase_segment AS geo_usecase_segment,
        customer_segments.rha_signal AS rha_signal,
        customer_segments.quick_commerce_signal AS quick_commerce_signal,
        customer_segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        customer_segments.new_repeat_signal AS new_repeat_signal,
        order_logs.city_name AS city_name,
        order_logs.yyyymmdd AS yyyymmdd,
        order_logs.epoch AS epoch,
        order_logs.service_detail_id AS service_detail_id,
        order_logs.customer_id AS customer_id,
        order_logs.order_id AS order_id,
        order_logs.customer_cancelled_epoch AS customer_cancelled_epoch,
        order_logs.order_requested_epoch AS order_requested_epoch,
        ((CAST(order_logs.customer_cancelled_epoch AS BIGINT) - CAST(order_logs.order_requested_epoch AS BIGINT))/1000) AS cobra_ttc_secs
    FROM
        customer_segments AS customer_segments
    LEFT JOIN
        order_logs AS order_logs 
        ON customer_segments.customer_id = order_logs.customer_id
    )

    SELECT
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal,
        PERCENTILE_APPROX(segment_order_merge.cobra_ttc_secs , 0.50) AS perc_50_cobra_ttc_secs_segment_weekly
    FROM
        segment_order_merge AS segment_order_merge
    group by
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal


-- perc_75_cobra_ttc_secs_segment_weekly
with customer_segments AS (
    SELECT 
        segments.customer_id AS customer_id, 
        segments.taxi_usecase_segment AS taxi_usecase_segment,
        segments.predicted_age AS predicted_age,
        segments.os AS os,
        segments.taxi_need_segment AS taxi_need_segment,
        segments.taxi_retention_segments AS taxi_retention_segment,
        CASE 
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) > 2 THEN 'TWO_PLUS'
        WHEN segments.taxi_lifetime_rr_city_list IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS multi_city_segment,
        segments.rr_last_90_days_service_tag AS cross_sell_segment,
        CASE 
        WHEN segments.geo_use_case_list IS NULL THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 2 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 4 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) > 2 THEN 'TWO_PLUS'
        ELSE 'UNKNOWN' END AS geo_usecase_segment,
        segments.rha_signal AS rha_signal,
        segments.quick_commerce_signal AS quick_commerce_signal,
        segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        CASE 
        WHEN segments.run_date = DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'FTU'
        WHEN segments.run_date > DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'RTU'
        WHEN segments.taxi_lifetime_first_rr_date IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS new_repeat_signal
      FROM 
        datasets.iallocator_customer_segments AS segments 
      WHERE 
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '1' DAY, '%Y-%m-%d')
    ),
    
    order_logs AS (
    SELECT 
        olf.yyyymmdd AS yyyymmdd,
        olf.epoch AS epoch,
        olf.service_detail_id AS service_detail_id,
        olf.customer_id AS customer_id, 
        olf.order_id AS order_id,
        olf.city_name AS city_name,
        olf.customer_cancelled_epoch AS customer_cancelled_epoch,
        olf.order_requested_epoch AS order_requested_epoch
    FROM 
        orders.order_logs_fact AS olf 
    WHERE 
        olf.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d') 
        AND olf.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
        and olf.modified_order_status = 'COBRA'
    ),
    
    segment_order_merge AS (
    SELECT
        customer_segments.taxi_usecase_segment AS taxi_usecase_segment,
        customer_segments.predicted_age AS predicted_age,
        customer_segments.os AS os,
        customer_segments.taxi_need_segment AS taxi_need_segment,
        customer_segments.taxi_retention_segment AS taxi_retention_segment,
        customer_segments.multi_city_segment AS multi_city_segment,
        customer_segments.cross_sell_segment AS cross_sell_segment,
        customer_segments.geo_usecase_segment AS geo_usecase_segment,
        customer_segments.rha_signal AS rha_signal,
        customer_segments.quick_commerce_signal AS quick_commerce_signal,
        customer_segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        customer_segments.new_repeat_signal AS new_repeat_signal,
        order_logs.city_name AS city_name,
        order_logs.yyyymmdd AS yyyymmdd,
        order_logs.epoch AS epoch,
        order_logs.service_detail_id AS service_detail_id,
        order_logs.customer_id AS customer_id,
        order_logs.order_id AS order_id,
        order_logs.customer_cancelled_epoch AS customer_cancelled_epoch,
        order_logs.order_requested_epoch AS order_requested_epoch,
        ((CAST(order_logs.customer_cancelled_epoch AS BIGINT) - CAST(order_logs.order_requested_epoch AS BIGINT))/1000) AS cobra_ttc_secs
    FROM
        customer_segments AS customer_segments
    LEFT JOIN
        order_logs AS order_logs 
        ON customer_segments.customer_id = order_logs.customer_id
    )

    SELECT
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal,
        PERCENTILE_APPROX(segment_order_merge.cobra_ttc_secs , 0.75) AS perc_75_cobra_ttc_secs_segment_weekly
    FROM
        segment_order_merge AS segment_order_merge
    group by
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal


-- perc_90_cobra_ttc_secs_segment_weekly
with customer_segments AS (
    SELECT 
        segments.customer_id AS customer_id, 
        segments.taxi_usecase_segment AS taxi_usecase_segment,
        segments.predicted_age AS predicted_age,
        segments.os AS os,
        segments.taxi_need_segment AS taxi_need_segment,
        segments.taxi_retention_segments AS taxi_retention_segment,
        CASE 
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) > 2 THEN 'TWO_PLUS'
        WHEN segments.taxi_lifetime_rr_city_list IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS multi_city_segment,
        segments.rr_last_90_days_service_tag AS cross_sell_segment,
        CASE 
        WHEN segments.geo_use_case_list IS NULL THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 2 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 4 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) > 2 THEN 'TWO_PLUS'
        ELSE 'UNKNOWN' END AS geo_usecase_segment,
        segments.rha_signal AS rha_signal,
        segments.quick_commerce_signal AS quick_commerce_signal,
        segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        CASE 
        WHEN segments.run_date = DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'FTU'
        WHEN segments.run_date > DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'RTU'
        WHEN segments.taxi_lifetime_first_rr_date IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS new_repeat_signal
      FROM 
        datasets.iallocator_customer_segments AS segments 
      WHERE 
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '1' DAY, '%Y-%m-%d')
    ),
    
    order_logs AS (
    SELECT 
        olf.yyyymmdd AS yyyymmdd,
        olf.epoch AS epoch,
        olf.service_detail_id AS service_detail_id,
        olf.customer_id AS customer_id, 
        olf.order_id AS order_id,
        olf.city_name AS city_name,
        olf.customer_cancelled_epoch AS customer_cancelled_epoch,
        olf.order_requested_epoch AS order_requested_epoch
    FROM 
        orders.order_logs_fact AS olf 
    WHERE 
        olf.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d') 
        AND olf.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
        and olf.modified_order_status = 'COBRA'
    ),
    
    segment_order_merge AS (
    SELECT
        customer_segments.taxi_usecase_segment AS taxi_usecase_segment,
        customer_segments.predicted_age AS predicted_age,
        customer_segments.os AS os,
        customer_segments.taxi_need_segment AS taxi_need_segment,
        customer_segments.taxi_retention_segment AS taxi_retention_segment,
        customer_segments.multi_city_segment AS multi_city_segment,
        customer_segments.cross_sell_segment AS cross_sell_segment,
        customer_segments.geo_usecase_segment AS geo_usecase_segment,
        customer_segments.rha_signal AS rha_signal,
        customer_segments.quick_commerce_signal AS quick_commerce_signal,
        customer_segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        customer_segments.new_repeat_signal AS new_repeat_signal,
        order_logs.city_name AS city_name,
        order_logs.yyyymmdd AS yyyymmdd,
        order_logs.epoch AS epoch,
        order_logs.service_detail_id AS service_detail_id,
        order_logs.customer_id AS customer_id,
        order_logs.order_id AS order_id,
        order_logs.customer_cancelled_epoch AS customer_cancelled_epoch,
        order_logs.order_requested_epoch AS order_requested_epoch,
        ((CAST(order_logs.customer_cancelled_epoch AS BIGINT) - CAST(order_logs.order_requested_epoch AS BIGINT))/1000) AS cobra_ttc_secs
    FROM
        customer_segments AS customer_segments
    LEFT JOIN
        order_logs AS order_logs 
        ON customer_segments.customer_id = order_logs.customer_id
    )

    SELECT
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal,
        PERCENTILE_APPROX(segment_order_merge.cobra_ttc_secs , 0.90) AS perc_90_cobra_ttc_secs_segment_weekly
    FROM
        segment_order_merge AS segment_order_merge
    group by
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal


-- perc_95_cobra_ttc_secs_segment_weekly
with customer_segments AS (
    SELECT 
        segments.customer_id AS customer_id, 
        segments.taxi_usecase_segment AS taxi_usecase_segment,
        segments.predicted_age AS predicted_age,
        segments.os AS os,
        segments.taxi_need_segment AS taxi_need_segment,
        segments.taxi_retention_segments AS taxi_retention_segment,
        CASE 
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) > 2 THEN 'TWO_PLUS'
        WHEN segments.taxi_lifetime_rr_city_list IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS multi_city_segment,
        segments.rr_last_90_days_service_tag AS cross_sell_segment,
        CASE 
        WHEN segments.geo_use_case_list IS NULL THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 2 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 4 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) > 2 THEN 'TWO_PLUS'
        ELSE 'UNKNOWN' END AS geo_usecase_segment,
        segments.rha_signal AS rha_signal,
        segments.quick_commerce_signal AS quick_commerce_signal,
        segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        CASE 
        WHEN segments.run_date = DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'FTU'
        WHEN segments.run_date > DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'RTU'
        WHEN segments.taxi_lifetime_first_rr_date IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS new_repeat_signal
      FROM 
        datasets.iallocator_customer_segments AS segments 
      WHERE 
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '1' DAY, '%Y-%m-%d')
    ),
    
    order_logs AS (
    SELECT 
        olf.yyyymmdd AS yyyymmdd,
        olf.epoch AS epoch,
        olf.service_detail_id AS service_detail_id,
        olf.customer_id AS customer_id, 
        olf.order_id AS order_id,
        olf.city_name AS city_name,
        olf.customer_cancelled_epoch AS customer_cancelled_epoch,
        olf.order_requested_epoch AS order_requested_epoch
    FROM 
        orders.order_logs_fact AS olf 
    WHERE 
        olf.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d') 
        AND olf.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
        and olf.modified_order_status = 'COBRA'
    ),
    
    segment_order_merge AS (
    SELECT
        customer_segments.taxi_usecase_segment AS taxi_usecase_segment,
        customer_segments.predicted_age AS predicted_age,
        customer_segments.os AS os,
        customer_segments.taxi_need_segment AS taxi_need_segment,
        customer_segments.taxi_retention_segment AS taxi_retention_segment,
        customer_segments.multi_city_segment AS multi_city_segment,
        customer_segments.cross_sell_segment AS cross_sell_segment,
        customer_segments.geo_usecase_segment AS geo_usecase_segment,
        customer_segments.rha_signal AS rha_signal,
        customer_segments.quick_commerce_signal AS quick_commerce_signal,
        customer_segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        customer_segments.new_repeat_signal AS new_repeat_signal,
        order_logs.city_name AS city_name,
        order_logs.yyyymmdd AS yyyymmdd,
        order_logs.epoch AS epoch,
        order_logs.service_detail_id AS service_detail_id,
        order_logs.customer_id AS customer_id,
        order_logs.order_id AS order_id,
        order_logs.customer_cancelled_epoch AS customer_cancelled_epoch,
        order_logs.order_requested_epoch AS order_requested_epoch,
        ((CAST(order_logs.customer_cancelled_epoch AS BIGINT) - CAST(order_logs.order_requested_epoch AS BIGINT))/1000) AS cobra_ttc_secs
    FROM
        customer_segments AS customer_segments
    LEFT JOIN
        order_logs AS order_logs 
        ON customer_segments.customer_id = order_logs.customer_id
    )

    SELECT
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal,
        PERCENTILE_APPROX(segment_order_merge.cobra_ttc_secs , 0.95) AS perc_95_cobra_ttc_secs_segment_weekly
    FROM
        segment_order_merge AS segment_order_merge
    group by
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal


-- sum_ocara_ttc_secs_segment_weekly
with customer_segments AS (
    SELECT 
        segments.customer_id AS customer_id, 
        segments.taxi_usecase_segment AS taxi_usecase_segment,
        segments.predicted_age AS predicted_age,
        segments.os AS os,
        segments.taxi_need_segment AS taxi_need_segment,
        segments.taxi_retention_segments AS taxi_retention_segment,
        CASE 
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) > 2 THEN 'TWO_PLUS'
        WHEN segments.taxi_lifetime_rr_city_list IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS multi_city_segment,
        segments.rr_last_90_days_service_tag AS cross_sell_segment,
        CASE 
        WHEN segments.geo_use_case_list IS NULL THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 2 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 4 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) > 2 THEN 'TWO_PLUS'
        ELSE 'UNKNOWN' END AS geo_usecase_segment,
        segments.rha_signal AS rha_signal,
        segments.quick_commerce_signal AS quick_commerce_signal,
        segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        CASE 
        WHEN segments.run_date = DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'FTU'
        WHEN segments.run_date > DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'RTU'
        WHEN segments.taxi_lifetime_first_rr_date IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS new_repeat_signal
      FROM 
        datasets.iallocator_customer_segments AS segments 
      WHERE 
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '1' DAY, '%Y-%m-%d')
    ),
    
    order_logs AS (
    SELECT 
        olf.yyyymmdd AS yyyymmdd,
        olf.epoch AS epoch,
        olf.service_detail_id AS service_detail_id,
        olf.customer_id AS customer_id, 
        olf.order_id AS order_id,
        olf.city_name AS city_name,
        olf.customer_cancelled_epoch AS customer_cancelled_epoch,
        olf.order_requested_epoch AS order_requested_epoch
    FROM 
        orders.order_logs_fact AS olf 
    WHERE 
        olf.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d') 
        AND olf.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
        and olf.modified_order_status = 'OCARA'
    ),
    
    segment_order_merge AS (
    SELECT
        customer_segments.taxi_usecase_segment AS taxi_usecase_segment,
        customer_segments.predicted_age AS predicted_age,
        customer_segments.os AS os,
        customer_segments.taxi_need_segment AS taxi_need_segment,
        customer_segments.taxi_retention_segment AS taxi_retention_segment,
        customer_segments.multi_city_segment AS multi_city_segment,
        customer_segments.cross_sell_segment AS cross_sell_segment,
        customer_segments.geo_usecase_segment AS geo_usecase_segment,
        customer_segments.rha_signal AS rha_signal,
        customer_segments.quick_commerce_signal AS quick_commerce_signal,
        customer_segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        customer_segments.new_repeat_signal AS new_repeat_signal,
        order_logs.city_name AS city_name,
        order_logs.yyyymmdd AS yyyymmdd,
        order_logs.epoch AS epoch,
        order_logs.service_detail_id AS service_detail_id,
        order_logs.customer_id AS customer_id,
        order_logs.order_id AS order_id,
        order_logs.customer_cancelled_epoch AS customer_cancelled_epoch,
        order_logs.order_requested_epoch AS order_requested_epoch,
        ((CAST(order_logs.customer_cancelled_epoch AS BIGINT) - CAST(order_logs.order_requested_epoch AS BIGINT))/1000) AS ocara_ttc_secs
    FROM
        customer_segments AS customer_segments
    LEFT JOIN
        order_logs AS order_logs 
        ON customer_segments.customer_id = order_logs.customer_id
    )

    SELECT
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal,
        SUM(segment_order_merge.ocara_ttc_secs) AS sum_ocara_ttc_secs_segment_weekly
    FROM
        segment_order_merge AS segment_order_merge
    group by
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal


-- perc_25_ocara_ttc_secs_segment_weekly
with customer_segments AS (
    SELECT 
        segments.customer_id AS customer_id, 
        segments.taxi_usecase_segment AS taxi_usecase_segment,
        segments.predicted_age AS predicted_age,
        segments.os AS os,
        segments.taxi_need_segment AS taxi_need_segment,
        segments.taxi_retention_segments AS taxi_retention_segment,
        CASE 
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) > 2 THEN 'TWO_PLUS'
        WHEN segments.taxi_lifetime_rr_city_list IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS multi_city_segment,
        segments.rr_last_90_days_service_tag AS cross_sell_segment,
        CASE 
        WHEN segments.geo_use_case_list IS NULL THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 2 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 4 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) > 2 THEN 'TWO_PLUS'
        ELSE 'UNKNOWN' END AS geo_usecase_segment,
        segments.rha_signal AS rha_signal,
        segments.quick_commerce_signal AS quick_commerce_signal,
        segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        CASE 
        WHEN segments.run_date = DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'FTU'
        WHEN segments.run_date > DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'RTU'
        WHEN segments.taxi_lifetime_first_rr_date IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS new_repeat_signal
      FROM 
        datasets.iallocator_customer_segments AS segments 
      WHERE 
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '1' DAY, '%Y-%m-%d')
    ),
    
    order_logs AS (
    SELECT 
        olf.yyyymmdd AS yyyymmdd,
        olf.epoch AS epoch,
        olf.service_detail_id AS service_detail_id,
        olf.customer_id AS customer_id, 
        olf.order_id AS order_id,
        olf.city_name AS city_name,
        olf.customer_cancelled_epoch AS customer_cancelled_epoch,
        olf.order_requested_epoch AS order_requested_epoch
    FROM 
        orders.order_logs_fact AS olf 
    WHERE 
        olf.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d') 
        AND olf.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
        and olf.modified_order_status = 'OCARA'
    ),
    
    segment_order_merge AS (
    SELECT
        customer_segments.taxi_usecase_segment AS taxi_usecase_segment,
        customer_segments.predicted_age AS predicted_age,
        customer_segments.os AS os,
        customer_segments.taxi_need_segment AS taxi_need_segment,
        customer_segments.taxi_retention_segment AS taxi_retention_segment,
        customer_segments.multi_city_segment AS multi_city_segment,
        customer_segments.cross_sell_segment AS cross_sell_segment,
        customer_segments.geo_usecase_segment AS geo_usecase_segment,
        customer_segments.rha_signal AS rha_signal,
        customer_segments.quick_commerce_signal AS quick_commerce_signal,
        customer_segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        customer_segments.new_repeat_signal AS new_repeat_signal,
        order_logs.city_name AS city_name,
        order_logs.yyyymmdd AS yyyymmdd,
        order_logs.epoch AS epoch,
        order_logs.service_detail_id AS service_detail_id,
        order_logs.customer_id AS customer_id,
        order_logs.order_id AS order_id,
        order_logs.customer_cancelled_epoch AS customer_cancelled_epoch,
        order_logs.order_requested_epoch AS order_requested_epoch,
        ((CAST(order_logs.customer_cancelled_epoch AS BIGINT) - CAST(order_logs.order_requested_epoch AS BIGINT))/1000) AS ocara_ttc_secs
    FROM
        customer_segments AS customer_segments
    LEFT JOIN
        order_logs AS order_logs 
        ON customer_segments.customer_id = order_logs.customer_id
    )

    SELECT
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal,
        PERCENTILE_APPROX(segment_order_merge.ocara_ttc_secs , 0.25) AS perc_25_ocara_ttc_secs_segment_weekly
    FROM
        segment_order_merge AS segment_order_merge
    group by
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal


-- perc_50_ocara_ttc_secs_segment_weekly
with customer_segments AS (
    SELECT 
        segments.customer_id AS customer_id, 
        segments.taxi_usecase_segment AS taxi_usecase_segment,
        segments.predicted_age AS predicted_age,
        segments.os AS os,
        segments.taxi_need_segment AS taxi_need_segment,
        segments.taxi_retention_segments AS taxi_retention_segment,
        CASE 
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) > 2 THEN 'TWO_PLUS'
        WHEN segments.taxi_lifetime_rr_city_list IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS multi_city_segment,
        segments.rr_last_90_days_service_tag AS cross_sell_segment,
        CASE 
        WHEN segments.geo_use_case_list IS NULL THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 2 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 4 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) > 2 THEN 'TWO_PLUS'
        ELSE 'UNKNOWN' END AS geo_usecase_segment,
        segments.rha_signal AS rha_signal,
        segments.quick_commerce_signal AS quick_commerce_signal,
        segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        CASE 
        WHEN segments.run_date = DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'FTU'
        WHEN segments.run_date > DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'RTU'
        WHEN segments.taxi_lifetime_first_rr_date IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS new_repeat_signal
      FROM 
        datasets.iallocator_customer_segments AS segments 
      WHERE 
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '1' DAY, '%Y-%m-%d')
    ),
    
    order_logs AS (
    SELECT 
        olf.yyyymmdd AS yyyymmdd,
        olf.epoch AS epoch,
        olf.service_detail_id AS service_detail_id,
        olf.customer_id AS customer_id, 
        olf.order_id AS order_id,
        olf.city_name AS city_name,
        olf.customer_cancelled_epoch AS customer_cancelled_epoch,
        olf.order_requested_epoch AS order_requested_epoch
    FROM 
        orders.order_logs_fact AS olf 
    WHERE 
        olf.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d') 
        AND olf.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
        and olf.modified_order_status = 'OCARA'
    ),
    
    segment_order_merge AS (
    SELECT
        customer_segments.taxi_usecase_segment AS taxi_usecase_segment,
        customer_segments.predicted_age AS predicted_age,
        customer_segments.os AS os,
        customer_segments.taxi_need_segment AS taxi_need_segment,
        customer_segments.taxi_retention_segment AS taxi_retention_segment,
        customer_segments.multi_city_segment AS multi_city_segment,
        customer_segments.cross_sell_segment AS cross_sell_segment,
        customer_segments.geo_usecase_segment AS geo_usecase_segment,
        customer_segments.rha_signal AS rha_signal,
        customer_segments.quick_commerce_signal AS quick_commerce_signal,
        customer_segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        customer_segments.new_repeat_signal AS new_repeat_signal,
        order_logs.city_name AS city_name,
        order_logs.yyyymmdd AS yyyymmdd,
        order_logs.epoch AS epoch,
        order_logs.service_detail_id AS service_detail_id,
        order_logs.customer_id AS customer_id,
        order_logs.order_id AS order_id,
        order_logs.customer_cancelled_epoch AS customer_cancelled_epoch,
        order_logs.order_requested_epoch AS order_requested_epoch,
        ((CAST(order_logs.customer_cancelled_epoch AS BIGINT) - CAST(order_logs.order_requested_epoch AS BIGINT))/1000) AS ocara_ttc_secs
    FROM
        customer_segments AS customer_segments
    LEFT JOIN
        order_logs AS order_logs 
        ON customer_segments.customer_id = order_logs.customer_id
    )

    SELECT
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal,
        PERCENTILE_APPROX(segment_order_merge.ocara_ttc_secs , 0.50) AS perc_50_ocara_ttc_secs_segment_weekly
    FROM
        segment_order_merge AS segment_order_merge
    group by
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal


-- perc_75_ocara_ttc_secs_segment_weekly
with customer_segments AS (
    SELECT 
        segments.customer_id AS customer_id, 
        segments.taxi_usecase_segment AS taxi_usecase_segment,
        segments.predicted_age AS predicted_age,
        segments.os AS os,
        segments.taxi_need_segment AS taxi_need_segment,
        segments.taxi_retention_segments AS taxi_retention_segment,
        CASE 
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) > 2 THEN 'TWO_PLUS'
        WHEN segments.taxi_lifetime_rr_city_list IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS multi_city_segment,
        segments.rr_last_90_days_service_tag AS cross_sell_segment,
        CASE 
        WHEN segments.geo_use_case_list IS NULL THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 2 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 4 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) > 2 THEN 'TWO_PLUS'
        ELSE 'UNKNOWN' END AS geo_usecase_segment,
        segments.rha_signal AS rha_signal,
        segments.quick_commerce_signal AS quick_commerce_signal,
        segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        CASE 
        WHEN segments.run_date = DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'FTU'
        WHEN segments.run_date > DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'RTU'
        WHEN segments.taxi_lifetime_first_rr_date IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS new_repeat_signal
      FROM 
        datasets.iallocator_customer_segments AS segments 
      WHERE 
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '1' DAY, '%Y-%m-%d')
    ),
    
    order_logs AS (
    SELECT 
        olf.yyyymmdd AS yyyymmdd,
        olf.epoch AS epoch,
        olf.service_detail_id AS service_detail_id,
        olf.customer_id AS customer_id, 
        olf.order_id AS order_id,
        olf.city_name AS city_name,
        olf.customer_cancelled_epoch AS customer_cancelled_epoch,
        olf.order_requested_epoch AS order_requested_epoch
    FROM 
        orders.order_logs_fact AS olf 
    WHERE 
        olf.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d') 
        AND olf.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
        and olf.modified_order_status = 'OCARA'
    ),
    
    segment_order_merge AS (
    SELECT
        customer_segments.taxi_usecase_segment AS taxi_usecase_segment,
        customer_segments.predicted_age AS predicted_age,
        customer_segments.os AS os,
        customer_segments.taxi_need_segment AS taxi_need_segment,
        customer_segments.taxi_retention_segment AS taxi_retention_segment,
        customer_segments.multi_city_segment AS multi_city_segment,
        customer_segments.cross_sell_segment AS cross_sell_segment,
        customer_segments.geo_usecase_segment AS geo_usecase_segment,
        customer_segments.rha_signal AS rha_signal,
        customer_segments.quick_commerce_signal AS quick_commerce_signal,
        customer_segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        customer_segments.new_repeat_signal AS new_repeat_signal,
        order_logs.city_name AS city_name,
        order_logs.yyyymmdd AS yyyymmdd,
        order_logs.epoch AS epoch,
        order_logs.service_detail_id AS service_detail_id,
        order_logs.customer_id AS customer_id,
        order_logs.order_id AS order_id,
        order_logs.customer_cancelled_epoch AS customer_cancelled_epoch,
        order_logs.order_requested_epoch AS order_requested_epoch,
        ((CAST(order_logs.customer_cancelled_epoch AS BIGINT) - CAST(order_logs.order_requested_epoch AS BIGINT))/1000) AS ocara_ttc_secs
    FROM
        customer_segments AS customer_segments
    LEFT JOIN
        order_logs AS order_logs 
        ON customer_segments.customer_id = order_logs.customer_id
    )

    SELECT
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal,
        PERCENTILE_APPROX(segment_order_merge.ocara_ttc_secs , 0.75) AS perc_75_ocara_ttc_secs_segment_weekly
    FROM
        segment_order_merge AS segment_order_merge
    group by
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal


-- perc_90_ocara_ttc_secs_segment_weekly
with customer_segments AS (
    SELECT 
        segments.customer_id AS customer_id, 
        segments.taxi_usecase_segment AS taxi_usecase_segment,
        segments.predicted_age AS predicted_age,
        segments.os AS os,
        segments.taxi_need_segment AS taxi_need_segment,
        segments.taxi_retention_segments AS taxi_retention_segment,
        CASE 
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) > 2 THEN 'TWO_PLUS'
        WHEN segments.taxi_lifetime_rr_city_list IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS multi_city_segment,
        segments.rr_last_90_days_service_tag AS cross_sell_segment,
        CASE 
        WHEN segments.geo_use_case_list IS NULL THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 2 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 4 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) > 2 THEN 'TWO_PLUS'
        ELSE 'UNKNOWN' END AS geo_usecase_segment,
        segments.rha_signal AS rha_signal,
        segments.quick_commerce_signal AS quick_commerce_signal,
        segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        CASE 
        WHEN segments.run_date = DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'FTU'
        WHEN segments.run_date > DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'RTU'
        WHEN segments.taxi_lifetime_first_rr_date IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS new_repeat_signal
      FROM 
        datasets.iallocator_customer_segments AS segments 
      WHERE 
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '1' DAY, '%Y-%m-%d')
    ),
    
    order_logs AS (
    SELECT 
        olf.yyyymmdd AS yyyymmdd,
        olf.epoch AS epoch,
        olf.service_detail_id AS service_detail_id,
        olf.customer_id AS customer_id, 
        olf.order_id AS order_id,
        olf.city_name AS city_name,
        olf.customer_cancelled_epoch AS customer_cancelled_epoch,
        olf.order_requested_epoch AS order_requested_epoch
    FROM 
        orders.order_logs_fact AS olf 
    WHERE 
        olf.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d') 
        AND olf.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
        and olf.modified_order_status = 'OCARA'
    ),
    
    segment_order_merge AS (
    SELECT
        customer_segments.taxi_usecase_segment AS taxi_usecase_segment,
        customer_segments.predicted_age AS predicted_age,
        customer_segments.os AS os,
        customer_segments.taxi_need_segment AS taxi_need_segment,
        customer_segments.taxi_retention_segment AS taxi_retention_segment,
        customer_segments.multi_city_segment AS multi_city_segment,
        customer_segments.cross_sell_segment AS cross_sell_segment,
        customer_segments.geo_usecase_segment AS geo_usecase_segment,
        customer_segments.rha_signal AS rha_signal,
        customer_segments.quick_commerce_signal AS quick_commerce_signal,
        customer_segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        customer_segments.new_repeat_signal AS new_repeat_signal,
        order_logs.city_name AS city_name,
        order_logs.yyyymmdd AS yyyymmdd,
        order_logs.epoch AS epoch,
        order_logs.service_detail_id AS service_detail_id,
        order_logs.customer_id AS customer_id,
        order_logs.order_id AS order_id,
        order_logs.customer_cancelled_epoch AS customer_cancelled_epoch,
        order_logs.order_requested_epoch AS order_requested_epoch,
        ((CAST(order_logs.customer_cancelled_epoch AS BIGINT) - CAST(order_logs.order_requested_epoch AS BIGINT))/1000) AS ocara_ttc_secs
    FROM
        customer_segments AS customer_segments
    LEFT JOIN
        order_logs AS order_logs 
        ON customer_segments.customer_id = order_logs.customer_id
    )

    SELECT
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal,
        PERCENTILE_APPROX(segment_order_merge.ocara_ttc_secs , 0.90) AS perc_90_ocara_ttc_secs_segment_weekly
    FROM
        segment_order_merge AS segment_order_merge
    group by
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal


-- perc_95_ocara_ttc_secs_segment_weekly
with customer_segments AS (
    SELECT 
        segments.customer_id AS customer_id, 
        segments.taxi_usecase_segment AS taxi_usecase_segment,
        segments.predicted_age AS predicted_age,
        segments.os AS os,
        segments.taxi_need_segment AS taxi_need_segment,
        segments.taxi_retention_segments AS taxi_retention_segment,
        CASE 
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) > 2 THEN 'TWO_PLUS'
        WHEN segments.taxi_lifetime_rr_city_list IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS multi_city_segment,
        segments.rr_last_90_days_service_tag AS cross_sell_segment,
        CASE 
        WHEN segments.geo_use_case_list IS NULL THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 2 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 4 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) > 2 THEN 'TWO_PLUS'
        ELSE 'UNKNOWN' END AS geo_usecase_segment,
        segments.rha_signal AS rha_signal,
        segments.quick_commerce_signal AS quick_commerce_signal,
        segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        CASE 
        WHEN segments.run_date = DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'FTU'
        WHEN segments.run_date > DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'RTU'
        WHEN segments.taxi_lifetime_first_rr_date IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS new_repeat_signal
      FROM 
        datasets.iallocator_customer_segments AS segments 
      WHERE 
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '1' DAY, '%Y-%m-%d')
    ),
    
    order_logs AS (
    SELECT 
        olf.yyyymmdd AS yyyymmdd,
        olf.epoch AS epoch,
        olf.service_detail_id AS service_detail_id,
        olf.customer_id AS customer_id, 
        olf.order_id AS order_id,
        olf.city_name AS city_name,
        olf.customer_cancelled_epoch AS customer_cancelled_epoch,
        olf.order_requested_epoch AS order_requested_epoch
    FROM 
        orders.order_logs_fact AS olf 
    WHERE 
        olf.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d') 
        AND olf.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
        and olf.modified_order_status = 'OCARA'
    ),
    
    segment_order_merge AS (
    SELECT
        customer_segments.taxi_usecase_segment AS taxi_usecase_segment,
        customer_segments.predicted_age AS predicted_age,
        customer_segments.os AS os,
        customer_segments.taxi_need_segment AS taxi_need_segment,
        customer_segments.taxi_retention_segment AS taxi_retention_segment,
        customer_segments.multi_city_segment AS multi_city_segment,
        customer_segments.cross_sell_segment AS cross_sell_segment,
        customer_segments.geo_usecase_segment AS geo_usecase_segment,
        customer_segments.rha_signal AS rha_signal,
        customer_segments.quick_commerce_signal AS quick_commerce_signal,
        customer_segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        customer_segments.new_repeat_signal AS new_repeat_signal,
        order_logs.city_name AS city_name,
        order_logs.yyyymmdd AS yyyymmdd,
        order_logs.epoch AS epoch,
        order_logs.service_detail_id AS service_detail_id,
        order_logs.customer_id AS customer_id,
        order_logs.order_id AS order_id,
        order_logs.customer_cancelled_epoch AS customer_cancelled_epoch,
        order_logs.order_requested_epoch AS order_requested_epoch,
        ((CAST(order_logs.customer_cancelled_epoch AS BIGINT) - CAST(order_logs.order_requested_epoch AS BIGINT))/1000) AS ocara_ttc_secs
    FROM
        customer_segments AS customer_segments
    LEFT JOIN
        order_logs AS order_logs 
        ON customer_segments.customer_id = order_logs.customer_id
    )

    SELECT
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal,
        PERCENTILE_APPROX(segment_order_merge.ocara_ttc_secs , 0.95) AS perc_95_ocara_ttc_secs_segment_weekly
    FROM
        segment_order_merge AS segment_order_merge
    group by
        segment_order_merge.taxi_usecase_segment,
        segment_order_merge.predicted_age,
        segment_order_merge.os,
        segment_order_merge.taxi_need_segment,
        segment_order_merge.taxi_retention_segment,
        segment_order_merge.multi_city_segment,
        segment_order_merge.cross_sell_segment,
        segment_order_merge.geo_usecase_segment,
        segment_order_merge.rha_signal,
        segment_order_merge.quick_commerce_signal,
        segment_order_merge.vehicle_ownership_signal,
        segment_order_merge.new_repeat_signal


-- count_ao_customers_segment_weekly
with customer_segments AS (
    SELECT 
        segments.run_date as run_date,
        segments.taxi_lifetime_last_ride_city as taxi_lifetime_last_ride_city,
        segments.customer_id AS customer_id, 
        segments.taxi_usecase_segment AS taxi_usecase_segment,
        segments.predicted_age AS predicted_age,
        segments.os AS os,
        segments.taxi_need_segment AS taxi_need_segment,
        segments.taxi_retention_segments AS taxi_retention_segment,
        CASE 
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) > 2 THEN 'TWO_PLUS'
        WHEN segments.taxi_lifetime_rr_city_list IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS multi_city_segment,
        segments.rr_last_90_days_service_tag AS cross_sell_segment,
        CASE 
        WHEN segments.geo_use_case_list IS NULL THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 2 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 4 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) > 2 THEN 'TWO_PLUS'
        ELSE 'UNKNOWN' END AS geo_usecase_segment,
        segments.rha_signal AS rha_signal,
        segments.quick_commerce_signal AS quick_commerce_signal,
        segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        CASE 
        WHEN segments.run_date = DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'FTU'
        WHEN segments.run_date > DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'RTU'
        WHEN segments.taxi_lifetime_first_rr_date IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS new_repeat_signal
        
      FROM 
        datasets.iallocator_customer_segments AS segments 
      WHERE
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '1' DAY, '%Y-%m-%d')
    ),
    
    ao_query AS (
    SELECT 
        ao.user_id AS user_id,
        ao.current_city AS current_city,
        (ao.user_id || '-' || ao.ct_session_id) AS ao_unique_id,
        (ao.user_id || '-' || CAST(ao.event_epoch AS VARCHAR) ) AS ao_event_id,
        ao.epoch AS epoch
    FROM 
        clevertap.clevertap_customer_order_activity AS ao 
    WHERE 
        ao.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d')
        AND ao.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
        AND ao.order_activity_source = 'appOpen'
    ),
    
    segment_ao_merge as (
    select
        customer_segments.run_date as run_date,
        customer_segments.taxi_usecase_segment AS taxi_usecase_segment,
        customer_segments.predicted_age AS predicted_age,
        customer_segments.os AS os,
        customer_segments.taxi_need_segment AS taxi_need_segment,
        customer_segments.taxi_retention_segment AS taxi_retention_segment,
        customer_segments.multi_city_segment AS multi_city_segment,
        customer_segments.cross_sell_segment AS cross_sell_segment,
        customer_segments.geo_usecase_segment AS geo_usecase_segment,
        customer_segments.rha_signal AS rha_signal,
        customer_segments.quick_commerce_signal AS quick_commerce_signal,
        customer_segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        customer_segments.new_repeat_signal AS new_repeat_signal,
        ao_query.user_id as user_id,
        ao_query.current_city as current_city,
        ao_query.ao_unique_id as ao_unique_id,
        ao_query.ao_event_id as ao_event_id,
        ao_query.epoch as epoch
    from
        customer_segments as customer_segments
        left join ao_query as ao_query on customer_segments.customer_id = ao_query.user_id
    )
    select
        segment_ao_merge.taxi_usecase_segment,
        segment_ao_merge.predicted_age,
        segment_ao_merge.os,
        segment_ao_merge.taxi_need_segment,
        segment_ao_merge.taxi_retention_segment,
        segment_ao_merge.multi_city_segment,
        segment_ao_merge.cross_sell_segment,
        segment_ao_merge.geo_usecase_segment,
        segment_ao_merge.rha_signal,
        segment_ao_merge.quick_commerce_signal,
        segment_ao_merge.vehicle_ownership_signal,
        segment_ao_merge.new_repeat_signal,
        count(distinct segment_ao_merge.user_id) as count_ao_customers_segment_weekly

    from
        segment_ao_merge as segment_ao_merge
    group by
        segment_ao_merge.taxi_usecase_segment,
        segment_ao_merge.predicted_age,
        segment_ao_merge.os,
        segment_ao_merge.taxi_need_segment,
        segment_ao_merge.taxi_retention_segment,
        segment_ao_merge.multi_city_segment,
        segment_ao_merge.cross_sell_segment,
        segment_ao_merge.geo_usecase_segment,
        segment_ao_merge.rha_signal,
        segment_ao_merge.quick_commerce_signal,
        segment_ao_merge.vehicle_ownership_signal,
        segment_ao_merge.new_repeat_signal


-- count_ao_events_serviceable_segment_weekly
with customer_segments AS (
    SELECT 
        segments.run_date as run_date,
        segments.taxi_lifetime_last_ride_city as taxi_lifetime_last_ride_city,
        segments.customer_id AS customer_id, 
        segments.taxi_usecase_segment AS taxi_usecase_segment,
        segments.predicted_age AS predicted_age,
        segments.os AS os,
        segments.taxi_need_segment AS taxi_need_segment,
        segments.taxi_retention_segments AS taxi_retention_segment,
        CASE 
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) > 2 THEN 'TWO_PLUS'
        WHEN segments.taxi_lifetime_rr_city_list IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS multi_city_segment,
        segments.rr_last_90_days_service_tag AS cross_sell_segment,
        CASE 
        WHEN segments.geo_use_case_list IS NULL THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 2 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 4 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) > 2 THEN 'TWO_PLUS'
        ELSE 'UNKNOWN' END AS geo_usecase_segment,
        segments.rha_signal AS rha_signal,
        segments.quick_commerce_signal AS quick_commerce_signal,
        segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        CASE 
        WHEN segments.run_date = DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'FTU'
        WHEN segments.run_date > DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'RTU'
        WHEN segments.taxi_lifetime_first_rr_date IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS new_repeat_signal
        
      FROM 
        datasets.iallocator_customer_segments AS segments 
      WHERE
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '1' DAY, '%Y-%m-%d')
    ),
    
    ao_query AS (
    SELECT 
        ao.user_id AS user_id,
        ao.current_city AS current_city,
        (ao.user_id || '-' || ao.ct_session_id) AS ao_unique_id,
        (ao.user_id || '-' || CAST(ao.event_epoch AS VARCHAR) ) AS ao_event_id,
        ao.epoch AS epoch
    FROM 
        clevertap.clevertap_customer_order_activity AS ao 
    WHERE 
        ao.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d')
        AND ao.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
        AND ao.serviceable = 'true'
        AND ao.order_activity_source = 'appOpen'
    ),
    
    segment_ao_merge as (
    select
        customer_segments.taxi_usecase_segment AS taxi_usecase_segment,
        customer_segments.predicted_age AS predicted_age,
        customer_segments.os AS os,
        customer_segments.taxi_need_segment AS taxi_need_segment,
        customer_segments.taxi_retention_segment AS taxi_retention_segment,
        customer_segments.multi_city_segment AS multi_city_segment,
        customer_segments.cross_sell_segment AS cross_sell_segment,
        customer_segments.geo_usecase_segment AS geo_usecase_segment,
        customer_segments.rha_signal AS rha_signal,
        customer_segments.quick_commerce_signal AS quick_commerce_signal,
        customer_segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        customer_segments.new_repeat_signal AS new_repeat_signal,
        ao_query.user_id as user_id,
        ao_query.current_city as current_city,
        ao_query.ao_unique_id as ao_unique_id,
        ao_query.ao_event_id as ao_event_id,
        ao_query.epoch as epoch
    from
        customer_segments as customer_segments
        left join ao_query as ao_query on customer_segments.customer_id = ao_query.user_id
    )
    select
        segment_ao_merge.taxi_usecase_segment,
        segment_ao_merge.predicted_age,
        segment_ao_merge.os,
        segment_ao_merge.taxi_need_segment,
        segment_ao_merge.taxi_retention_segment,
        segment_ao_merge.multi_city_segment,
        segment_ao_merge.cross_sell_segment,
        segment_ao_merge.geo_usecase_segment,
        segment_ao_merge.rha_signal,
        segment_ao_merge.quick_commerce_signal,
        segment_ao_merge.vehicle_ownership_signal,
        segment_ao_merge.new_repeat_signal,
        count(distinct segment_ao_merge.ao_event_id) as count_ao_events_serviceable_segment_weekly
    from
        segment_ao_merge as segment_ao_merge
    group by
        segment_ao_merge.taxi_usecase_segment,
        segment_ao_merge.predicted_age,
        segment_ao_merge.os,
        segment_ao_merge.taxi_need_segment,
        segment_ao_merge.taxi_retention_segment,
        segment_ao_merge.multi_city_segment,
        segment_ao_merge.cross_sell_segment,
        segment_ao_merge.geo_usecase_segment,
        segment_ao_merge.rha_signal,
        segment_ao_merge.quick_commerce_signal,
        segment_ao_merge.vehicle_ownership_signal,
        segment_ao_merge.new_repeat_signal


-- count_ao_sessions_serviceable_segment_weekly
with customer_segments AS (
    SELECT 
        segments.run_date as run_date,
        segments.taxi_lifetime_last_ride_city as taxi_lifetime_last_ride_city,
        segments.customer_id AS customer_id, 
        segments.taxi_usecase_segment AS taxi_usecase_segment,
        segments.predicted_age AS predicted_age,
        segments.os AS os,
        segments.taxi_need_segment AS taxi_need_segment,
        segments.taxi_retention_segments AS taxi_retention_segment,
        CASE 
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) > 2 THEN 'TWO_PLUS'
        WHEN segments.taxi_lifetime_rr_city_list IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS multi_city_segment,
        segments.rr_last_90_days_service_tag AS cross_sell_segment,
        CASE 
        WHEN segments.geo_use_case_list IS NULL THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 2 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 4 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) > 2 THEN 'TWO_PLUS'
        ELSE 'UNKNOWN' END AS geo_usecase_segment,
        segments.rha_signal AS rha_signal,
        segments.quick_commerce_signal AS quick_commerce_signal,
        segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        CASE 
        WHEN segments.run_date = DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'FTU'
        WHEN segments.run_date > DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'RTU'
        WHEN segments.taxi_lifetime_first_rr_date IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS new_repeat_signal
        
      FROM 
        datasets.iallocator_customer_segments AS segments 
      WHERE
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '1' DAY, '%Y-%m-%d')
    ),
    
    ao_query AS (
    SELECT 
        ao.user_id AS user_id,
        ao.current_city AS current_city,
        (ao.user_id || '-' || ao.ct_session_id) AS ao_unique_id,
        (ao.user_id || '-' || CAST(ao.event_epoch AS VARCHAR) ) AS ao_event_id,
        ao.epoch AS epoch
    FROM 
        clevertap.clevertap_customer_order_activity AS ao 
    WHERE 
        ao.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d')
        AND ao.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
        AND ao.serviceable = 'true'
        AND ao.order_activity_source = 'appOpen'
    ),
    
    segment_ao_merge as (
    select
        customer_segments.taxi_usecase_segment AS taxi_usecase_segment,
        customer_segments.predicted_age AS predicted_age,
        customer_segments.os AS os,
        customer_segments.taxi_need_segment AS taxi_need_segment,
        customer_segments.taxi_retention_segment AS taxi_retention_segment,
        customer_segments.multi_city_segment AS multi_city_segment,
        customer_segments.cross_sell_segment AS cross_sell_segment,
        customer_segments.geo_usecase_segment AS geo_usecase_segment,
        customer_segments.rha_signal AS rha_signal,
        customer_segments.quick_commerce_signal AS quick_commerce_signal,
        customer_segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        customer_segments.new_repeat_signal AS new_repeat_signal,
        ao_query.user_id as user_id,
        ao_query.current_city as current_city,
        ao_query.ao_unique_id as ao_unique_id,
        ao_query.ao_event_id as ao_event_id,
        ao_query.epoch as epoch
    from
        customer_segments as customer_segments
        left join ao_query as ao_query on customer_segments.customer_id = ao_query.user_id
    )
    select
        segment_ao_merge.taxi_usecase_segment,
        segment_ao_merge.predicted_age,
        segment_ao_merge.os,
        segment_ao_merge.taxi_need_segment,
        segment_ao_merge.taxi_retention_segment,
        segment_ao_merge.multi_city_segment,
        segment_ao_merge.cross_sell_segment,
        segment_ao_merge.geo_usecase_segment,
        segment_ao_merge.rha_signal,
        segment_ao_merge.quick_commerce_signal,
        segment_ao_merge.vehicle_ownership_signal,
        segment_ao_merge.new_repeat_signal,
        count(distinct segment_ao_merge.ao_unique_id) as count_ao_sessions_serviceable_segment_weekly
    from
        segment_ao_merge as segment_ao_merge
    group by
        segment_ao_merge.taxi_usecase_segment,
        segment_ao_merge.predicted_age,
        segment_ao_merge.os,
        segment_ao_merge.taxi_need_segment,
        segment_ao_merge.taxi_retention_segment,
        segment_ao_merge.multi_city_segment,
        segment_ao_merge.cross_sell_segment,
        segment_ao_merge.geo_usecase_segment,
        segment_ao_merge.rha_signal,
        segment_ao_merge.quick_commerce_signal,
        segment_ao_merge.vehicle_ownership_signal,
        segment_ao_merge.new_repeat_signal


-- count_ao_events_unserviceable_segment_weekly
with customer_segments AS (
    SELECT 
        segments.run_date as run_date,
        segments.taxi_lifetime_last_ride_city as taxi_lifetime_last_ride_city,
        segments.customer_id AS customer_id, 
        segments.taxi_usecase_segment AS taxi_usecase_segment,
        segments.predicted_age AS predicted_age,
        segments.os AS os,
        segments.taxi_need_segment AS taxi_need_segment,
        segments.taxi_retention_segments AS taxi_retention_segment,
        CASE 
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) > 2 THEN 'TWO_PLUS'
        WHEN segments.taxi_lifetime_rr_city_list IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS multi_city_segment,
        segments.rr_last_90_days_service_tag AS cross_sell_segment,
        CASE 
        WHEN segments.geo_use_case_list IS NULL THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 2 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 4 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) > 2 THEN 'TWO_PLUS'
        ELSE 'UNKNOWN' END AS geo_usecase_segment,
        segments.rha_signal AS rha_signal,
        segments.quick_commerce_signal AS quick_commerce_signal,
        segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        CASE 
        WHEN segments.run_date = DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'FTU'
        WHEN segments.run_date > DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'RTU'
        WHEN segments.taxi_lifetime_first_rr_date IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS new_repeat_signal
        
      FROM 
        datasets.iallocator_customer_segments AS segments 
      WHERE
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '1' DAY, '%Y-%m-%d')
    ),
    
    ao_query AS (
    SELECT 
        ao.user_id AS user_id,
        ao.current_city AS current_city,
        (ao.user_id || '-' || ao.ct_session_id) AS ao_unique_id,
        (ao.user_id || '-' || CAST(ao.event_epoch AS VARCHAR) ) AS ao_event_id,
        ao.epoch AS epoch
    FROM 
        clevertap.clevertap_customer_order_activity AS ao 
    WHERE 
        ao.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d')
        AND ao.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
        AND ao.serviceable != 'true'
        AND ao.order_activity_source = 'appOpen'
    ),
    
    segment_ao_merge as (
    select
        customer_segments.taxi_usecase_segment AS taxi_usecase_segment,
        customer_segments.predicted_age AS predicted_age,
        customer_segments.os AS os,
        customer_segments.taxi_need_segment AS taxi_need_segment,
        customer_segments.taxi_retention_segment AS taxi_retention_segment,
        customer_segments.multi_city_segment AS multi_city_segment,
        customer_segments.cross_sell_segment AS cross_sell_segment,
        customer_segments.geo_usecase_segment AS geo_usecase_segment,
        customer_segments.rha_signal AS rha_signal,
        customer_segments.quick_commerce_signal AS quick_commerce_signal,
        customer_segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        customer_segments.new_repeat_signal AS new_repeat_signal,
        ao_query.user_id as user_id,
        ao_query.current_city as current_city,
        ao_query.ao_unique_id as ao_unique_id,
        ao_query.ao_event_id as ao_event_id,
        ao_query.epoch as epoch
    from
        customer_segments as customer_segments
        left join ao_query as ao_query on customer_segments.customer_id = ao_query.user_id
    )
    select
        segment_ao_merge.taxi_usecase_segment,
        segment_ao_merge.predicted_age,
        segment_ao_merge.os,
        segment_ao_merge.taxi_need_segment,
        segment_ao_merge.taxi_retention_segment,
        segment_ao_merge.multi_city_segment,
        segment_ao_merge.cross_sell_segment,
        segment_ao_merge.geo_usecase_segment,
        segment_ao_merge.rha_signal,
        segment_ao_merge.quick_commerce_signal,
        segment_ao_merge.vehicle_ownership_signal,
        segment_ao_merge.new_repeat_signal,
        count(distinct segment_ao_merge.ao_event_id) as count_ao_events_unserviceable_segment_weekly
    from
        segment_ao_merge as segment_ao_merge
    group by
        segment_ao_merge.taxi_usecase_segment,
        segment_ao_merge.predicted_age,
        segment_ao_merge.os,
        segment_ao_merge.taxi_need_segment,
        segment_ao_merge.taxi_retention_segment,
        segment_ao_merge.multi_city_segment,
        segment_ao_merge.cross_sell_segment,
        segment_ao_merge.geo_usecase_segment,
        segment_ao_merge.rha_signal,
        segment_ao_merge.quick_commerce_signal,
        segment_ao_merge.vehicle_ownership_signal,
        segment_ao_merge.new_repeat_signal


-- count_ao_sessions_unserviceable_segment_weekly
with customer_segments AS (
    SELECT 
        segments.run_date as run_date,
        segments.taxi_lifetime_last_ride_city as taxi_lifetime_last_ride_city,
        segments.customer_id AS customer_id, 
        segments.taxi_usecase_segment AS taxi_usecase_segment,
        segments.predicted_age AS predicted_age,
        segments.os AS os,
        segments.taxi_need_segment AS taxi_need_segment,
        segments.taxi_retention_segments AS taxi_retention_segment,
        CASE 
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) > 2 THEN 'TWO_PLUS'
        WHEN segments.taxi_lifetime_rr_city_list IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS multi_city_segment,
        segments.rr_last_90_days_service_tag AS cross_sell_segment,
        CASE 
        WHEN segments.geo_use_case_list IS NULL THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 2 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 4 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) > 2 THEN 'TWO_PLUS'
        ELSE 'UNKNOWN' END AS geo_usecase_segment,
        segments.rha_signal AS rha_signal,
        segments.quick_commerce_signal AS quick_commerce_signal,
        segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        CASE 
        WHEN segments.run_date = DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'FTU'
        WHEN segments.run_date > DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'RTU'
        WHEN segments.taxi_lifetime_first_rr_date IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS new_repeat_signal
        
      FROM 
        datasets.iallocator_customer_segments AS segments 
      WHERE
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '1' DAY, '%Y-%m-%d')
    ),
    
    ao_query AS (
    SELECT 
        ao.user_id AS user_id,
        ao.current_city AS current_city,
        (ao.user_id || '-' || ao.ct_session_id) AS ao_unique_id,
        (ao.user_id || '-' || CAST(ao.event_epoch AS VARCHAR) ) AS ao_event_id,
        ao.epoch AS epoch
    FROM 
        clevertap.clevertap_customer_order_activity AS ao 
    WHERE 
        ao.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d')
        AND ao.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
        AND ao.serviceable != 'true'
        AND ao.order_activity_source = 'appOpen'
    ),
    
    segment_ao_merge as (
    select
        customer_segments.taxi_usecase_segment AS taxi_usecase_segment,
        customer_segments.predicted_age AS predicted_age,
        customer_segments.os AS os,
        customer_segments.taxi_need_segment AS taxi_need_segment,
        customer_segments.taxi_retention_segment AS taxi_retention_segment,
        customer_segments.multi_city_segment AS multi_city_segment,
        customer_segments.cross_sell_segment AS cross_sell_segment,
        customer_segments.geo_usecase_segment AS geo_usecase_segment,
        customer_segments.rha_signal AS rha_signal,
        customer_segments.quick_commerce_signal AS quick_commerce_signal,
        customer_segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        customer_segments.new_repeat_signal AS new_repeat_signal,
        ao_query.user_id as user_id,
        ao_query.current_city as current_city,
        ao_query.ao_unique_id as ao_unique_id,
        ao_query.ao_event_id as ao_event_id,
        ao_query.epoch as epoch
    from
        customer_segments as customer_segments
        left join ao_query as ao_query on customer_segments.customer_id = ao_query.user_id
    )
    select
        segment_ao_merge.taxi_usecase_segment,
        segment_ao_merge.predicted_age,
        segment_ao_merge.os,
        segment_ao_merge.taxi_need_segment,
        segment_ao_merge.taxi_retention_segment,
        segment_ao_merge.multi_city_segment,
        segment_ao_merge.cross_sell_segment,
        segment_ao_merge.geo_usecase_segment,
        segment_ao_merge.rha_signal,
        segment_ao_merge.quick_commerce_signal,
        segment_ao_merge.vehicle_ownership_signal,
        segment_ao_merge.new_repeat_signal,
        count(distinct segment_ao_merge.ao_unique_id) as count_ao_sessions_unserviceable_segment_weekly
    from
        segment_ao_merge as segment_ao_merge
    group by
        segment_ao_merge.taxi_usecase_segment,
        segment_ao_merge.predicted_age,
        segment_ao_merge.os,
        segment_ao_merge.taxi_need_segment,
        segment_ao_merge.taxi_retention_segment,
        segment_ao_merge.multi_city_segment,
        segment_ao_merge.cross_sell_segment,
        segment_ao_merge.geo_usecase_segment,
        segment_ao_merge.rha_signal,
        segment_ao_merge.quick_commerce_signal,
        segment_ao_merge.vehicle_ownership_signal,
        segment_ao_merge.new_repeat_signal


-- count_rr_sessions_segment_weekly
with customer_segments AS (
    SELECT 
        segments.run_date as run_date,
        segments.taxi_lifetime_last_ride_city as taxi_lifetime_last_ride_city,
        segments.customer_id AS customer_id, 
        segments.taxi_usecase_segment AS taxi_usecase_segment,
        segments.predicted_age AS predicted_age,
        segments.os AS os,
        segments.taxi_need_segment AS taxi_need_segment,
        segments.taxi_retention_segments AS taxi_retention_segment,
        CASE 
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.taxi_lifetime_rr_city_list) > 2 THEN 'TWO_PLUS'
        WHEN segments.taxi_lifetime_rr_city_list IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS multi_city_segment,
        segments.rr_last_90_days_service_tag AS cross_sell_segment,
        CASE 
        WHEN segments.geo_use_case_list IS NULL THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'UNKNOWN'
        WHEN cardinality(segments.geo_use_case_list) = 1 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 2 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'RESIDENTIAL'
        WHEN cardinality(segments.geo_use_case_list) = 1 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 2 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'ONE'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 3 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) = 4 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%residential%')) > 0 AND cardinality(filter(segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%')) > 0 THEN 'TWO'
        WHEN cardinality(segments.geo_use_case_list) > 2 THEN 'TWO_PLUS'
        ELSE 'UNKNOWN' END AS geo_usecase_segment,
        segments.rha_signal AS rha_signal,
        segments.quick_commerce_signal AS quick_commerce_signal,
        segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        CASE 
        WHEN segments.run_date = DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'FTU'
        WHEN segments.run_date > DATE_FORMAT(DATE(segments.taxi_lifetime_first_rr_date), '%Y-%m-%d') THEN 'RTU'
        WHEN segments.taxi_lifetime_first_rr_date IS NULL THEN 'UNKNOWN'
        ELSE 'UNKNOWN' END AS new_repeat_signal
        
      FROM 
        datasets.iallocator_customer_segments AS segments 
      WHERE
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '1' DAY, '%Y-%m-%d')
    ),
    
    rr_query AS (
    SELECT 
        rr_tbl.user_id as user_id,
        rr_tbl.current_city AS current_city,
        (rr_tbl.user_id || '-' || rr_tbl.ct_session_id) AS rr_unique_id,
        rr_tbl.epoch as epoch,
        rr_tbl.service_details_id as service_details_id
    FROM 
        canonical.clevertap_customer_request_rapido AS rr_tbl
    WHERE 
        rr_tbl.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d')
        AND rr_tbl.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
    ),
    
    segment_rr_merge as (
    select
        customer_segments.taxi_usecase_segment AS taxi_usecase_segment,
        customer_segments.predicted_age AS predicted_age,
        customer_segments.os AS os,
        customer_segments.taxi_need_segment AS taxi_need_segment,
        customer_segments.taxi_retention_segment AS taxi_retention_segment,
        customer_segments.multi_city_segment AS multi_city_segment,
        customer_segments.cross_sell_segment AS cross_sell_segment,
        customer_segments.geo_usecase_segment AS geo_usecase_segment,
        customer_segments.rha_signal AS rha_signal,
        customer_segments.quick_commerce_signal AS quick_commerce_signal,
        customer_segments.vehicle_ownership_signal AS vehicle_ownership_signal,
        customer_segments.new_repeat_signal AS new_repeat_signal,
        rr_query.user_id as user_id,
        rr_query.current_city as current_city,
        rr_query.rr_unique_id as rr_unique_id,
        rr_query.epoch as epoch,
        rr_query.service_details_id as service_details_id
    from
        customer_segments as customer_segments
        left join rr_query as rr_query on customer_segments.customer_id = rr_query.user_id
    )
    select
        segment_rr_merge.taxi_usecase_segment,
        segment_rr_merge.predicted_age,
        segment_rr_merge.os,
        segment_rr_merge.taxi_need_segment,
        segment_rr_merge.taxi_retention_segment,
        segment_rr_merge.multi_city_segment,
        segment_rr_merge.cross_sell_segment,
        segment_rr_merge.geo_usecase_segment,
        segment_rr_merge.rha_signal,
        segment_rr_merge.quick_commerce_signal,
        segment_rr_merge.vehicle_ownership_signal,
        segment_rr_merge.new_repeat_signal,
        count(distinct segment_rr_merge.rr_unique_id) as count_rr_sessions_segment_weekly
    from
        segment_rr_merge as segment_rr_merge
    group by
        segment_rr_merge.taxi_usecase_segment,
        segment_rr_merge.predicted_age,
        segment_rr_merge.os,
        segment_rr_merge.taxi_need_segment,
        segment_rr_merge.taxi_retention_segment,
        segment_rr_merge.multi_city_segment,
        segment_rr_merge.cross_sell_segment,
        segment_rr_merge.geo_usecase_segment,
        segment_rr_merge.rha_signal,
        segment_rr_merge.quick_commerce_signal,
        segment_rr_merge.vehicle_ownership_signal,
        segment_rr_merge.new_repeat_signal










