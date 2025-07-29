-- count_gross_customer_segment_service
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
        ELSE 'UNKNOWN' END AS geo_usecase_segment
      FROM 
        datasets.iallocator_customer_segments AS segments 
      WHERE
        segments.run_date = '2025-07-08'
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
        olf.yyyymmdd >= '20250709' 
        AND olf.yyyymmdd <= '20250710'
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
        COUNT(DISTINCT segment_order_merge.customer_id) AS count_gross_customer_segment_service
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
        segment_order_merge.geo_usecase_segment

-- perc_50_cobra_ttc_secs_segment
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
        ELSE 'UNKNOWN' END AS geo_usecase_segment 
      FROM 
        datasets.iallocator_customer_segments AS segments 
      WHERE 
        segments.run_date = '2025-07-08'
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
        olf.yyyymmdd >= '20250709' 
        AND olf.yyyymmdd <= '20250710'
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
        order_logs.city_name AS city_name,
        order_logs.yyyymmdd AS yyyymmdd,
        order_logs.epoch AS epoch,
        order_logs.service_detail_id AS service_detail_id,
        order_logs.customer_id AS customer_id,
        order_logs.order_id AS order_id,
        order_logs.customer_cancelled_epoch AS customer_cancelled_epoch,
        order_logs.order_requested_epoch AS order_requested_epoch,
        ((CAST(order_logs.customer_cancelled_epoch AS BIGINT) - CAST(order_logs.order_requested_epoch AS BIGINT))/1000) AS avg_cobrm_ttc_secs
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
        PERCENTILE_APPROX(segment_order_merge.avg_cobrm_ttc_secs , 0.50) AS perc_50_cobra_ttc_secs_segment
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
        segment_order_merge.geo_usecase_segment

-- sum_ocara_ttc_secs_segment
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
        ELSE 'UNKNOWN' END AS geo_usecase_segment 
      FROM 
        datasets.iallocator_customer_segments AS segments 
      WHERE 
        segments.run_date = '2025-07-08'
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
        olf.yyyymmdd >= '20250709' 
        AND olf.yyyymmdd <= '20250710'
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
        SUM(segment_order_merge.ocara_ttc_secs) AS sum_ocara_ttc_secs_segment
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
        segment_order_merge.geo_usecase_segment

-- perc_50_ao_to_fe_time_secs_segment
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
        ELSE 'UNKNOWN' END AS geo_usecase_segment 
      FROM 
        datasets.iallocator_customer_segments AS segments 
      WHERE 
        segments.run_date = '2025-07-08'
    ),

    ao_query AS (
    SELECT 
        ao.user_id AS user_id,
        ao.epoch AS epoch,
        ao.current_city AS current_city,
        (ao.user_id || '-' || ao.ct_session_id) AS ao_unique_id,
        ROW_NUMBER() OVER(PARTITION BY ao.user_id, ao.ct_session_id ORDER BY ao.epoch) AS rnk 
    FROM 
        clevertap.clevertap_customer_order_activity AS ao 
    WHERE 
        ao.yyyymmdd >= '20250709'
        AND ao.yyyymmdd <= '20250710'
    ),

    ao_query1 AS (
    SELECT 
        ao_query.user_id AS customer_id,
        ao_query.epoch AS epoch,
        ao_query.current_city AS current_city,
        ao_query.ao_unique_id AS ao_unique_id  
    FROM 
        ao_query AS ao_query 
    WHERE 
        ao_query.rnk = 1 
    ),

    fe_query AS (
    SELECT 
        fe.epoch AS fe_epoch,
        (fe.user_id || '-' || fe.ct_session_id) AS fe_unique_id,
        ROW_NUMBER() OVER(PARTITION BY fe.user_id, fe.ct_session_id ORDER BY fe.epoch) AS rnk 
    from 
        canonical.clevertap_customer_fare_estimate as fe
    where 
        fe.yyyymmdd >= '20250709'
        AND fe.yyyymmdd <= '20250710'
        AND fe.fare_estimate_id IS NOT NULL 
        AND fe.fare_estimate_id != '' 
    ),

    fe_query1 AS (
    SELECT 
        fe_query.fe_epoch AS fe_epoch,
        fe_query.fe_unique_id AS fe_unique_id
    FROM
        fe_query AS fe_query 
    WHERE 
        fe_query.rnk = 1 
    ),

    merge_df AS (
    SELECT  
        customer_segments.taxi_usecase_segment AS taxi_usecase_segment,
        customer_segments.predicted_age AS predicted_age,
        customer_segments.os AS os,
        customer_segments.taxi_need_segment AS taxi_need_segment,
        customer_segments.taxi_retention_segment AS taxi_retention_segment,
        customer_segments.multi_city_segment AS multi_city_segment,
        customer_segments.cross_sell_segment AS cross_sell_segment,
        customer_segments.geo_usecase_segment AS geo_usecase_segment,
        ao_query1.epoch AS epoch,
        ao_query1.current_city AS current_city,
        fe_query1.fe_epoch AS fe_epoch   
    FROM
        customer_segments AS customer_segments
    LEFT JOIN 
        ao_query1 AS ao_query1 
        ON customer_segments.customer_id = ao_query1.customer_id
    JOIN 
        fe_query1 AS fe_query1
        ON ao_query1.ao_unique_id = fe_query1.fe_unique_id
    ), 

    final_data AS ( 
    SELECT
        merge_df.taxi_usecase_segment AS taxi_usecase_segment,
        merge_df.predicted_age AS predicted_age,
        merge_df.os AS os,
        merge_df.taxi_need_segment AS taxi_need_segment,
        merge_df.taxi_retention_segment AS taxi_retention_segment,
        merge_df.multi_city_segment AS multi_city_segment,
        merge_df.cross_sell_segment AS cross_sell_segment,
        merge_df.geo_usecase_segment AS geo_usecase_segment,
        merge_df.epoch AS epoch,
        merge_df.current_city AS current_city,
        ((CAST(merge_df.fe_epoch AS BIGINT) - CAST(merge_df.epoch AS BIGINT))/1000) AS ao2fe_time_sec
    FROM 
        merge_df AS merge_df    
    )

    SELECT
        final_data.taxi_usecase_segment,
        final_data.predicted_age,
        final_data.os,
        final_data.taxi_need_segment,
        final_data.taxi_retention_segment,
        final_data.multi_city_segment,
        final_data.cross_sell_segment,
        final_data.geo_usecase_segment,
        PERCENTILE_APPROX(final_data.ao2fe_time_sec , 0.50) AS perc_50_ao_to_fe_time_secs_segment
    FROM
        final_data AS final_data
    WHERE 
        final_data.ao2fe_time_sec > 0
    GROUP BY
        final_data.taxi_usecase_segment,
        final_data.predicted_age,
        final_data.os,
        final_data.taxi_need_segment,
        final_data.taxi_retention_segment,
        final_data.multi_city_segment,
        final_data.cross_sell_segment,
        final_data.geo_usecase_segment



-- count_net_customer_segment_service
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
        ELSE 'UNKNOWN' END AS geo_usecase_segment  
      FROM 
        datasets.iallocator_customer_segments AS segments 
      WHERE
        segments.run_date = '2025-07-08'
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
        olf.yyyymmdd >= '20250709' 
        AND olf.yyyymmdd <= '20250710'
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
        COUNT(DISTINCT segment_order_merge.customer_id) AS count_net_customer_segment_service
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
        segment_order_merge.geo_usecase_segment

-- count_cobrm_orders_segment
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
        ELSE 'UNKNOWN' END AS geo_usecase_segment  
      FROM 
        datasets.iallocator_customer_segments AS segments 
      WHERE
        segments.run_date = '2025-07-08'
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
        olf.yyyymmdd >= '20250709' 
        AND olf.yyyymmdd <= '20250710'
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
        COUNT(DISTINCT segment_order_merge.order_id) AS count_cobrm_orders_segment
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
        segment_order_merge.geo_usecase_segment

-- count_rr_orders_segment
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
        ELSE 'UNKNOWN' END AS geo_usecase_segment  
      FROM 
        datasets.iallocator_customer_segments AS segments 
      WHERE
        segments.run_date = '2025-07-08'
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
        olf.yyyymmdd >= '20250709' 
        AND olf.yyyymmdd <= '20250710'
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
        COUNT(DISTINCT segment_order_merge.order_id) AS count_rr_orders_segment
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
        segment_order_merge.geo_usecase_segment

-- perc_50_ocara_ttc_secs_segment
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
        ELSE 'UNKNOWN' END AS geo_usecase_segment  
      FROM 
        datasets.iallocator_customer_segments AS segments 
      WHERE 
        segments.run_date = '2025-07-08'
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
        olf.yyyymmdd >= '20250709' 
        AND olf.yyyymmdd <= '20250710'
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
        PERCENTILE_APPROX(segment_order_merge.ocara_ttc_secs , 0.50) AS perc_50_ocara_ttc_secs_segment
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
        segment_order_merge.geo_usecase_segment