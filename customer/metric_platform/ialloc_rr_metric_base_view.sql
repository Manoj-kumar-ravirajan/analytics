-- Daily

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
        segments.run_date = DATE_FORMAT(DATE('{{StartDate}}'), '%Y-%m-%d')
    ),
    
    rr_query AS (
    SELECT 
        rr_tbl.user_id as user_id,
        rr_tbl.current_city AS current_city,
        (CAST(CAST(rr_tbl.ct_session_id AS decimal) AS varchar) || ' - ' || rr_tbl.phone) AS rr_session,
        rr_tbl.epoch as epoch,
        rr_tbl.service_details_id as service_details_id
    FROM 
        canonical.clevertap_customer_request_rapido AS rr_tbl
    WHERE 
        rr_tbl.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d')
        AND rr_tbl.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
    ),
    
    service_mapping AS (
    SELECT
        mapping.mode_name AS mode_name,
        mapping.service_category AS service_category,
        mapping.service_type AS service_type,
        mapping.service_level_l2 AS service_level_l2,
        mapping.service_level AS service_level,
        mapping.service_detail_id AS service_detail_id
    FROM 
        datasets.service_level_mapping_qc AS mapping
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
        service_mapping.mode_name AS mode_name,
        service_mapping.service_category AS service_category,
        service_mapping.service_type AS service_type,
        service_mapping.service_level_l2 AS service_level_l2,
        service_mapping.service_level AS service_level,
        rr_query.user_id as user_id,
        rr_query.current_city as current_city,
        rr_query.rr_session as rr_session,
        rr_query.epoch as epoch,
        rr_query.service_details_id as service_details_id
    from
        customer_segments as customer_segments
    left join 
        rr_query as rr_query 
            on customer_segments.customer_id = rr_query.user_id
    join
        service_mapping as service_mapping
        on rr_query.service_details_id = service_mapping.service_detail_id

    )
    select
        taxi_usecase_segment,
        predicted_age,
        os,
        taxi_need_segment,
        taxi_retention_segment,
        multi_city_segment,
        cross_sell_segment,
        geo_usecase_segment,
        rha_signal,
        quick_commerce_signal,
        vehicle_ownership_signal,
        new_repeat_signal,
        mode_name,
        service_category,
        service_type,
        service_level_l2,
        service_level,
        user_id,
        current_city,
        epoch,
        service_details_id,
        rr_session
    from
        segment_rr_merge


-- Weekly 
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
        (CAST(CAST(rr_tbl.ct_session_id AS decimal) AS varchar) || ' - ' || rr_tbl.phone) AS rr_session,
        rr_tbl.epoch as epoch,
        rr_tbl.service_details_id as service_details_id
    FROM 
        canonical.clevertap_customer_request_rapido AS rr_tbl
    WHERE 
        rr_tbl.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d')
        AND rr_tbl.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
    ),
    
    service_mapping AS (
    SELECT
        mapping.mode_name AS mode_name,
        mapping.service_category AS service_category,
        mapping.service_type AS service_type,
        mapping.service_level_l2 AS service_level_l2,
        mapping.service_level AS service_level,
        mapping.service_detail_id AS service_detail_id
    FROM 
        datasets.service_level_mapping_qc AS mapping
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
        service_mapping.mode_name AS mode_name,
        service_mapping.service_category AS service_category,
        service_mapping.service_type AS service_type,
        service_mapping.service_level_l2 AS service_level_l2,
        service_mapping.service_level AS service_level,
        rr_query.user_id as user_id,
        rr_query.current_city as current_city,
        rr_query.rr_session as rr_session,
        rr_query.epoch as epoch,
        rr_query.service_details_id as service_details_id
    from
        customer_segments as customer_segments
    left join 
        rr_query as rr_query 
            on customer_segments.customer_id = rr_query.user_id
    join
        service_mapping as service_mapping
        on rr_query.service_details_id = service_mapping.service_detail_id

    )
    select
        taxi_usecase_segment,
        predicted_age,
        os,
        taxi_need_segment,
        taxi_retention_segment,
        multi_city_segment,
        cross_sell_segment,
        geo_usecase_segment,
        rha_signal,
        quick_commerce_signal,
        vehicle_ownership_signal,
        new_repeat_signal,
        mode_name,
        service_category,
        service_type,
        service_level_l2,
        service_level,
        user_id,
        current_city,
        epoch,
        service_details_id,
        rr_session
    from
        segment_rr_merge


-- Monthly
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
        segments.run_date = DATE_FORMAT(DATE_TRUNC('MONTH', DATE('{{StartDate}}')) - INTERVAL '1' DAY, '%Y-%m-%d')
    ),
    
    rr_query AS (
    SELECT 
        rr_tbl.user_id as user_id,
        rr_tbl.current_city AS current_city,
        (CAST(CAST(rr_tbl.ct_session_id AS decimal) AS varchar) || ' - ' || rr_tbl.phone) AS rr_session,
        rr_tbl.epoch as epoch,
        rr_tbl.service_details_id as service_details_id
    FROM 
        canonical.clevertap_customer_request_rapido AS rr_tbl
    WHERE 
        rr_tbl.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d')
        AND rr_tbl.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
    ),
    
    service_mapping AS (
    SELECT
        mapping.mode_name AS mode_name,
        mapping.service_category AS service_category,
        mapping.service_type AS service_type,
        mapping.service_level_l2 AS service_level_l2,
        mapping.service_level AS service_level,
        mapping.service_detail_id AS service_detail_id
    FROM 
        datasets.service_level_mapping_qc AS mapping
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
        service_mapping.mode_name AS mode_name,
        service_mapping.service_category AS service_category,
        service_mapping.service_type AS service_type,
        service_mapping.service_level_l2 AS service_level_l2,
        service_mapping.service_level AS service_level,
        rr_query.user_id as user_id,
        rr_query.current_city as current_city,
        rr_query.rr_session as rr_session,
        rr_query.epoch as epoch,
        rr_query.service_details_id as service_details_id
    from
        customer_segments as customer_segments
    left join 
        rr_query as rr_query 
            on customer_segments.customer_id = rr_query.user_id
    join
        service_mapping as service_mapping
        on rr_query.service_details_id = service_mapping.service_detail_id

    )
    select
        taxi_usecase_segment,
        predicted_age,
        os,
        taxi_need_segment,
        taxi_retention_segment,
        multi_city_segment,
        cross_sell_segment,
        geo_usecase_segment,
        rha_signal,
        quick_commerce_signal,
        vehicle_ownership_signal,
        new_repeat_signal,
        mode_name,
        service_category,
        service_type,
        service_level_l2,
        service_level,
        user_id,
        current_city,
        epoch,
        service_details_id,
        rr_session
    from
        segment_rr_merge


