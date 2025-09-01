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
    
    ao_query AS (
    SELECT 
        ao.user_id AS user_id,
        ao.current_city AS current_city,
        (CAST(CAST(ao.ct_session_id AS decimal) AS varchar) || ' - ' || ao.phone) AS ao_session,
        (ao.phone || '-' || CAST(ao.event_epoch AS VARCHAR) ) AS ao_event_id,
        ao.order_activity_source AS order_activity_source,
        ao.serviceable AS serviceable,
        ao.epoch AS epoch
    FROM 
        clevertap.clevertap_customer_order_activity AS ao 
    WHERE 
        ao.yyyymmdd >= DATE_FORMAT(DATE({{StartDate}}), '%Y%m%d')
        AND ao.yyyymmdd <= DATE_FORMAT(DATE({{EndDate}}), '%Y%m%d')
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
        ao_query.ao_session as ao_session,
        ao_query.ao_event_id as ao_event_id,
        ao_query.order_activity_source as order_activity_source,
        ao_query.serviceable as serviceable,
        ao_query.epoch as epoch
    from
        customer_segments as customer_segments
        left join ao_query as ao_query on customer_segments.customer_id = ao_query.user_id
    )
    select
        run_date,
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
        user_id,
        current_city,
        ao_session,
        ao_event_id,
        order_activity_source,
        serviceable,
        epoch
    from
        segment_ao_merge


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
    
    ao_query AS (
    SELECT 
        ao.user_id AS user_id,
        ao.current_city AS current_city,
        (CAST(CAST(ao.ct_session_id AS decimal) AS varchar) || ' - ' || ao.phone) AS ao_session,
        (ao.phone || '-' || CAST(ao.event_epoch AS VARCHAR) ) AS ao_event_id,
        ao.order_activity_source AS order_activity_source,
        ao.serviceable AS serviceable,
        ao.epoch AS epoch
    FROM 
        clevertap.clevertap_customer_order_activity AS ao 
    WHERE 
        ao.yyyymmdd >= DATE_FORMAT(DATE({{StartDate}}), '%Y%m%d')
        AND ao.yyyymmdd <= DATE_FORMAT(DATE({{EndDate}}), '%Y%m%d')
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
        ao_query.ao_session as ao_session,
        ao_query.ao_event_id as ao_event_id,
        ao_query.order_activity_source as order_activity_source,
        ao_query.serviceable as serviceable,
        ao_query.epoch as epoch
    from
        customer_segments as customer_segments
        left join ao_query as ao_query on customer_segments.customer_id = ao_query.user_id
    )
    select
        run_date,
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
        user_id,
        current_city,
        ao_session,
        ao_event_id,
        order_activity_source,
        serviceable,
        epoch
    from
        segment_ao_merge


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
    
    ao_query AS (
    SELECT 
        ao.user_id AS user_id,
        ao.current_city AS current_city,
        (CAST(CAST(ao.ct_session_id AS decimal) AS varchar) || ' - ' || ao.phone) AS ao_session,
        (ao.phone || '-' || CAST(ao.event_epoch AS VARCHAR) ) AS ao_event_id,
        ao.order_activity_source AS order_activity_source,
        ao.serviceable AS serviceable,
        ao.epoch AS epoch
    FROM 
        clevertap.clevertap_customer_order_activity AS ao 
    WHERE 
        ao.yyyymmdd >= DATE_FORMAT(DATE({{StartDate}}), '%Y%m%d')
        AND ao.yyyymmdd <= DATE_FORMAT(DATE({{EndDate}}), '%Y%m%d')
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
        ao_query.ao_session as ao_session,
        ao_query.ao_event_id as ao_event_id,
        ao_query.order_activity_source as order_activity_source,
        ao_query.serviceable as serviceable,
        ao_query.epoch as epoch
    from
        customer_segments as customer_segments
        left join ao_query as ao_query on customer_segments.customer_id = ao_query.user_id
    )
    select
        run_date,
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
        user_id,
        current_city,
        ao_session,
        ao_event_id,
        order_activity_source,
        serviceable,
        epoch
    from
        segment_ao_merge
