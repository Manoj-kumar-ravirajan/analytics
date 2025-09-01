-- Daily
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
        segments.run_date = DATE_FORMAT(DATE('{{StartDate}}'), '%Y-%m-%d')
    ),
    
    order_logs AS (
    SELECT 
        olf.yyyymmdd AS yyyymmdd,
        olf.epoch AS epoch,
        olf.service_detail_id AS service_detail_id,
        olf.customer_id AS customer_id, 
        olf.order_id AS order_id,
        olf.city_name AS city_name,
        olf.modified_order_status AS modified_order_status,
        olf.spd_fraud_flag AS spd_fraud_flag,
        olf.customer_cancelled_epoch AS customer_cancelled_epoch,
        olf.order_requested_epoch AS order_requested_epoch,
        olf.accepted_epoch AS accepted_epoch,
        olf.customer_set_pickup_ride_started_distance_meters AS customer_set_pickup_ride_started_distance_meters,
        olf.amount_breakup_bid_delta_total AS amount_breakup_bid_delta_total,
        olf.bid_type AS bid_type,
        olf.bfse_tag AS bfse_tag,
        olf.bfse_bin AS bfse_bin
    FROM 
        orders.order_logs_fact AS olf 
    WHERE 
        olf.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d') 
        AND olf.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
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
        service_mapping.mode_name AS mode_name,
        service_mapping.service_category AS service_category,
        service_mapping.service_type AS service_type,
        service_mapping.service_level_l2 AS service_level_l2,
        service_mapping.service_level AS service_level,
        order_logs.city_name AS city_name,
        order_logs.yyyymmdd AS yyyymmdd,
        order_logs.epoch AS epoch,
        order_logs.service_detail_id AS service_detail_id,
        order_logs.customer_id AS customer_id,
        order_logs.order_id AS order_id,
        order_logs.modified_order_status AS modified_order_status,
        order_logs.spd_fraud_flag AS spd_fraud_flag,
        order_logs.customer_cancelled_epoch AS customer_cancelled_epoch,
        order_logs.order_requested_epoch AS order_requested_epoch,
        order_logs.accepted_epoch AS accepted_epoch,
        ((CAST(order_logs.customer_cancelled_epoch AS BIGINT) - CAST(order_logs.order_requested_epoch AS BIGINT))/1000) AS ttc_secs,
        ((CAST(order_logs.customer_cancelled_epoch AS BIGINT) - CAST(order_logs.accepted_epoch AS BIGINT))/1000) AS tta_to_ttc_secs,
        ((CAST(order_logs.accepted_epoch AS BIGINT) - CAST(order_logs.order_requested_epoch AS BIGINT))/1000) AS tta_secs,
        order_logs.customer_set_pickup_ride_started_distance_meters AS customer_set_pickup_ride_started_distance_meters,
        order_logs.amount_breakup_bid_delta_total AS amount_breakup_bid_delta_total,
        order_logs.bid_type AS bid_type,
        order_logs.bfse_tag AS bfse_tag,
        order_logs.bfse_bin AS bfse_bin
    FROM
        customer_segments AS customer_segments
    LEFT JOIN 
        order_logs AS order_logs 
        ON customer_segments.customer_id = order_logs.customer_id
    LEFT JOIN
        service_mapping AS service_mapping
        ON order_logs.service_detail_id = service_mapping.service_detail_id
    )

    SELECT
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
        city_name,
        yyyymmdd,
        epoch,
        service_detail_id,
        mode_name,
        service_category,
        service_type,
        service_level_l2,
        service_level,
        customer_id,
        order_id,
        modified_order_status,
        spd_fraud_flag,
        order_requested_epoch,
        accepted_epoch,
        customer_cancelled_epoch,
        ttc_secs,
        tta_to_ttc_secs,
        tta_secs,
        customer_set_pickup_ride_started_distance_meters,
        amount_breakup_bid_delta_total,
        bid_type,
        bfse_tag,
        bfse_bin

    FROM
        segment_order_merge


-- Weekly 
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
        olf.modified_order_status AS modified_order_status,
        olf.spd_fraud_flag AS spd_fraud_flag,
        olf.customer_cancelled_epoch AS customer_cancelled_epoch,
        olf.order_requested_epoch AS order_requested_epoch,
        olf.accepted_epoch AS accepted_epoch,
        olf.customer_set_pickup_ride_started_distance_meters AS customer_set_pickup_ride_started_distance_meters,
        olf.amount_breakup_bid_delta_total AS amount_breakup_bid_delta_total,
        olf.bid_type AS bid_type,
        olf.bfse_tag AS bfse_tag,
        olf.bfse_bin AS bfse_bin
    FROM 
        orders.order_logs_fact AS olf 
    WHERE 
        olf.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d') 
        AND olf.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
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
        service_mapping.mode_name AS mode_name,
        service_mapping.service_category AS service_category,
        service_mapping.service_type AS service_type,
        service_mapping.service_level_l2 AS service_level_l2,
        service_mapping.service_level AS service_level,
        order_logs.city_name AS city_name,
        order_logs.yyyymmdd AS yyyymmdd,
        order_logs.epoch AS epoch,
        order_logs.service_detail_id AS service_detail_id,
        order_logs.customer_id AS customer_id,
        order_logs.order_id AS order_id,
        order_logs.modified_order_status AS modified_order_status,
        order_logs.spd_fraud_flag AS spd_fraud_flag,
        order_logs.customer_cancelled_epoch AS customer_cancelled_epoch,
        order_logs.order_requested_epoch AS order_requested_epoch,
        order_logs.accepted_epoch AS accepted_epoch,
        ((CAST(order_logs.customer_cancelled_epoch AS BIGINT) - CAST(order_logs.order_requested_epoch AS BIGINT))/1000) AS ttc_secs,
        ((CAST(order_logs.customer_cancelled_epoch AS BIGINT) - CAST(order_logs.accepted_epoch AS BIGINT))/1000) AS tta_to_ttc_secs,
        ((CAST(order_logs.accepted_epoch AS BIGINT) - CAST(order_logs.order_requested_epoch AS BIGINT))/1000) AS tta_secs,
        order_logs.customer_set_pickup_ride_started_distance_meters AS customer_set_pickup_ride_started_distance_meters,
        order_logs.amount_breakup_bid_delta_total AS amount_breakup_bid_delta_total,
        order_logs.bid_type AS bid_type,
        order_logs.bfse_tag AS bfse_tag,
        order_logs.bfse_bin AS bfse_bin
    FROM
        customer_segments AS customer_segments
    LEFT JOIN 
        order_logs AS order_logs 
        ON customer_segments.customer_id = order_logs.customer_id
    LEFT JOIN
        service_mapping AS service_mapping
        ON order_logs.service_detail_id = service_mapping.service_detail_id
    )

    SELECT
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
        city_name,
        yyyymmdd,
        epoch,
        service_detail_id,
        mode_name,
        service_category,
        service_type,
        service_level_l2,
        service_level,
        customer_id,
        order_id,
        modified_order_status,
        spd_fraud_flag,
        order_requested_epoch,
        accepted_epoch,
        customer_cancelled_epoch,
        ttc_secs,
        tta_to_ttc_secs,
        tta_secs,
        customer_set_pickup_ride_started_distance_meters,
        amount_breakup_bid_delta_total,
        bid_type,
        bfse_tag,
        bfse_bin

    FROM
        segment_order_merge


-- Monthly
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
        segments.run_date = DATE_FORMAT(DATE_TRUNC('MONTH', DATE('{{StartDate}}')) - INTERVAL '1' DAY, '%Y-%m-%d')
    ),
    
    order_logs AS (
    SELECT 
        olf.yyyymmdd AS yyyymmdd,
        olf.epoch AS epoch,
        olf.service_detail_id AS service_detail_id,
        olf.customer_id AS customer_id, 
        olf.order_id AS order_id,
        olf.city_name AS city_name,
        olf.modified_order_status AS modified_order_status,
        olf.spd_fraud_flag AS spd_fraud_flag,
        olf.customer_cancelled_epoch AS customer_cancelled_epoch,
        olf.order_requested_epoch AS order_requested_epoch,
        olf.accepted_epoch AS accepted_epoch,
        olf.customer_set_pickup_ride_started_distance_meters AS customer_set_pickup_ride_started_distance_meters,
        olf.amount_breakup_bid_delta_total AS amount_breakup_bid_delta_total,
        olf.bid_type AS bid_type,
        olf.bfse_tag AS bfse_tag,
        olf.bfse_bin AS bfse_bin
    FROM 
        orders.order_logs_fact AS olf 
    WHERE 
        olf.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d') 
        AND olf.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
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
        service_mapping.mode_name AS mode_name,
        service_mapping.service_category AS service_category,
        service_mapping.service_type AS service_type,
        service_mapping.service_level_l2 AS service_level_l2,
        service_mapping.service_level AS service_level,
        order_logs.city_name AS city_name,
        order_logs.yyyymmdd AS yyyymmdd,
        order_logs.epoch AS epoch,
        order_logs.service_detail_id AS service_detail_id,
        order_logs.customer_id AS customer_id,
        order_logs.order_id AS order_id,
        order_logs.modified_order_status AS modified_order_status,
        order_logs.spd_fraud_flag AS spd_fraud_flag,
        order_logs.customer_cancelled_epoch AS customer_cancelled_epoch,
        order_logs.order_requested_epoch AS order_requested_epoch,
        order_logs.accepted_epoch AS accepted_epoch,
        ((CAST(order_logs.customer_cancelled_epoch AS BIGINT) - CAST(order_logs.order_requested_epoch AS BIGINT))/1000) AS ttc_secs,
        ((CAST(order_logs.customer_cancelled_epoch AS BIGINT) - CAST(order_logs.accepted_epoch AS BIGINT))/1000) AS tta_to_ttc_secs,
        ((CAST(order_logs.accepted_epoch AS BIGINT) - CAST(order_logs.order_requested_epoch AS BIGINT))/1000) AS tta_secs,
        order_logs.customer_set_pickup_ride_started_distance_meters AS customer_set_pickup_ride_started_distance_meters,
        order_logs.amount_breakup_bid_delta_total AS amount_breakup_bid_delta_total,
        order_logs.bid_type AS bid_type,
        order_logs.bfse_tag AS bfse_tag,
        order_logs.bfse_bin AS bfse_bin
    FROM
        customer_segments AS customer_segments
    LEFT JOIN 
        order_logs AS order_logs 
        ON customer_segments.customer_id = order_logs.customer_id
    LEFT JOIN
        service_mapping AS service_mapping
        ON order_logs.service_detail_id = service_mapping.service_detail_id
    )

    SELECT
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
        city_name,
        yyyymmdd,
        epoch,
        service_detail_id,
        mode_name,
        service_category,
        service_type,
        service_level_l2,
        service_level,
        customer_id,
        order_id,
        modified_order_status,
        spd_fraud_flag,
        order_requested_epoch,
        accepted_epoch,
        customer_cancelled_epoch,
        ttc_secs,
        tta_to_ttc_secs,
        tta_secs,
        customer_set_pickup_ride_started_distance_meters,
        amount_breakup_bid_delta_total,
        bid_type,
        bfse_tag,
        bfse_bin

    FROM
        segment_order_merge