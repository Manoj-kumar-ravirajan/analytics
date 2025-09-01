-- `daily`

WITH customer_segments AS (
  SELECT 
    segments.customer_id AS customer_id, 
    segments.taxi_usecase_segment AS taxi_usecase_segment, 
    segments.predicted_age AS predicted_age, 
    segments.os AS os, 
    segments.taxi_need_segment AS taxi_need_segment, 
    segments.taxi_retention_segments AS taxi_retention_segment, 
    CASE WHEN CARDINALITY(
      segments.taxi_lifetime_rr_city_list
    ) = 1 THEN 'ONE' WHEN CARDINALITY(
      segments.taxi_lifetime_rr_city_list
    ) = 2 THEN 'TWO' WHEN CARDINALITY(
      segments.taxi_lifetime_rr_city_list
    ) > 2 THEN 'TWO_PLUS' WHEN segments.taxi_lifetime_rr_city_list IS NULL THEN 'UNKNOWN' ELSE 'UNKNOWN' END AS multi_city_segment, 
    segments.rr_last_90_days_service_tag AS cross_sell_segment, 
    CASE WHEN segments.geo_use_case_list IS NULL THEN 'UNKNOWN' WHEN CARDINALITY(segments.geo_use_case_list) = 1 
    AND CARDINALITY(
      FILTER(
        segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%'
      )
    ) > 0 THEN 'UNKNOWN' WHEN CARDINALITY(segments.geo_use_case_list) = 1 
    AND CARDINALITY(
      FILTER(
        segments.geo_use_case_list, x -> x LIKE '%residential%'
      )
    ) > 0 THEN 'RESIDENTIAL' WHEN CARDINALITY(segments.geo_use_case_list) = 2 
    AND CARDINALITY(
      FILTER(
        segments.geo_use_case_list, x -> x LIKE '%residential%'
      )
    ) > 0 
    AND CARDINALITY(
      FILTER(
        segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%'
      )
    ) > 0 THEN 'RESIDENTIAL' WHEN CARDINALITY(segments.geo_use_case_list) = 1 THEN 'ONE' WHEN CARDINALITY(segments.geo_use_case_list) = 2 THEN 'TWO' WHEN CARDINALITY(segments.geo_use_case_list) = 3 
    AND CARDINALITY(
      FILTER(
        segments.geo_use_case_list, x -> x LIKE '%residential%'
      )
    ) > 0 
    AND CARDINALITY(
      FILTER(
        segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%'
      )
    ) > 0 THEN 'ONE' WHEN CARDINALITY(segments.geo_use_case_list) = 3 
    AND CARDINALITY(
      FILETR(
        segments.geo_use_case_list, x -> x LIKE '%residential%'
      )
    ) > 0 THEN 'TWO' WHEN CARDINALITY(segments.geo_use_case_list) = 3 
    AND CARDINALITY(
      FILTER(
        segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%'
      )
    ) > 0 THEN 'TWO' WHEN CARDINALITY(segments.geo_use_case_list) = 4 
    AND CARDINALITY(
      FILETR(
        segments.geo_use_case_list, x -> x LIKE '%residential%'
      )
    ) > 0 
    AND CARDINALITY(
      FILTER(
        segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%'
      )
    ) > 0 THEN 'TWO' WHEN CARDINALITY(segments.geo_use_case_list) > 2 THEN 'TWO_PLUS' ELSE 'UNKNOWN' END AS geo_usecase_segment, 
    segments.rha_signal AS rha_signal, 
    segments.quick_commerce_signal AS quick_commerce_signal, 
    segments.vehicle_ownership_signal AS vehicle_ownership_signal, 
    CASE WHEN segments.run_date = DATE_FORMAT(
      DATE(
        segments.taxi_lifetime_first_rr_date
      ), 
      '%Y-%m-%d'
    ) THEN 'FTU' WHEN segments.run_date > DATE_FORMAT(
      DATE(
        segments.taxi_lifetime_first_rr_date
      ), 
      '%Y-%m-%d'
    ) THEN 'RTU' WHEN segments.taxi_lifetime_first_rr_date IS NULL THEN 'UNKNOWN' ELSE 'UNKNOWN' END AS new_repeat_signal 
  FROM 
    datasets.iallocator_customer_segments AS segments 
  WHERE 
    segments.run_date = DATE_FORMAT(
      DATE('{{StartDate}}'), 
      '%Y-%m-%d'
    )
), 
service_level_mapping AS (
  SELECT 
    sdid.service_detail_id AS service_detail_id,
    sdid.service_category AS service_category,
    sdid.service_level_l2 AS service_level_l2
  FROM 
    datasets.service_level_mapping_qc AS sdid 
), 
ct_fe_tbl AS (
  SELECT 
    ct_fe.yyyymmdd AS yyyymmdd, 
    ct_fe.epoch AS epoch, 
    ct_fe.service_details_id AS service_details_id, 
    ct_fe.user_id AS customer_id, 
    (
      CAST(
        CAST(ct_fe.ct_session_id AS decimal) AS varchar
      ) || ' - ' || ct_fe.phone
    ) AS fe_session, 
    ct_fe.fare_estimate_id AS fe_id, 
    ct_fe.current_city AS current_city 
  FROM 
    canonical.clevertap_customer_fare_estimate AS ct_fe 
  WHERE 
    ct_fe.yyyymmdd >= DATE_FORMAT('{{StartDate}}', '%Y%m%d') 
    AND ct_fe.yyyymmdd <= DATE_FORMAT('{{EndDate}}', '%Y%m%d')
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
    ct_fe_tbl.fe_session AS fe_session, 
    ct_fe_tbl.fe_id AS fe_id,
    service_level_mapping.service_category AS service_category,
    service_level_mapping.service_level_l2 AS service_level_l2
  FROM 
    customer_segments AS customer_segments 
    LEFT JOIN ct_fe_tbl AS ct_fe_tbl ON customer_segments.customer_id = ct_fe_tbl.customer_id 
    JOIN service_level_mapping AS service_level_mapping ON service_level_mapping.service_detail_id = ct_fe_tbl.service_details_id
)

```
-- `weekly`
```

WITH customer_segments AS (
  SELECT 
    segments.customer_id AS customer_id, 
    segments.taxi_usecase_segment AS taxi_usecase_segment, 
    segments.predicted_age AS predicted_age, 
    segments.os AS os, 
    segments.taxi_need_segment AS taxi_need_segment, 
    segments.taxi_retention_segments AS taxi_retention_segment, 
    CASE WHEN CARDINALITY(
      segments.taxi_lifetime_rr_city_list
    ) = 1 THEN 'ONE' WHEN CARDINALITY(
      segments.taxi_lifetime_rr_city_list
    ) = 2 THEN 'TWO' WHEN CARDINALITY(
      segments.taxi_lifetime_rr_city_list
    ) > 2 THEN 'TWO_PLUS' WHEN segments.taxi_lifetime_rr_city_list IS NULL THEN 'UNKNOWN' ELSE 'UNKNOWN' END AS multi_city_segment, 
    segments.rr_last_90_days_service_tag AS cross_sell_segment, 
    CASE WHEN segments.geo_use_case_list IS NULL THEN 'UNKNOWN' WHEN CARDINALITY(segments.geo_use_case_list) = 1 
    AND CARDINALITY(
      FILTER(
        segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%'
      )
    ) > 0 THEN 'UNKNOWN' WHEN CARDINALITY(segments.geo_use_case_list) = 1 
    AND CARDINALITY(
      FILTER(
        segments.geo_use_case_list, x -> x LIKE '%residential%'
      )
    ) > 0 THEN 'RESIDENTIAL' WHEN CARDINALITY(segments.geo_use_case_list) = 2 
    AND CARDINALITY(
      FILTER(
        segments.geo_use_case_list, x -> x LIKE '%residential%'
      )
    ) > 0 
    AND CARDINALITY(
      FILTER(
        segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%'
      )
    ) > 0 THEN 'RESIDENTIAL' WHEN CARDINALITY(segments.geo_use_case_list) = 1 THEN 'ONE' WHEN CARDINALITY(segments.geo_use_case_list) = 2 THEN 'TWO' WHEN CARDINALITY(segments.geo_use_case_list) = 3 
    AND CARDINALITY(
      FILTER(
        segments.geo_use_case_list, x -> x LIKE '%residential%'
      )
    ) > 0 
    AND CARDINALITY(
      FILTER(
        segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%'
      )
    ) > 0 THEN 'ONE' WHEN CARDINALITY(segments.geo_use_case_list) = 3 
    AND CARDINALITY(
      FILETR(
        segments.geo_use_case_list, x -> x LIKE '%residential%'
      )
    ) > 0 THEN 'TWO' WHEN CARDINALITY(segments.geo_use_case_list) = 3 
    AND CARDINALITY(
      FILTER(
        segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%'
      )
    ) > 0 THEN 'TWO' WHEN CARDINALITY(segments.geo_use_case_list) = 4 
    AND CARDINALITY(
      FILETR(
        segments.geo_use_case_list, x -> x LIKE '%residential%'
      )
    ) > 0 
    AND CARDINALITY(
      FILTER(
        segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%'
      )
    ) > 0 THEN 'TWO' WHEN CARDINALITY(segments.geo_use_case_list) > 2 THEN 'TWO_PLUS' ELSE 'UNKNOWN' END AS geo_usecase_segment, 
    segments.rha_signal AS rha_signal, 
    segments.quick_commerce_signal AS quick_commerce_signal, 
    segments.vehicle_ownership_signal AS vehicle_ownership_signal, 
    CASE WHEN segments.run_date = DATE_FORMAT(
      DATE(
        segments.taxi_lifetime_first_rr_date
      ), 
      '%Y-%m-%d'
    ) THEN 'FTU' WHEN segments.run_date > DATE_FORMAT(
      DATE(
        segments.taxi_lifetime_first_rr_date
      ), 
      '%Y-%m-%d'
    ) THEN 'RTU' WHEN segments.taxi_lifetime_first_rr_date IS NULL THEN 'UNKNOWN' ELSE 'UNKNOWN' END AS new_repeat_signal 
  FROM 
    datasets.iallocator_customer_segments AS segments 
  WHERE 
    segments.run_date = DATE_FORMAT(
      DATE_TRUNC(
        'WEEK', 
        DATE('{{StartDate}}')
      )
    ), 
    service_level_mapping AS (
      SELECT 
        sdid.service_detail_id AS service_detail_id, 
        sdid.service_category AS service_category, 
        sdid.service_level_l2 AS service_level_l2 
      FROM 
        datasets.service_level_mapping_qc AS sdid
    ), 
    ct_fe_tbl AS (
      SELECT 
        ct_fe.yyyymmdd AS yyyymmdd, 
        ct_fe.epoch AS epoch, 
        ct_fe.service_details_id AS service_details_id, 
        ct_fe.user_id AS customer_id, 
        (
          CAST(
            CAST(ct_fe.ct_session_id AS decimal) AS varchar
          ) || ' - ' || ct_fe.phone
        ) AS fe_session, 
        ct_fe.fare_estimate_id AS fe_id, 
        ct_fe.current_city AS current_city 
      FROM 
        canonical.clevertap_customer_fare_estimate AS ct_fe 
      WHERE 
        ct_fe.yyyymmdd >= DATE_FORMAT('{{StartDate}}', '%Y%m%d') 
        AND ct_fe.yyyymmdd <= DATE_FORMAT('{{EndDate}}', '%Y%m%d')
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
        ct_fe_tbl.fe_session AS fe_session, 
        ct_fe_tbl.fe_id AS fe_id, 
        service_level_mapping.service_category AS service_category, 
        service_level_mapping.service_level_l2 AS service_level_l2 
      FROM 
        customer_segments AS customer_segments 
        LEFT JOIN ct_fe_tbl AS ct_fe_tbl ON customer_segments.customer_id = ct_fe_tbl.customer_id 
        JOIN service_level_mapping AS service_level_mapping ON service_level_mapping.service_detail_id = ct_fe_tbl.service_details_id
    )

```
-- `monthly`
```

WITH customer_segments AS (
  SELECT 
    segments.customer_id AS customer_id, 
    segments.taxi_usecase_segment AS taxi_usecase_segment, 
    segments.predicted_age AS predicted_age, 
    segments.os AS os, 
    segments.taxi_need_segment AS taxi_need_segment, 
    segments.taxi_retention_segments AS taxi_retention_segment, 
    CASE WHEN CARDINALITY(
      segments.taxi_lifetime_rr_city_list
    ) = 1 THEN 'ONE' WHEN CARDINALITY(
      segments.taxi_lifetime_rr_city_list
    ) = 2 THEN 'TWO' WHEN CARDINALITY(
      segments.taxi_lifetime_rr_city_list
    ) > 2 THEN 'TWO_PLUS' WHEN segments.taxi_lifetime_rr_city_list IS NULL THEN 'UNKNOWN' ELSE 'UNKNOWN' END AS multi_city_segment, 
    segments.rr_last_90_days_service_tag AS cross_sell_segment, 
    CASE WHEN segments.geo_use_case_list IS NULL THEN 'UNKNOWN' WHEN CARDINALITY(segments.geo_use_case_list) = 1 
    AND CARDINALITY(
      FILTER(
        segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%'
      )
    ) > 0 THEN 'UNKNOWN' WHEN CARDINALITY(segments.geo_use_case_list) = 1 
    AND CARDINALITY(
      FILTER(
        segments.geo_use_case_list, x -> x LIKE '%residential%'
      )
    ) > 0 THEN 'RESIDENTIAL' WHEN CARDINALITY(segments.geo_use_case_list) = 2 
    AND CARDINALITY(
      FILTER(
        segments.geo_use_case_list, x -> x LIKE '%residential%'
      )
    ) > 0 
    AND CARDINALITY(
      FILTER(
        segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%'
      )
    ) > 0 THEN 'RESIDENTIAL' WHEN CARDINALITY(segments.geo_use_case_list) = 1 THEN 'ONE' WHEN CARDINALITY(segments.geo_use_case_list) = 2 THEN 'TWO' WHEN CARDINALITY(segments.geo_use_case_list) = 3 
    AND CARDINALITY(
      FILTER(
        segments.geo_use_case_list, x -> x LIKE '%residential%'
      )
    ) > 0 
    AND CARDINALITY(
      FILTER(
        segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%'
      )
    ) > 0 THEN 'ONE' WHEN CARDINALITY(segments.geo_use_case_list) = 3 
    AND CARDINALITY(
      FILETR(
        segments.geo_use_case_list, x -> x LIKE '%residential%'
      )
    ) > 0 THEN 'TWO' WHEN CARDINALITY(segments.geo_use_case_list) = 3 
    AND CARDINALITY(
      FILTER(
        segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%'
      )
    ) > 0 THEN 'TWO' WHEN CARDINALITY(segments.geo_use_case_list) = 4 
    AND CARDINALITY(
      FILETR(
        segments.geo_use_case_list, x -> x LIKE '%residential%'
      )
    ) > 0 
    AND CARDINALITY(
      FILTER(
        segments.geo_use_case_list, x -> x LIKE '%UNKNOWN%'
      )
    ) > 0 THEN 'TWO' WHEN CARDINALITY(segments.geo_use_case_list) > 2 THEN 'TWO_PLUS' ELSE 'UNKNOWN' END AS geo_usecase_segment, 
    segments.rha_signal AS rha_signal, 
    segments.quick_commerce_signal AS quick_commerce_signal, 
    segments.vehicle_ownership_signal AS vehicle_ownership_signal, 
    CASE WHEN segments.run_date = DATE_FORMAT(
      DATE(
        segments.taxi_lifetime_first_rr_date
      ), 
      '%Y-%m-%d'
    ) THEN 'FTU' WHEN segments.run_date > DATE_FORMAT(
      DATE(
        segments.taxi_lifetime_first_rr_date
      ), 
      '%Y-%m-%d'
    ) THEN 'RTU' WHEN segments.taxi_lifetime_first_rr_date IS NULL THEN 'UNKNOWN' ELSE 'UNKNOWN' END AS new_repeat_signal 
  FROM 
    datasets.iallocator_customer_segments AS segments 
  WHERE 
    segments.run_date = DATE_FORMAT(
      DATE_TRUNC(
        'MONTH', 
        DATE('{{StartDate}}')
      )
    ), 
    service_level_mapping AS (
      SELECT 
        sdid.service_detail_id AS service_detail_id, 
        sdid.service_category AS service_category, 
        sdid.service_level_l2 AS service_level_l2 
      FROM 
        datasets.service_level_mapping_qc AS sdid
    ), 
    ct_fe_tbl AS (
      SELECT 
        ct_fe.yyyymmdd AS yyyymmdd, 
        ct_fe.epoch AS epoch, 
        ct_fe.service_details_id AS service_details_id, 
        ct_fe.user_id AS customer_id, 
        (
          CAST(
            CAST(ct_fe.ct_session_id AS decimal) AS varchar
          ) || ' - ' || ct_fe.phone
        ) AS fe_session, 
        ct_fe.fare_estimate_id AS fe_id, 
        ct_fe.current_city AS current_city 
      FROM 
        canonical.clevertap_customer_fare_estimate AS ct_fe 
      WHERE 
        ct_fe.yyyymmdd >= DATE_FORMAT('{{StartDate}}', '%Y%m%d') 
        AND ct_fe.yyyymmdd <= DATE_FORMAT('{{EndDate}}', '%Y%m%d')
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
        ct_fe_tbl.fe_session AS fe_session, 
        ct_fe_tbl.fe_id AS fe_id, 
        service_level_mapping.service_category AS service_category, 
        service_level_mapping.service_level_l2 AS service_level_l2 
      FROM 
        customer_segments AS customer_segments 
        LEFT JOIN ct_fe_tbl AS ct_fe_tbl ON customer_segments.customer_id = ct_fe_tbl.customer_id 
        JOIN service_level_mapping AS service_level_mapping ON service_level_mapping.service_detail_id = ct_fe_tbl.service_details_id
    )
```