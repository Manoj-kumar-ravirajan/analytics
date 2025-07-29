with customer_segments as
(
    SELECT 
        segments.customer_id AS customer_id, 
        segments.taxi_usecase_segment as taxi_usecase_segment,
        segments.predicted_age as predicted_age,
        segments.os as os,
        segments.taxi_need_segment as taxi_need_segment,
        segments.taxi_ltr_segment AS taxi_ltr_segment, 
        segments.taxi_retention_segments AS taxi_retention_segment 
      FROM 
        datasets.iallocator_customer_segments AS segments 
      WHERE 
        segments.run_date = DATE_FORMAT(
          DATE_TRUNC(
            'WEEK', 
            DATE('{{StartDate}}')
          ) - INTERVAL '1' DAY, 
          '%Y-%m-%d'
        )
),
order_logs as
(
    SELECT 
        olf.yyyymmdd AS yyyymmdd,
        olf.epoch as epoch,
        olf.service_detail_id as service_detail_id,
        olf.customer_id AS customer_id, 
        olf.order_id AS order_id,
        olf.city_name as city_name
    FROM 
        orders.order_logs_fact AS olf 
    WHERE 
        olf.yyyymmdd >= DATE_FORMAT(
          '{{StartDate}}', 
          '%Y%m%d'
        ) 
        AND olf.yyyymmdd <= DATE_FORMAT(
         '{{EndDate}}',
          '%Y%m%d'
        )
        and olf.modified_order_status = 'COBRA'
),
segment_order_merge as
(
    select
        order_logs.city_name as city_name,
        order_logs.yyyymmdd as yyyymmdd,
        order_logs.epoch as epoch,
        order_logs.service_detail_id as service_detail_id,
        order_logs.customer_id as customer_id,
        order_logs.order_id as order_id,
        customer_segments.taxi_usecase_segment as taxi_usecase_segment,
        customer_segments.predicted_age as predicted_age,
        customer_segments.os as os,
        customer_segments.taxi_need_segment as taxi_need_segment,
        customer_segments.taxi_ltr_segment as taxi_ltr_segment,
        customer_segments.taxi_retention_segment as taxi_retention_segment
    from
        customer_segments as customer_segments
        left join order_logs as order_logs on customer_segments.customer_id = order_logs.customer_id
)
select
    -- segment_order_merge.city_name as city_name,
    -- segment_order_merge.service_detail_id as service_detail_id,
    segment_order_merge.taxi_usecase_segment as taxi_usecase_segment,
    segment_order_merge.predicted_age as predicted_age,
    segment_order_merge.os as os,
    segment_order_merge.taxi_need_segment as taxi_need_segment,
    segment_order_merge.taxi_ltr_segment as taxi_ltr_segment,
    segment_order_merge.taxi_retention_segment as taxi_retention_segment,
    count(distinct segment_order_merge.order_id) as count_cobra_orders_segment
from
    segment_order_merge as segment_order_merge
group by
    city_name,
    service_detail_id,
    taxi_usecase_segment,
    predicted_age,
    os,
    taxi_need_segment,
    taxi_ltr_segment,
    taxi_retention_segment