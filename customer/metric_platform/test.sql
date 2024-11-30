with base_data as (
     SELECT   
        base_data.city_name AS city,
        base_data.order_id as order_id,
        base_data.amount AS order_amount,
        base_data.epoch as epoch,
        base_data.estimate_id as estimate_id,
        base_data.estimate_ids as estimate_ids,
        base_data.service_obj_service_name as service_level,
        base_data.channel_host AS device_type,
        base_data.fare_recalculated_reason as fare_recalculated_reason,
        explode(cast(base_data.estimate_ids as array(json))) as fare_estimate_id
    FROM orders.order_logs_fact AS base_data
    WHERE base_data.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d') 
        AND base_data.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
        AND base_data.order_status IN ('dropped')
        AND base_data.spd_fraud_flag != true
)
,  final_orders_base as (
    select 
        final_orders.city as city,
        final_orders.service_level as service_level,
        final_orders.order_id as order_id,
        final_orders.device_type as device_type,
        final_orders.order_amount as order_amount,
        final_orders.epoch as epoch,
        final_orders.fare_recalculated_reason as fare_recalculated_reason,
        final_orders.estimate_id as estimate_id,
        final_orders.estimate_ids as estimate_ids,
        cast(final_orders.fare_estimate_id as varchar) as fare_estimate_id
        from base_data as final_orders
)
, fe_tbl as (
        SELECT
            fe.city AS city,
            fe.fare_estimate_id as fare_estimate_id,
            fe.service_name as service_level,
            fe.api_context as api_context,
            cast(fe.final_amount AS double) AS fe_amount,
            cast(fe.previous_due AS double) AS prev_due,
            fe.epoch as epoch
        FROM pricing.fare_estimates_enriched as fe
    WHERE fe.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d') 
      and fe.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
)
, merged_tbl as (
     SELECT  
            o.city as city,
            o.device_type as device_type,
            o.service_level as service_level,
            o.order_id as order_id,
            o.fare_recalculated_reason as fare_recalculated_reason,
            f.api_context as api_context,
            o.fare_estimate_id as fare_estimate_id,
            f.fe_amount as fe_amount,
            o.epoch as epoch,
            o.order_amount as order_amount,
            (o.order_amount -  (f.fe_amount + f.prev_due)) AS amt_diff,
            row_number() over (partition by o.estimate_id order by f.epoch asc) as updated_seq 
        From 
            final_orders_base o
        INNER JOIN 
            fe_tbl f ON o.fare_estimate_id = f.fare_estimate_id AND o.service_level = f.service_level
)
, edited_ord as (
    select 
        DISTINCT merged_tbl.order_id as order_id 
    from 
        merged_tbl as merged_tbl 
    where merged_tbl.api_context = '/fare/editOrder'
    )
, fs_calculation as (
    select 
    merged_tbl.city as city_name,
    merged_tbl.service_level as service_level,
    merged_tbl.device_type as device_type,
    merged_tbl.fare_recalculated_reason as fare_recalculated_reason,
    case 
        when merged_tbl.amt_diff < 0 then 'Negative'
        when merged_tbl.amt_diff > 0 then 'Positive'
        else 'No Breach' 
    end as fare_shock_type,
    merged_tbl.order_id as order_id, 
    merged_tbl.epoch as epoch 
    from merged_tbl as merged_tbl 
    left join edited_ord as edited_ord on merged_tbl.order_id = edited_ord.order_id 
    where merged_tbl.updated_seq = 1
    and edited_ord.order_id  is null
    and merged_tbl.amt_diff != 0 
)
select
    fs.service_level as service_level,
    fs.device_type as device_type,
    fs.fare_shock_type as fare_shock_type,
    fs.fare_recalculated_reason as fare_recalculated_reason,
    COUNT(DISTINCT fs.order_id) as sl_fare_shock_orders -- add sl (service_name)
from 
    fs_calculation as fs 
GROUP BY 
    service_level,
    device_type,
    fare_shock_type,
    fare_recalculated_reason



----- 

WITH view AS ( 
    
    SELECT
        DATE_FORMAT(FROM_UTC_TIMESTAMP(segments.run_date,'IST'),'%Y%m%d')  AS run_date,
        DATE_FORMAT(FROM_UTC_TIMESTAMP(segments.run_date,'IST') + INTERVAL '1' DAY,'%Y%m%d') AS week_start_date,
        segments.customer_id AS customer_id, 
        segments.taxi_lifetime_last_ride_city AS taxi_lifetime_last_ride_city,
        segments.taxi_regularity_segment AS customer_taxi_rr_regularity_segments,
        segments.taxi_intent_segment AS customer_taxi_rr_intent_segments,
        segments.taxi_fe_regularity_segment AS customer_taxi_fe_regularity_segments,
        segments.taxi_fe_intent_segment AS customer_taxi_fe_intent_segments,
        segments.taxi_service_affinity AS customer_taxi_service_affinity
    FROM 
        datasets.iallocator_customer_segments AS segments
    where 
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '1' DAY, '%Y-%m-%d')
    ),
    
    olf_cte AS (
    
    SELECT 
        olf.yyyymmdd AS yyyymmdd,
        olf.service_obj_service_name AS service_name,
        olf.customer_id AS customer_id,
        olf.order_id AS order_id,
        olf.modified_order_status AS modified_order_status,
        olf.spd_fraud_flag AS spd_fraud_flag
    FROM 
        orders.order_logs_fact AS olf
    WHERE 
        olf.yyyymmdd >= DATE_FORMAT(DATE_TRUNC('WEEK', '{{StartDate}}'), '%Y%m%d')
        AND olf.yyyymmdd <= DATE_FORMAT(DATE_TRUNC('WEEK', '{{EndDate}}') + INTERVAL '6' DAY, '%Y%m%d')
    ),
    
    merge AS (
    
    SELECT
        view.run_date AS run_date,
        view.week_start_date AS week_start_date,
        view.taxi_lifetime_last_ride_city AS taxi_lifetime_last_ride_city,
        view.customer_taxi_rr_regularity_segments AS customer_taxi_rr_regularity_segments,
        view.customer_taxi_rr_intent_segments AS customer_taxi_rr_intent_segments,
        view.customer_taxi_fe_regularity_segments AS customer_taxi_fe_regularity_segments,
        view.customer_taxi_fe_intent_segments AS customer_taxi_fe_intent_segments,
        view.customer_taxi_service_affinity AS customer_taxi_service_affinity,
        olf_cte.customer_id AS customer_id,
        olf_cte.service_name AS service_name,
        olf_cte.order_id AS order_id,
        olf_cte.modified_order_status AS modified_order_status,
        olf_cte.spd_fraud_flag AS spd_fraud_flag
    FROM 
        view AS view
    LEFT JOIN 
        olf_cte AS olf_cte
        ON view.customer_id = olf_cte.customer_id
    )
    
    SELECT
        merge.week_start_date AS week_start_date,
        merge.customer_taxi_rr_regularity_segments AS customer_taxi_rr_regularity_segments,
        merge.customer_taxi_rr_intent_segments AS customer_taxi_rr_intent_segments,
        merge.customer_taxi_fe_regularity_segments AS customer_taxi_fe_regularity_segments,
        merge.customer_taxi_fe_intent_segments AS customer_taxi_fe_intent_segments,
        merge.customer_taxi_service_affinity AS customer_taxi_service_affinity,
        COUNT(DISTINCT merge.customer_id) AS rsa_segment_gross_week1_retention_customers
    FROM 
        merge AS merge
    GROUP BY 
        week_start_date,
        customer_taxi_rr_regularity_segments,
        customer_taxi_rr_intent_segments,
        customer_taxi_fe_regularity_segments,
        customer_taxi_fe_intent_segments,
        customer_taxi_service_affinity