-- Inactive base 

select 
    * 
from 
    hive.experiments_internal.customer_inactive_base_20250303_bangalore

WITH base AS (

    SELECT 
        customer_id
    FROM 
        datasets.iallocator_customer_segments
    WHERE 
        run_date = '2025-03-03'
        AND taxi_lifetime_last_ride_city = 'Bangalore'
        AND taxi_ltr_segment = 'PHH'
    ),
    
    gross_customers AS (
    
    SELECT
        DISTINCT customer_id    
    FROM 
        orders.order_logs_fact
    WHERE 
        yyyymmdd BETWEEN '20241202' AND '20250303'
        AND service_category in ('link','auto','cab')
    ),
    
    inactive_customer AS (
    
    SELECT 
        customer_id
    FROM 
        base
    WHERE 
        customer_id NOT IN (SELECT customer_id FROM gross_customers)
    ),
    
    previous_segment AS (
    
    SELECT 
        customer_id,
        taxi_regularity_segment
    FROM 
        datasets.iallocator_customer_segments
    WHERE 
        run_date = '2024-10-01'
    )
    
    SELECT
        inactive_customer.customer_id,
        COALESCE(previous_segment.taxi_regularity_segment, 'NA') regularity_segment
    FROM 
        inactive_customer
    LEFT JOIN 
        previous_segment
        on inactive_customer.customer_id = previous_segment.customer_id


-- High Time ASON
select 
    * 
from 
    hive.experiments_internal.high_time_inactive_customer_base_aug_sep_oct_nov_bangalore

WITH inactive_base AS (

    SELECT 
        customer_id,
        regularity_segment
    FROM 
        hive.experiments_internal.customer_inactive_base_20250303_bangalore
    WHERE 
        regularity_segment in ('DAILY', 'WEEKLY', 'BI_WEEKLY')
    ),
    
    eligible_customer AS (
    
    SELECT
        customer_id,
        MAX(CASE WHEN yyyymmdd BETWEEN '20240801' AND '20240930' THEN 1 END) aug_sep24,
        MAX(CASE WHEN yyyymmdd BETWEEN '20241001' AND '20241130' THEN 1 END) oct_nov24
    FROM 
        orders.order_logs_fact
    WHERE
        yyyymmdd BETWEEN '20240801' AND '20241130'
        AND service_category in ('link','auto','cab')
        AND customer_id IN (SELECT customer_id FROM inactive_base)
    GROUP BY 1
    ),
    
    resurrection_customer AS (
    
    SELECT
        customer_id,
        MAX(CASE WHEN yyyymmdd BETWEEN '20250304' AND '20250402' THEN 1 END) mar25
    FROM 
        orders.order_logs_fact
    WHERE
        yyyymmdd BETWEEN '20250304' AND '20250402'
        AND service_category in ('link','auto','cab')
        AND customer_id IN (SELECT customer_id FROM inactive_base)
    GROUP BY 1
    
    ) 
    
    
    SELECT
        *
    FROM 
        inactive_base
    LEFT JOIN 
        eligible_customer
        ON inactive_base.customer_id = eligible_customer.customer_id
    LEFT JOIN 
        resurrection_customer
        ON inactive_base.customer_id = resurrection_customer.customer_id


-- Query 1 - OLF

WITH base AS (

    SELECT
        CASE 
        WHEN yyyymmdd BETWEEN '20240801' AND '20240930' THEN 'aug_sep24'
        WHEN yyyymmdd BETWEEN '20241001' AND '20241130' THEN 'oct_nov24'
        END month, tod, quarter_hour,
        customer_id,
        order_id, lower(modified_order_status) order_status,
        service_obj_state_name state_name, city_name, service_category, service_obj_service_name service_name,
        channel_host,
        customer_set_pickup_ride_started_distance_meters haps,
        CASE WHEN bid_type IN ('positive bid', 'negative bid') THEN bid_type END bid_type,
        CASE WHEN bid_type IN ('positive bid', 'negative bid') THEN amount_breakup_bid_delta_total END  bid_amount,
        CASE WHEN fare_recalculated_diff_amount != 0 THEN fare_recalculated_reason END fare_recalc_reason, 
        CASE WHEN fare_recalculated_diff_amount != 0 THEN fare_recalculated_type END fare_recalc_type, 
        order_requested_epoch, accepted_epoch, rider_cancelled_epoch, customer_cancelled_epoch,
        (accepted_epoch/1000 - order_requested_epoch/1000) tta,
        CASE WHEN modified_order_status = 'COBRA' THEN (customer_cancelled_epoch/1000 - order_requested_epoch/1000) END cobra_ttc,
        CASE WHEN modified_order_status = 'OCARA' THEN (customer_cancelled_epoch/1000 - order_requested_epoch/1000) END ocara_ttc,
        
        CASE WHEN modified_order_status = 'COBRA' and (customer_cancelled_epoch/1000 - order_requested_epoch/1000) <= 30 THEN 'QUICK_CANCEL' END quick_cobra,
        CASE WHEN modified_order_status = 'OCARA' and (customer_cancelled_epoch/1000 - order_requested_epoch/1000) <= 30 THEN 'QUICK_CANCEL' END quick_ocara,
        
        
        pickup_location_hex_8, drop_location_hex_8, 
        pickup_location_hex_9, drop_location_hex_9,
        (pickup_location_hex_8 || ' - ' || drop_location_hex_8) hex_8_route,
        (pickup_location_hex_9 || ' - ' || drop_location_hex_9) hex_9_route,
        pickup_location_hex_12, drop_location_hex_12,
        case when customer_feedback_rating between 1 and 5 then customer_feedback_rating end customer_feedback_rating
    FROM 
        orders.order_logs_fact
    WHERE
        yyyymmdd BETWEEN '20240801' AND '20241130'
        AND service_category in ('link','auto','cab')
        AND customer_id IN (SELECT customer_id FROM hive.experiments_internal.high_time_inactive_customer_base_aug_sep_oct_nov_bangalore)
        
    ),
    
    order_support AS (
    
    SELECT
        DISTINCT 
        createdBy, 
        booking_id
    FROM 
        (
        SELECT 
            ticket_id, yyyymmdd, hhmmss, updated_hhmmss, issue_reason_selected, sub_reason918254 as sub_reason, reason, booking_id, status, service_type, service, source, 
            subject, mobile,createdBy,
            tags, 
            json_extract_scalar(data,'$.type') as type,
            json_extract_scalar(data,'$.chat_tags') as chat_tags,
            description_text,
            row_number() over(partition by ticket_id order by updated_epoch desc) as row 
        FROM 
            raw.kafka_domain_support_tickets_v2_immutable 
        WHERE 
            yyyymmdd BETWEEN '20240801' AND '20241130'
            AND (json_format(tags) like '%customer-app%' or json_format(tags) like '%customer-ticket%' or json_format(tags) like '%rapido_contactus%' or json_format(tags) like '%customer-website%'
                                                                                    or json_extract_scalar(data,'$.chat_tags') like '%action__chat%' or source = '1') 
        )
    WHERE 
        row = 1 
    )
    
    SELECT
        base.*,
        lower(coalesce(pickup_loc.primary_tag, 'NA')) pickup_usecase,
        lower(coalesce(drop_loc.primary_tag, 'NA')) drop_usecase,
        CASE WHEN order_support.booking_id IS NOT NULL THEN 1 END support_ticket
    FROM 
        base
    LEFT JOIN 
        experiments_internal.combined_geo_usecase_hex_12_level pickup_loc
        ON pickup_location_hex_12 = pickup_loc.hex_12
    LEFT JOIN 
        experiments_internal.combined_geo_usecase_hex_12_level drop_loc
        ON pickup_location_hex_12 = drop_loc.hex_12            
    LEFT JOIN 
        order_support
        ON createdBy = customer_id
        AND base.order_id = order_support.booking_id


-- Query 2 - communication

with orders as (
    
    select
        yyyymmdd,
        customer_id,
        service_obj_service_name service_name,
        order_id,
        modified_order_status
    from 
        orders.order_logs_fact
    where 
        yyyymmdd BETWEEN '20240801' AND '20241130'
        AND service_category in ('link','auto','cab')
        AND customer_id IN (SELECT customer_id FROM hive.experiments_internal.high_time_inactive_customer_base_aug_sep_oct_nov_bangalore)
    ),
    
    customer_call_captain as (
    
    select
        yyyymmdd,
        order_id,
        count(*) as customer_to_captain_call_count
    from
        clevertap.customer_callrider_immutable
    where
        yyyymmdd BETWEEN '20240801' AND '20241130'
        and order_id is not null -- why lots of order_id and captain_id is null
    group by 1,2
    ),
    
    captain_call_customer as (
    
    select
        yyyymmdd,
        event_props_order_id as order_id,
        -- profile_identity as captain_id,
        count(*) as captain_to_customer_call_count
    from
        clevertap.captain_call_customer
    where
        yyyymmdd BETWEEN '20240801' AND '20241130'
    group by 1,2
    
    ),
    
    customer_msg_captain as (
    
    select
        yyyymmdd,
        eventprops__eventprops__orderid as order_id,
        -- eventprops__eventprops__captainid as captain_id,
        count(*) as customer_to_captain_message_count
    from
        canonical.iceberg_production_data_raw_clevertap_customer_clevertap_customer_events_v2_eventname_chatcommunication_immutable
    where
        yyyymmdd BETWEEN '20240801' AND '20241130'
        and eventprops__eventprops__status = 'sent' -- received
    group by 1,2
    ),
    
    captain_msg_customer as (
    
    select
        yyyymmdd,
        eventprops__eventprops__orderid as order_id,
        -- eventprops__eventprops__captainid as captain_id,
        -- eventprops__eventprops__source, -- chatNudgeSent
        -- eventprops__eventprops__cannedmessagesent,
        -- eventprops__eventprops__messagetype,
        -- eventprops__eventprops__message,
        -- *
        count(*) as captain_to_customer_message_count
    from
        canonical.iceberg_production_data_raw_clevertap_captain_clevertap_captain_events_v2_eventname_chatcommunication_immutable
    where
        yyyymmdd BETWEEN '20240801' AND '20241130'
        and eventprops__eventprops__status = 'sent'
    group by 1,2
    
    ),
    
    merge as (
    
    select 
        orders.*,
        case when a.order_id is not null then 1 end customer_call_captain,
        case when b.order_id is not null then 1 end captain_call_customer,
        case when c.order_id is not null then 1 end customer_msg_captain,
        case when d.order_id is not null then 1 end captain_msg_customer
    from
        orders
    left join 
        customer_call_captain a
        on orders.yyyymmdd = a.yyyymmdd
        and orders.order_id = a.order_id
    left join 
        captain_call_customer b
        on orders.yyyymmdd = b.yyyymmdd
        and orders.order_id = b.order_id
    left join 
        customer_msg_captain c
        on orders.yyyymmdd = c.yyyymmdd
        and orders.order_id = c.order_id
    left join 
        captain_msg_customer d
        on orders.yyyymmdd = d.yyyymmdd
        and orders.order_id = d.order_id
    )
    
    select
        customer_id,
        order_id,
        count(distinct case when customer_call_captain > 0 then order_id end) customer_call_captain,
        count(distinct case when captain_call_customer > 0 then order_id end) captain_call_customer,
        count(distinct case when customer_msg_captain > 0 then order_id end) customer_msg_captain,
        count(distinct case when captain_msg_customer > 0 then order_id end) captain_msg_customer,
        count(distinct case when customer_call_captain > 0 or captain_call_customer > 0 then order_id end) call_communication,
        count(distinct case when customer_msg_captain > 0 or captain_msg_customer > 0 then order_id end) msg_communication,
        count(distinct case when customer_call_captain > 0 or captain_call_customer > 0 or customer_msg_captain > 0 or captain_msg_customer > 0 then order_id end) communication,
        
        count(distinct case when modified_order_status = 'OCARA' then order_id end) ocara_no,
        count(distinct case when modified_order_status = 'OCARA' and (customer_call_captain > 0 or captain_call_customer > 0 or customer_msg_captain > 0 or captain_msg_customer > 0) then order_id end) ocara_with_comm,
        count(distinct case when modified_order_status = 'OCARA' and customer_call_captain is null and captain_call_customer is null and customer_msg_captain is null and captain_msg_customer is null then order_id end) ocara_without_comm,
        
        count(distinct case when modified_order_status = 'OCARA' and (customer_call_captain > 0 or captain_call_customer > 0) then order_id end) ocara_with_call,
        count(distinct case when modified_order_status = 'OCARA' and customer_call_captain is null and captain_call_customer is null then order_id end) ocara_without_call
    
    from 
        merge
    group by 1,2


-- Query 3 - AO/FE/RR 

with ao_retry as (

    select
        CASE 
        WHEN yyyymmdd BETWEEN '20240801' AND '20240930' THEN 'aug_sep24'
        WHEN yyyymmdd BETWEEN '20241001' AND '20241130' THEN 'oct_nov24'
        END month,
        user_id as customer_id,
        count(distinct epoch) AS total_aos,
        count(distinct case when CAST(CAST(ct_session_id AS DECIMAL) AS VARCHAR) || ' - ' || phone is not null then yyyymmdd end) as total_ao_days,
        count(distinct CAST(CAST(ct_session_id AS DECIMAL) AS VARCHAR) || ' - ' || phone) total_ao_sessions
    from 
        clevertap.clevertap_customer_order_activity
    where 
        yyyymmdd BETWEEN '20240801' AND '20241130'
        and serviceable = 'true'
        and order_activity_source = 'appOpen'
        and user_id IN (SELECT customer_id FROM hive.experiments_internal.high_time_inactive_customer_base_aug_sep_oct_nov_bangalore)
    group by 1,2
    ),

    fe_retry as (
    
    select
        CASE 
        WHEN yyyymmdd BETWEEN '20240801' AND '20240930' THEN 'aug_sep24'
        WHEN yyyymmdd BETWEEN '20241001' AND '20241130' THEN 'oct_nov24'
        END month,
        user_id as customer_id,
        count(distinct fare_estimate_id) AS total_fes,
        count(distinct case when CAST(CAST(ct_session_id AS DECIMAL) AS VARCHAR) || ' - ' || phone is not null then yyyymmdd end) as total_fe_days,
        count(distinct CAST(CAST(ct_session_id AS DECIMAL) AS VARCHAR) || ' - ' || phone) total_fe_sessions
    from 
        canonical.clevertap_customer_fare_estimate
    where 
        yyyymmdd BETWEEN '20240801' AND '20241130'
        and user_id IN (SELECT customer_id FROM hive.experiments_internal.high_time_inactive_customer_base_aug_sep_oct_nov_bangalore)
    group by 1,2
    ),
    
    rr_retry as (
    
    select
        CASE 
        WHEN yyyymmdd BETWEEN '20240801' AND '20240930' THEN 'aug_sep24'
        WHEN yyyymmdd BETWEEN '20241001' AND '20241130' THEN 'oct_nov24'
        END month,
        user_id as customer_id,
        count(distinct epoch) AS total_rrs,
        count(distinct case when CAST(CAST(ct_session_id AS DECIMAL) AS VARCHAR) || ' - ' || phone is not null then yyyymmdd end) as total_rr_days,
        count(distinct CAST(CAST(ct_session_id AS DECIMAL) AS VARCHAR) || ' - ' || phone) total_rr_sessions
    from 
        canonical.clevertap_customer_request_rapido
    where
        yyyymmdd BETWEEN '20240801' AND '20241130'
        and user_id IN (SELECT customer_id FROM hive.experiments_internal.high_time_inactive_customer_base_aug_sep_oct_nov_bangalore)
    group by 1,2
      )
      
    select 
        ao.*, 
        fe.total_fes, fe.total_fe_days, fe.total_fe_sessions,
        rr.total_rrs, rr.total_rr_days, rr.total_rr_sessions
    from 
        ao_retry ao 
    left join 
        fe_retry fe 
        on ao.customer_id=fe.customer_id
        and ao.month = fe.month
    left join 
        rr_retry rr 
        on fe.customer_id=rr.customer_id
        and fe.month = rr.month