with customer_base as (
select 
    cast(date_format(date_trunc('week',current_date-interval '1' day), '%Y-%m-%d') as date) as WeekStartDate,
    taxi_lifetime_last_ride_city segment_city,
    auto_lifetime_stage,
    customer_id
from 
    datasets_internal.customer_segments_v1
where 
    auto_ltr_segment = 'PHH' 
    AND auto_recency_segment in ('RECENT','STATIONARY')
    AND auto_lifetime_stage != 'UNKNOWN' 
    AND run_date = date_format(date_trunc('week',current_date-interval '1' day)- interval '2' day, '%Y-%m-%d')
),

orders_1 as ( 
select 
    cast(date_format(date_trunc('week',current_date-interval '1' day), '%Y-%m-%d') as date) as WeekStartDate,
    now() -interval '1' day Week_Till_Date,
    order_id,
    city_name,
    offer_details_offer_id ,
    offer_details_offer_code ,
    offer_details_offer_type ,
    discount,
    sub_total,
    customer_id
FROM 
    "orders"."order_logs_snapshot" 
WHERE
    yyyymmdd between date_format(date_trunc('week',current_date-interval '1' day), '%Y%m%d') and date_format(date_trunc('week',current_date-interval '0' day), '%Y%m%d')
    and order_status = 'dropped'
    and service_obj_service_name = 'Auto'
    
 ),
 
 orders_tbl as (
 select 
    a.*,
    b.auto_lifetime_stage
 from orders_1 a join customer_base b 
 on  a.WeekStartDate = b.WeekStartDate
 and a.city_name = b.segment_city
 and a.customer_id = b.customer_id
 
 
 ),


allocator_active_city as (
 select  distinct city from experiments.allocator_customers 
 where start_date = date_format(date_trunc('week',current_date-interval '1' day), '%Y-%m-%d')
 union
 select  distinct taxi_lifetime_last_ride_city city from datasets_internal.customer_offers_v2 
 where 
    offer_start_date = date_format(date_trunc('week',current_date-interval '1' day), '%Y-%m-%d')
    and run_date =  date_format(date_trunc('week',current_date-interval '1' day)-interval '3' day, '%Y-%m-%d')
),
 
 
offer_details as (
  select 
                yyyymmdd, startdate, endDate, userselectors, 
                id,
                lower(offertext) as offertext, 
                offercode, 
                expressions as offer_expressions,
                selector_type, 
                peruserlimit, 
                json_extract(expressions, '$.0.maxLimit') as discountLimit,
                json_extract(expressions, '$.0.expression') as expression,
                cast(userselectors as ARRAY<VARCHAR>) as selectors_list,
                NAME offer_name,
                type as offer_type,
                NAME recommendation_date,
                cast(json_extract(services,'$.0') as varchar) services,
                cast(json_extract(city,'$.0') as varchar) city,
                description
                
            from raw.kafka_offers_immutable 
            where
            yyyymmdd >= date_format(date_trunc('week',current_date-interval '1' day)- interval '3' day, '%Y%m%d')
            and startdate <= date_format(current_date-interval '1' day, '%Y-%m-%d')
 ),
 
 orders_with_offer as (
 select 
    a.*,
    b.description,
    case when b.description like '%IALLOCATOR%' then 'IALLOCATOR'  
        when offer_details_offer_type like '%coupon%' then 'other' else offer_details_offer_type
    end offerSource,
    b.offertext
 from orders_tbl a
 left join offer_details b
 on a.offer_details_offer_id = b.id
 ),
 
 allocator_orders as (
 select
    WeekStartDate,
    city_name,
    cast(max(Week_Till_Date) as date) Latest_Timestamp,
    coalesce(offerSource,'non_disc_rides') offerSource,
    count(distinct order_id) total_orders,
    sum(discount) total_discount,
    sum(discount)*100.0/cast(sum(sub_total)as real) disc_pct,
    sum(discount)*1.00/cast(count(distinct order_id)as real) dpr
    
    -- count(distinct case when offerSource = 'IALLOCATOR' then order_id end ) allocator_orders,
    -- count(distinct case when offerSource = 'IALLOCATOR' then order_id end ) *100 /count(distinct order_id) allocator_order_coverage,
    
    -- -- sum( case when offerSource = 'IALLOCATOR' then discount end ) allocator_discount,
    -- -- sum( case when offerSource = 'IALLOCATOR' then discount end ) *100 /sum(discount) allocator_disc_coverage
    
from 
    orders_with_offer
 group by 1,2,4
 order by 4 desc
 )
 
select 
    concat(cast(WeekStartDate as varchar),' To ',cast(Latest_Timestamp as varchar)) time_priod,
    concat(city_name,' (',
      case when city is not null then 'Yes' else 'No' end,')') "city (allocator_status)",
    max(city_orders) total_orders,
    max(case when offerSource = 'non_disc_rides' then order_coverage end) non_disc_rides,
    max(case when offerSource = 'IALLOCATOR' then order_coverage end) allocator_rides,
    max(case when offerSource = 'location' then order_coverage end) location_rides,
    max(case when offerSource = 'rides' then order_coverage end) nthRide_rides,
    max(case when offerSource = 'retention' then order_coverage end) retention_rides,
    max(case when offerSource = 'subscription' then order_coverage end) subscription_rides,
    max(case when offerSource = 'other' then order_coverage end) other_rides
    -- max(case when offerSource = 'rides_coupon' then order_coverage end) rides_coupon_rides,
    -- max(case when offerSource = 'retention_coupon' then order_coverage end) ret_coupon_rides,
    -- max(case when offerSource = 'partner_coupon' then order_coverage end) partner_coupon
    
from 
(
 select 
    *,
    sum(total_orders) over(partition by WeekStartDate,city_name) city_orders,
    total_orders*100.00/sum(total_orders) over(partition by WeekStartDate,city_name) order_coverage,
    sum(total_discount) over(partition by WeekStartDate,city_name) city_discount,
    total_discount*100/sum(total_discount) over(partition by WeekStartDate,city_name) discount_coverage
 from 
    allocator_orders 
    
left join 
    allocator_active_city
on allocator_active_city.city = allocator_orders.city_name
where city_name is not null 
order by 1,2,5 desc  
)
group by 1,2
order by 1,3 desc
limit 20
