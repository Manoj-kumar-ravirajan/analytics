with transaction_query as (
    select 
        entity_id orderid,
        concat(owner_id,'-',order_unique_id) unique_id,
        -- sum(case when cast(offer_type as varchar) = 'scratchCardOffer' then cast(amount as int) end) scratchcard_amount,
        sum(case when cast(offer_type as varchar) != 'scratchCardOffer' then cast(amount as int) end) custCashBackAmount
    from 
        (
        select *
        from (
            select
                id, yyyymmdd, coin_wallet_amount, transaction_type, subtype, entity_id, owner_id, order_unique_id, coin_wallet_changes,coin_wallet_offer_type,subsub_type
            from 
                payments.transactions_snapshot
            where yyyymmdd between date_format({{start_date}}, '%Y%m%d')  and date_format({{endDate}}, '%Y%m%d')  
                and owner_type = 'customer'
                and transaction_status = 'done'
                and cardinality(coin_id) != 0
            )
        cross join unnest(coin_wallet_offer_type, coin_wallet_amount) as tbl1(offer_type, amount)
        )
    where 
        transaction_type = 'credit'
        and subtype in ('locationOffer', 'rideOffer','customerCashBack','scratchCard')
        and cast(offer_type as varchar) in ('locationOffer','rideOffer','scratchCardOffer')
    group by 1,2
),
orders_1 as ( 
select 
    week(cast(order_date as date)) week_number,
    date_format(date_trunc('week',cast(order_date as date)), '%Y-%m-%d') as WeekStartDate,
    order_date,
    order_id order_id,
    concat(customer_id,'-',unique_id) unique_id,
    city_name,
    offer_details_offer_id ,
    offer_details_offer_code ,
    offer_details_offer_type ,
    cast(discount as int) discount_amount,
    sub_total as subtotal,
    customer_id,
    distance_final_distance distance
FROM 
    "orders"."order_logs_snapshot" 
WHERE
    yyyymmdd between date_format({{start_date}}, '%Y%m%d') and date_format({{endDate}}, '%Y%m%d')
    and order_status = 'dropped'
    and service_obj_service_name = 'Link'
    and (spd_fraud_flag != true OR spd_fraud_flag is null)
    and city_name = {{city_name}}
    
 ),
raw_orders as 
(
select
week(date(concat(substr(yyyymmdd,1,4),'-',substr(yyyymmdd,5,2),'-',substr(yyyymmdd,7,2)))) week_number,
date(concat(substr(yyyymmdd,1,4),'-',substr(yyyymmdd,5,2),'-',substr(yyyymmdd,7,2))) as order_date,
orderId as order_id,
title offer_details_offer_type,
subType,
couponCode,
cast(discount as real) discount_amount,
cast(baseFare as real) as subtotal

from raw.mongodb_rapidopayments_transactions_immutable 
where 
yyyymmdd between date_format({{start_date}},'%Y%m%d') and date_format({{endDate}},'%Y%m%d')
-- and title='scratchCard' 
and transactionstatus='done'
-- and transactiontype='credit'

),
orders_cb as 
(
    select 
    a.*,
    -- coalesce(b.scratchcard_amount,0) scratchcard_amount,
    coalesce(b.custCashBackAmount,0) custCashBackAmount
    from orders_1 a 
    left join transaction_query b 
        on (a.order_id = b.orderid)
),
v1 as
(
    select *
    from
    (
        select day,
        customer_rf_segment,
        case
        when customer_rf_segment like '%HH%' then '02. HH'
        when customer_rf_segment like '%NEW%' then '02. HH'
        when customer_rf_segment like '%GOLD%' then '05. Gold'
        when customer_rf_segment like '%SILVER%' then '04. Silver'
        when customer_rf_segment like '%PLATINUM%' then '06. Platinum'
        when customer_rf_segment like '%PRIME%' then '03. Prime'
        when customer_rf_segment like '%ELITE%' then '07. Elite'
        when customer_rf_segment like '%INACTIVE%' then '09. Inactive'
        when customer_rf_segment like '%DORMANT%' then '08. Dormant'
        end as finalSegment,
        customerid, geo_city, service_name,
        row_number() over(partition by customerid, day order by recency asc) as row
        from
        datasets.customer_retention_immutable
        where
        day >= {{start_date}} - interval '1' day
        and day <= {{endDate}}
        -- and day_of_week(cast(day as date)) = 7
        and service_name = 'Link'
        and geo_city in (select distinct city_name from orders_1)
    )
    where
    row = 1 
),
orders_seg as
(
select 
a.*,
b.finalSegment
from orders_cb a left join v1 b on date(a.order_date) = b.day and a.city_name = b.geo_city
)