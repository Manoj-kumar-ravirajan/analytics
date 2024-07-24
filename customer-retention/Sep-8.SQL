with base as
(
select CONCAT('WK-', DATE_FORMAT(DATE(substr(cast(day as varchar), 1, 10)), '%v')) AS OrderDate,
        DATE_FORMAT(DATE_TRUNC('week', DATE(substr(cast(day as varchar), 1, 10))), '%Y-%m-%d') AS StartDate,
        city,
        customerid as customers,
        sum(gross_rides_daily) gross_rides,
        sum(net_rides_daily) net_rides,
        sum(subtotal_daily) subtotal,
        sum(discount_daily) burn
from datasets.customer_rf_daily_kpi
where 1=1
    AND day >= coalesce([[date_format({{Start_Date}},'%Y-%m-%d'), ]] date_format((DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '14' day), '%Y-%m-%d'), '0')
    and day <=coalesce([[date_format({{End_Date}},'%Y-%m-%d'), ]] date_format((DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '1' day), '%Y-%m-%d'), '0')
    and service_name in ('Link')
    and {{City}}
group by 1,2,3,4
),

customers as
(
select 
        StartDate as start, finalcity,
        count(distinct customerid) as base,
        count(distinct case when net_rides > 0 then customerid end) as net_customers
from base
group by 1,2
),

v0 as
(
select 
        week as OrderDate, Start as StartDate,
        finalcity as city,
        sum (gross_rides) gross_rides,
        sum(net_rides) net_rides,
        sum(subtotal) subtotal,
        sum(burn) burn,
        sum(burn)*1.0/sum(net_rides) as DPR
from base
group by 1,2,3
),

v1 as
(
select  a.base,
        a.net_customers,
        b.*
from customers a
right join v0 b on a.start = b.StartDate and a.finalcity = b.city
),

orders as
(
select 
        *,
        substr(cast(date_trunc('week', date_parse(yyyymmdd, '%Y%m%d')) as varchar), 1, 10) Start, 
        service_obj_city_display_name city2
from orders.order_logs_snapshot
where service_obj_service_name in ('Link') 
        and service_obj_city_display_name in (select distinct city from base) 
        and order_id is not null and service_obj_city_display_name is not null
        AND yyyymmdd >= coalesce([[date_format({{Start_Date}} - interval '7' day,'%Y%m%d'), ]] date_format((DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '21' day), '%Y%m%d'), '0')
        and yyyymmdd <=coalesce([[date_format({{End_Date}},'%Y%m%d'), ]] date_format((DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '1' day), '%Y%m%d'), '0') 
),

order_coin as
(
select 
        cast(week(date_parse(yyyymmdd, '%Y%m%d')) as varchar) week_number,
        substr(cast(date_trunc('week', date_parse(yyyymmdd, '%Y%m%d')) as varchar), 1, 10) StartDate,
        owner_id customerid,
        entity_id,
        cast(json_extract(coin_wallet_changes, '$[0].offerType') as varchar) offerType,
        coalesce(round(sum(case when transaction_type = 'credit' then cast(amount as double) end)), 0) as coin_credited,
        coalesce(round(sum(case when transaction_type = 'debit' and subtype != 'coinExpired' then cast(amount as double) end)), 0) as coin_utilized,
        coalesce(round(sum(case when transaction_type = 'debit' and subtype = 'coinExpired' then cast(amount as double) end)), 0) as coin_expired 

from payments.transactions_snapshot
where yyyymmdd between date_format({{Start_Date}},'%Y%m%d') and date_format({{End_Date}},'%Y%m%d')
    and owner_type ='customer'
    and transaction_status = 'done'
    and json_extract(coin_wallet_changes, '$[0].id') is not null
group by 1,2,3,4,5
),

service_total_coins as
(
select StartDate,
        week_number,
        customerid,
        city2,
        entity_id,
        sum(case when offerType in ('locationOffer', 'rideOffer', 'scratchCardOffer') and service_obj_service_name = 'Link' then coin_credited end) total_coin_credited,
        sum(case when offerType in ('locationOffer', 'rideOffer', 'scratchCardOffer') and service_obj_service_name = 'Link' then coin_expired end) total_coin_expired,
        sum(case when offerType = 'giftOffer' and service_obj_service_name = 'Link' then coin_utilized end) giftOffer_utilized,
        sum(case when offerType = 'walletRechargeOffer' and service_obj_service_name = 'Link' then coin_utilized end) walletOffer_utilized,
        sum(case when offerType = 'dashboardCoinsCredit' and service_obj_service_name = 'Link' then coin_utilized end) dashboardCoinsCredit_utilized
from
    (
    select a.*, b.*
    from order_coin a
    join orders b on a.customerid = b.customer_id and a.entity_id = b.order_id and a.StartDate = b.Start
    )
where StartDate is not null
group by 1,2,3,4,5
),

coin_credit as
(
select 
    Start, week_number,
    city, 
    sum(total_coin_credited) total_coin_credited,
    sum(walletOffer_utilized) walletOffer_utilized,
    sum(giftOffer_utilized) giftOffer_utilized,
    sum(dashboardCoinsCredit_utilized) dashboardCoinsCredit_utilized
from
    (
    select 
        (case when a.city is null then b.city2 else a.city end) as city,
        (case when cast(a.Week as varchar) is null then b.week_number else cast(a.Week as varchar) end) as Week,
        (case when a.customerid is null then b.customerid else a.customerid end) as customerid, 
        (case when a.Start is null then b.StartDate else a.Start end) as Start, -- a.finalSegment,
        b.week_number,
        b.walletOffer_utilized,
        b.giftOffer_utilized,
        b.dashboardCoinsCredit_utilized,
        b.total_coin_credited
    from base a
    right join service_total_coins b on a.Start = b.StartDate and a.customerid = b.customerid and a.city = b.city2
    )
where Start is not null
group by 1, 2, 3
),

coin_expired_customer_level as
(
select 
        StartDate as Start2,
        city2, 
        sum(coin_expired) coin_expired
from order_coin a
join orders b on a.entity_id = b.order_id
where coin_expired > 0
group by 1, 2
),

agg_coin as
(
select 
        a.*,
        b.coin_expired,
        (coalesce(a.total_coin_credited, 0) + coalesce(a.walletOffer_utilized, 0) + coalesce(giftOffer_utilized, 0) + coalesce(a.dashboardCoinsCredit_utilized, 0) - coalesce(b.coin_expired, 0)) total_coin_burn
from coin_credit a
join coin_expired_customer_level b on a.Start = b.Start2 and a.city = b.city2
),

v1_with_coins as
(
select 
        a.*,
        b.total_coin_burn Total_Coin_Burn 
from v1 a
join agg_coin b on a.StartDate = b.Start and a.city = b.city
),

discounted_rides as
(
select 
    cast(DATE_TRUNC('week', date(substr(yyyymmdd, 1, 4) ||'-'|| substr(yyyymmdd, 5, 2) ||'-'|| substr(yyyymmdd, 7, 2))) as varchar) as StartDate,
    city_name,
    count(distinct case when discount > 0 then order_id end) Discounted_Rides,
    count(distinct case when discount > 0 then order_id end)*100.00/count(distinct order_id) Discounted_Rides_perc
from orders
where order_id is not null and order_status = 'dropped'
group by 1, 2
),

final as
(
select 
        v1_with_coins.*,
        discounted_rides.Discounted_Rides_perc as "Discounted_Rides%"
from v1_with_coins
join discounted_rides on v1_with_coins.StartDate = discounted_rides.StartDate and v1_with_coins.city = discounted_rides.city_name
)

select 
        concat('WK', '-', cast(OrderDate as varchar)) OrderDate,
        StartDate,
        city, -- base as Base,
        net_customers as Net_Customers,
        gross_rides as GrossRides,
        net_rides as NetRides,
        net_rides*1.00/net_customers as RPC,
        subtotal Subtotal,
        burn as Discount,
        Total_Coin_Burn,
        burn + Total_Coin_Burn as Discount_with_coin
from final
order by 3, 1 desc