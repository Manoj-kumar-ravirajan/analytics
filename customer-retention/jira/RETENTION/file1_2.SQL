with Base as (
select
    run_date,
    (extract(week from cast(run_date as date)) + 1) as Week,
    -- auto_lifetime_stage lifetime_stage,
    CASE
    WHEN taxi_retention_segments LIKE '%HH%' THEN '02. HH'
    WHEN taxi_retention_segments LIKE '%NEW%' THEN '02. HH'
    WHEN taxi_retention_segments LIKE '%GOLD%' THEN '05. Gold'
    WHEN taxi_retention_segments LIKE '%SILVER%' THEN '04. Silver'
    WHEN taxi_retention_segments LIKE '%PLATINUM%' THEN '06. Platinum'
    WHEN taxi_retention_segments LIKE '%PRIME%' THEN '03. Prime'
    WHEN taxi_retention_segments LIKE '%ELITE%' THEN '07. Elite'
    WHEN taxi_retention_segments LIKE '%INACTIVE%' THEN '09. Inactive'
    WHEN taxi_retention_segments LIKE '%DORMANT%' THEN '08. Dormant'
    END AS segments,
    taxi_lifetime_last_ride_city city,
    customer_id,
    count(*) records
    from datasets.iallocator_customer_segments 
    where taxi_lifetime_last_ride_city = {{city_name}}
    AND run_date between date_format({{start_date}} - interval '1' day,'%Y-%m-%d') and date_format({{end_date}} - interval '5' day,'%Y-%m-%d')
    AND day_of_week(cast (run_date as date)) = 7
    AND auto_lifetime_stage in ('COMMITTED','DETOX','SUSTENANCE','HOOK','CHURN_OTB','SOFT_CHURN','HANDHOLDING')
    AND auto_recency_segment in ('STATIONARY','RECENT')
    group by 1,2,3,4,5
),

v01 as
(
    select
        city,
        customerid,
        extract(week from cast(day as date)) as Week,
        sum(case when service_name = 'Auto' then gross_rides_daily end) as gross_rides,
        sum(case when service_name = 'Auto' and net_rides_daily > 0 then net_rides_daily end) as net_rides,
        -- sum(case when service_name = 'Link' and net_rides_daily > 0 then net_rides_daily end) as net_rides_link,
        sum(case when service_name = 'Auto' and net_rides_daily > 0 then subscription_rides_daily end) as sub_rides,
        sum(case when service_name = 'Auto' and net_rides_daily > 0 then burn_daily end) as burn,
        sum(case when service_name = 'Auto' and subscription_rides_daily > 0 then burn_daily end) as subs_discount,
        sum(case when service_name = 'Auto' and net_rides_daily > 0 then subtotal_daily end) as subtotal,
        sum(case when (service_name = 'Auto' or service_name is null) then ao_sessions_unique_daily end) as ao,
        sum(case when service_name = 'Auto' then fe_sessions_unique_daily end) as fe,
        sum(case when service_name = 'Auto' then rr_sessions_unique_daily end) as rr
    from 
    datasets.customer_rf_daily_kpi
    where
    cast(day as date) >= {{start_date}}
    and cast(day as date) <= {{end_date}}
    and service_name = 'Auto'
    and city = {{city_name}}
    group by 
    1, 2,3
),

v2 as (
    select 
    -- extract(week from cast(coalesce(date(day) + interval '1' day, week) as date)) as Week,
    Base.Week,
    Base.city,
    -- Base.lifetime_stage,
    Base.segments,
    count(distinct Base.customer_id) as Base,
    count(distinct case when net_rides > 0 then v01.customerid end) as Net_Customers,
    count(distinct case when net_rides > 0 then v01.customerid end)*100.00/count(distinct Base.customer_id)  Conversion,
    sum(gross_rides) as GrossRides,
    sum(net_rides) as NetRides,
    -- sum(net_rides_link) as NetRidesLink,
    --sum(net_rides)*100/sum(gross_rides) G2N,
    sum(net_rides)*1.00/count(distinct case when net_rides > 0 then v01.customerid end) RPC,
    sum(sub_rides) as Subs_Rides,
    --sum(burn) as Discount,
    sum(subs_discount) as Subs_Discount,
    sum(subtotal) as Subtotal,
    sum(subtotal)*1.0/sum(net_rides) as ATV,
    --sum(burn)*100.0/sum(subtotal) as "Discount%",
    sum(burn)*1.0/sum(net_rides) as DPR,
    -- count(distinct case when ao > 0 then v01.customerid end) as AO_Cust,
    100.0*count(distinct case when ao > 0 then v01.customerid end)/count(distinct Base.customer_id) as "AO_Cust%",
    -- count(distinct case when fe > 0 then v01.customerid end) as FE_Cust,
    100.0*count(distinct case when fe > 0 then v01.customerid end)/count(distinct Base.customer_id) as "FE_Cust%",
    -- count(distinct case when rr > 0 then v01.customerid end) as RR_Cust,
    100.0*count(distinct case when rr > 0 then v01.customerid end)/count(distinct Base.customer_id) as "RR_Cust%",
    sum(ao) as AO_Sessions,
    sum(fe) as FE_Sessions,
    sum(rr) as RR_Sessions,
    100*sum(rr)/cast(sum(fe) as real) as "Fe-RR%"
    
    from Base left join v01
        on Base.customer_id = v01.customerid 
        and Base.Week = v01.Week 
    group by 1, 2, 3
    order by 1,2,3
),

--Discount logic 
daily_kpi as
    (
        select DATE_FORMAT(DATE(substr(cast(day as varchar), 1, 10)), '%v') AS OrderDate,
            DATE_FORMAT(DATE_TRUNC('week', DATE(substr(cast(day as varchar), 1, 10))), '%Y-%m-%d') AS StartDate,
            extract(week from cast(day as date)) as Week,
            city,
            customerid as customers,
            sum(subtotal_daily) subtotal,
            sum(discount_daily) burn
        from datasets.customer_rf_daily_kpi
        where 1=1
            AND day >= date_format({{start_date}},'%Y-%m-%d')
            and day <= date_format({{end_date}},'%Y-%m-%d')
            and service_name in ('Auto')
            and City = {{city_name}}
        group by 1,2,3,4,5 
    ),

v1 as
    (
        select  b.OrderDate, 
                b.StartDate,
                m.city,
                -- m.lifetime_stage,
                m.segments,
                sum(b.subtotal) subtotal,
                sum(b.burn) burn,
                sum(b.burn)*100.0/sum(b.subtotal) as Discount
        from Base as m 
        join daily_kpi as b on m.customer_id = b.customers and m.Week = b.Week and m.city = b.city 
        group by 1,2,3,4
    ),
    
orders as
    (
        select *,
          substr(cast(date_trunc('week', date_parse(yyyymmdd, '%Y%m%d')) as varchar), 1, 10) Start
        from orders.order_logs_snapshot
        where service_obj_service_name in ('Auto')
            and service_obj_city_display_name in (select distinct city from daily_kpi) 
            and customer_id in (select distinct customers from daily_kpi)
            and order_id is not null
            and service_obj_city_display_name is not null
            AND yyyymmdd >= date_format({{start_date}},'%Y%m%d')
            and yyyymmdd <= date_format({{end_date}},'%Y%m%d')
    ),

order_coin as
    (
        select Concat('WK-', cast(week(date_parse(yyyymmdd, '%Y%m%d')) as varchar)) week_number,
                substr(cast(date_trunc('week', date_parse(yyyymmdd, '%Y%m%d')) as varchar), 1, 10) StartDate, -- DATE_FORMAT(DATE_TRUNC('week', DATE(substr(yyyymmdd,1,10))), '%Y%m%d') AS StartDate,
                owner_id customerid,
                entity_id, -- 'week' week,
                cast(json_extract(coin_wallet_changes, '$[0].offerType') as varchar) offerType,
                coalesce(round(sum(case when transaction_type = 'credit' then cast(amount as double) end)), 0) as coin_credited,
                coalesce(round(sum(case when transaction_type = 'debit' and subtype != 'coinExpired' then cast(amount as double) end)), 0) as coin_utilized,
                coalesce(round(sum(case when transaction_type = 'debit' and subtype = 'coinExpired' then cast(amount as double) end)), 0) as coin_expired,
                round(sum(case when transaction_type = 'credit' then cast(amount as double) end)) - round(sum(case when transaction_type = 'debit' and subtype = 'coinExpired' then cast(amount as double) end)) coin_burn
        from payments.transactions_snapshot
        where 1=1
            AND yyyymmdd >=  date_format({{start_date}},'%Y%m%d')
            and yyyymmdd <= date_format({{end_date}},'%Y%m%d')
            and owner_id in (select distinct customers from daily_kpi)
            and entity_id in (select distinct order_id from orders)
            and entity_id is not null
            and owner_type ='customer'
            and transaction_status = 'done'
            and json_extract(coin_wallet_changes, '$[0].id') is not null
        group by 1,2,3,4,5

    ),

coin_data as
    (
        select a.*
        from order_coin a
        join orders b on a.customerid = b.customer_id and a.StartDate = b.Start and a.entity_id = b.order_id
    ),

coin_city_data as
    (
        select a.*,
            m.city,
            m.customers,
            -- b.lifetime_stage
            b.segments
        from coin_data a
        join daily_kpi m on a.customerid = m.customers and a.StartDate = m.StartDate
        join Base b on a.customerid = b.customer_id and b.Week = b.Week and b.city = b.city  
    ),

final_coin as
    (
        select StartDate,
            city,
            -- lifetime_stage,
            segments,
            sum(case when offerType = 'scratchCardOffer' then coin_credited end) - sum(case when offerType = 'scratchCardOffer' then coin_expired end) ScratchCardOffer_burn,
            sum(case when offerType = 'locationOffer' then coin_credited end) - sum(case when offerType = 'locationOffer' then coin_expired end) LocationOffer_burn,
            sum(case when offerType = 'rideOffer' then coin_credited end) - sum(case when offerType = 'rideOffer' then coin_expired end) RideOffer_burn,
            sum(case when offerType = 'giftOffer' then coin_utilized end) giftOffer_utilized,
            sum(case when offerType = 'walletRechargeOffer' then coin_utilized end) walletOffer_utilized,
            sum(case when offerType = 'dashboardCoinsCredit' then coin_utilized end) dashboardCoinsCredit_utilized
        from coin_city_data
        group by 1,2,3
    ),

v1_with_coins as
    (
        select a.*,
            coalesce(b.ScratchCardOffer_burn, 0) ScratchCardOffer_burn,
            coalesce(b.LocationOffer_burn, 0) LocationOffer_burn,
            coalesce(b.RideOffer_burn, 0) RideOffer_burn,
            coalesce(b.giftOffer_utilized, 0) giftOffer_utilized,
            coalesce(b.walletOffer_utilized, 0) walletOffer_utilized,
            coalesce(b.dashboardCoinsCredit_utilized, 0) dashboardCoinsCredit_utilized
        from v1 a
        join final_coin b on a.StartDate = b.StartDate and a.city = b.city and a.segments = b.segments --and a.lifetime_stage = b.lifetime_stage
    ),
    
life_stage as 
    (
    select  OrderDate,
            city,
            -- lifetime_stage,
            segments,
            burn as Discount,
            subtotal as Subtotal,
            ScratchCardOffer_burn + LocationOffer_burn + RideOffer_burn + giftOffer_utilized + walletOffer_utilized + dashboardCoinsCredit_utilized Total_coins,
            burn + ScratchCardOffer_burn + LocationOffer_burn + RideOffer_burn + giftOffer_utilized + walletOffer_utilized + dashboardCoinsCredit_utilized as Discount_with_coin,
            Discount as Discount_perc_without_coins,
            (ScratchCardOffer_burn + LocationOffer_burn + RideOffer_burn + giftOffer_utilized + walletOffer_utilized + dashboardCoinsCredit_utilized + burn) * 100.0/subtotal as discount_perc
    from v1_with_coins
    )
    
select  v2.Week,
        v2.city,
        -- v2.lifetime_stage,
        v2.segments,
        -- v2.Base,
        -- v2.Net_Customers,
        -- Conversion,
        -- v2.GrossRides,
        -- v2.NetRides,
        v2.NetRides*100.0/v2.GrossRides G2N, 
        v2.RPC,
        -- v2.Subs_Rides,
        -- ls.Discount as Discount,
        -- ls.Total_coins as Total_coins, 
        -- ls.Discount_with_coin as Discount_with_coin,
        -- v2.Subs_Discount,
        -- v2.Subtotal,
        -- ls.Discount_perc_without_coins as "Discount_wo_coins_%",
        ls.discount_perc as "Discount_%",
        -- v2.ATV,
        v2.DPR,
        -- "AO_Cust%",
        -- "FE_Cust%",
        -- "RR_Cust%",
        -- v2.AO_Sessions,
        -- v2.FE_Sessions,
        -- v2.RR_Sessions,
        "Fe-RR%"
        
from v2
left join life_stage as ls on ls.OrderDate = cast(v2.Week as varchar) and ls.city = v2.city and ls.segments =  v2.segments--and ls.lifetime_stage = v2.lifetime_stage 
order by 1,2,3 