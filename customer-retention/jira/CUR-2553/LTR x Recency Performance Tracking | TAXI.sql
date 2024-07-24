WITH base_month_customer AS 
(
SELECT 
    DATE_FORMAT(DATE_TRUNC('month', date(run_date)), '%Y-%m-%d') AS month,
    customer_id,
    --MAX(taxi_lifetime_last_ride_date) 
    taxi_lifetime_last_ride_date,
    --MAX(taxi_lifetime_rides) 
    taxi_lifetime_rides

FROM datasets.iallocator_customer_segments
WHERE 
    run_date BETWEEN DATE_FORMAT(DATE_TRUNC('month', {{start_date}}), '%Y-%m-%d')
    AND DATE_FORMAT(DATE_TRUNC('month', {{end_date}}) , '%Y-%m-%d') 
    AND run_date = DATE_FORMAT(DATE_TRUNC('month', date(run_date)), '%Y-%m-%d')
    AND (taxi_lifetime_rides > 0 or taxi_lifetime_rides is not null or taxi_lifetime_last_ride_date is not null)
--GROUP BY 1,2
),

base_month_segment AS
(
SELECT 
    DATE_FORMAT(DATE_TRUNC('month', DATE(month)) , '%Y-%m-%d') AS month,
    customer_id,
    CASE
    WHEN taxi_lifetime_rides IS NULL THEN 'LTR=0'
    WHEN taxi_lifetime_rides BETWEEN 1 AND 4 THEN '1-4'
    WHEN (taxi_lifetime_rides BETWEEN 5 AND 20) AND (taxi_lifetime_last_ride_date >= (DATE_TRUNC('month', DATE(month)) - INTERVAL '30' DAY)) THEN '5-20'
    WHEN (taxi_lifetime_rides BETWEEN 21 AND 50) AND (taxi_lifetime_last_ride_date >= (DATE_TRUNC('month', DATE(month)) - INTERVAL '30' DAY)) THEN '21-50'
    WHEN (taxi_lifetime_rides > 50) AND (taxi_lifetime_last_ride_date >= (DATE_TRUNC('month', DATE(month)) - INTERVAL '30' DAY)) THEN '50+'
    WHEN taxi_lifetime_rides >= 5 THEN '>=5'
    ELSE 'LTR=0'
    END ltr_taxi,
    CASE 
    WHEN taxi_lifetime_last_ride_date IS NULL THEN 'LTR=0'
    WHEN taxi_lifetime_last_ride_date >= (DATE_TRUNC('month', DATE(month)) - INTERVAL '30' DAY) THEN '1. LAST_30_DAYS'
    WHEN taxi_lifetime_last_ride_date BETWEEN (DATE_TRUNC('month', DATE(month)) - INTERVAL '90' DAY) AND (DATE_TRUNC('month', DATE(month)) - INTERVAL '31' DAY) THEN '2. 31_90_DAYS'
    WHEN taxi_lifetime_last_ride_date BETWEEN (DATE_TRUNC('month', DATE(month)) - INTERVAL '180' DAY) AND (DATE_TRUNC('month', DATE(month)) - INTERVAL '91' DAY) THEN '3. 91_180_DAYS'
    WHEN taxi_lifetime_last_ride_date BETWEEN (DATE_TRUNC('month', DATE(month)) - INTERVAL '365' DAY) AND (DATE_TRUNC('month', DATE(month)) - INTERVAL '181' DAY) THEN '4. 181_365_DAYS'
    WHEN taxi_lifetime_last_ride_date < (DATE_TRUNC('month', DATE(month)) - INTERVAL '365' DAY) THEN '5. 365_ABOVE'
    ELSE 'LTR=0'
    END recency_taxi
FROM base_month_customer
),

base_month_total AS
(
SELECT 
    DATE_FORMAT(DATE_TRUNC('month', DATE(month)) , '%Y-%m-%d') AS month,
    COUNT(DISTINCT customer_id) existing_custr_total 
FROM base_month_customer
GROUP BY 1
),

customer_rf_daily_kpi AS
(
SELECT 
    day,
    customerid,
    rr_sessions_unique_daily,
    net_rides_daily,
    ao_sessions_unique_daily,
    fe_sessions_unique_daily,
    discount_daily,
    subtotal_daily
        
FROM datasets.customer_rf_daily_kpi
WHERE 
    day BETWEEN DATE_FORMAT(DATE_TRUNC('month', {{start_date}}), '%Y-%m-%d')
    AND DATE_FORMAT(DATE_TRUNC('month', {{end_date}} + INTERVAL '28' DAY) - INTERVAL '1' DAY, '%Y-%m-%d')
    AND service_name IN ('Link', 'Auto')
    AND customerid IS NOT NULL
),

rr_net AS
(
SELECT 
    DATE_FORMAT(DATE_TRUNC('month', DATE(day)), '%Y-%m-%d') AS month,
    customerid as customer_id,
    SUM(rr_sessions_unique_daily) rr,
    SUM(net_rides_daily) net_rides,
    SUM(discount_daily) burn,
    SUM(subtotal_daily) subtotal
FROM customer_rf_daily_kpi
WHERE 
    day BETWEEN DATE_FORMAT(DATE_TRUNC('month', {{start_date}}), '%Y-%m-%d') 
    AND DATE_FORMAT(DATE_TRUNC('month', {{end_date}} + INTERVAL '28' DAY) - INTERVAL '1' DAY, '%Y-%m-%d')
GROUP BY 1,2
),

max_daily_ao AS
(
SELECT 
        day,
        customer_id,
        SUM(ao) AS ao,
        SUM(fe) AS fe
FROM
    (
    SELECT 
        customerid as customer_id,
        day,
        max(ao_sessions_unique_daily) AS ao,
        max(fe_sessions_unique_daily) AS fe
    FROM datasets.customer_rf_daily_kpi
    WHERE 
        day BETWEEN DATE_FORMAT(DATE_TRUNC('month', {{start_date}}), '%Y-%m-%d') 
        AND DATE_FORMAT(DATE_TRUNC('month', {{end_date}} + INTERVAL '28' DAY) - INTERVAL '1' DAY, '%Y-%m-%d')
        AND customerid IS NOT NULL
    GROUP BY 1,2
    )
GROUP BY 1,2
),

monthly_ao AS
(
SELECT  
    DATE_FORMAT(DATE_TRUNC('month', DATE(day)), '%Y-%m-%d') AS month,
    customer_id,
    SUM(ao) AS ao,
    SUM(fe) AS fe
FROM max_daily_ao
GROUP BY 1,2
),

orders as
(
select 
    DATE_FORMAT(DATE_TRUNC('month', DATE(date_parse(yyyymmdd,'%Y%m%d'))), '%Y-%m-%d') Start,
    customer_id,
    order_id,
    service_obj_service_name
from orders.order_logs_snapshot
where service_obj_service_name in ('Auto','Link')
    and order_id is not null
    and service_obj_city_display_name is not null
    and yyyymmdd between DATE_FORMAT(DATE_TRUNC('month', {{start_date}})- interval '14' day,'%Y%m%d') 
    and DATE_FORMAT(DATE_TRUNC('month', {{end_date}} + INTERVAL '28' DAY) - INTERVAL '1' DAY, '%Y%m%d')
),

order_coin as
(
select
    DATE_FORMAT(DATE_TRUNC('month', DATE(date_parse(yyyymmdd,'%Y%m%d'))), '%Y-%m-%d') StartDate,
    owner_id customerid,
    entity_id,
    cast(json_extract(coin_wallet_changes, '$[0].offerType') as varchar) offerType,
    coalesce(round(sum(case when transaction_type = 'credit' then cast(amount as double) end)),0) as coin_credited,
    coalesce(round(sum(case when transaction_type = 'debit' and subtype != 'coinExpired' then cast(amount as double) end)),0) as coin_utilized,
    coalesce(round(sum(case when transaction_type = 'debit' and subtype = 'coinExpired' then cast(amount as double) end)),0) as coin_expired
from payments.transactions_snapshot
where yyyymmdd between DATE_FORMAT(DATE_TRUNC('month', {{start_date}}),'%Y%m%d') 
    and DATE_FORMAT(DATE_TRUNC('month', {{end_date}} + INTERVAL '28' DAY) - INTERVAL '1' DAY, '%Y%m%d')
    and owner_type ='customer'
    and transaction_status = 'done'
    and json_extract(coin_wallet_changes, '$[0].id') is not null
group by 1,2,3,4
),

service_total_coins as
(
select
    StartDate,
    customerid,
    entity_id,
    sum(case when offerType in ('locationOffer','rideOffer','scratchCardOffer') and service_obj_service_name = 'Auto' then coin_credited end) total_coin_credited,
    sum(case when offerType in ('locationOffer','rideOffer','scratchCardOffer') and service_obj_service_name = 'Auto' then coin_expired end) total_coin_expired,
    sum(case when offerType = 'giftOffer' and service_obj_service_name = 'Auto' then coin_utilized end) giftOffer_utilized,
    sum(case when offerType = 'walletRechargeOffer' and service_obj_service_name = 'Auto' then coin_utilized end) walletOffer_utilized,
    sum(case when offerType = 'dashboardCoinsCredit' and service_obj_service_name = 'Auto' then coin_utilized end) dashboardCoinsCredit_utilized
from
    (
    select a.*, b.*
    from order_coin a join orders b on a.customerid = b.customer_id and a.entity_id = b.order_id and a.StartDate = b.Start
    )
where StartDate is not null
group by 1,2,3
),

coin_credit as
(
select
    Start,
    customerid,
    sum(total_coin_credited) total_coin_credited,
    sum(walletOffer_utilized) walletOffer_utilized,
    sum(giftOffer_utilized) giftOffer_utilized,
    sum(dashboardCoinsCredit_utilized) dashboardCoinsCredit_utilized
from
    (
    select
        (case when a.customer_id is null then b.customerid else a.customer_id end) as customerid,
        (case when a.month is null then b.StartDate else a.month end) as Start,
        b.walletOffer_utilized,
        b.giftOffer_utilized,
        b.dashboardCoinsCredit_utilized,
        b.total_coin_credited
    from rr_net a 
    right join service_total_coins b on a.month = b.StartDate and a.customer_id = b.customerid
    )
where Start is not null
group by 1,2
),

coin_expired_customer_level as
(
select
    StartDate as Start2,
    customerid,
    sum(coin_expired) coin_expired
from order_coin a 
join orders b on a.entity_id = b.order_id 
where coin_expired > 0
group by 1,2
),

agg_coin as
(
select
    a.*,
    b.coin_expired,
    (coalesce(a.total_coin_credited,0) + coalesce(a.walletOffer_utilized,0) + coalesce(giftOffer_utilized,0) + coalesce(a.dashboardCoinsCredit_utilized,0) - coalesce(b.coin_expired,0)) total_coin_burn
from coin_credit a 
join coin_expired_customer_level b on a.Start = b.Start2 and a.customerid = b.customerid
),

base_current AS (
SELECT 
        COALESCE(b.month,c.month,r.month) month,
        -- b.month,
        COALESCE(b.customer_id,c.customer_id,r.customer_id) customers,
        b.customer_id bcustomer_id,
        COALESCE(b.ltr_taxi,'NEW') ltr_taxi,
        COALESCE(b.recency_taxi, 'NEW') recency_taxi,
        c.customer_id acustomer_id,
        r.customer_id rcustomer_id,
        CASE 
        WHEN c.ao = 0 AND r.net_rides > 0 THEN r.net_rides
        WHEN c.ao = 0 AND c.fe > 0 THEN c.fe
        WHEN c.ao = 0 AND r.rr > 0 THEN r.rr
        WHEN c.ao IS NULL THEN 0
        ELSE c.ao END ao,
        
        CASE
        WHEN c.fe = 0 AND r.net_rides > 0 THEN r.net_rides 
        WHEN c.fe = 0 AND r.rr > 0 THEN r.rr
        WHEN c.fe IS NULL THEN 0
        ELSE c.fe END fe,
        
        CASE
        WHEN r.rr = 0 AND r.net_rides > 0 THEN r.net_rides
        WHEN r.rr IS NULL THEN 0
        ELSE r.rr END rr,
        
        COALESCE(r.net_rides, 0) net_rides,
        COALESCE(r.burn, 0) burn,
        COALESCE(ac.total_coin_burn, 0) total_coin_burn,
        COALESCE(r.subtotal, 0) subtotal

FROM base_month_segment AS b
FULL JOIN monthly_ao AS c ON b.customer_id = c.customer_id AND b.month = c.month 
FULL JOIN rr_net AS r ON b.customer_id = r.customer_id AND c.month = r.month
LEFT JOIN agg_coin ac ON r.customer_id = ac.customerid AND r.month = ac.Start
),

detailed_view AS 
(
SELECT 
        c.month,
        ltr_taxi,
        case 
        when ltr_taxi = '1-4' then 1
        when ltr_taxi = '5-20' then 2
        when ltr_taxi = '21-50' then 3
        when ltr_taxi = '50+' then 4
        when ltr_taxi = '>=5' then 5
        when ltr_taxi = 'LTR=0' then 6
        when ltr_taxi = 'NEW' then 7
        end row_order,
        recency_taxi,
        COUNT(DISTINCT bcustomer_id) existing_custr_count,
        COUNT(DISTINCT CASE WHEN ao > 0 THEN acustomer_id END) ao_users,
        COUNT(DISTINCT CASE WHEN fe > 0 THEN acustomer_id END) fe_users,
        COUNT(DISTINCT CASE WHEN rr > 0 THEN rcustomer_id END) rr_users,
        COUNT(DISTINCT CASE WHEN net_rides > 0 THEN rcustomer_id END) net_users,
        SUM(net_rides) net_rides,
        SUM(burn)+SUM(total_coin_burn) discount_with_coin,
        SUM(subtotal) subtotal
        
FROM base_current c
GROUP BY 1,2,3,4
),

summary_view AS (
SELECT 
        c.month,
        COUNT(DISTINCT CASE WHEN net_rides > 0 THEN c.customers END) net_users,
        SUM(net_rides) net_rides,
        SUM(burn)+SUM(total_coin_burn) discount_with_coin,
        SUM(subtotal) subtotal
FROM base_current c
GROUP BY 1
),

a as 
(
SELECT  
        a.month,
        a.row_order,
        a.ltr_taxi,
        a.recency_taxi,
        a.existing_custr_count,
        COALESCE(try(a.existing_custr_count*100.00/b.existing_custr_total), 0) existing_custr_distr,
        a.ao_users,
        COALESCE(try(a.ao_users*100.00/a.existing_custr_count), 0) ao_conversion, 
        a.fe_users,
        COALESCE(try(a.fe_users*100.00/a.existing_custr_count), 0) fe_conversion, 
        a.rr_users,
        COALESCE(try(a.rr_users*100.00/a.existing_custr_count), 0) rr_conversion,
        COALESCE(try(a.rr_users*100.00/a.fe_users), 0) fe_rr,
        a.net_users,
        COALESCE(try(a.net_users*100.00/a.existing_custr_count), 0) net_conversion,
        COALESCE(try(a.net_users*100.00/a.rr_users), 0) rr_net,
        COALESCE(try(a.net_users*100.00/c.net_users), 0) net_users_distr,
        a.net_rides,
        COALESCE(try(a.net_rides*100.00/c.net_rides), 0) net_rides_distr,
        COALESCE(try(a.net_rides*1.00/a.net_users), 0) rpc,
        a.discount_with_coin,
        a.subtotal,
        COALESCE(try(a.discount_with_coin*100.0/a.subtotal), 0) discount_perc,
        COALESCE(try(a.discount_with_coin*1.0/a.net_rides), 0) dpr
        
FROM detailed_view a
LEFT JOIN base_month_total b ON a.month = b.month
LEFT JOIN summary_view c ON a.month = c.month
ORDER BY 1,2,3
),

b as 
(
SELECT
        month,
        8 row_order,
        'TOTAL' ltr_taxi,
        '' recency_taxi,
        SUM(existing_custr_count) existing_custr_count,
        100.00 existing_custr_distr,
        SUM(ao_users) ao_users,
        COALESCE(try(SUM(ao_users)*100.00/SUM(existing_custr_count)), 0) ao_conversion,
        SUM(fe_users) fe_users,
        COALESCE(try(SUM(fe_users)*100.00/SUM(existing_custr_count)), 0) fe_conversion,
        SUM(rr_users) rr_users,
        COALESCE(try(SUM(rr_users)*100.00/SUM(existing_custr_count)), 0) rr_conversion,
        COALESCE(try(SUM(rr_users)*100.00/SUM(fe_users)), 0) fe_rr,
        SUM(net_users) net_users,
        COALESCE(try(SUM(net_users)*100.00/SUM(existing_custr_count)), 0) net_conversion,
        COALESCE(try(SUM(net_users)*100.00/SUM(rr_users)), 0) rr_net,
        100.00 net_users_distr,
        SUM(net_rides) net_rides,
        100.00 net_rides_distr,
        COALESCE(try(SUM(net_rides)*1.0/SUM(net_users)), 0) rpc,
        SUM(discount_with_coin) discount_with_coin,
        SUM(subtotal) subtotal,
        COALESCE(try(SUM(discount_with_coin)*100.0/SUM(subtotal)), 0) discount_perc,
        COALESCE(try(SUM(discount_with_coin)*1.0/SUM(net_rides)), 0) dpr
FROM a
GROUP BY 1
),

final as 
(
select * from a
union
select * from b
)
select 
        month,
        ltr_taxi,recency_taxi,
        existing_custr_count,existing_custr_distr,
        ao_users,ao_conversion,
        fe_users,fe_conversion,
        rr_users,rr_conversion,fe_rr "fe_rr(users)",
        net_users,net_conversion, rr_net "rr_net(users)", net_users_distr,
        net_rides,net_rides_distr,
        rpc,
        discount_with_coin,
        subtotal,
        discount_perc,
        dpr
from final
order by 1,row_order,3