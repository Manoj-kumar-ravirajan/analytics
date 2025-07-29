WITH base_month_customer AS 
(
SELECT 
    DATE_FORMAT(DATE_TRUNC('month', date(run_date)), '%Y-%m-%d') AS month,
    taxi_lifetime_last_ride_city city,
    customer_id,
    --MAX(auto_lifetime_last_ride_date) 
    auto_lifetime_last_ride_date,
    --MAX(auto_lifetime_rides) 
    auto_lifetime_rides

FROM datasets.iallocator_customer_segments
WHERE 
    run_date BETWEEN DATE_FORMAT(DATE_TRUNC('month', {{start_date}}), '%Y-%m-%d')
    AND DATE_FORMAT(DATE_TRUNC('month', {{end_date}}) , '%Y-%m-%d') 
    AND run_date = DATE_FORMAT(DATE_TRUNC('month', date(run_date)), '%Y-%m-%d')
    AND (auto_lifetime_rides > 0 or auto_lifetime_rides is not null or auto_lifetime_last_ride_date is not null)
    AND [[ {{city}} --]] taxi_lifetime_last_ride_city = 'Delhi'
--GROUP BY 1,2
),

base_month_segment AS
(
SELECT 
    DATE_FORMAT(DATE_TRUNC('month', DATE(month)) , '%Y-%m-%d') AS month,
    customer_id,
    city,
    CASE
    WHEN auto_lifetime_rides IS NULL THEN 'LTR=0'
    WHEN auto_lifetime_rides BETWEEN 1 AND 4 THEN '1-4'
    WHEN (auto_lifetime_rides BETWEEN 5 AND 20) AND (auto_lifetime_last_ride_date >= (DATE_TRUNC('month', DATE(month)) - INTERVAL '30' DAY)) THEN '5-20'
    WHEN (auto_lifetime_rides BETWEEN 21 AND 50) AND (auto_lifetime_last_ride_date >= (DATE_TRUNC('month', DATE(month)) - INTERVAL '30' DAY)) THEN '21-50'
    WHEN (auto_lifetime_rides > 50) AND (auto_lifetime_last_ride_date >= (DATE_TRUNC('month', DATE(month)) - INTERVAL '30' DAY)) THEN '50+'
    WHEN auto_lifetime_rides >= 5 THEN '>=5'
    ELSE 'LTR=0'
    END ltr_auto,
    CASE 
    WHEN auto_lifetime_last_ride_date IS NULL THEN 'LTR=0'
    WHEN auto_lifetime_last_ride_date >= (DATE_TRUNC('month', DATE(month)) - INTERVAL '30' DAY) THEN '1. LAST_30_DAYS'
    WHEN auto_lifetime_last_ride_date BETWEEN (DATE_TRUNC('month', DATE(month)) - INTERVAL '90' DAY) AND (DATE_TRUNC('month', DATE(month)) - INTERVAL '31' DAY) THEN '2. 31_90_DAYS'
    WHEN auto_lifetime_last_ride_date BETWEEN (DATE_TRUNC('month', DATE(month)) - INTERVAL '180' DAY) AND (DATE_TRUNC('month', DATE(month)) - INTERVAL '91' DAY) THEN '3. 91_180_DAYS'
    WHEN auto_lifetime_last_ride_date BETWEEN (DATE_TRUNC('month', DATE(month)) - INTERVAL '365' DAY) AND (DATE_TRUNC('month', DATE(month)) - INTERVAL '181' DAY) THEN '4. 181_365_DAYS'
    WHEN auto_lifetime_last_ride_date < (DATE_TRUNC('month', DATE(month)) - INTERVAL '365' DAY) THEN '5. 365_ABOVE'
    ELSE 'LTR=0'
    END recency_auto
FROM base_month_customer
),

base_month_total AS
(
SELECT 
    DATE_FORMAT(DATE_TRUNC('month', DATE(month)) , '%Y-%m-%d') AS month,
    city,
    COUNT(DISTINCT customer_id) existing_custr_total 
FROM base_month_customer
GROUP BY 1,2
),

customer_rf_daily_kpi AS
(
SELECT 
    day,
    city,
    customerid,
    rr_sessions_unique_daily,
    net_rides_daily,
    ao_sessions_unique_daily,
    fe_sessions_unique_daily
        
FROM datasets.customer_rf_daily_kpi
WHERE 
    day BETWEEN DATE_FORMAT(DATE_TRUNC('month', {{start_date}}), '%Y-%m-%d')
    AND DATE_FORMAT(DATE_TRUNC('month', {{end_date}} + INTERVAL '28' DAY) - INTERVAL '1' DAY, '%Y-%m-%d')
    AND service_name = 'Auto'
    AND customerid IS NOT NULL
    AND city IN (SELECT DISTINCT city FROM base_month_customer )
),

rr_net AS
(
SELECT 
    DATE_FORMAT(DATE_TRUNC('month', DATE(day)), '%Y-%m-%d') AS month,
    city,
    customerid as customer_id,
    SUM(rr_sessions_unique_daily) rr,
    SUM(net_rides_daily) net_rides
FROM customer_rf_daily_kpi
WHERE 
    day BETWEEN DATE_FORMAT(DATE_TRUNC('month', {{start_date}}), '%Y-%m-%d') 
    AND DATE_FORMAT(DATE_TRUNC('month', {{end_date}} + INTERVAL '28' DAY) - INTERVAL '1' DAY, '%Y-%m-%d')
GROUP BY 1,2,3
),

max_daily_ao AS
(
SELECT 
        day,
        city,
        customer_id,
        SUM(ao) AS ao,
        SUM(fe) AS fe
FROM
    (
    SELECT 
        customerid as customer_id,
        city,
        day,
        max(ao_sessions_unique_daily) AS ao,
        max(fe_sessions_unique_daily) AS fe
    FROM datasets.customer_rf_daily_kpi
    WHERE 
        day BETWEEN DATE_FORMAT(DATE_TRUNC('month', {{start_date}}), '%Y-%m-%d') 
        AND DATE_FORMAT(DATE_TRUNC('month', {{end_date}} + INTERVAL '28' DAY) - INTERVAL '1' DAY, '%Y-%m-%d')
        AND customerid IS NOT NULL
        AND city IN (SELECT DISTINCT city FROM base_month_customer )
    GROUP BY 1,2,3
    )
GROUP BY 1,2,3
),

monthly_ao AS
(
SELECT  
    DATE_FORMAT(DATE_TRUNC('month', DATE(day)), '%Y-%m-%d') AS month,
    city,
    customer_id,
    SUM(ao) AS ao,
    SUM(fe) AS fe
FROM max_daily_ao
GROUP BY 1,2,3
),

base_current AS (
SELECT 
        COALESCE(b.city,c.city,r.city) city,
        COALESCE(b.month,c.month,r.month) month,
        -- b.month,
        COALESCE(b.customer_id,c.customer_id,r.customer_id) customers,
        b.customer_id bcustomer_id,
        COALESCE(b.ltr_auto,'NEW') ltr_auto,
        COALESCE(b.recency_auto, 'NEW') recency_auto,
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
        
        COALESCE(r.net_rides, 0) net_rides

FROM base_month_segment AS b
FULL JOIN monthly_ao AS c ON b.customer_id = c.customer_id AND b.month = c.month AND b.city = c.city
FULL JOIN rr_net AS r ON b.customer_id = r.customer_id AND c.month = r.month AND b.city = r.city

),

detailed_view AS 
(
SELECT 
        c.month,
        c.city,
        ltr_auto,
        case 
        when ltr_auto = '1-4' then 1
        when ltr_auto = '5-20' then 2
        when ltr_auto = '21-50' then 3
        when ltr_auto = '50+' then 4
        when ltr_auto = '>=5' then 5
        when ltr_auto = 'LTR=0' then 6
        when ltr_auto = 'NEW' then 7
        end row_order,
        recency_auto,
        COUNT(DISTINCT bcustomer_id) existing_custr_count,
        COUNT(DISTINCT CASE WHEN ao > 0 THEN acustomer_id END) ao_users,
        COUNT(DISTINCT CASE WHEN fe > 0 THEN acustomer_id END) fe_users,
        COUNT(DISTINCT CASE WHEN rr > 0 THEN rcustomer_id END) rr_users,
        COUNT(DISTINCT CASE WHEN net_rides > 0 THEN rcustomer_id END) net_users,
        SUM(net_rides) net_rides
        
FROM base_current c
GROUP BY 1,2,3,4,5
),

summary_view AS (
SELECT 
        c.month,
        c.city,
        COUNT(DISTINCT CASE WHEN net_rides > 0 THEN c.customers END) net_users,
        SUM(net_rides) net_rides
FROM base_current c
GROUP BY 1,2
),

a as 
(
SELECT  
        a.month,
        a.city,
        a.row_order,
        a.ltr_auto,
        a.recency_auto,
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
        COALESCE(try(a.net_rides*1.00/a.net_users), 0) rpc
        
FROM detailed_view a
LEFT JOIN base_month_total b ON a.month = b.month AND a.city = b.city
LEFT JOIN summary_view c ON a.month = c.month AND a.city = c.city
ORDER BY 1,2,3,4
),

b as 
(
SELECT
        month,
        city,
        8 row_order,
        'TOTAL' ltr_auto,
        '' recency_auto,
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
        COALESCE(try(SUM(net_rides)*1.0/SUM(net_users)), 0) rpc
FROM a
GROUP BY 1,2
),

final as 
(
select * from a
union
select * from b
)
select 
        month,
        city,
        ltr_auto,recency_auto,
        existing_custr_count,existing_custr_distr,
        ao_users,ao_conversion,
        fe_users,fe_conversion,
        rr_users,rr_conversion,fe_rr "fe_rr(users)",
        net_users,net_conversion, rr_net "rr_net(users)", net_users_distr,
        net_rides,net_rides_distr,
        rpc 
from final
order by 1,2,row_order,4