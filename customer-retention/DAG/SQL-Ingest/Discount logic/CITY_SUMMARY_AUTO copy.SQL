with base0 as
(
select
extract(week from cast(day as date)) OrderDate,
DATE_FORMAT(DATE_TRUNC('week', DATE(substr(cast(day as varchar),1,10))), '%Y-%m-%d') AS StartDate,
city,
customerid as customers,
MAX(day) AS EndDate,
sum(rr_sessions_unique_daily) rr,
sum(gross_rides_daily) gross_rides,
sum(net_rides_daily) net_rides,
sum(subscription_rides_daily) subs_ride,
sum(case when subscription_rides_daily>0 then burn_daily end) as subs_discount,
sum(subtotal_daily) subtotal,
sum(discount_daily) burn
from datasets.customer_rf_daily_kpi
where
day between DATE_FORMAT({{start_date}}, '%Y-%m-%d') and DATE_FORMAT({{end_date}}, '%Y-%m-%d')
and service_name in ('Auto')
and city in ('Delhi','Hyderabad','Bangalore','Chennai','Jaipur','Mumbai','Vishakapatnam','Vijayawada','Lucknow','Pune','Ahmedabad','Indore','Mysore','Bhopal','Tirupati','Guntur','Coimbatore','Chandigarh','Bhubaneswar','Madurai')
group by 1,2,3,4
),
max_daily_ao as
(
select  city,
customerid,
day,
sum(ao) as ao,
sum(fe) as fe
from
(
select  city,
customerid,
day,
max(ao_sessions_unique_daily) as ao,
max(fe_sessions_unique_daily) as fe
from datasets.customer_rf_daily_kpi
where
day between DATE_FORMAT({{start_date}}, '%Y-%m-%d') and DATE_FORMAT({{end_date}}, '%Y-%m-%d')
and city in (select distinct city from base0 )
group by 1,2,3
order by 1,2
)
group by 1,2,3
order by 1,2
),
weekly_ao as
(
select
extract(week from cast(day as date)) as week,
DATE_FORMAT(DATE_TRUNC('week', DATE(substr(day,1,10))), '%Y-%m-%d') Start,
city as finalcity,
customerid,
max(day) end_date,
sum(ao) ao,
sum(fe) fe
from max_daily_ao
group by 1,2,3,4
),
base as
(
select
a.*,
b.week,
b.Start,
b.end_date,
b.customerid,
b.finalcity,
b.ao,
b.fe
from base0 a right join weekly_ao b on a.customers = b.customerid and a.StartDate = b.Start and a.city = b.finalcity
),
customers as
(
select
StartDate as start,
finalcity,
max(EndDate) as end_date,
count(distinct customerid) as base,
count(distinct case when net_rides > 0 then customerid end) as net_customers,
count(distinct case when ao > 0 then customerid end) as AO_Cust,
count(distinct case when fe > 0 then customerid end) as FE_Cust,
count(distinct case when rr > 0 then customerid end) as RR_Cust
from base
group by 1,2
),
v0 as
(
select
week as OrderDate,
Start as StartDate,
finalcity as city,
max(EndDate) EndDate,
sum(ao) ao,
sum(fe) fe,
sum(rr) rr,
sum (gross_rides) gross_rides,
sum(net_rides) net_rides,
sum(subs_ride) subs_ride,
sum(subs_discount) subs_discount,
sum(subtotal) subtotal,
sum(burn) burn,
sum(burn)*100.0/sum(subtotal) as Discount,
sum(burn)*1.0/sum(net_rides) as DPR,
100*sum(rr)/cast(sum(fe) as float) as Fe_RR,
100*sum(subs_ride)/cast(sum(net_rides) as float) as Subs_Rides,
100*sum(net_rides )/cast(sum(gross_rides) as float) as G2N
from base
group by 1,2,3
),
v1 as
(
select
a.base,
a.net_customers,
a.AO_Cust,
a.FE_Cust,
a.RR_Cust,
b.*
from customers a right join v0 b on a.start = b.StartDate and a.finalcity = b.city
),
orders as
(
select *,
substr(cast(date_trunc('week',date_parse(yyyymmdd,'%Y%m%d')) as varchar),1,10) Start,
service_obj_city_display_name city2
from orders.order_logs_snapshot
where service_obj_service_name in ('Auto')
and service_obj_city_display_name in (select distinct city from base)
and order_id is not null
and service_obj_city_display_name is not null
and yyyymmdd between date_format({{start_date}} - interval '14' day,'%Y%m%d') and date_format({{end_date}},'%Y%m%d')
),
order_coin as
(
select
extract(week from cast(date_parse(yyyymmdd,'%Y%m%d') as date)) week_number,
substr(cast(date_trunc('week',date_parse(yyyymmdd,'%Y%m%d')) as varchar),1,10) StartDate,
owner_id customerid,
entity_id,
cast(json_extract(coin_wallet_changes, '$[0].offerType') as varchar) offerType,
coalesce(round(sum(case when transaction_type = 'credit' then cast(amount as double) end)),0) as coin_credited,
coalesce(round(sum(case when transaction_type = 'debit' and subtype != 'coinExpired' then cast(amount as double) end)),0) as coin_utilized,
coalesce(round(sum(case when transaction_type = 'debit' and subtype = 'coinExpired' then cast(amount as double) end)),0) as coin_expired
from payments.transactions_snapshot
where yyyymmdd between date_format({{start_date}},'%Y%m%d') and date_format({{end_date}},'%Y%m%d')
and owner_type ='customer'
and transaction_status = 'done'
and json_extract(coin_wallet_changes, '$[0].id') is not null
group by 1,2,3,4,5
),
service_total_coins as
(
select
StartDate,
week_number,
customerid,
city2,
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
group by 1,2,3,4,5
),
coin_credit as
(
select
Start,
week_number,
city,
sum(total_coin_credited) total_coin_credited,
sum(walletOffer_utilized) walletOffer_utilized,
sum(giftOffer_utilized) giftOffer_utilized,
sum(dashboardCoinsCredit_utilized) dashboardCoinsCredit_utilized
from
(
select
(case when a.city is null then b.city2 else a.city end) as city,
(case when cast(a.Week as varchar) is null then cast(b.week_number as varchar) else cast(a.Week as varchar) end) as Week,
(case when a.customerid is null then b.customerid else a.customerid end) as customerid,
(case when a.Start is null then b.StartDate else a.Start end) as Start,
b.week_number,
b.walletOffer_utilized,
b.giftOffer_utilized,
b.dashboardCoinsCredit_utilized,
b.total_coin_credited
from base a right join service_total_coins b on a.Start = b.StartDate and a.customerid = b.customerid and a.city = b.city2
)
where Start is not null
group by 1,2,3
),
coin_expired_customer_level as
(
select
StartDate as Start2,
city2,
sum(coin_expired) coin_expired
from order_coin a join orders b on a.entity_id = b.order_id
where 
coin_expired > 0
group by 1,2
),
agg_coin as
(
select
a.*,
b.coin_expired,
(coalesce(a.total_coin_credited,0) + coalesce(a.walletOffer_utilized,0) + coalesce(giftOffer_utilized,0) + coalesce(a.dashboardCoinsCredit_utilized,0) - coalesce(b.coin_expired,0)) total_coin_burn
from coin_credit a join coin_expired_customer_level b on a.Start = b.Start2 and a.city = b.city2
),
v1_with_coins as
(
select a.*, b.total_coin_burn Total_Coin_Burn
from v1 a join agg_coin b on a.StartDate = b.Start and a.city = b.city
),
discounted_rides as
(
select
DATE_FORMAT(DATE_TRUNC('week', DATE(substr(cast(date_parse(yyyymmdd, '%Y%m%d') as varchar),1,10))), '%Y-%m-%d') AS StartDate,
city_name,
count(distinct case when discount > 0 then order_id end) Discounted_Rides,
count(distinct case when discount > 0 then order_id end)*100.00/count(distinct order_id) Discounted_Rides_perc
from orders.order_logs_snapshot
where service_obj_service_name in ('Auto')
and city_name in (select distinct city from base)
and order_id is not null
and order_status = 'dropped'
and yyyymmdd between date_format({{start_date}},'%Y%m%d') and date_format({{end_date}},'%Y%m%d')
group by 1,2
),
final as
(
select
v1_with_coins.*,
discounted_rides.Discounted_Rides_perc as Discounted_Rides
from v1_with_coins join discounted_rides on v1_with_coins.StartDate = discounted_rides.StartDate and v1_with_coins.city = discounted_rides.city_name
),
final_with_allocator as
(
select
a.*
from final a
),
t1 AS
(
SELECT
customerid,
city,
DATE_TRUNC('week',CAST(day AS DATE)) AS orderdate1
from datasets.customer_rf_daily_kpi
where (service_name = 'Auto')
AND day BETWEEN DATE_FORMAT((DATE_TRUNC('week',{{start_date}}) - INTERVAL '14' day),'%Y-%m-%d') AND DATE_FORMAT({{end_date}},'%Y-%m-%d')
and net_rides_daily!=0
and city in (select distinct city from base0)
),
t2 as
(
SELECT
customerid,
city,
DATE_TRUNC('week',CAST(day AS DATE)) AS orderdate2
from datasets.customer_rf_daily_kpi
where (service_name = 'Auto')
AND day BETWEEN DATE_FORMAT((DATE_TRUNC('week',{{start_date}}) - INTERVAL '14' day),'%Y-%m-%d') AND DATE_FORMAT({{end_date}},'%Y-%m-%d')
and net_rides_daily!=0
and city in (select distinct city from base0)
),
t3 as
(
select date_format(orderdate1,'%Y-%m-%d') as orderdate3,
city,
count(distinct(customerid)) as total_cx
from t1
group by 1,2
order by 1,2
),
t4 as
(
select
t1.city,
date_format(orderdate1,'%Y-%m-%d') as orderdate1,
date_format(orderdate2,'%Y-%m-%d') as orderdate2,
count(distinct(t1.customerid)) as count_t1,
count(distinct(t1.customerid)) as count_t2
from t1 left join t2 on t1.customerid = t2.customerid and t1.city = t2.city and orderdate2 >= orderdate1
group by 1, 2,3
order by 1, 3,2
),
retention_final as
(
select
extract(week from cast(orderdate2 as date)) Retention_week,
t4.city,
orderdate1 Week_StartDate,
orderdate2,
case when orderdate1 = orderdate2 then 100.00 else (100.00*(t4.count_t1))/total_cx end as W1_retention
from t4 join t3 on t3.orderdate3 = t4.orderdate1 and t3.city = t4.city
where orderdate2 is not null
and orderdate2 !=orderdate3
and extract(week from cast(orderdate2 as date)) = extract(week from cast(orderdate1 as date)) + 1
order by 2, 4, 3
),
ride_metrics as
(
select
OrderDate,
StartDate,
EndDate,
city,
net_customers as Net_Customers,
net_rides as NetRides,
G2N,
net_rides*1.00/net_customers as RPC,
Discounted_Rides,
Discount as Discount_without_coins,
(Total_Coin_Burn + burn) as Discount_with_coins,
(Total_Coin_Burn + burn) * 100.0/subtotal Discount,
DPR,
Fe_RR
from final_with_allocator
),
metrics_w1_retention as
(
select
a.*,
b.W1_retention
from ride_metrics a join retention_final b on a.StartDate = b.orderdate2 and a.city = b.city
)
select
extract(year from cast(StartDate as date)) year,
OrderDate as week,
city,
StartDate as startdate,
EndDate as enddate,
W1_retention as w1_retention,
Net_Customers as net_customers,
NetRides as netrides,
G2N as g2n,
RPC as rides_per_customer,
Discounted_Rides as discounted_rides_percentage,
Discount_without_coins as discount_without_coins_percentage,
Discount as discount_percentage,
DPR as dpr_without_coin,
Discount_with_coins/NetRides as dpr_with_coin,
Fe_RR as fe_rr_percentage
from metrics_w1_retention
order by city,2 desc