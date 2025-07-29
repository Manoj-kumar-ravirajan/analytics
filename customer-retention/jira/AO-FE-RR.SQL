with base0 as
  (
    SELECT 
            DATE_FORMAT(DATE_TRUNC('month', DATE(substr(cast(day as varchar),1,10))), '%Y-%m-%d') as day,
            customerid as customers,
            SUM(rr_sessions_unique_daily) rr,
            SUM(net_rides_daily) net_rides
    FROM datasets.customer_rf_daily_kpi

    WHERE   date(day) >= date_trunc('month', {{Start_Date}}) 
            AND date(day) <= date_trunc('month', {{End_Date}} + interval '1' month)  - interval '1' day
            AND service_name IN ('Link')
    GROUP BY 1, 2
    HAVING SUM(net_rides_daily) <= 0
   ),
            
max_daily_ao as
  (select 
          customerid,
          day,
          sum(ao) as ao,
          sum(fe) as fe
   from
     (
     select 
             customerid,
             DATE_FORMAT(DATE_TRUNC('month', DATE(substr(cast(day as varchar),1,10))), '%Y-%m-%d') as day,
             max(ao_sessions_unique_daily) as ao,
             max(fe_sessions_unique_daily) as fe
      from datasets.customer_rf_daily_kpi
      where 1=1
        and date(day) >= date_trunc('month', {{Start_Date}}) 
        and date(day) <= date_trunc('month', {{End_Date}} + interval '1' month)  - interval '1' day
        and customerid in (select customers from base0)
      group by 1,2
      order by 1,2
      )
   group by 1,2
   order by 1,2
    ),

weekly_ao as
  (
  select  --extract(week from cast(day as date)) as week,
        --DATE_FORMAT(DATE_TRUNC('week', DATE(substr(day, 1, 10))), '%Y-%m-%d') Start, 
        day,
        customerid,
        sum(ao) ao,
        sum(fe) fe
   from max_daily_ao
   group by 1,2
   ),
   
base as
  (
    select a.*,
          --b.week, 
          b.day AS days, b.customerid, b.ao, b.fe
   from base0 a
   right join weekly_ao b on a.customers = b.customerid and a.day = b.day
   ),

customers as
  (
  select    
            DATE_FORMAT(DATE_TRUNC('month', DATE(substr(cast(days as varchar),1,10))), '%Y-%m-%d'),
            count(distinct case when net_rides > 0 then customerid end) as Net_Users,
            count(distinct case when ao > 0 then customerid end) as AO_Users,
            count(distinct case when fe > 0 then customerid end) as FE_Users,
            count(distinct case when rr > 0 then customerid end) as RR_Users
   from base
   group by 1
   )
SELECT * from customers
