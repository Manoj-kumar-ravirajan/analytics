with base0 as
  (
  select 
          day,
          --city,
          customerid as customers,
          sum(rr_sessions_unique_daily) rr
   from datasets.customer_rf_daily_kpi
   where  
        day between date_format({{offer_start_date}}, '%Y-%m-%d') AND date_format({{offer_end_date}}, '%Y-%m-%d') 
        AND service_name in ('Link','Auto')
        --and {{City}}
   group by 1,2
   ),

customers as
  (
  select    

        day,
        count(distinct case when rr > 0 then customers end) as RR_Users
   from base0
   group by 1
   )
SELECT * from customers
ORDER BY 1

-----------------------------------
SELECT
    day,
    count(distinct case when rr > 0 then customers end) as RR_Users
FROM
  (
  select 
          day,
          --city,
          customerid as customers,
          sum(rr_sessions_unique_daily) rr
   from datasets.customer_rf_daily_kpi
   where  
        day between date_format({{offer_start_date}}, '%Y-%m-%d') AND date_format({{offer_end_date}}, '%Y-%m-%d') 
        AND service_name in ('Link','Auto')
        --and {{City}}
   group by 1,2
   )A
group by 1