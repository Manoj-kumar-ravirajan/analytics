with fe_rr as
  (select *
   from
     (select date_format(Week, '%M %d, %Y') || ' - ' || date_format(date_add('day', day_of_week(current_date - interval '1' day) -1, Week), '%M %d, %Y') as Week,
             city,
             FE_Users,
             RR_Users,
             Conversion as "FE-RR Conversion",
                           avg(case
                                   when row >1 then cast(Conversion as double)
                               end) over(partition by city
                                         order by Week) as "FE-RR Benchmark",
                           Conversion - avg(case
                                                when row >1 then cast(Conversion as double)
                                            end) over(partition by city
                                                      order by Week) as "FE-RR Compare to Benchmark",
                                        row
      from
        (select date_fe as Week,
                fe_users as FE_Users,
                rr_users as RR_Users,
                Conversion,
                geo_value as city,
                conversion - lag(conversion) over(
                                                  order by date_fe asc) as "Compared to previous week",
                             row_number() over(partition by geo_value
                                               order by date_fe desc) as row
         from
           (select date_trunc('week', date_parse(time_value, '%Y-%m-%d-00-00')) as date_fe,
                   geo_value,
                   sum(fe_users) as fe_users,
                   sum(rr_users) as rr_users,
                   cast(sum(rr_users) as double)/sum(fe_users) as conversion
            from datasets.funnel_servicelevel_kpi
            where time_value >= date_format(date_trunc('week', date_add('week', -4, current_date)), '%Y-%m-%d-00-00')
              and time_value <= date_format(current_date - interval '1' day, '%Y-%m-%d-00-00')
              and day_of_week(date_parse(time_value, '%Y-%m-%d-00-00')) <= day_of_week(current_date - interval '1' day)
              and time_level = 'daily'
              and geo_level = 'city'
              and geo_value in ('bangalore',
                                'chandigarh',
                                'chennai',
                                'delhi',
                                'guwahati',
                                'hyderabad',
                                'jaipur',
                                'kolkata',
                                'lucknow',
                                'patna',
                                'vijayawada',
                                'vishakapatnam')
              and service_name = 'Link'
            group by 1,
                     2)))
   where row = 1 ),
     orders as
  (select customer_id,
          yyyymmdd,
          order_id,
          city_name,
          order_status -- max(yyyymmdd), min(yyyymmdd)
from orders.order_logs_snapshot
   where yyyymmdd >= date_format(date_trunc('week', date_add('week', -5, current_date)), '%Y%m%d')
     and yyyymmdd <= date_format(current_date - interval '1' day, '%Y%m%d')
     and order_type = 'app'
     and day_of_week(date_parse(yyyymmdd, '%Y%m%d')) <= day_of_week(current_date - interval '1' day)
     and city_name in ('Bangalore',
                       'Chandigarh',
                       'Chennai',
                       'Delhi',
                       'Guwahati',
                       'Hyderabad',
                       'Jaipur',
                       'Kolkata',
                       'Lucknow',
                       'Patna',
                       'Vijayawada',
                       'Vishakapatnam') -- and city_name in ('Kolkata')
) ,
     RPC as
  (select *
   from
     (select -- orderweek,
 date_format(orderweek, '%M %d, %Y') || ' - ' || date_format(date_add('day', day_of_week(current_date - interval '1' day) -1, orderweek), '%M %d, %Y') as Week,
 city,
 customers,
 net_orders,
 RPC,
 G2N,
 gross_requests,
 avg(case
         when row >1 then RPC
     end) over(
               order by orderweek) as "RPC Benchmark",
 (RPC - avg(case
                when row >1 then RPC
            end) over(
                      order by orderweek))/avg(case
                                                   when row >1 then RPC
                                               end) over(
                                                         order by orderweek) as "RPC Compare to Benchmark",
 avg(case
         when row >1 then G2N
     end) over(
               order by orderweek) as "G2N Benchmark",
 (G2N - avg(case
                when row >1 then G2N
            end) over(
                      order by orderweek)) as "G2N Compare to Benchmark",
 row
      from
        (select date_trunc('week', date_parse(yyyymmdd, '%Y%m%d')) as orderweek,
                city_name as city,
                count(distinct case
                                   when order_status = 'dropped' then customer_id
                               end) as customers,
                count(distinct case
                                   when order_status = 'dropped' then order_id
                               end) as net_orders,
                cast(count(distinct case
                                        when order_status = 'dropped' then order_id
                                    end) as double)/count(distinct case
                                                                       when order_status = 'dropped' then customer_id
                                                                   end) as RPC,
                count(distinct order_id) as gross_requests,
                cast(count(distinct case
                                        when order_status = 'dropped' then order_id
                                    end) as double)/ count(distinct order_id) as G2N,
                rank() over(
                            order by date_trunc('week', date_parse(yyyymmdd, '%Y%m%d')) desc) as row
         from orders
         group by 1,
                  2))
   where row =1 ) ,
     retention as
  (with w_1 as
     (select distinct customer_id as w1_id,
                      date_trunc('week', date_parse(yyyymmdd, '%Y%m%d')) as w1_week,
                      city_name
      from orders
      where yyyymmdd >= date_format(date_add('week', -5, date_trunc('week', current_date)), '%Y%m%d')
        and yyyymmdd <= date_format(current_date - interval '8' day, '%Y%m%d')
        and order_status = 'dropped' ),
        w_0 as
     (select distinct customer_id,
                      date_trunc('week', date_parse(yyyymmdd, '%Y%m%d')) as w0_week,
                      city_name
      from orders
      where yyyymmdd >= date_format(date_add('week', -4, date_trunc('week', current_date)), '%Y%m%d')
        and yyyymmdd <= date_format(current_date - interval '1' day, '%Y%m%d')
        and order_status = 'dropped' ) select *
   from
     (select date_format(Week, '%M %d, %Y') || ' - ' || date_format(date_add('day', day_of_week(current_date - interval '1' day) -1, Week), '%M %d, %Y') as Week,
             city,
             "W-1",
             W0,
             Retention, -- "Compare to previous Week",
 avg(case
         when row >1 then Retention
     end) over(partition by city
               order by week) as "Retention Benchmark",
 Retention - avg(case
                     when row >1 then Retention
                 end) over(partition by city
                           order by week) as "Retention Compare to Benchmark",
 row
      from
        (select date_trunc('week', w1_week + interval '7' day) as Week,
                w_1.city_name as city,
                count(w1_week) as "W-1",
                count(w0_week) as W0,
                cast(count(w0_week) as double)/count(w1_week) as Retention,
                cast(count(w0_week) as double)/count(w1_week) - lag(cast(count(w0_week) as double)/count(w1_week)) over(
                                                                                                                        order by date_trunc('week', w1_week + interval '7' day)) as "Compare to previous Week",
                rank() over(
                            order by date_trunc('week', w1_week + interval '7' day) desc) as row
         from w_1
         left join w_0 on w_1.w1_id = w_0.customer_id
         and date_trunc('week', date_add('week', 1, w_1.w1_week)) = w_0.w0_week
         group by 1,
                  2))
   where row = 1 ) -- discount as (


select fe_rr.Week as "Week Till Date",
       fe_rr.City,
       cast(round("FE-RR Conversion"*100.00, 1) as varchar) || '%' as "FE-RR Conversion",
       "FE-RR Compare to Benchmark",
       cast(round("Retention"*100.00, 1) as varchar) || '%' as "Week-1 Retention",
       "Retention Compare to Benchmark" as "Week-1 Retention Compare to Benchmark",
       RPC,
       "RPC Compare to Benchmark",
       cast(round(G2N*100.00, 1) as varchar) || '%' as G2N,
       "G2N Compare to Benchmark" -- cast(round("Discount %"*100.00, 1) as varchar) || '%' as "Discount %", "Disc % Compare to Benchmark",
 -- cast(round("DPR"*100.00, 1) as varchar) || '%' as "DPR", "DPR Compare to Benchmark"
 -- cast(round("Discount Rides %"*100.00, 1) as varchar) || '%' as "Discount Rides %", "Disc Rides % Compare to Benchmark",
 -- cast(round("Pass Rides %"*100.00, 1) as varchar) || '%' as "Pass Rides %", "Pass Rides % Compare to Benchmark"
from fe_rr
join retention on fe_rr.week = retention.week
and lower(fe_rr.city) = lower(retention.city)
join RPC on fe_rr.week = RPC.week
and lower(fe_rr.city) = lower(RPC.city) -- join discount on fe_rr.week = discount.week and lower(fe_rr.city) = lower(discount.city)
 -- join disc_pass_rides on fe_rr.week = disc_pass_rides.orderweek
order by 2