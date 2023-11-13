with Base as 
    (
    select
        run_date,
        (extract(week from cast(run_date as date)) + 1) as Week,
        auto_lifetime_stage lifetime_stage,
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
        group by 1, 2,3
    ),

v2 as 
    (
        select 
            -- extract(week from cast(coalesce(date(day) + interval '1' day, week) as date)) as Week,
            Base.Week,
            Base.city,
            Base.lifetime_stage,
            count(distinct Base.customer_id) as Base,
            count(distinct case when net_rides > 0 then v01.customerid end) as Net_Customers,
            count(distinct case when net_rides > 0 then v01.customerid end)*100.00/count(distinct Base.customer_id)  "Conversion%",
            -- sum(gross_rides) as GrossRides,
            sum(net_rides) as NetRides,
            -- sum(net_rides_link) as NetRidesLink,
            sum(net_rides)*100/sum(gross_rides) "G2N%",
            sum(net_rides)*1.00/count(distinct case when net_rides > 0 then v01.customerid end) RPC,
            sum(sub_rides) as Subs_Rides,
            sum(burn) as Discount,
            sum(subs_discount) as Subs_Discount,
            sum(subtotal) as Subtotal,
            sum(subtotal)*1.0/sum(net_rides) as ATV,
            sum(burn)*100.0/sum(subtotal) as "Discount%",
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
        from Base 
        left join v01 on Base.customer_id = v01.customerid and Base.Week = v01.Week 
        group by 1, 2, 3
        order by 1,2,3
    ),

city_metric as 
    (
        select 
            -- extract(week from cast(coalesce(date(day) + interval '1' day, week) as date)) as Week,
            Base.Week,
            Base.city,
            'Z_PHH' as lifetime_stage,
            count(distinct Base.customer_id) as Base,
            count(distinct case when net_rides > 0 then v01.customerid end) as Net_Customers,
            count(distinct case when net_rides > 0 then v01.customerid end)*100.00/count(distinct Base.customer_id)  "Conversion%",
            -- sum(gross_rides) as GrossRides,
            sum(net_rides) as NetRides,
            -- sum(net_rides_link) as NetRidesLink,
            sum(net_rides)*100/sum(gross_rides) "G2N%",
            sum(net_rides)*1.00/count(distinct case when net_rides > 0 then v01.customerid end) RPC,
            sum(sub_rides) as Subs_Rides,
            sum(burn) as Discount,
            sum(subs_discount) as Subs_Discount,
            sum(subtotal) as Subtotal,
            sum(subtotal)*1.0/sum(net_rides) as ATV,
            sum(burn)*100.0/sum(subtotal) as "Discount%",
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
        from Base 
        left join v01 on Base.customer_id = v01.customerid and Base.Week = v01.Week 
        where Base.lifetime_stage != 'HANDHOLDING'
        group by 1, 2, 3
        order by 1,2,3
    )

select * from v2
union 
select * from city_metric

order by 1,2,3 desc
   
-- select * from Base
