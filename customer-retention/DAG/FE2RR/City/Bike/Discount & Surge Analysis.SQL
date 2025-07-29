with iprice_city AS 
    (
    SELECT distinct(City) AS city 
    FROM hive.experiments.iprice_city_config
    WHERE 1=1 [[ AND {{city}}]]
    ),

v1 AS
    (
    select *,
        case when week(week_start_date) = week({{start_date}}) and year(week_start_date) = year({{start_date}}) then 'ThisWeek'
            when (week({{start_date}}) = week(week_start_date) + 1 and year({{start_date}}) = year(week_start_date)) or (week({{start_date}}) = week(week_start_date) + 2 and year({{start_date}}) = year(week_start_date)) or (week({{start_date}}) = week(week_start_date) + 3 and year({{start_date}}) = year(week_start_date)) then 'Last3Week'
            when week(week_start_date) = week({{target_date}})  and year({{target_date}}) = year(week_start_date) then 'TargetWeek'
        end as timeframe
    from reports_internal.sql_ingest_city_fe2rr_diagnosis_v0 
    where 
        ((week(week_start_date) = week({{start_date}}) and year(week_start_date) = year({{start_date}})) or (week({{start_date}}) = week(week_start_date) + 1 and year({{start_date}}) = year(week_start_date)) 
        or (week({{start_date}}) = week(week_start_date) + 2 and year({{start_date}}) = year(week_start_date)) or (week({{start_date}}) = week(week_start_date) + 3 and year({{start_date}}) = year(week_start_date))
        or (week(week_start_date) = week({{target_date}})  and year({{target_date}}) = year(week_start_date)))
        and city in (select city from iprice_city)
    order by week_start_date
    ),

v2 as
    (
    select city, timeframe,
        sum(cast(link_rr as double))/sum(cast(link_fe as double)) as link_fe2rr,
        sum(cast(discounted_link_fe as double))/sum(cast(link_fe as double)) as discounted_link_fe_pct,
        sum(cast(link_discount as double))/sum(cast(link_subtotal as double)) as link_discount_pct,
        sum(cast(surged_link_fe as double))/sum(cast(link_fe as double)) as surged_link_fe_pct,
        sum(cast(link_surge as double))/sum(cast(link_subtotal as double)) as link_surge_pct
    from v1
    group by city, timeframe
    order by city, timeframe
    ),

v3 as
    (
    select *, (ThisWeek)-(Last3Week) as Delta_1, (ThisWeek)-(TargetWeek) as Delta_2 
    from
        (
        select city, kpi,
            kv['ThisWeek'] AS ThisWeek,
            kv['Last3Week'] AS Last3Week,
            kv['TargetWeek'] AS TargetWeek
        from
            (
            select city, kpi, map_agg(timeframe, value) as kv
            from v2
            cross join unnest(array['link_fe2rr', 'discounted_link_fe_pct', 'link_discount_pct', 'surged_link_fe_pct', 'link_surge_pct'],
                array[link_fe2rr, discounted_link_fe_pct, link_discount_pct, surged_link_fe_pct, link_surge_pct]) as t(kpi, value)
            group by 1,2
            )
        )
    ),

v4 as
    (
    select city,
        Time_Frame,
        kv['link_fe2rr'] as link_fe2rr,
        kv['discounted_link_fe_pct'] as discounted_link_fe_pct,
        kv['link_discount_pct'] as link_discount_pct,
        kv['surged_link_fe_pct'] as surged_link_fe_pct,
        kv['link_surge_pct'] as link_surge_pct
    from
        (
        select city,
            Time_Frame, map_agg(kpi, value) as kv
        from v3
        cross join unnest(array[ '1. Delta_1', '2. Delta_2', '3. ThisWeek', '4. Last3Week', '5. TargetWeek'],
            array[Delta_1, Delta_2, ThisWeek, Last3Week, TargetWeek]) as t(Time_Frame, value)
        group by 1,2
        ) 
    order by city
    )

select * 
from v4 
order by city, Time_Frame