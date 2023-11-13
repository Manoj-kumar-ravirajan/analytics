with v1 AS
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
    order by week_start_date
    ),

v2 as
    (
    select timeframe,
        sum(cast(auto_rr as double))/sum(cast(auto_fe as double)) as auto_fe2rr,
        sum(cast(morning_link_rr as double))/(sum(cast(morning_auto_rr as double))+sum(cast(morning_link_rr as double))) as morning_link_rr_pct,
        sum(cast(afternoon_link_rr as double))/(sum(cast(afternoon_auto_rr as double))+sum(cast(afternoon_link_rr as double))) as afternoon_link_rr_pct,
        sum(cast(evening_link_rr as double))/(sum(cast(evening_auto_rr as double))+sum(cast(evening_link_rr as double))) as evening_link_rr_pct,
        sum(cast(rest_link_rr as double))/(sum(cast(rest_auto_rr as double))+sum(cast(rest_link_rr as double))) as night_link_rr_pct,
        sum(cast(morning_auto_rr as double))/sum(cast(morning_auto_fe as double)) as morning_auto_fe2rr,
        sum(cast(afternoon_auto_rr as double))/sum(cast(afternoon_auto_fe as double)) as afternoon_auto_fe2rr,
        sum(cast(evening_auto_rr as double))/sum(cast(evening_auto_fe as double)) as evening_auto_fe2rr,
        sum(cast(rest_auto_rr as double))/sum(cast(rest_auto_fe as double)) as night_auto_fe2rr
    from v1
    group by timeframe
    order by timeframe
    ),

v3 as
    (
    select *, (ThisWeek)-(Last3Week) as Delta_1, (ThisWeek)-(TargetWeek) as Delta_2 
    from
        (
        select kpi,
            kv['ThisWeek'] AS ThisWeek,
            kv['Last3Week'] AS Last3Week,
            kv['TargetWeek'] AS TargetWeek
        from
            (
            select kpi, map_agg(timeframe, value) as kv
            from v2
            cross join unnest(array['auto_fe2rr', 'morning_link_rr_pct', 'afternoon_link_rr_pct', 'evening_link_rr_pct', 'night_link_rr_pct', 'morning_auto_fe2rr', 'afternoon_auto_fe2rr', 'evening_auto_fe2rr', 'night_auto_fe2rr'],
                array[auto_fe2rr, morning_link_rr_pct, afternoon_link_rr_pct, evening_link_rr_pct, night_link_rr_pct, morning_auto_fe2rr, afternoon_auto_fe2rr, evening_auto_fe2rr, night_auto_fe2rr]) as t(kpi, value)
            group by 1
            )
        )
    ),

v4 as
    (
    select Time_Frame,
        kv['auto_fe2rr'] as auto_fe2rr,
        kv['morning_link_rr_pct'] as morning_link_rr_pct,
        kv['morning_auto_fe2rr'] as morning_auto_fe2rr,
        kv['afternoon_link_rr_pct'] as afternoon_link_rr_pct,
        kv['afternoon_auto_fe2rr'] as afternoon_auto_fe2rr,
        kv['evening_link_rr_pct'] as evening_link_rr_pct,
        kv['evening_auto_fe2rr'] as evening_auto_fe2rr,
        kv['night_link_rr_pct'] as night_link_rr_pct,
        kv['night_auto_fe2rr'] as night_auto_fe2rr
    from
        (
        select Time_Frame, map_agg(kpi, value) as kv
        from v3
        cross join unnest(array[ '1. Delta_1', '2. Delta_2', '3. ThisWeek', '4. Last3Week', '5. TargetWeek'],
            array[Delta_1, Delta_2, ThisWeek, Last3Week, TargetWeek]) as t(Time_Frame, value)
        group by 1
        )
    )

select 'India' as city, * 
from v4 
order by Time_Frame