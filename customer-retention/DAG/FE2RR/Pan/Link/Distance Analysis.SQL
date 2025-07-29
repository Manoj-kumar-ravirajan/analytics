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
        sum(cast(link_rr as double))/sum(cast(link_fe as double)) as link_fe2rr,
        sum(cast(zero_two_auto_rr as double))/(sum(cast(zero_two_auto_rr as double))+sum(cast(zero_two_link_rr as double))) as zero_two_auto_rr_pct,
        sum(cast(two_four_auto_rr as double))/(sum(cast(two_four_auto_rr as double))+sum(cast(two_four_link_rr as double))) as two_four_auto_rr_pct,
        sum(cast(four_six_auto_rr as double))/(sum(cast(four_six_auto_rr as double))+sum(cast(four_six_link_rr as double))) as four_six_auto_rr_pct,
        sum(cast(six_eight_auto_rr as double))/(sum(cast(six_eight_auto_rr as double))+sum(cast(six_eight_link_rr as double))) as six_eight_auto_rr_pct,
        sum(cast(eight_ten_auto_rr as double))/(sum(cast(eight_ten_auto_rr as double))+sum(cast(eight_ten_link_rr as double))) as eight_ten_auto_rr_pct,
        sum(cast(ten_plus_auto_rr as double))/(sum(cast(ten_plus_auto_rr as double))+sum(cast(ten_plus_link_rr as double))) as ten_plus_auto_rr_pct,
        sum(cast(zero_two_link_rr as double))/sum(cast(zero_two_link_fe as double)) as zero_two_link_fe2rr,
        sum(cast(two_four_link_rr as double))/sum(cast(two_four_link_fe as double)) as two_four_link_fe2rr,
        sum(cast(four_six_link_rr as double))/sum(cast(four_six_link_fe as double)) as four_six_link_fe2rr,
        sum(cast(six_eight_link_rr as double))/sum(cast(six_eight_link_fe as double)) as six_eight_link_fe2rr,
        sum(cast(eight_ten_link_rr as double))/sum(cast(eight_ten_link_fe as double)) as eight_ten_link_fe2rr,
        sum(cast(ten_plus_link_rr as double))/sum(cast(ten_plus_link_fe as double)) as ten_plus_link_fe2rr
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
        cross join unnest(array['link_fe2rr', 'zero_two_auto_rr_pct', 'two_four_auto_rr_pct', 'four_six_auto_rr_pct', 'six_eight_auto_rr_pct', 'eight_ten_auto_rr_pct', 'ten_plus_auto_rr_pct', 
            'zero_two_link_fe2rr', 'two_four_link_fe2rr', 'four_six_link_fe2rr', 'six_eight_link_fe2rr', 'eight_ten_link_fe2rr', 'ten_plus_link_fe2rr'],
            array[link_fe2rr, zero_two_auto_rr_pct, two_four_auto_rr_pct, four_six_auto_rr_pct, six_eight_auto_rr_pct, eight_ten_auto_rr_pct, ten_plus_auto_rr_pct, 
            zero_two_link_fe2rr, two_four_link_fe2rr, four_six_link_fe2rr, six_eight_link_fe2rr, eight_ten_link_fe2rr, ten_plus_link_fe2rr]) as t(kpi, value)
        group by 1
        )
        )
    ),

v4 as
    (
    select Time_Frame,
        kv['link_fe2rr'] as link_fe2rr,
        kv['zero_two_auto_rr_pct'] as zero_two_auto_rr_pct,
        kv['zero_two_link_fe2rr'] as zero_two_link_fe2rr,
        kv['two_four_auto_rr_pct'] as two_four_auto_rr_pct,
        kv['two_four_link_fe2rr'] as two_four_link_fe2rr,
        kv['four_six_auto_rr_pct'] as four_six_auto_rr_pct,
        kv['four_six_link_fe2rr'] as four_six_link_fe2rr,
        kv['six_eight_auto_rr_pct'] as six_eight_auto_rr_pct,
        kv['six_eight_link_fe2rr'] as six_eight_link_fe2rr,
        kv['eight_ten_auto_rr_pct'] as eight_ten_auto_rr_pct,
        kv['eight_ten_link_fe2rr'] as eight_ten_link_fe2rr,
        kv['ten_plus_auto_rr_pct'] as ten_plus_auto_rr_pct,
        kv['ten_plus_link_fe2rr'] as ten_plus_link_fe2rr
    from
        (
        select Time_Frame, map_agg(kpi, value) as kv
        from v3
        cross join unnest(array[ '1. Delta_1', '2. Delta_2', '3. ThisWeek', '4. Last3Week', '5. TargetWeek'],
        array[Delta_1, Delta_2, ThisWeek, Last3Week, TargetWeek]) as t(Time_Frame, value)
        group by 1
        )
    )

select 'India' as city, * from v4 order by Time_Frame