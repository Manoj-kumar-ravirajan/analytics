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
        sum(cast(above_rc_auto_fe as double))/sum(cast(auto_fe as double)) as above_rc_auto_fe_pct,
        sum(cast(at_rc_auto_fe as double))/sum(cast(auto_fe as double)) as at_rc_auto_fe_pct,
        sum(cast(below_rc_auto_fe as double))/sum(cast(auto_fe as double)) as below_rc_auto_fe_pct,
        sum(cast(link_rr as double))/(sum(cast(auto_rr as double))+sum(cast(link_rr as double))) as link_rr_pct,
        sum(cast(discounted_auto_fe as double))/sum(cast(auto_fe as double)) as discounted_auto_fe_pct,
        sum(cast(auto_discount as double))/sum(cast(auto_subtotal as double)) as auto_discount_pct,
        sum(cast(surged_auto_fe as double))/sum(cast(auto_fe as double)) as surged_auto_fe_pct,
        sum(cast(auto_surge as double))/sum(cast(auto_subtotal as double)) as auto_surge_pct,
        sum(cast(zero_two_link_rr as double))/(sum(cast(zero_two_auto_rr as double))+sum(cast(zero_two_link_rr as double))) as zero_two_link_rr_pct,
        sum(cast(two_four_link_rr as double))/(sum(cast(two_four_auto_rr as double))+sum(cast(two_four_link_rr as double))) as two_four_link_rr_pct,
        sum(cast(four_six_link_rr as double))/(sum(cast(four_six_auto_rr as double))+sum(cast(four_six_link_rr as double))) as four_six_link_rr_pct,
        sum(cast(six_eight_link_rr as double))/(sum(cast(six_eight_auto_rr as double))+sum(cast(six_eight_link_rr as double))) as six_eight_link_rr_pct,
        sum(cast(eight_ten_link_rr as double))/(sum(cast(eight_ten_auto_rr as double))+sum(cast(eight_ten_link_rr as double))) as eight_ten_link_rr_pct,
        sum(cast(ten_plus_link_rr as double))/(sum(cast(ten_plus_auto_rr as double))+sum(cast(ten_plus_link_rr as double))) as ten_plus_link_rr_pct,
        sum(cast(morning_link_rr as double))/(sum(cast(morning_auto_rr as double))+sum(cast(morning_link_rr as double))) as morning_link_rr_pct,
        sum(cast(afternoon_link_rr as double))/(sum(cast(afternoon_auto_rr as double))+sum(cast(afternoon_link_rr as double))) as afternoon_link_rr_pct,
        sum(cast(evening_link_rr as double))/(sum(cast(evening_auto_rr as double))+sum(cast(evening_link_rr as double))) as evening_link_rr_pct,
        sum(cast(rest_link_rr as double))/(sum(cast(rest_auto_rr as double))+sum(cast(rest_link_rr as double))) as rest_link_rr_pct
    from v1
    group by timeframe
    ),

v3 as
    (
    select *, (ThisWeek)-(Last3Week) as Delta_1, (ThisWeek)-(TargetWeek) as Delta_2 
    from
        (
        select
        kpi,
        kv['ThisWeek'] AS ThisWeek,
        kv['Last3Week'] AS Last3Week,
        kv['TargetWeek'] AS TargetWeek
        from
            (
            select
            kpi, map_agg(timeframe, value) as kv
            from v2
            cross join unnest(array['auto_fe2rr', 'above_rc_auto_fe_pct', 'at_rc_auto_fe_pct', 'below_rc_auto_fe_pct', 'link_rr_pct', 'discounted_auto_fe_pct', 'auto_discount_pct', 'surged_auto_fe_pct', 'auto_surge_pct',
            'zero_two_link_rr_pct', 'two_four_link_rr_pct', 'four_six_link_rr_pct', 'six_eight_link_rr_pct', 'eight_ten_link_rr_pct', 'ten_plus_link_rr_pct', 'morning_link_rr_pct', 'afternoon_link_rr_pct', 'evening_link_rr_pct', 'rest_link_rr_pct'],
            array[auto_fe2rr, above_rc_auto_fe_pct, at_rc_auto_fe_pct, below_rc_auto_fe_pct, link_rr_pct, discounted_auto_fe_pct, auto_discount_pct, surged_auto_fe_pct, auto_surge_pct,
            zero_two_link_rr_pct, two_four_link_rr_pct, four_six_link_rr_pct, six_eight_link_rr_pct, eight_ten_link_rr_pct, ten_plus_link_rr_pct, morning_link_rr_pct, afternoon_link_rr_pct, evening_link_rr_pct, rest_link_rr_pct]) as t(kpi, value)
            group by 1
            )
        )
    ),

v4 as
    (
    select
        Time_Frame,
        kv['auto_fe2rr'] as auto_fe2rr,
        kv['above_rc_auto_fe_pct'] as above_rc_auto_fe_pct,
        kv['at_rc_auto_fe_pct'] as at_rc_auto_fe_pct,
        kv['below_rc_auto_fe_pct'] as below_rc_auto_fe_pct,
        kv['link_rr_pct'] as link_rr_pct,
        kv['discounted_auto_fe_pct'] as discounted_auto_fe_pct,
        kv['auto_discount_pct'] as auto_discount_pct,
        kv['surged_auto_fe_pct'] as surged_auto_fe_pct,
        kv['auto_surge_pct'] as auto_surge_pct,
        kv['zero_two_link_rr_pct'] as zero_two_link_rr_pct,
        kv['two_four_link_rr_pct'] as two_four_link_rr_pct,
        kv['four_six_link_rr_pct'] as four_six_link_rr_pct,
        kv['six_eight_link_rr_pct'] as six_eight_link_rr_pct,
        kv['eight_ten_link_rr_pct'] as eight_ten_link_rr_pct,
        kv['ten_plus_link_rr_pct'] as ten_plus_link_rr_pct,
        kv['morning_link_rr_pct'] as morning_link_rr_pct,
        kv['afternoon_link_rr_pct'] as afternoon_link_rr_pct,
        kv['evening_link_rr_pct'] as evening_link_rr_pct,
        kv['rest_link_rr_pct'] as rest_link_rr_pct
    from
        (
        select
            Time_Frame, map_agg(kpi, value) as kv
        from v3
        cross join unnest(array['1. Delta_1', '2. Delta_2', '3. ThisWeek', '4. Last3Week', '5. TargetWeek'],
        array[Delta_1, Delta_2, ThisWeek, Last3Week, TargetWeek]) as t(Time_Frame, value)
        group by 1
        )
    )

select 
    'India' as city, Time_Frame, auto_fe2rr,
    case when (Time_Frame='1. Delta_1' or Time_Frame='2. Delta_2') and below_rc_auto_fe_pct<0 then 'Below RC FEs decreased by ' || cast(abs(round(100*below_rc_auto_fe_pct,1)) as varchar) || '%'
        when (Time_Frame='1. Delta_1' or Time_Frame='2. Delta_2') and below_rc_auto_fe_pct>0 then 'Below RC FEs increased by ' || cast(abs(round(100*below_rc_auto_fe_pct,1)) as varchar) || '%'
    end as rate_card_summary,
    case when (Time_Frame='1. Delta_1' or Time_Frame='2. Delta_2') and discounted_auto_fe_pct<0 then 'Discounted FEs decreased by ' || cast(abs(round(100*discounted_auto_fe_pct,1)) as varchar) || '%'
        when (Time_Frame='1. Delta_1' or Time_Frame='2. Delta_2') and discounted_auto_fe_pct>0 then 'Discounted FEs increased by ' || cast(abs(round(100*discounted_auto_fe_pct,1)) as varchar) || '%'
    end as discounted_fe_summary,
    case when (Time_Frame='1. Delta_1' or Time_Frame='2. Delta_2') and auto_discount_pct<0 then 'Discount decreased by ' || cast(abs(round(100*auto_discount_pct,1)) as varchar) || '%'
        when (Time_Frame='1. Delta_1' or Time_Frame='2. Delta_2') and auto_discount_pct>0 then 'Discount increased by ' || cast(abs(round(100*auto_discount_pct,1)) as varchar) || '%'
    end as discount_summary,
    case when (Time_Frame='1. Delta_1' or Time_Frame='2. Delta_2') and surged_auto_fe_pct<0 then 'Surged FEs decreased by ' || cast(abs(round(100*surged_auto_fe_pct,1)) as varchar) || '%'
        when (Time_Frame='1. Delta_1' or Time_Frame='2. Delta_2') and surged_auto_fe_pct>0 then 'Surged FEs increased by ' || cast(abs(round(100*surged_auto_fe_pct,1)) as varchar) || '%'
    end as surged_fe_summary,
    case when (Time_Frame='1. Delta_1' or Time_Frame='2. Delta_2') and auto_surge_pct<0 then 'Surge decreased by ' || cast(abs(round(100*auto_surge_pct,1)) as varchar) || '%'
        when (Time_Frame='1. Delta_1' or Time_Frame='2. Delta_2') and auto_surge_pct>0 then 'Surge increased by ' || cast(abs(round(100*auto_surge_pct,1)) as varchar) || '%'
    end as surge_summary,
    case when (Time_Frame='1. Delta_1' or Time_Frame='2. Delta_2') and link_rr_pct<0 then 'Link RRs decreased by ' || cast(abs(round(100*link_rr_pct,1)) as varchar) || '%'
        when (Time_Frame='1. Delta_1' or Time_Frame='2. Delta_2') and link_rr_pct>0 then 'Link RRs increased by ' || cast(abs(round(100*link_rr_pct,1)) as varchar) || '%'
    end as link_rr_summary,
    case when Time_Frame='3. ThisWeek' and zero_two_link_rr_pct<ten_plus_link_rr_pct and two_four_link_rr_pct<eight_ten_link_rr_pct then 'Link RRs increases with increasing distance'
        when Time_Frame='3. ThisWeek' and zero_two_link_rr_pct>ten_plus_link_rr_pct and two_four_link_rr_pct>eight_ten_link_rr_pct then 'Link RRs decreases with increasing distance'
        when Time_Frame='4. Last3Week' and zero_two_link_rr_pct<ten_plus_link_rr_pct and two_four_link_rr_pct<eight_ten_link_rr_pct then 'Link RRs increases with increasing distance'
        when Time_Frame='4. Last3Week' and zero_two_link_rr_pct>ten_plus_link_rr_pct and two_four_link_rr_pct>eight_ten_link_rr_pct then 'Link RRs decreases with increasing distance'
        when Time_Frame='5. TargetWeek' and zero_two_link_rr_pct<ten_plus_link_rr_pct and two_four_link_rr_pct<eight_ten_link_rr_pct then 'Link RRs increases with increasing distance'
        when Time_Frame='5. TargetWeek' and zero_two_link_rr_pct>ten_plus_link_rr_pct and two_four_link_rr_pct>eight_ten_link_rr_pct then 'Link RRs decreases with increasing distance'
        when (Time_Frame='1. Delta_1' or Time_Frame='2. Delta_2') and abs(zero_two_link_rr_pct)=cast(greatest(abs(zero_two_link_rr_pct), abs(two_four_link_rr_pct), abs(four_six_link_rr_pct), abs(six_eight_link_rr_pct), abs(eight_ten_link_rr_pct), abs(ten_plus_link_rr_pct)) as double) and link_rr_pct<0 then 'Highest drop in Link RRs of ' || cast((round(100*cast(greatest(abs(zero_two_link_rr_pct), abs(two_four_link_rr_pct), abs(four_six_link_rr_pct), abs(six_eight_link_rr_pct), abs(eight_ten_link_rr_pct), abs(ten_plus_link_rr_pct)) as double),1)) as varchar) || '% in 0-2 KM'
        when (Time_Frame='1. Delta_1' or Time_Frame='2. Delta_2') and abs(two_four_link_rr_pct)=cast(greatest(abs(zero_two_link_rr_pct), abs(two_four_link_rr_pct), abs(four_six_link_rr_pct), abs(six_eight_link_rr_pct), abs(eight_ten_link_rr_pct), abs(ten_plus_link_rr_pct)) as double) and link_rr_pct<0 then 'Highest drop in Link RRs of ' || cast((round(100*cast(greatest(abs(zero_two_link_rr_pct), abs(two_four_link_rr_pct), abs(four_six_link_rr_pct), abs(six_eight_link_rr_pct), abs(eight_ten_link_rr_pct), abs(ten_plus_link_rr_pct)) as double),1)) as varchar) || '% in 2-4 KM'
        when (Time_Frame='1. Delta_1' or Time_Frame='2. Delta_2') and abs(four_six_link_rr_pct)=cast(greatest(abs(zero_two_link_rr_pct), abs(two_four_link_rr_pct), abs(four_six_link_rr_pct), abs(six_eight_link_rr_pct), abs(eight_ten_link_rr_pct), abs(ten_plus_link_rr_pct)) as double) and link_rr_pct<0 then 'Highest drop in Link RRs of ' || cast((round(100*cast(greatest(abs(zero_two_link_rr_pct), abs(two_four_link_rr_pct), abs(four_six_link_rr_pct), abs(six_eight_link_rr_pct), abs(eight_ten_link_rr_pct), abs(ten_plus_link_rr_pct)) as double),1)) as varchar) || '% in 4-6 KM'
        when (Time_Frame='1. Delta_1' or Time_Frame='2. Delta_2') and abs(six_eight_link_rr_pct)=cast(greatest(abs(zero_two_link_rr_pct), abs(two_four_link_rr_pct), abs(four_six_link_rr_pct), abs(six_eight_link_rr_pct), abs(eight_ten_link_rr_pct), abs(ten_plus_link_rr_pct)) as double) and link_rr_pct<0 then 'Highest drop in Link RRs of ' || cast((round(100*cast(greatest(abs(zero_two_link_rr_pct), abs(two_four_link_rr_pct), abs(four_six_link_rr_pct), abs(six_eight_link_rr_pct), abs(eight_ten_link_rr_pct), abs(ten_plus_link_rr_pct)) as double),1)) as varchar) || '% in 6-8 KM'
        when (Time_Frame='1. Delta_1' or Time_Frame='2. Delta_2') and abs(eight_ten_link_rr_pct)=cast(greatest(abs(zero_two_link_rr_pct), abs(two_four_link_rr_pct), abs(four_six_link_rr_pct), abs(six_eight_link_rr_pct), abs(eight_ten_link_rr_pct), abs(ten_plus_link_rr_pct)) as double) and link_rr_pct<0 then 'Highest drop in Link RRs of ' || cast((round(100*cast(greatest(abs(zero_two_link_rr_pct), abs(two_four_link_rr_pct), abs(four_six_link_rr_pct), abs(six_eight_link_rr_pct), abs(eight_ten_link_rr_pct), abs(ten_plus_link_rr_pct)) as double),1)) as varchar) || '% in 8-10 KM'
        when (Time_Frame='1. Delta_1' or Time_Frame='2. Delta_2') and abs(ten_plus_link_rr_pct)=cast(greatest(abs(zero_two_link_rr_pct), abs(two_four_link_rr_pct), abs(four_six_link_rr_pct), abs(six_eight_link_rr_pct), abs(eight_ten_link_rr_pct), abs(ten_plus_link_rr_pct)) as double) and link_rr_pct<0 then 'Highest drop in Link RRs of ' || cast((round(100*cast(greatest(abs(zero_two_link_rr_pct), abs(two_four_link_rr_pct), abs(four_six_link_rr_pct), abs(six_eight_link_rr_pct), abs(eight_ten_link_rr_pct), abs(ten_plus_link_rr_pct)) as double),1)) as varchar) || '% in 10+ KM'
        when (Time_Frame='1. Delta_1' or Time_Frame='2. Delta_2') and abs(zero_two_link_rr_pct)=cast(greatest(abs(zero_two_link_rr_pct), abs(two_four_link_rr_pct), abs(four_six_link_rr_pct), abs(six_eight_link_rr_pct), abs(eight_ten_link_rr_pct), abs(ten_plus_link_rr_pct)) as double) and link_rr_pct>0 then 'Highest jump in Link RRs of ' || cast((round(100*cast(greatest(abs(zero_two_link_rr_pct), abs(two_four_link_rr_pct), abs(four_six_link_rr_pct), abs(six_eight_link_rr_pct), abs(eight_ten_link_rr_pct), abs(ten_plus_link_rr_pct)) as double),1)) as varchar) || '% in 0-2 KM'
        when (Time_Frame='1. Delta_1' or Time_Frame='2. Delta_2') and abs(two_four_link_rr_pct)=cast(greatest(abs(zero_two_link_rr_pct), abs(two_four_link_rr_pct), abs(four_six_link_rr_pct), abs(six_eight_link_rr_pct), abs(eight_ten_link_rr_pct), abs(ten_plus_link_rr_pct)) as double) and link_rr_pct>0 then 'Highest jump in Link RRs of ' || cast((round(100*cast(greatest(abs(zero_two_link_rr_pct), abs(two_four_link_rr_pct), abs(four_six_link_rr_pct), abs(six_eight_link_rr_pct), abs(eight_ten_link_rr_pct), abs(ten_plus_link_rr_pct)) as double),1)) as varchar) || '% in 2-4 KM'
        when (Time_Frame='1. Delta_1' or Time_Frame='2. Delta_2') and abs(four_six_link_rr_pct)=cast(greatest(abs(zero_two_link_rr_pct), abs(two_four_link_rr_pct), abs(four_six_link_rr_pct), abs(six_eight_link_rr_pct), abs(eight_ten_link_rr_pct), abs(ten_plus_link_rr_pct)) as double) and link_rr_pct>0 then 'Highest jump in Link RRs of ' || cast((round(100*cast(greatest(abs(zero_two_link_rr_pct), abs(two_four_link_rr_pct), abs(four_six_link_rr_pct), abs(six_eight_link_rr_pct), abs(eight_ten_link_rr_pct), abs(ten_plus_link_rr_pct)) as double),1)) as varchar) || '% in 4-6 KM'
        when (Time_Frame='1. Delta_1' or Time_Frame='2. Delta_2') and abs(six_eight_link_rr_pct)=cast(greatest(abs(zero_two_link_rr_pct), abs(two_four_link_rr_pct), abs(four_six_link_rr_pct), abs(six_eight_link_rr_pct), abs(eight_ten_link_rr_pct), abs(ten_plus_link_rr_pct)) as double) and link_rr_pct>0 then 'Highest jump in Link RRs of ' || cast((round(100*cast(greatest(abs(zero_two_link_rr_pct), abs(two_four_link_rr_pct), abs(four_six_link_rr_pct), abs(six_eight_link_rr_pct), abs(eight_ten_link_rr_pct), abs(ten_plus_link_rr_pct)) as double),1)) as varchar) || '% in 6-8 KM'
        when (Time_Frame='1. Delta_1' or Time_Frame='2. Delta_2') and abs(eight_ten_link_rr_pct)=cast(greatest(abs(zero_two_link_rr_pct), abs(two_four_link_rr_pct), abs(four_six_link_rr_pct), abs(six_eight_link_rr_pct), abs(eight_ten_link_rr_pct), abs(ten_plus_link_rr_pct)) as double) and link_rr_pct>0 then 'Highest jump in Link RRs of ' || cast((round(100*cast(greatest(abs(zero_two_link_rr_pct), abs(two_four_link_rr_pct), abs(four_six_link_rr_pct), abs(six_eight_link_rr_pct), abs(eight_ten_link_rr_pct), abs(ten_plus_link_rr_pct)) as double),1)) as varchar) || '% in 8-10 KM'
        when (Time_Frame='1. Delta_1' or Time_Frame='2. Delta_2') and abs(ten_plus_link_rr_pct)=cast(greatest(abs(zero_two_link_rr_pct), abs(two_four_link_rr_pct), abs(four_six_link_rr_pct), abs(six_eight_link_rr_pct), abs(eight_ten_link_rr_pct), abs(ten_plus_link_rr_pct)) as double) and link_rr_pct>0 then 'Highest jump in Link RRs of ' || cast((round(100*cast(greatest(abs(zero_two_link_rr_pct), abs(two_four_link_rr_pct), abs(four_six_link_rr_pct), abs(six_eight_link_rr_pct), abs(eight_ten_link_rr_pct), abs(ten_plus_link_rr_pct)) as double),1)) as varchar) || '% in 10+ KM'
    end as distance_link_rr_summary,
    case when Time_Frame='3. ThisWeek' and (morning_link_rr_pct<afternoon_link_rr_pct or  morning_link_rr_pct<evening_link_rr_pct) and (rest_link_rr_pct<afternoon_link_rr_pct or  rest_link_rr_pct<evening_link_rr_pct) then 'Link RRs highest in afternoon and evening'
        when Time_Frame='4. Last3Week' and (morning_link_rr_pct<afternoon_link_rr_pct or  morning_link_rr_pct<evening_link_rr_pct) and (rest_link_rr_pct<afternoon_link_rr_pct or  rest_link_rr_pct<evening_link_rr_pct) then 'Link RRs highest in afternoon and evening'
        when Time_Frame='5. TargetWeek' and (morning_link_rr_pct<afternoon_link_rr_pct or  morning_link_rr_pct<evening_link_rr_pct) and (rest_link_rr_pct<afternoon_link_rr_pct or  rest_link_rr_pct<evening_link_rr_pct) then 'Link RRs highest in afternoon and evening'
        when (Time_Frame='1. Delta_1' or Time_Frame='2. Delta_2') and abs(morning_link_rr_pct)=cast(greatest(abs(morning_link_rr_pct), abs(afternoon_link_rr_pct), abs(evening_link_rr_pct), abs(rest_link_rr_pct)) as double) and link_rr_pct<0 then 'Highest drop in Link RRs of ' || cast((round(100*cast(greatest(abs(morning_link_rr_pct), abs(afternoon_link_rr_pct), abs(evening_link_rr_pct), abs(rest_link_rr_pct)) as double),1)) as varchar) || '% in morning'
        when (Time_Frame='1. Delta_1' or Time_Frame='2. Delta_2') and abs(afternoon_link_rr_pct)=cast(greatest(abs(morning_link_rr_pct), abs(afternoon_link_rr_pct), abs(evening_link_rr_pct), abs(rest_link_rr_pct)) as double) and link_rr_pct<0 then 'Highest drop in Link RRs of ' || cast((round(100*cast(greatest(abs(morning_link_rr_pct), abs(afternoon_link_rr_pct), abs(evening_link_rr_pct), abs(rest_link_rr_pct)) as double),1)) as varchar) || '% in afternoon'
        when (Time_Frame='1. Delta_1' or Time_Frame='2. Delta_2') and abs(evening_link_rr_pct)=cast(greatest(abs(morning_link_rr_pct), abs(afternoon_link_rr_pct), abs(evening_link_rr_pct), abs(rest_link_rr_pct)) as double) and link_rr_pct<0 then 'Highest drop in Link RRs of ' || cast((round(100*cast(greatest(abs(morning_link_rr_pct), abs(afternoon_link_rr_pct), abs(evening_link_rr_pct), abs(rest_link_rr_pct)) as double),1)) as varchar) || '% in evening'
        when (Time_Frame='1. Delta_1' or Time_Frame='2. Delta_2') and abs(rest_link_rr_pct)=cast(greatest(abs(morning_link_rr_pct), abs(afternoon_link_rr_pct), abs(evening_link_rr_pct), abs(rest_link_rr_pct)) as double) and link_rr_pct<0 then 'Highest drop in Link RRs of ' || cast((round(100*cast(greatest(abs(morning_link_rr_pct), abs(afternoon_link_rr_pct), abs(evening_link_rr_pct), abs(rest_link_rr_pct)) as double),1)) as varchar) || '% in night'
        when (Time_Frame='1. Delta_1' or Time_Frame='2. Delta_2') and abs(morning_link_rr_pct)=cast(greatest(abs(morning_link_rr_pct), abs(afternoon_link_rr_pct), abs(evening_link_rr_pct), abs(rest_link_rr_pct)) as double) and link_rr_pct>0 then 'Highest jump in Link RRs of ' || cast((round(100*cast(greatest(abs(morning_link_rr_pct), abs(afternoon_link_rr_pct), abs(evening_link_rr_pct), abs(rest_link_rr_pct)) as double),1)) as varchar) || '% in morning'
        when (Time_Frame='1. Delta_1' or Time_Frame='2. Delta_2') and abs(afternoon_link_rr_pct)=cast(greatest(abs(morning_link_rr_pct), abs(afternoon_link_rr_pct), abs(evening_link_rr_pct), abs(rest_link_rr_pct)) as double) and link_rr_pct>0 then 'Highest jump in Link RRs of ' || cast((round(100*cast(greatest(abs(morning_link_rr_pct), abs(afternoon_link_rr_pct), abs(evening_link_rr_pct), abs(rest_link_rr_pct)) as double),1)) as varchar) || '% in afternoon'
        when (Time_Frame='1. Delta_1' or Time_Frame='2. Delta_2') and abs(evening_link_rr_pct)=cast(greatest(abs(morning_link_rr_pct), abs(afternoon_link_rr_pct), abs(evening_link_rr_pct), abs(rest_link_rr_pct)) as double) and link_rr_pct>0 then 'Highest jump in Link RRs of ' || cast((round(100*cast(greatest(abs(morning_link_rr_pct), abs(afternoon_link_rr_pct), abs(evening_link_rr_pct), abs(rest_link_rr_pct)) as double),1)) as varchar) || '% in evening'
        when (Time_Frame='1. Delta_1' or Time_Frame='2. Delta_2') and abs(rest_link_rr_pct)=cast(greatest(abs(morning_link_rr_pct), abs(afternoon_link_rr_pct), abs(evening_link_rr_pct), abs(rest_link_rr_pct)) as double) and link_rr_pct>0 then 'Highest jump in Link RRs of ' || cast((round(100*cast(greatest(abs(morning_link_rr_pct), abs(afternoon_link_rr_pct), abs(evening_link_rr_pct), abs(rest_link_rr_pct)) as double),1)) as varchar) || '% in night'
    end as temporal_link_rr_summary
from v4
order by Time_Frame