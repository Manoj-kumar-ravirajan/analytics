with fe_tbl as
    (
    select *
    from
     (select *,
             1.0 as fe_count,
             row_number() over (partition by cast(cast(eventprops_ctsessionid as decimal) as varchar) || ' - ' || profile_phone || ' - ' || servicename
                                order by eventprops_epoch desc) as updated_seq
      from
        (SELECT eventprops_currentcity AS city,
                week(DATE_PARSE(yyyymmdd, '%Y%m%d')) as wk,
                DATE_TRUNC('week', DATE_PARSE(yyyymmdd, '%Y%m%d')) AS week_start_date,
                DATE_PARSE(yyyymmdd, '%Y%m%d') as dd,
                eventprops_userid as customer_id,
                eventprops_fareestimateid as fareestimateid,
                eventprops_servicename as servicename,
                cast(eventprops_finalamount AS double) AS finalamount,
                eventprops_ctsessionid,
                profile_phone,
                eventprops_epoch,
                cast(eventprops_hfdistance AS double) AS distance,
                cast(eventprops_eta AS double) AS eta,
                case
                    when cast(substr(hhmmss, 1, 2) as double) between 8 and 11 then 'morning_peak'
                    when cast(substr(hhmmss, 1, 2) as double) between 12 and 16 then 'afternoon'
                    when cast(substr(hhmmss, 1, 2) as double) between 17 and 21 then 'evening_peak'
                    else 'rest'
                end as temporal,
                cast(cast(eventprops_ctsessionid as decimal) as varchar) || ' - ' || profile_phone || ' - ' || eventProps_serviceName AS unique_id,
                date_format(from_unixtime(cast(eventprops_epoch as double) / 1000, 'Asia/Kolkata'), '%Y-%m-%d') AS orderdate,
                cast(eventprops_discountamount AS double) AS discount,
                cast(eventprops_subtotal AS double) AS subtotal
         FROM raw.clevertap_customer_fareestimate
         WHERE (yyyymmdd >= date_format({{start_date}}, '%Y%m%d')
                AND yyyymmdd <= date_format({{end_date}}, '%Y%m%d'))
           AND eventProps_serviceName in ('Link',
                                          'Auto') ))
    where updated_seq=1 
    ),

rr_tbl as
    (select *
    from
     (select *,
             1.0 as rr_count,
             row_number() over (partition by cast(cast(eventprops_ctsessionid as decimal) as varchar) || ' - ' || profile_phone || ' - ' || servicename
                                order by eventprops_epoch desc) as updated_seq
      from
        (SELECT eventprops_currentcity AS city,
                week(DATE_PARSE(yyyymmdd, '%Y%m%d')) as wk,
                DATE_PARSE(yyyymmdd, '%Y%m%d') as dd,
                eventprops_userid as customer_id,
                eventprops_fareestimateid as fareestimateid,
                eventprops_servicename as servicename,
                eventprops_ctsessionid,
                profile_phone,
                eventprops_epoch,
                cast(cast(eventprops_ctsessionid as decimal) as varchar) || ' - ' || profile_phone || ' - ' || eventprops_servicename AS unique_id,
                date_format(from_unixtime(cast(eventprops_epoch as double) / 1000, 'Asia/Kolkata'), '%Y-%m-%d') AS orderdate
         FROM raw.clevertap_customer_request_rapido
         WHERE (yyyymmdd >= date_format({{start_date}}, '%Y%m%d')
                AND yyyymmdd <= date_format({{end_date}}, '%Y%m%d'))
           AND eventProps_serviceName in ('Link',
                                          'Auto') ))
    where updated_seq=1 
   ),
    
response_tbl AS
    (SELECT city,
            fare_estimate_id,
            week(DATE_PARSE(yyyymmdd, '%Y%m%d')) as wk,
            DATE_PARSE(yyyymmdd, '%Y%m%d') as dd,
            service_level,
            CAST(dynamic_surge_amount AS DOUBLE) AS dynamic_surge,
            CAST(dynamic_fare_amount AS DOUBLE) AS dynamic_fare,
            CAST(rate_card_amount AS DOUBLE) AS rate_card
    FROM experiments.iprice_cleaned_responses_v2
    WHERE (yyyymmdd >= date_format({{start_date}}, '%Y%m%d') AND yyyymmdd <= date_format({{end_date}}, '%Y%m%d'))
        AND service_level in ('Link', 'Auto') 
    ),

rr_merged AS
    (SELECT rr_tbl.city,
            rr_tbl.wk,
            customer_id,
            rr_tbl.dd,
            rr_tbl.fareestimateid AS fareestimateid,
            unique_id,
            servicename,
            orderdate,
            rr_count
    FROM rr_tbl
    LEFT JOIN response_tbl ON rr_tbl.fareestimateid = response_tbl.fare_estimate_id and rr_tbl.servicename = response_tbl.service_level and rr_tbl.dd = response_tbl.dd and rr_tbl.city = response_tbl.city
    ),

fe_merged AS
    (SELECT fe_tbl.city,
            fe_tbl.wk,
            customer_id,
            fe_tbl.fareestimateid AS fareestimateid,
            fe_tbl.dd,
            unique_id,
            servicename,
            discount,
            subtotal,
            orderdate,
            week_start_date,
            distance,
            eta,
            temporal,
            finalamount,
            rate_card,
            (dynamic_fare+dynamic_surge) as total_surge,
            fe_count
    FROM fe_tbl
    LEFT JOIN response_tbl ON fe_tbl.fareestimateid = response_tbl.fare_estimate_id and fe_tbl.servicename = response_tbl.service_level and fe_tbl.dd = response_tbl.dd and fe_tbl.city = response_tbl.city
    ),

fe_rr AS
    (SELECT fe_merged.city AS city,
            fe_merged.wk,
            fe_merged.customer_id AS customer_id,
            fe_merged.week_start_date,
            fe_merged.fareestimateid AS fareestimateid,
            fe_merged.unique_id AS unique_id,
            fe_merged.orderdate AS orderdate,
            fe_merged.servicename as servicename,
            fe_merged.rate_card,
            fe_merged.finalamount,
            fe_merged.distance,
            fe_merged.eta,
            fe_merged.temporal,
            fe_merged.total_surge,
            fe_merged.discount,
            fe_merged.subtotal,
            fe_count,
            coalesce(rr_merged.rr_count, 0) AS rr_count
    FROM fe_merged
    LEFT JOIN rr_merged ON fe_merged.city = rr_merged.city AND fe_merged.unique_id = rr_merged.unique_id AND fe_merged.orderdate = rr_merged.orderdate  and fe_merged.servicename = rr_merged.servicename and fe_merged.dd = rr_merged.dd
    order by city, wk, unique_id
    ),

final_v0 as
    (select week_start_date,
            city,
            wk,
            servicename,
            discount,
            subtotal,
            total_surge,
            temporal,
            fe_count,
            rr_count,
            case when servicename='Link' then fe_count else 0 end as link_fe,
            case when servicename='Auto' then fe_count else 0 end as auto_fe,
            case when servicename='Link' then rr_count else 0 end as link_rr,
            case when servicename='Auto' then rr_count else 0 end as auto_rr,
            case
                when cast(rate_card as double) is null then '4. Rate_card_not_available'
                when 100*((cast(rate_card as double)-cast(finalamount as double))/cast(rate_card as double))<=-5 then '1. Above_Rate_Card'
                when 100*((cast(rate_card as double)-cast(finalamount as double))/cast(rate_card as double))<=5 then '2. At_Rate_Card'
                else '3. Below_Rate_Card'
            end as pct_delta_from_rc_bucket,
            case
                when 100*(cast(discount as double)/cast(subtotal as double))>00 then '1. Discounted_FE'
                when 100*(cast(discount as double)/cast(subtotal as double))=00 then '2. Zero_Discount_FE'
                else '3. NA'
            end as discount_bucket,
            case
                when 100*(cast(total_surge as double)/cast(subtotal as double))>00 then '1. Surged FE'
                when 100*(cast(total_surge as double)/cast(subtotal as double))=00 then '2. Zero Surged FE'
                else '3. NA'
            end as surge_bucket,
            case
                when cast(distance as double)<=2000 then '00 - 02 km'
                when cast(distance as double)<=4000 then '02 - 04 km'
                when cast(distance as double)<=6000 then '04 - 06 km'
                when cast(distance as double)<=8000 then '06 - 08 km'
                when cast(distance as double)<=10000 then '08 - 10 km'
                else '10+ km'
            end as distance_bucket,
            case
                when cast(eta as double)<=5 then '00 - 05'
                when cast(eta as double)<=10 then '05 - 10'
                when cast(eta as double)<=20 then '10 - 20'
                when cast(eta as double)<=30 then '20 - 30'
                else '30+'
            end as eta_bucket
    from fe_rr
    ),

v0 as
    (
    select city,
        week_start_date,
        sum(link_fe) as link_fe,
        sum(link_rr) as link_rr,
        sum(auto_fe) as auto_fe,
        sum(auto_rr) as auto_rr,
        sum(case when pct_delta_from_rc_bucket='1. Above_Rate_Card' then link_fe end) as above_rc_link_fe,
        sum(case when pct_delta_from_rc_bucket='2. At_Rate_Card' then link_fe end) as at_rc_link_fe,
        sum(case when pct_delta_from_rc_bucket='3. Below_Rate_Card' then link_fe end) as below_rc_link_fe,
        sum(case when pct_delta_from_rc_bucket='1. Above_Rate_Card' then auto_fe end) as above_rc_auto_fe,
        sum(case when pct_delta_from_rc_bucket='2. At_Rate_Card' then auto_fe end) as at_rc_auto_fe,
        sum(case when pct_delta_from_rc_bucket='3. Below_Rate_Card' then auto_fe end) as below_rc_auto_fe,
        sum(case when pct_delta_from_rc_bucket='1. Above_Rate_Card' then auto_rr end) as above_rc_auto_rr,
        sum(case when pct_delta_from_rc_bucket='2. At_Rate_Card' then auto_rr end) as at_rc_auto_rr,
        sum(case when pct_delta_from_rc_bucket='3. Below_Rate_Card' then auto_rr end) as below_rc_auto_rr,
        sum(case when discount_bucket='1. Discounted_FE' then link_fe end) as discounted_link_fe,
        sum(case when discount_bucket='1. Discounted_FE' then auto_fe end) as discounted_auto_fe,
        sum(case when servicename='Link' then discount end) as link_discount,
        sum(case when servicename='Link' then subtotal end) as link_subtotal,
        sum(case when servicename='Auto' then discount end) as auto_discount,
        sum(case when servicename='Auto' then subtotal end) as auto_subtotal,
        sum(case when servicename='Link' then total_surge end) as link_surge,
        sum(case when servicename='Auto' then total_surge end) as auto_surge,
        sum(case when surge_bucket='1. Surged FE' then link_fe end) as surged_link_fe,
        sum(case when surge_bucket='1. Surged FE' then auto_fe end) as surged_auto_fe,
        sum(case when distance_bucket='00 - 02 km' then link_fe end) as zero_two_link_fe,
        sum(case when distance_bucket='02 - 04 km' then link_fe end) as two_four_link_fe,
        sum(case when distance_bucket='04 - 06 km' then link_fe end) as four_six_link_fe,
        sum(case when distance_bucket='06 - 08 km' then link_fe end) as six_eight_link_fe,
        sum(case when distance_bucket='08 - 10 km' then link_fe end) as eight_ten_link_fe,
        sum(case when distance_bucket='10+ km' then link_fe end) as ten_plus_link_fe,
        sum(case when distance_bucket='00 - 02 km' then link_rr end) as zero_two_link_rr,
        sum(case when distance_bucket='02 - 04 km' then link_rr end) as two_four_link_rr,
        sum(case when distance_bucket='04 - 06 km' then link_rr end) as four_six_link_rr,
        sum(case when distance_bucket='06 - 08 km' then link_rr end) as six_eight_link_rr,
        sum(case when distance_bucket='08 - 10 km' then link_rr end) as eight_ten_link_rr,
        sum(case when distance_bucket='10+ km' then link_rr end) as ten_plus_link_rr,
        sum(case when distance_bucket='00 - 02 km' then auto_fe end) as zero_two_auto_fe,
        sum(case when distance_bucket='02 - 04 km' then auto_fe end) as two_four_auto_fe,
        sum(case when distance_bucket='04 - 06 km' then auto_fe end) as four_six_auto_fe,
        sum(case when distance_bucket='06 - 08 km' then auto_fe end) as six_eight_auto_fe,
        sum(case when distance_bucket='08 - 10 km' then auto_fe end) as eight_ten_auto_fe,
        sum(case when distance_bucket='10+ km' then auto_fe end) as ten_plus_auto_fe,
        sum(case when distance_bucket='00 - 02 km' then auto_rr end) as zero_two_auto_rr,
        sum(case when distance_bucket='02 - 04 km' then auto_rr end) as two_four_auto_rr,
        sum(case when distance_bucket='04 - 06 km' then auto_rr end) as four_six_auto_rr,
        sum(case when distance_bucket='06 - 08 km' then auto_rr end) as six_eight_auto_rr,
        sum(case when distance_bucket='08 - 10 km' then auto_rr end) as eight_ten_auto_rr,
        sum(case when distance_bucket='10+ km' then auto_rr end) as ten_plus_auto_rr,
        sum(case when eta_bucket='00 - 05' then link_fe end) as zero_five_link_fe,
        sum(case when eta_bucket='05 - 10' then link_fe end) as five_ten_link_fe,
        sum(case when eta_bucket='10 - 20' then link_fe end) as ten_twenty_link_fe,
        sum(case when eta_bucket='20 - 30' then link_fe end) as twenty_thirty_link_fe,
        sum(case when eta_bucket='30+' then link_fe end) as thirty_plus_link_fe,
        sum(case when eta_bucket='00 - 05' then link_rr end) as zero_five_link_rr,
        sum(case when eta_bucket='05 - 10' then link_rr end) as five_ten_link_rr,
        sum(case when eta_bucket='10 - 20' then link_rr end) as ten_twenty_link_rr,
        sum(case when eta_bucket='20 - 30' then link_rr end) as twenty_thirty_link_rr,
        sum(case when eta_bucket='30+' then link_rr end) as thirty_plus_link_rr,
        sum(case when eta_bucket='00 - 05' then auto_fe end) as zero_five_auto_fe,
        sum(case when eta_bucket='05 - 10' then auto_fe end) as five_ten_auto_fe,
        sum(case when eta_bucket='10 - 20' then auto_fe end) as ten_twenty_auto_fe,
        sum(case when eta_bucket='20 - 30' then auto_fe end) as twenty_thirty_auto_fe,
        sum(case when eta_bucket='30+' then auto_fe end) as thirty_plus_auto_fe,
        sum(case when eta_bucket='00 - 05' then auto_rr end) as zero_five_auto_rr,
        sum(case when eta_bucket='05 - 10' then auto_rr end) as five_ten_auto_rr,
        sum(case when eta_bucket='10 - 20' then auto_rr end) as ten_twenty_auto_rr,
        sum(case when eta_bucket='20 - 30' then auto_rr end) as twenty_thirty_auto_rr,
        sum(case when eta_bucket='30+' then auto_rr end) as thirty_plus_auto_rr,
        sum(case when temporal='morning_peak' then link_fe end) as morning_link_fe,
        sum(case when temporal='afternoon' then link_fe end) as afternoon_link_fe,
        sum(case when temporal='evening_peak' then link_fe end) as evening_link_fe,
        sum(case when temporal='rest' then link_fe end) as rest_link_fe,
        sum(case when temporal='morning_peak' then link_rr end) as morning_link_rr,
        sum(case when temporal='afternoon' then link_rr end) as afternoon_link_rr,
        sum(case when temporal='evening_peak' then link_rr end) as evening_link_rr,
        sum(case when temporal='rest' then link_rr end) as rest_link_rr,
        sum(case when temporal='morning_peak' then auto_fe end) as morning_auto_fe,
        sum(case when temporal='afternoon' then auto_fe end) as afternoon_auto_fe,
        sum(case when temporal='evening_peak' then auto_fe end) as evening_auto_fe,
        sum(case when temporal='rest' then auto_fe end) as rest_auto_fe,
        sum(case when temporal='morning_peak' then auto_rr end) as morning_auto_rr,
        sum(case when temporal='afternoon' then auto_rr end) as afternoon_auto_rr,
        sum(case when temporal='evening_peak' then auto_rr end) as evening_auto_rr,
        sum(case when temporal='rest' then auto_rr end) as rest_auto_rr
    from final_v0
    group by city,
            week_start_date
    order by city,
            week_start_date
    )
    
select *
from v0