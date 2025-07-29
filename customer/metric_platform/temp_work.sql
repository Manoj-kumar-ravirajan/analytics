with ao_query as(
        select 
            ao.epoch as epoch,
            ao.current_city as current_city,
            ao.platform as platform,
            ao.app_version as app_version,
            (ao.user_id || '-' || ao.ct_session_id) as ao_unique_id,
            row_number() over(partition by ao.user_id, ao.ct_session_id order by ao.epoch) as rnk 
        from clevertap.clevertap_customer_order_activity as ao 
        where 
          ao.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d')
         AND ao.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
),
ao_query1 as (
select 
        ao_query.epoch as epoch,
        ao_query.current_city as current_city,
        ao_query.platform as platform,
        ao_query.app_version as app_version,
        ao_query.ao_unique_id as ao_unique_id  
from 
     ao_query as ao_query 
where 
     ao_query.rnk =1 
)
, fe_query as (
            select 
                fe.epoch as fe_epoch,
                (fe.user_id || '-' || fe.ct_session_id) as fe_unique_id,
                row_number() over(partition by fe.ct_session_id,fe.user_id  order by fe.epoch ) as rnk 
            from 
                canonical.clevertap_customer_fare_estimate as fe
            where 
                fe.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d')
AND fe.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
                and fe.fare_estimate_id is not null and fe.fare_estimate_id != '' 
),
fe_query1 as (
select 
      fe_query.fe_epoch as fe_epoch,
      fe_query.fe_unique_id as fe_unique_id
from  fe_query as fe_query 
where fe_query.rnk =1 
)
,merge_df as(
    select  
        ao_query1.epoch as epoch,
        ao_query1.current_city as current_city,
        ao_query1.platform as platform,
        ao_query1.app_version as app_version,
        fe_query1.fe_epoch as fe_epoch       
    from ao_query1 as ao_query1
     join fe_query1 as fe_query1
        on ao_query1.ao_unique_id = fe_query1.fe_unique_id
), 
final_data as ( 
   select    
        merge_df.epoch as epoch,
        merge_df.current_city as current_city,
        merge_df.platform as platform,
        merge_df.app_version as app_version,
(cast(merge_df.fe_epoch as bigint) - cast(merge_df.epoch as bigint))/1000 AS AO2FE_time_sec
    from merge_df as merge_df    
    )   
select 
    final_data.platform,
    final_data.app_version,
    PERCENTILE_APPROX(final_data.AO2FE_time_sec , 0.25) as p25_ao2fe_time_channel_host

from
final_data as final_data
where final_data.AO2FE_time_sec > 0 
group by final_data.platform , final_data.app_version


--------- 
