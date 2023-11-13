with city_details as (
    Select 
        city_id, 
        city_display_name as city_name,
        service_level,
        service_id
    from datasets.service_mapping  where [[{{city_display_name}}]] AND  [[{{service_level}}]]
    -- service_level in ('Link','Auto')
    ), 

-- Get the list of offered created for the given date range 
created_offer as
    (
    select 
        city,
        list as user_selector_id,
        services
    from
        (
            select 
                yyyymmdd, startdate, endDate, userselectors,
                cast(json_extract(services,'$.0') as varchar) services,
                cast(json_extract(city,'$.0') as varchar) city,
                timings,
                -- applyascoupon offer
                case when applyascoupon = 'true' then 'Coupon' else 'AutoApplied' end offer                

            from raw.kafka_offers_immutable 
            where
            yyyymmdd >= date_format({{offer_start_date}} - interval '3' day, '%Y%m%d') 
            and startdate = date_format({{offer_start_date}}, '%Y-%m-%d')
            and endDate = date_format({{offer_end_date}}, '%Y-%m-%d')
            and substr(NAME,1,3) = 'IA_'    -- Prefix s IA_
            and day_of_week(date_parse(substr(NAME,10,8), '%Y%m%d')) = 5   -- Recommendations for friday
        ) 
        cross join unnest(selectors_list) as selectors_list(list)
    ),
    
-- Get the list of selector created since 4 days before to start offer date.
allocator_selectors as ( 
    SELECT
        date_format(date_parse(yyyymmdd,'%Y%m%d'),'%Y-%m-%d') selector_created_date,
        cast(json_extract(data, '$.data.id') as varchar) as selector_id,
        name,
        userCount,
        status
    FROM
        hive.raw.kafka_user_selector_event_immutable_internal
    where 1=1
        and userType='customer' 
        and type = 'custom'
        and owner = 'iAllocator'
        and yyyymmdd between date_format({{offer_start_date}} - interval '3' day,'%Y%m%d') and  date_format({{offer_start_date}}, '%Y%m%d')
        ),
        
allocatoroffers_v1 as (
 Select 
    created_offer.*,
    city_details.service_level
 from 
    created_offer
 inner join city_details on created_offer.city = city_details.city_name and created_offer.services = city_details.service_id
)

Select 
    allocator_selectors.name selector_name,
    allocator_selectors.userCount,
    allocatoroffers_v1.*
from  allocatoroffers_v1
left join allocator_selectors on allocatoroffers_v1.user_selector_id = allocator_selectors.selector_id
order by 12
 