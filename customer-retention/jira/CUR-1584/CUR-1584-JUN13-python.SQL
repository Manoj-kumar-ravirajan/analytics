with city_details AS 
    (
    SELECT 
        city_id, 
        city_display_name AS city_name,
        service_level,
        service_id
    FROM datasets.service_mapping  
    --WHERE {{city_display_name}} AND {{service_level}}
    WHERE city_display_name IN ('{city_name}') AND service_level IN ('{service}')
    ),
created_offer AS
    (
    SELECT 
        yyyymmdd, startdate, endDate,
        city,
        list AS selectorid,
        services
    FROM
        (
        SELECT 
            yyyymmdd, startdate, endDate, userselectors,
            cast(userselectors AS ARRAY<VARCHAR>) AS selectors_list,
            NAME offer_name,
            cast(json_extract(services,'$.0') AS varchar) services,
            cast(json_extract(city,'$.0') AS varchar) city
        
        FROM raw.kafka_offers_immutable 
        WHERE
            --yyyymmdd BETWEEN date_format({{offer_start_date}} - interval '3' day, '%Y%m%d') AND date_format({{offer_end_date}}, '%Y%m%d')
            --yyyymmdd >= date_format({{offer_start_date}} - interval '3' day, '%Y%m%d') 
            --AND startdate = date_format({{offer_start_date}}, '%Y-%m-%d')
            --AND endDate = date_format({{offer_end_date}}, '%Y-%m-%d')
            
            yyyymmdd = '20220605'
            AND description = 'IALLOCATOR'
            --AND substr(NAME,1,3) = 'IA_'    -- Prefix s IA_
    
            --AND substr(NAME,1,3) = 'IA_Banga_20220603_15UPTO09RETD2A0000_V1_AU'
            AND cast(json_extract(city,'$.0') AS varchar) IN (SELECT DISTINCT city_name FROM city_details)
            AND cast(json_extract(services,'$.0') AS varchar) IN (SELECT DISTINCT service_id FROM city_details)
        ) 
    CROSS JOIN unnest(selectors_list) AS selectors_list(list)
    --WHERE list = '629c8ff63eb4180001d6464a'
    ),

user_selector AS
    (
    SELECT 
        co.yyyymmdd,
        co.startdate, 
        co.endDate,
        co.selectorid,
        u.name AS selector_name,
        co.city,
        --co.services,
        u.customer_id
    FROM
    (
    SELECT 
        u.yyyymmdd,
        u.selectorid,
        u.userid AS customer_id,
        u.name
    FROM hive.canonical.user_selector AS u
    WHERE u.yyyymmdd IN (SELECT DISTINCT yyyymmdd FROM created_offer)
        AND u.selectorid IN (SELECT DISTINCT selectorid FROM created_offer)
    ) AS u
    RIGHT JOIN created_offer AS co ON co.yyyymmdd = u.yyyymmdd AND co.selectorid = u.selectorid   
    ),

notification_send AS 
    (
    SELECT 
        us.*,
        ns.profile_identity AS customerid_send,
        ns.profile_phone AS profile_phone_send,
        ns.eventProps_uuid
        
    FROM user_selector AS us
    LEFT JOIN raw.clevertap_customer_notifier_notification_send AS ns ON us.customer_id = ns.profile_identity AND us.yyyymmdd = ns.yyyymmdd 
    
    --WHERE yyyymmdd between date_format({{offer_start_date}} - interval '3' day, '%Y%m%d') and  date_format({{offer_end_date}}, '%Y%m%d')
        --AND eventProps_notification_channel = 'promotion' -- ( 'promotion','transactional', 'Rapido Coins and Power Pass')
    ),
notification_viewed AS
    (
    SELECT 
        ns.*,
        nv.profile_identity AS customerid_view,
        nv.profile_phone AS profile_phone_view,
        nv.eventProps_notification_unique_id
    
    FROM notification_send AS ns
    LEFT JOIN raw.clevertap_customer_notifier_notification_viewed AS nv ON ns.customerid_send = nv.profile_identity AND ns.yyyymmdd = nv.yyyymmdd AND ns.profile_phone_send = nv.profile_phone 
    
    --WHERE yyyymmdd between date_format({{offer_start_date}} - interval '3' day, '%Y%m%d') and  date_format({{offer_end_date}}, '%Y%m%d')
    ),
notification_clicked AS
    (
    SELECT 
        nv.*,
        nc.profile_identity AS customerid_click,
        nc.profile_phone AS profile_phone_click,
        nc.eventProps_notification_unique_id 
    
    FROM notification_viewed AS nv
    LEFT JOIN raw.clevertap_customer_notifier_notification_clicked AS nc ON nv.customerid_view = nc.profile_identity AND nv.yyyymmdd = nc.yyyymmdd AND nv.profile_phone_view = nc.profile_phone
    
    --WHERE yyyymmdd between date_format({{offer_start_date}} - interval '3' day, '%Y%m%d') and  date_format({{offer_end_date}}, '%Y%m%d')
    ),
rr AS
    (
    SELECT  
        day,
        customerid AS customerid_rr
    FROM datasets.customer_rf_daily_kpi 
    WHERE  
        --day between date_format({{offer_start_date}} - interval '3' day, '%Y-%m-%d') AND date_format({{offer_end_date}}, '%Y-%m-%d')
        day = '{offer_start_date}'
        AND customerid IN (SELECT DISTINCT customerid_click FROM notification_clicked)
        AND service_name IN (SELECT DISTINCT service_level FROM city_details)
    GROUP BY 1,2
    HAVING sum(rr_sessions_unique_daily) > 0
    )
    
SELECT
        yyyymmdd, startdate, endDate, selectorid, selector_name, city,
        COUNT(DISTINCT customer_id) AS selector_list,
        COUNT(DISTINCT customerid_send) AS send,
        COUNT(DISTINCT customerid_view) AS view,
        COUNT(DISTINCT customerid_click) AS click,
        TRY(COUNT(DISTINCT customerid_click)*100.00/COUNT(DISTINCT customerid_send)) AS clicked_send,
        TRY(COUNT(DISTINCT customerid_click)*100.00/COUNT(DISTINCT customerid_view)) AS clicked_viewed,
        COUNT(DISTINCT customerid_rr) AS rr,
        COALESCE(TRY(COUNT(DISTINCT customerid_rr)*100.00/ COUNT(DISTINCT customerid_click)), 0) AS rr_clicked
        
         
FROM notification_clicked AS nc
LEFT JOIN rr ON nc.customerid_click = rr.customerid_rr
GROUP BY 1,2,3,4,5,6