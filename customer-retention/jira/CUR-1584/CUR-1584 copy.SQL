with city_details AS 
    (
    SELECT 
        city_id, 
        city_display_name AS city_name,
        service_level,
        service_id
    FROM datasets.service_mapping  
    WHERE {{city_display_name}} AND {{service_level}}
    -- service_level in ('Link','Auto')
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
            yyyymmdd BETWEEN date_format({{offer_start_date}} - interval '3' day, '%Y%m%d') AND date_format({{offer_end_date}}, '%Y%m%d')
            --yyyymmdd >= date_format({{offer_start_date}} - interval '3' day, '%Y%m%d') 
            --AND startdate = date_format({{offer_start_date}}, '%Y-%m-%d')
            --AND endDate = date_format({{offer_end_date}}, '%Y-%m-%d')
            --AND substr(NAME,1,3) = 'IA_Banga_20220603_15UPTO09RETD2A0000_V1_AU'
            AND substr(NAME,1,3) = 'IA_'    -- Prefix s IA_
        ) 
    CROSS JOIN unnest(selectors_list) AS selectors_list(list)
    WHERE
        city IN (SELECT DISTINCT city_name FROM city_details)--= 'Chennai'
        AND services IN (SELECT DISTINCT service_id FROM city_details) --= '5bd6c6e2e79cc313a94728d0'
    ),


user_selector AS
    (
    SELECT 
        yyyymmdd,
        selectorid,
        name AS selector_name,
        userid AS customer_id
    FROM hive.canonical.user_selector
    
    WHERE yyyymmdd between date_format({{offer_start_date}} - interval '3' day, '%Y%m%d') and  date_format({{offer_end_date}}, '%Y%m%d') 
        AND substr(name,1,3) = 'IA_'    -- Prefix s IA_
        --AND selectorid = '6289065ed1c12a00018d6495'
    ),

notification_send AS 
    (
    SELECT 
        yyyymmdd,
        profile_identity AS customer_id,
        profile_phone,
        eventProps_uuid
        
    FROM raw.clevertap_customer_notifier_notification_send
    
    WHERE yyyymmdd between date_format({{offer_start_date}} - interval '3' day, '%Y%m%d') and  date_format({{offer_end_date}}, '%Y%m%d')
        --AND eventProps_notification_channel = 'promotion' -- ( 'promotion','transactional', 'Rapido Coins and Power Pass')
    ),
notification_viewed AS
    (
    SELECT 
        yyyymmdd,
        profile_identity AS customer_id,
        profile_phone,
        eventProps_notification_unique_id
    
    FROM raw.clevertap_customer_notifier_notification_viewed
    WHERE yyyymmdd between date_format({{offer_start_date}} - interval '3' day, '%Y%m%d') and  date_format({{offer_end_date}}, '%Y%m%d')
    ),
notification_clicked AS
    (
    SELECT 
        yyyymmdd,
        profile_identity AS customer_id,
        profile_phone,
        eventProps_notification_unique_id
    
    FROM raw.clevertap_customer_notifier_notification_clicked
    WHERE yyyymmdd between date_format({{offer_start_date}} - interval '3' day, '%Y%m%d') and  date_format({{offer_end_date}}, '%Y%m%d')
    ),
rr AS
    (
    SELECT  
        day,
        customerid AS customer_id, 
        COUNT(customerid) AS customers
    FROM  datasets.customer_rf_daily_kpi
    WHERE  
        day between date_format({{offer_start_date}} - interval '3' day, '%Y-%m-%d') AND date_format({{offer_end_date}}, '%Y-%m-%d')
        AND service_name in ('Link','Auto')
    GROUP BY 1,2
    HAVING sum(rr_sessions_unique_daily) > 0
    ),

rided AS
    (
    SELECT 
           yyyymmdd,
           order_date,
           customer_id
    FROM
        (
        SELECT 
               yyyymmdd,
               order_date,
               customer_id,
               row_number() over(partition by customer_id order by yyyymmdd desc) offer_rank
               
        FROM "orders"."order_logs_snapshot"
        
        WHERE yyyymmdd between date_format({{offer_start_date}} - interval '3' day, '%Y%m%d') and  date_format({{offer_end_date}}, '%Y%m%d')
            AND order_status = 'dropped'
            AND service_obj_service_name IN (SELECT DISTINCT service_level FROM city_details)--('Auto', 'Link')
            AND (spd_fraud_flag != true OR spd_fraud_flag IS NULL)
        )
        WHERE offer_rank = 1
    )  

SELECT
        u.yyyymmdd,
        u.selector_name AS campaign_id,
        u.selectorid,
        co.city,
        COUNT(DISTINCT u.customer_id) AS selector_list,
        --COUNT(DISTINCT u.customer_id)-COUNT(DISTINCT ns.customer_id) AS CTR,
        COUNT(DISTINCT ns.customer_id) AS Send,
        COUNT(DISTINCT nv.customer_id) AS viewed,
        COUNT(DISTINCT nc.customer_id) AS clicked,
        TRY(COUNT(DISTINCT nc.customer_id)*100.00/COUNT(DISTINCT ns.customer_id)) AS clicked_send,
        TRY(COUNT(DISTINCT nc.customer_id)*100.00/COUNT(DISTINCT nv.customer_id)) AS clicked_viewed,
        
        COUNT(DISTINCT rr.customer_id) AS rr,
        coalesce(TRY(COUNT(DISTINCT rr.customer_id)*100.00/ COUNT(DISTINCT nc.customer_id)), 0) AS rr_clicked,
        
        COUNT(DISTINCT r.customer_id) AS net_customers,
        COUNT(DISTINCT rs.customer_id) AS CT_Test_net_customers
        
FROM created_offer AS co

LEFT JOIN user_selector AS u ON co.selectorid = u.selectorid
LEFT JOIN notification_send AS ns ON u.customer_id = ns.customer_id AND u.yyyymmdd = ns.yyyymmdd
LEFT JOIN notification_viewed AS nv ON ns.customer_id = nv.customer_id AND ns.yyyymmdd = nv.yyyymmdd AND ns.profile_phone = nv.profile_phone
LEFT JOIN notification_clicked AS nc ON nv.customer_id = nc.customer_id AND nv.yyyymmdd = nc.yyyymmdd AND nv.profile_phone = nc.profile_phone
LEFT JOIN rr AS rr ON nc.customer_id = rr.customer_id AND date_format(DATE(rr.day), '%Y%m%d') = nc.yyyymmdd
LEFT JOIN rided AS rs ON ns.customer_id = rs.customer_id AND u.yyyymmdd = rs.yyyymmdd
LEFT JOIN rided AS r ON u.customer_id = r.customer_id AND u.yyyymmdd = r.yyyymmdd

GROUP BY 1,2,3,4
ORDER BY 9 DESC
