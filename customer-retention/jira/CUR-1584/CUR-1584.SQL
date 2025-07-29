WITH user_selector AS
    (
    SELECT 
        yyyymmdd,
        selectorid,
        name AS selector_name,
        userid AS customer_id
    FROM hive.canonical.user_selector
    
    WHERE yyyymmdd between date_format({{offer_start_date}}, '%Y%m%d') and  date_format({{offer_end_date}}, '%Y%m%d') 
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
    
    WHERE yyyymmdd between date_format({{offer_start_date}}, '%Y%m%d') and  date_format({{offer_end_date}}, '%Y%m%d')
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
    WHERE yyyymmdd between date_format({{offer_start_date}}, '%Y%m%d') and  date_format({{offer_end_date}}, '%Y%m%d')
    ),
notification_clicked AS
    (
    SELECT 
        yyyymmdd,
        profile_identity AS customer_id,
        profile_phone,
        eventProps_notification_unique_id
    
    FROM raw.clevertap_customer_notifier_notification_clicked
    WHERE yyyymmdd between date_format({{offer_start_date}}, '%Y%m%d') and  date_format({{offer_end_date}}, '%Y%m%d')
    ),
rr AS
    (
    SELECT  
        day,
        customerid AS customer_id, 
        COUNT(customerid) AS customers
    FROM  datasets.customer_rf_daily_kpi
    WHERE  
        day between date_format({{offer_start_date}}, '%Y-%m-%d') AND date_format({{offer_end_date}}, '%Y-%m-%d')
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
        
        WHERE yyyymmdd between date_format({{offer_start_date}}, '%Y%m%d') and  date_format({{offer_end_date}}, '%Y%m%d')
            AND order_status = 'dropped'
            AND service_obj_service_name IN ('Auto', 'Link')
            AND (spd_fraud_flag != true OR spd_fraud_flag IS NULL)
        )
        WHERE offer_rank = 1
    )    

SELECT
        u.yyyymmdd,
        u.selector_name AS campaign_id,
        u.selectorid,
        COUNT(DISTINCT u.customer_id) AS selector_list,
        --s.Segment,
        COUNT(DISTINCT u.customer_id)-COUNT(DISTINCT ns.customer_id) AS CT_Control,


        COUNT(DISTINCT ns.customer_id) AS CT_Test_Send,
        --ns.eventProps_uuid AS Send,
        COUNT(DISTINCT nv.customer_id) AS viewed,
        --nv.eventProps_notification_unique_id AS viewed,
        COUNT(DISTINCT nc.customer_id) AS clicked,
        --nc.eventProps_notification_unique_id AS clicked
        TRY(COUNT(DISTINCT nc.customer_id)*100.00/COUNT(DISTINCT ns.customer_id)) AS clicked_send,
        TRY(COUNT(DISTINCT nc.customer_id)*100.00/COUNT(DISTINCT nv.customer_id)) AS clicked_viewed,
        
        
        COUNT(DISTINCT rr.customer_id) AS rr,
        --COUNT(DISTINCT rr.customer_id) AS RR,
        
        --TRY(COUNT(DISTINCT rr.customer_id)*100.00/COUNT(DISTINCT ns.customer_id)) AS rr_send,
        --TRY(COUNT(DISTINCT rr.customer_id)*100.00/COUNT(DISTINCT nv.customer_id)) AS rr_viewed,
        TRY(COUNT(DISTINCT rr.customer_id)*100.00/ COUNT(DISTINCT nc.customer_id)) AS rr_clicked,
        
        COUNT(DISTINCT r.customer_id) AS net_customers,
        COUNT(DISTINCT rs.customer_id) AS CT_Test_net_customers
        
FROM user_selector AS u

LEFT JOIN notification_send AS ns ON u.customer_id = ns.customer_id AND u.yyyymmdd = ns.yyyymmdd
LEFT JOIN notification_viewed AS nv ON ns.customer_id = nv.customer_id AND ns.yyyymmdd = nv.yyyymmdd AND ns.profile_phone = nv.profile_phone
LEFT JOIN notification_clicked AS nc ON nv.customer_id = nc.customer_id AND nv.yyyymmdd = nc.yyyymmdd AND nv.profile_phone = nc.profile_phone
LEFT JOIN rr AS rr ON nc.customer_id = rr.customer_id AND date_format(DATE(rr.day), '%Y%m%d') = nc.yyyymmdd
LEFT JOIN rided AS rs ON ns.customer_id = rs.customer_id AND u.yyyymmdd = rs.yyyymmdd
LEFT JOIN rided AS r ON u.customer_id = r.customer_id AND u.yyyymmdd = r.yyyymmdd
--WHERE nc.customer_id is not null
GROUP BY 1,2,3
ORDER BY 8 DESC
