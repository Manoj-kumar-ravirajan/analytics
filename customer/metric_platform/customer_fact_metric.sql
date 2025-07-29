-- taxi_rr_last_30_days (30, 60, 90, 365, lifetime)
WITH BASE AS (
    SELECT 
        fact.epoch AS epoch,
        fact.customer_id AS customer_id,
        fact.city_name AS city_name,
        fact.service_category AS service_category,
        fact.service_detail_id AS service_detail_id,
        fact.service_obj_service_name AS service_obj_service_name,
        fact.modified_order_status AS modified_order_status,
        fact.order_id AS order_id
    FROM 
        orders.order_logs_fact AS fact
    WHERE 
        fact.yyyymmdd > DATE_FORMAT(DATE('{{StartDate}}') - INTERVAL '30' DAY, '%Y%m%d')
        AND fact.yyyymmdd <= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d')
        AND fact.service_category in ('link', 'auto','cab')                              
    )
    
    SELECT 
        BASE.customer_id,
        COUNT(DISTINCT BASE.order_id) AS taxi_rr_last_30_days
    FROM 
        BASE AS BASE
    GROUP BY 
        BASE.customer_id

-- taxi_net_last_30_days (30, 60, 90, 365, lifetime)                
WITH BASE AS (
    SELECT 
        fact.epoch AS epoch,
        fact.customer_id AS customer_id,
        fact.city_name AS city_name,
        fact.service_category AS service_category,
        fact.service_detail_id AS service_detail_id,
        fact.service_obj_service_name AS service_obj_service_name,
        fact.modified_order_status AS modified_order_status,
        fact.order_id AS order_id
    FROM 
        orders.order_logs_fact AS fact
    WHERE 
        fact.yyyymmdd > DATE_FORMAT(DATE('{{StartDate}}') - INTERVAL '30' DAY, '%Y%m%d')
        AND fact.yyyymmdd <= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d')
        AND fact.modified_order_status = 'dropped' 
        AND fact.service_category in ('link', 'auto','cab')
    )
    
    SELECT 
        BASE.customer_id,
        COUNT(DISTINCT BASE.order_id) AS taxi_net_last_30_days
    FROM 
        BASE AS BASE
    GROUP BY 
        BASE.customer_id
        
-- taxi_accepted_last_30_days (30, 60, 90, 365, lifetime)
WITH BASE AS (
    SELECT 
        fact.epoch AS epoch,
        fact.customer_id AS customer_id,
        fact.city_name AS city_name,
        fact.service_category AS service_category,
        fact.service_detail_id AS service_detail_id,
        fact.service_obj_service_name AS service_obj_service_name,
        fact.modified_order_status AS modified_order_status,
        fact.order_id AS order_id
    FROM 
        orders.order_logs_fact AS fact
    WHERE 
        fact.yyyymmdd > DATE_FORMAT(DATE('{{StartDate}}') - INTERVAL '30' DAY, '%Y%m%d')
        AND fact.yyyymmdd <= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d')
        AND fact.accepted_epoch IS NOT NULL 
        AND fact.service_category in ('link', 'auto','cab')
    )
    
    SELECT 
        BASE.customer_id,
        COUNT(DISTINCT BASE.order_id) AS taxi_accepted_last_30_days
    FROM 
        BASE AS BASE
    GROUP BY 
        BASE.customer_id
        
-- taxi_cobra_last_30_days (30, 60, 90, 365, lifetime)
WITH BASE AS (
    SELECT 
        fact.epoch AS epoch,
        fact.customer_id AS customer_id,
        fact.city_name AS city_name,
        fact.service_category AS service_category,
        fact.service_detail_id AS service_detail_id,
        fact.service_obj_service_name AS service_obj_service_name,
        fact.modified_order_status AS modified_order_status,
        fact.order_id AS order_id
    FROM 
        orders.order_logs_fact AS fact
    WHERE 
        fact.yyyymmdd > DATE_FORMAT(DATE('{{StartDate}}') - INTERVAL '30' DAY, '%Y%m%d')
        AND fact.yyyymmdd <= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d')
        AND fact.modified_order_status = 'COBRA'
        AND fact.service_category in ('link', 'auto','cab')
    )
    
    SELECT 
        BASE.customer_id,
        COUNT(DISTINCT BASE.order_id) AS taxi_cobra_last_30_days
    FROM 
        BASE AS BASE
    GROUP BY 
        BASE.customer_id

-- taxi_ocara_last_30_days (30, 60, 90, 365, lifetime)
WITH BASE AS ( 
    SELECT 
        fact.epoch AS epoch,
        fact.customer_id AS customer_id,
        fact.city_name AS city_name,
        fact.service_category AS service_category,
        fact.service_detail_id AS service_detail_id,
        fact.service_obj_service_name AS service_obj_service_name,
        fact.modified_order_status AS modified_order_status,
        fact.order_id AS order_id
    FROM 
        orders.order_logs_fact AS fact
    WHERE 
        fact.yyyymmdd > DATE_FORMAT(DATE('{{StartDate}}') - INTERVAL '30' DAY, '%Y%m%d')
        AND fact.yyyymmdd <= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d')
        AND fact.modified_order_status = 'OCARA'
        AND fact.service_category in ('link', 'auto','cab')
    )
    
    SELECT
        BASE.customer_id,
        COUNT(DISTINCT BASE.order_id) AS taxi_ocara_last_30_days
    FROM 
        BASE AS BASE
    GROUP BY 
        BASE.customer_id
        
-- taxi_expired_last_30_days (30, 60, 90, 365, lifetime)
WITH BASE AS (
    SELECT 
        fact.epoch AS epoch,
        fact.customer_id AS customer_id,
        fact.city_name AS city_name,
        fact.service_category AS service_category,
        fact.service_detail_id AS service_detail_id,
        fact.service_obj_service_name AS service_obj_service_name,
        fact.modified_order_status AS modified_order_status,
        fact.order_id AS order_id
    FROM 
        orders.order_logs_fact AS fact
    WHERE 
        fact.yyyymmdd > DATE_FORMAT(DATE('{{StartDate}}') - INTERVAL '30' DAY, '%Y%m%d')
        AND fact.yyyymmdd <= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d')
        AND fact.modified_order_status = 'expired'
        AND fact.service_category in ('link', 'auto','cab')
    )
    
    SELECT
        BASE.customer_id,
        COUNT(DISTINCT BASE.order_id) AS taxi_expired_last_30_days
    FROM 
        BASE AS BASE
    GROUP BY 
        BASE.customer_id

-- taxi_rr_active_days_last_30_days (30, 60, 90, 365, lifetime)
WITH BASE AS (
    SELECT 
        fact.epoch AS epoch,
        fact.yyyymmdd AS yyyymmdd,
        fact.customer_id AS customer_id,
        fact.city_name AS city_name,
        fact.service_category AS service_category,
        fact.service_detail_id AS service_detail_id,
        fact.service_obj_service_name AS service_obj_service_name,
        fact.modified_order_status AS modified_order_status,
        fact.order_id AS order_id
    FROM 
        orders.order_logs_fact AS fact
    WHERE 
        fact.yyyymmdd > DATE_FORMAT(DATE('{{StartDate}}') - INTERVAL '30' DAY, '%Y%m%d')
        AND fact.yyyymmdd <= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d')
        AND fact.service_category in ('link', 'auto','cab')                              
    )
    
    SELECT 
        BASE.customer_id,
        COUNT(DISTINCT BASE.yyyymmdd) AS taxi_rr_active_days_last_30_days
    FROM 
        BASE AS BASE
    GROUP BY 
        BASE.customer_id

-- taxi_net_active_days_last_30_days (30, 60, 90, 365, lifetime)
WITH BASE AS (
    SELECT 
        fact.epoch AS epoch,
        fact.yyyymmdd AS yyyymmdd,
        fact.customer_id AS customer_id,
        fact.city_name AS city_name,
        fact.service_category AS service_category,
        fact.service_detail_id AS service_detail_id,
        fact.service_obj_service_name AS service_obj_service_name,
        fact.modified_order_status AS modified_order_status,
        fact.order_id AS order_id
    FROM 
        orders.order_logs_fact AS fact
    WHERE 
        fact.yyyymmdd > DATE_FORMAT(DATE('{{StartDate}}') - INTERVAL '30' DAY, '%Y%m%d')
        AND fact.yyyymmdd <= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d')
        AND fact.modified_order_status = 'dropped' 
        AND fact.service_category in ('link', 'auto','cab')
    )
    
    SELECT 
        BASE.customer_id,
        COUNT(DISTINCT BASE.yyyymmdd) AS taxi_net_active_days_last_30_days
    FROM 
        BASE AS BASE
    GROUP BY 
        BASE.customer_id

-- taxi_haps_last_30_days (30, 60, 90, 365, lifetime)
WITH BASE AS (
    SELECT 
        fact.epoch AS epoch,
        fact.customer_id AS customer_id,
        fact.city_name AS city_name,
        fact.service_category AS service_category,
        fact.service_detail_id AS service_detail_id,
        fact.service_obj_service_name AS service_obj_service_name,
        fact.modified_order_status AS modified_order_status,
        fact.order_id AS order_id
    FROM 
        orders.order_logs_fact AS fact
    WHERE 
        fact.yyyymmdd > DATE_FORMAT(DATE('{{StartDate}}') - INTERVAL '30' DAY, '%Y%m%d')
        AND fact.yyyymmdd <= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d')
        AND fact.modified_order_status = 'dropped'
        AND fact.customer_set_pickup_ride_started_distance_meters <= 30
        AND fact.service_category in ('link', 'auto','cab')
    )
    
    SELECT
        BASE.customer_id,
        COUNT(DISTINCT BASE.order_id) AS taxi_haps_last_30_days
    FROM 
        BASE AS BASE
    GROUP BY 
        BASE.customer_id

-- taxi_rr_bid_last_30_days (30, 60, 90, 365, lifetime) and positive and negative
WITH BASE AS (
    SELECT 
        fact.epoch AS epoch,
        fact.customer_id AS customer_id,
        fact.city_name AS city_name,
        fact.service_category AS service_category,
        fact.service_detail_id AS service_detail_id,
        fact.service_obj_service_name AS service_obj_service_name,
        fact.modified_order_status AS modified_order_status,
        fact.order_id AS order_id
    FROM 
        orders.order_logs_fact AS fact
    WHERE 
        fact.yyyymmdd > DATE_FORMAT(DATE('{{StartDate}}') - INTERVAL '30' DAY, '%Y%m%d')
        AND fact.yyyymmdd <= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d')
        AND fact.amount_breakup_bid_delta_total <> 0
        AND fact.service_category in ('link', 'auto','cab')
    )
    
    SELECT
        BASE.customer_id,
        COUNT(DISTINCT BASE.order_id) AS taxi_rr_bid_last_30_days
    FROM 
        BASE AS BASE
    GROUP BY 
        BASE.customer_id

-- taxi_short_lm_rr_last_30_days (30, 60, 90, 365, lifetime) # p33 and p66 (3.5 and 7.5) May vary for service to service
WITH BASE AS (
    SELECT 
        fact.epoch AS epoch,
        fact.customer_id AS customer_id,
        fact.city_name AS city_name,
        fact.service_category AS service_category,
        fact.service_detail_id AS service_detail_id,
        fact.service_obj_service_name AS service_obj_service_name,
        fact.modified_order_status AS modified_order_status,
        fact.order_id AS order_id
    FROM 
        orders.order_logs_fact AS fact
    WHERE 
        fact.yyyymmdd > DATE_FORMAT(DATE('{{StartDate}}') - INTERVAL '30' DAY, '%Y%m%d')
        AND fact.yyyymmdd <= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d')
        AND fact.ride_distance <= 3.5
        AND fact.service_category in ('link', 'auto','cab')
    )
    
    SELECT
        BASE.customer_id,
        COUNT(DISTINCT BASE.order_id) AS taxi_short_lm_rr_last_30_days
    FROM 
        BASE AS BASE
    GROUP BY 
        BASE.customer_id

-- taxi_morning_rr_last_30_days (30, 60, 90, 365, lifetime)
WITH BASE AS (
    
    SELECT 
        fact.epoch AS epoch,
        fact.customer_id AS customer_id,
        fact.city_name AS city_name,
        fact.service_category AS service_category,
        fact.service_detail_id AS service_detail_id,
        fact.service_obj_service_name AS service_obj_service_name,
        fact.modified_order_status AS modified_order_status,
        fact.order_id AS order_id
    FROM 
        orders.order_logs_fact AS fact
    WHERE 
        fact.yyyymmdd > DATE_FORMAT(DATE('{{StartDate}}') - INTERVAL '30' DAY, '%Y%m%d')
        AND fact.yyyymmdd <= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d')
        AND fact.quarter_hour >= '0800' 
        AND fact.quarter_hour < '1100' 
        AND fact.service_category in ('link', 'auto','cab')
    )
    
    SELECT
        BASE.customer_id,
        COUNT(DISTINCT BASE.order_id) AS taxi_morning_rr_last_30_days
    FROM 
        BASE AS BASE
    GROUP BY 
        BASE.customer_id

/*
    case
        WHEN quarter_hour >= '0800' AND quarter_hour < '1100' THEN 'morning'
        WHEN quarter_hour >= '1100' AND quarter_hour < '1700' THEN 'afternoon'
        WHEN quarter_hour >= '1700' AND quarter_hour < '2100' THEN 'evening'
        ELSE 'rest' -- THis can be further breaked into rest_morning and rest_evening
    end as peak
*/

-- taxi_last_5_rides_haps_in_last_90_days
WITH BASE AS (
    SELECT 
        fact.epoch AS epoch,
        fact.customer_id AS customer_id,
        fact.city_name AS city_name,
        fact.service_category AS service_category,
        fact.service_detail_id AS service_detail_id,
        fact.service_obj_service_name AS service_obj_service_name,
        fact.modified_order_status AS modified_order_status,
        fact.order_id AS order_id,
        fact.customer_set_pickup_ride_started_distance_meters AS customer_set_pickup_ride_started_distance_meters,
        ROW_NUMBER() OVER(PARTITION BY FACT.customer_id ORDER BY fact.epoch DESC) rn
    FROM 
        orders.order_logs_fact AS fact
    WHERE 
        fact.yyyymmdd > DATE_FORMAT(DATE('{{StartDate}}') - INTERVAL '90' DAY, '%Y%m%d')
        AND fact.yyyymmdd <= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d')
        AND fact.modified_order_status = 'dropped'
        AND fact.service_category in ('link', 'auto','cab')
    )
    
    SELECT
        BASE.customer_id,
        COUNT(DISTINCT CASE WHEN customer_set_pickup_ride_started_distance_meters <= 30 THEN BASE.order_id END) AS taxi_last_5_rides_haps_in_last_90_days
    FROM 
        BASE AS BASE
    WHERE 
        rn <= 5
    GROUP BY 
        BASE.customer_id
    HAVING MAX(rn) = 5

-- taxi_last_5_rides_ocara_in_last_90_days
WITH BASE AS (
    SELECT 
        fact.epoch AS epoch,
        fact.customer_id AS customer_id,
        fact.city_name AS city_name,
        fact.service_category AS service_category,
        fact.service_detail_id AS service_detail_id,
        fact.service_obj_service_name AS service_obj_service_name,
        fact.modified_order_status AS modified_order_status,
        fact.order_id AS order_id,
        ROW_NUMBER() OVER(PARTITION BY FACT.customer_id ORDER BY fact.epoch DESC) rn
    FROM 
        orders.order_logs_fact AS fact
    WHERE 
        fact.yyyymmdd > DATE_FORMAT(DATE('{{StartDate}}') - INTERVAL '90' DAY, '%Y%m%d')
        AND fact.yyyymmdd <= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d')
        AND fact.service_category in ('link', 'auto','cab')
    )
    
    SELECT
        BASE.customer_id,
        COUNT(DISTINCT CASE WHEN modified_order_status = 'OCARA' THEN BASE.order_id END) AS taxi_last_5_rides_ocara_in_last_90_days
    FROM 
        BASE AS BASE
    WHERE 
        rn <= 5
    GROUP BY 
        BASE.customer_id
    HAVING MAX(rn) = 5

-- taxi_last_5_rides_expired_in_last_90_days
WITH BASE AS (
    SELECT 
        fact.epoch AS epoch,
        fact.customer_id AS customer_id,
        fact.city_name AS city_name,
        fact.service_category AS service_category,
        fact.service_detail_id AS service_detail_id,
        fact.service_obj_service_name AS service_obj_service_name,
        fact.modified_order_status AS modified_order_status,
        fact.order_id AS order_id,
        ROW_NUMBER() OVER(PARTITION BY FACT.customer_id ORDER BY fact.epoch DESC) rn
    FROM 
        orders.order_logs_fact AS fact
    WHERE 
        fact.yyyymmdd > DATE_FORMAT(DATE('{{StartDate}}') - INTERVAL '90' DAY, '%Y%m%d')
        AND fact.yyyymmdd <= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d')
        AND fact.service_category in ('link', 'auto','cab')
    )
    
    SELECT
        BASE.customer_id,
        COUNT(DISTINCT CASE WHEN modified_order_status = 'expired' THEN BASE.order_id END) AS taxi_last_5_rides_expired_in_last_90_days
    FROM 
        BASE AS BASE
    WHERE 
        rn <= 5
    GROUP BY 
        BASE.customer_id
    HAVING MAX(rn) = 5

-- taxi_last_5_rides_avg_rating_in_last_90_days
WITH BASE AS (
    SELECT 
        fact.epoch AS epoch,
        fact.customer_id AS customer_id,
        fact.city_name AS city_name,
        fact.service_category AS service_category,
        fact.service_detail_id AS service_detail_id,
        fact.service_obj_service_name AS service_obj_service_name,
        fact.modified_order_status AS modified_order_status,
        fact.order_id AS order_id,
        CASE WHEN fact.customer_feedback_rating BETWEEN 1 AND 5 THEN fact.customer_feedback_rating END AS customer_feedback_rating,
        ROW_NUMBER() OVER(PARTITION BY FACT.customer_id ORDER BY fact.epoch DESC) rn
    FROM 
        orders.order_logs_fact AS fact
    WHERE 
        fact.yyyymmdd > DATE_FORMAT(DATE('{{StartDate}}') - INTERVAL '90' DAY, '%Y%m%d')
        AND fact.yyyymmdd <= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d')
        AND fact.modified_order_status = 'dropped'
        AND fact.service_category in ('link', 'auto','cab')
    )
    
    SELECT
        BASE.customer_id,
        AVG(BASE.customer_feedback_rating) AS taxi_last_5_rides_avg_rating_in_last_90_days
    FROM 
        BASE AS BASE
    WHERE 
        rn <= 5
    GROUP BY 
        BASE.customer_id
    HAVING MAX(rn) = 5