WITH service_mapping AS (
    
        SELECT 
            sm.city_display_name AS city_name,
            sm.service_type AS service_type,
            sm.service_level AS service_name,
            sm.service_detail_id AS service_detail_id
        FROM
            datasets.service_mapping sm
        WHERE
            sm.service_level IN ('Link', 'Auto', 'Auto Pool', 'CabEconomy')
            
            and sm.city_display_name = 'Hyderabad'
    ),
    
    city_cluster_hex_mapping AS (
    
        SELECT
            cch.city AS city_name,
            cch.cluster AS cluster,
            cch.hex_id AS hex_id
        FROM
            datasets.city_cluster_hex cch
        WHERE
            cch.resolution = 8
    ),
    
    supplycursory AS (
        
        SELECT 
            DISTINCT
            sch.yyyymmdd AS yyyymmdd,
            sch.quarter_hour AS quarter_hour,
            sch.hhmm AS hhmm,
            sch.epoch AS epoch,
            sch.servicedetailid AS service_detail_id,
            sch.captain_id AS captain_id,
            sch.service AS service_type,
            sch.location AS pickup_location_hex_8,
            sch.status AS status,
            sch.duration AS duration
        
        FROM 
            datasets.supplycursory_history  sch
        
        WHERE 
            sch.yyyymmdd >= DATE_FORMAT(DATE({{StartDate}}), '%Y%m%d') 
            AND sch.yyyymmdd <= DATE_FORMAT(DATE({{EndDate}}), '%Y%m%d')
            AND status IN (2,3,6,7,8,10)
            AND resolution = '8'
    ),
    
    combined_df AS (
        
        SELECT 
            sch.epoch AS epoch,
            sch.yyyymmdd AS yyyymmdd,
            sm.city_name AS city_name,
            sm.service_name AS service_name,
            COALESCE(sm.service_detail_id, sch.service_detail_id) AS service_detail_id,
            cch.cluster AS pickup_location,
            sch.pickup_location_hex_8 AS pickup_location_hex_8,
            sch.captain_id AS captain_id,
            sch.duration AS duration
            
        FROM
           service_mapping sm
        
        LEFT JOIN
            city_cluster_hex_mapping cch
            ON sm.city_name = cch.city_name
            
        LEFT JOIN 
            supplycursory sch
            ON cch.hex_id = sch.pickup_location_hex_8
            AND (sch.service_detail_id = sm.service_detail_id
                OR 
                sch.service_type = sm.service_type)
    )

SELECT 
    cdf.city_name AS city_name,
    cdf.service_name AS service_name,
    cdf.service_detail_id AS service_detail_id,
    cdf.pickup_location AS pickup_location,
    (SUM(cdf.duration)/CAST(3600 as REAL)*1.00) AS login_hours
FROM
    combined_df cdf
    
GROUP BY
    city_name,
    service_name,
    service_detail_id,
    pickup_location
ORDER BY 4,2