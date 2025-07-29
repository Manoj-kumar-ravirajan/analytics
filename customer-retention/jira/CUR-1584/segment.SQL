customer_segments AS 
    (
    SELECT  
        run_date AS day,
        customer_id AS customer_id,
        --taxi_retention_segments,
        CASE
        WHEN taxi_retention_segments LIKE '%HH%' THEN '02. HH'
        WHEN taxi_retention_segments LIKE '%NEW%' THEN '02. HH'
        WHEN taxi_retention_segments LIKE '%GOLD%' THEN '05. Gold'
        WHEN taxi_retention_segments LIKE '%SILVER%' THEN '04. Silver'
        WHEN taxi_retention_segments LIKE '%PLATINUM%' THEN '06. Platinum'
        WHEN taxi_retention_segments LIKE '%PRIME%' THEN '03. Prime'
        WHEN taxi_retention_segments LIKE '%ELITE%' THEN '07. Elite'
        WHEN taxi_retention_segments LIKE '%INACTIVE%' THEN '09. Inactive'
        WHEN taxi_retention_segments LIKE '%DORMANT%' THEN '08. Dormant'
        ELSE '01. New'
        END AS Segment
    FROM "datasets"."iallocator_customer_segments"    
    
    WHERE 
        run_date >= date_format({{offer_start_date}}, '%Y-%m-%d') AND run_date <= date_format({{offer_end_date}}, '%Y-%m-%d')
        --run_date >= date_format({{offer_start_date}} - interval '1' day , '%Y-%m-%d') AND run_date <= date_format({{offer_end_date}}, '%Y-%m-%d')

    ),