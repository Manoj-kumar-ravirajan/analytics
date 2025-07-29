-- rf_segment_gross_orders_w1

WITH iallocator_cte AS ( 
    
    SELECT
        segments.run_date AS run_date,
        segments.customer_id AS customer_id, 
        segments.taxi_lifetime_last_ride_city AS city,
        segments.taxi_ltr_segment AS taxi_ltr_segments,
        segments.taxi_retention_segments AS taxi_retention_segments
    FROM 
        datasets.iallocator_customer_segments AS segments
    where 
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '1' DAY, '%Y-%m-%d')
    ),

    view AS (

    SELECT
        cte.run_date AS run_date,
        WEEKOFYEAR(DATE_FORMAT(FROM_UTC_TIMESTAMP(cte.run_date,'IST') + INTERVAL '1' DAY,'%Y-%m-%d')) AS week_no,
        DATE_FORMAT(FROM_UTC_TIMESTAMP(cte.run_date,'IST') + INTERVAL '1' DAY,'%Y-%m-%d') AS week_start_date,
        cte.customer_id AS customer_id,
        cte.city AS city,
        cte.taxi_ltr_segments AS taxi_ltr_segments,
        cte.taxi_retention_segments AS taxi_retention_segments
    FROM 
        iallocator_cte AS cte
    ),
    
    olf_cte AS (
    
    SELECT 
        olf.yyyymmdd AS yyyymmdd,
        olf.service_obj_service_name AS service_name,
        olf.customer_id AS customer_id,
        olf.order_id AS order_id,
        olf.modified_order_status AS modified_order_status,
        olf.spd_fraud_flag AS spd_fraud_flag
    FROM 
        orders.order_logs_fact AS olf
    WHERE 
        olf.yyyymmdd >= DATE_FORMAT(DATE_TRUNC('WEEK', '{{StartDate}}'), '%Y%m%d')
        AND olf.yyyymmdd <= DATE_FORMAT(DATE_TRUNC('WEEK', '{{EndDate}}') + INTERVAL '6' DAY, '%Y%m%d')
        AND olf.service_obj_service_name IN ('Auto','Auto Lite','Auto NCR','Auto Pool','AutoPremium','CityAuto','Link','Bike Lite','Bike Metro','Bike Pink','CabEconomy','CabPremium')
    ),
    
    merge AS (
    
    SELECT
        view.run_date AS run_date,
        view.week_no AS week_no,
        view.week_start_date AS week_start_date,
        view.city AS city,
        view.taxi_ltr_segments AS taxi_ltr_segments,
        view.taxi_retention_segments AS taxi_retention_segments,
        olf_cte.customer_id AS customer_id,
        olf_cte.service_name AS service_name,
        olf_cte.order_id AS order_id,
        olf_cte.modified_order_status AS modified_order_status,
        olf_cte.spd_fraud_flag AS spd_fraud_flag
    FROM 
        view AS view
    LEFT JOIN 
        olf_cte AS olf_cte
        ON view.customer_id = olf_cte.customer_id
    )
    
    SELECT
        merge.week_no AS week_no,
        merge.week_start_date AS week_start_date,
        merge.taxi_ltr_segments AS taxi_ltr_segments,
        merge.taxi_retention_segments AS taxi_retention_segments,
        merge.service_name AS service_name,
        COUNT(DISTINCT merge.order_id) AS rf_segment_gross_orders_w1
    FROM 
        merge AS merge
    GROUP BY 
        week_no,
        week_start_date,
        taxi_ltr_segments,
        taxi_retention_segments,
        service_name



-- rf_segment_gross_orders_w2

WITH iallocator_cte AS ( 
    
    SELECT
        segments.run_date AS run_date,
        segments.customer_id AS customer_id, 
        segments.taxi_lifetime_last_ride_city AS city,
        segments.taxi_ltr_segment AS taxi_ltr_segments,
        segments.taxi_retention_segments AS taxi_retention_segments
    FROM 
        datasets.iallocator_customer_segments AS segments
    where 
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '8' DAY, '%Y-%m-%d')
    ),

    view AS (

    SELECT
        cte.run_date AS run_date,
        WEEKOFYEAR(DATE_FORMAT(FROM_UTC_TIMESTAMP(cte.run_date,'IST') + INTERVAL '1' DAY,'%Y-%m-%d')) AS week_no,
        DATE_FORMAT(FROM_UTC_TIMESTAMP(cte.run_date,'IST') + INTERVAL '1' DAY,'%Y-%m-%d') AS week_start_date,
        cte.customer_id AS customer_id,
        cte.city AS city,
        cte.taxi_ltr_segments AS taxi_ltr_segments,
        cte.taxi_retention_segments AS taxi_retention_segments
    FROM 
        iallocator_cte AS cte
    ),
    
    olf_cte AS (
    
    SELECT 
        olf.yyyymmdd AS yyyymmdd,
        olf.service_obj_service_name AS service_name,
        olf.customer_id AS customer_id,
        olf.order_id AS order_id,
        olf.modified_order_status AS modified_order_status,
        olf.spd_fraud_flag AS spd_fraud_flag
    FROM 
        orders.order_logs_fact AS olf
    WHERE 
        olf.yyyymmdd >= DATE_FORMAT(DATE_TRUNC('WEEK', '{{StartDate}}'), '%Y%m%d')
        AND olf.yyyymmdd <= DATE_FORMAT(DATE_TRUNC('WEEK', '{{EndDate}}') + INTERVAL '6' DAY, '%Y%m%d')
        AND olf.service_obj_service_name IN ('Auto','Auto Lite','Auto NCR','Auto Pool','AutoPremium','CityAuto','Link','Bike Lite','Bike Metro','Bike Pink','CabEconomy','CabPremium')
    ),
    
    merge AS (
    
    SELECT
        view.run_date AS run_date,
        view.week_no AS week_no,
        view.week_start_date AS week_start_date,
        view.city AS city,
        view.taxi_ltr_segments AS taxi_ltr_segments,
        view.taxi_retention_segments AS taxi_retention_segments,
        olf_cte.customer_id AS customer_id,
        olf_cte.service_name AS service_name,
        olf_cte.order_id AS order_id,
        olf_cte.modified_order_status AS modified_order_status,
        olf_cte.spd_fraud_flag AS spd_fraud_flag
    FROM 
        view AS view
    LEFT JOIN 
        olf_cte AS olf_cte
        ON view.customer_id = olf_cte.customer_id
    )
    
    SELECT
        merge.week_no AS week_no,
        merge.week_start_date AS week_start_date,
        merge.taxi_ltr_segments AS taxi_ltr_segments,
        merge.taxi_retention_segments AS taxi_retention_segments,
        merge.service_name AS service_name,
        COUNT(DISTINCT merge.order_id) AS rf_segment_gross_orders_w2
    FROM 
        merge AS merge
    GROUP BY 
        week_no,
        week_start_date,
        taxi_ltr_segments,
        taxi_retention_segments,
        service_name


-- rf_segment_gross_orders_w3


WITH iallocator_cte AS ( 
    
    SELECT
        segments.run_date AS run_date,
        segments.customer_id AS customer_id, 
        segments.taxi_lifetime_last_ride_city AS city,
        segments.taxi_ltr_segment AS taxi_ltr_segments,
        segments.taxi_retention_segments AS taxi_retention_segments
    FROM 
        datasets.iallocator_customer_segments AS segments
    where 
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '15' DAY, '%Y-%m-%d')
    ),

    view AS (

    SELECT
        cte.run_date AS run_date,
        WEEKOFYEAR(DATE_FORMAT(FROM_UTC_TIMESTAMP(cte.run_date,'IST') + INTERVAL '1' DAY,'%Y-%m-%d')) AS week_no,
        DATE_FORMAT(FROM_UTC_TIMESTAMP(cte.run_date,'IST') + INTERVAL '1' DAY,'%Y-%m-%d') AS week_start_date,
        cte.customer_id AS customer_id,
        cte.city AS city,
        cte.taxi_ltr_segments AS taxi_ltr_segments,
        cte.taxi_retention_segments AS taxi_retention_segments
    FROM 
        iallocator_cte AS cte
    ),
    
    olf_cte AS (
    
    SELECT 
        olf.yyyymmdd AS yyyymmdd,
        olf.service_obj_service_name AS service_name,
        olf.customer_id AS customer_id,
        olf.order_id AS order_id,
        olf.modified_order_status AS modified_order_status,
        olf.spd_fraud_flag AS spd_fraud_flag
    FROM 
        orders.order_logs_fact AS olf
    WHERE 
        olf.yyyymmdd >= DATE_FORMAT(DATE_TRUNC('WEEK', '{{StartDate}}'), '%Y%m%d')
        AND olf.yyyymmdd <= DATE_FORMAT(DATE_TRUNC('WEEK', '{{EndDate}}') + INTERVAL '6' DAY, '%Y%m%d')
        AND olf.service_obj_service_name IN ('Auto','Auto Lite','Auto NCR','Auto Pool','AutoPremium','CityAuto','Link','Bike Lite','Bike Metro','Bike Pink','CabEconomy','CabPremium')
    ),
    
    merge AS (
    
    SELECT
        view.run_date AS run_date,
        view.week_no AS week_no,
        view.week_start_date AS week_start_date,
        view.city AS city,
        view.taxi_ltr_segments AS taxi_ltr_segments,
        view.taxi_retention_segments AS taxi_retention_segments,
        olf_cte.customer_id AS customer_id,
        olf_cte.service_name AS service_name,
        olf_cte.order_id AS order_id,
        olf_cte.modified_order_status AS modified_order_status,
        olf_cte.spd_fraud_flag AS spd_fraud_flag
    FROM 
        view AS view
    LEFT JOIN 
        olf_cte AS olf_cte
        ON view.customer_id = olf_cte.customer_id
    )
    
    SELECT
        merge.week_no AS week_no,
        merge.week_start_date AS week_start_date,
        merge.taxi_ltr_segments AS taxi_ltr_segments,
        merge.taxi_retention_segments AS taxi_retention_segments,
        merge.service_name AS service_name,
        COUNT(DISTINCT merge.order_id) AS rf_segment_gross_orders_w3
    FROM 
        merge AS merge
    GROUP BY 
        week_no,
        week_start_date,
        taxi_ltr_segments,
        taxi_retention_segments,
        service_name

-- rf_segment_gross_orders_w4

WITH iallocator_cte AS ( 
    
    SELECT
        segments.run_date AS run_date,
        segments.customer_id AS customer_id, 
        segments.taxi_lifetime_last_ride_city AS city,
        segments.taxi_ltr_segment AS taxi_ltr_segments,
        segments.taxi_retention_segments AS taxi_retention_segments
    FROM 
        datasets.iallocator_customer_segments AS segments
    where 
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '22' DAY, '%Y-%m-%d')
    ),

    view AS (

    SELECT
        cte.run_date AS run_date,
        WEEKOFYEAR(DATE_FORMAT(FROM_UTC_TIMESTAMP(cte.run_date,'IST') + INTERVAL '1' DAY,'%Y-%m-%d')) AS week_no,
        DATE_FORMAT(FROM_UTC_TIMESTAMP(cte.run_date,'IST') + INTERVAL '1' DAY,'%Y-%m-%d') AS week_start_date,
        cte.customer_id AS customer_id,
        cte.city AS city,
        cte.taxi_ltr_segments AS taxi_ltr_segments,
        cte.taxi_retention_segments AS taxi_retention_segments
    FROM 
        iallocator_cte AS cte
    ),
    
    olf_cte AS (
    
    SELECT 
        olf.yyyymmdd AS yyyymmdd,
        olf.service_obj_service_name AS service_name,
        olf.customer_id AS customer_id,
        olf.order_id AS order_id,
        olf.modified_order_status AS modified_order_status,
        olf.spd_fraud_flag AS spd_fraud_flag
    FROM 
        orders.order_logs_fact AS olf
    WHERE 
        olf.yyyymmdd >= DATE_FORMAT(DATE_TRUNC('WEEK', '{{StartDate}}'), '%Y%m%d')
        AND olf.yyyymmdd <= DATE_FORMAT(DATE_TRUNC('WEEK', '{{EndDate}}') + INTERVAL '6' DAY, '%Y%m%d')
        AND olf.service_obj_service_name IN ('Auto','Auto Lite','Auto NCR','Auto Pool','AutoPremium','CityAuto','Link','Bike Lite','Bike Metro','Bike Pink','CabEconomy','CabPremium')
    ),
    
    merge AS (
    
    SELECT
        view.run_date AS run_date,
        view.week_no AS week_no,
        view.week_start_date AS week_start_date,
        view.city AS city,
        view.taxi_ltr_segments AS taxi_ltr_segments,
        view.taxi_retention_segments AS taxi_retention_segments,
        olf_cte.customer_id AS customer_id,
        olf_cte.service_name AS service_name,
        olf_cte.order_id AS order_id,
        olf_cte.modified_order_status AS modified_order_status,
        olf_cte.spd_fraud_flag AS spd_fraud_flag
    FROM 
        view AS view
    LEFT JOIN 
        olf_cte AS olf_cte
        ON view.customer_id = olf_cte.customer_id
    )
    
    SELECT
        merge.week_no AS week_no,
        merge.week_start_date AS week_start_date,
        merge.taxi_ltr_segments AS taxi_ltr_segments,
        merge.taxi_retention_segments AS taxi_retention_segments,
        merge.service_name AS service_name,
        COUNT(DISTINCT merge.order_id) AS rf_segment_gross_orders_w4
    FROM 
        merge AS merge
    GROUP BY 
        week_no,
        week_start_date,
        taxi_ltr_segments,
        taxi_retention_segments,
        service_name
