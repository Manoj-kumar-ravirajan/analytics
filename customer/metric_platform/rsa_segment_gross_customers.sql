-- rsa_segment_gross_week1_retention_customers


WITH iallocator_cte AS ( 
    
    SELECT
        segments.run_date AS run_date,
        segments.customer_id AS customer_id, 
        segments.taxi_lifetime_last_ride_city AS taxi_lifetime_last_ride_city,
        segments.taxi_regularity_segment AS customer_taxi_rr_regularity_segments,
        segments.taxi_intent_segment AS customer_taxi_rr_intent_segments,
        segments.taxi_fe_regularity_segment AS customer_taxi_fe_regularity_segments,
        segments.taxi_fe_intent_segment AS customer_taxi_fe_intent_segments,
        segments.taxi_service_affinity AS customer_taxi_service_affinity
    FROM 
        datasets.iallocator_customer_segments AS segments
    where 
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '1' DAY, '%Y-%m-%d')
    ),

    view AS (

    SELECT
        DATE_FORMAT(FROM_UTC_TIMESTAMP(cte.run_date,'IST'),'%Y%m%d')  AS run_date,
        DATE_FORMAT(FROM_UTC_TIMESTAMP(cte.run_date,'IST') + INTERVAL '1' DAY,'%Y%m%d') AS week_start_date,
        cte.customer_id AS customer_id,
        cte.taxi_lifetime_last_ride_city AS taxi_lifetime_last_ride_city,
        cte.customer_taxi_rr_regularity_segments AS customer_taxi_rr_regularity_segments,
        cte.customer_taxi_rr_intent_segments AS customer_taxi_rr_intent_segments,
        cte.customer_taxi_fe_regularity_segments AS customer_taxi_fe_regularity_segments,
        cte.customer_taxi_fe_intent_segments AS customer_taxi_fe_intent_segments,
        cte.customer_taxi_service_affinity AS customer_taxi_service_affinity
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
    ),
    
    merge AS (
    
    SELECT
        view.run_date AS run_date,
        view.week_start_date AS week_start_date,
        view.taxi_lifetime_last_ride_city AS taxi_lifetime_last_ride_city,
        view.customer_taxi_rr_regularity_segments AS customer_taxi_rr_regularity_segments,
        view.customer_taxi_rr_intent_segments AS customer_taxi_rr_intent_segments,
        view.customer_taxi_fe_regularity_segments AS customer_taxi_fe_regularity_segments,
        view.customer_taxi_fe_intent_segments AS customer_taxi_fe_intent_segments,
        view.customer_taxi_service_affinity AS customer_taxi_service_affinity
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
        merge.week_start_date AS week_start_date,
        merge.customer_taxi_rr_regularity_segments AS customer_taxi_rr_regularity_segments,
        merge.customer_taxi_rr_intent_segments AS customer_taxi_rr_intent_segments,
        merge.customer_taxi_fe_regularity_segments AS customer_taxi_fe_regularity_segments,
        merge.customer_taxi_fe_intent_segments AS customer_taxi_fe_intent_segments,
        merge.customer_taxi_service_affinity AS customer_taxi_service_affinity
        COUNT(DISTINCT merge.customer_id) AS rsa_segment_gross_week1_retention_customers
    FROM 
        merge AS merge
    GROUP BY 
        week_start_date,
        customer_taxi_rr_regularity_segments,
        customer_taxi_rr_intent_segments,
        customer_taxi_fe_regularity_segments,
        customer_taxi_fe_intent_segments
        customer_taxi_service_affinity


-- rsa_segment_gross_week2_retention_customers

WITH iallocator_cte AS ( 
    
    SELECT
        segments.run_date AS run_date,
        segments.customer_id AS customer_id, 
        segments.taxi_lifetime_last_ride_city AS taxi_lifetime_last_ride_city,
        segments.taxi_regularity_segment AS customer_taxi_rr_regularity_segments,
        segments.taxi_intent_segment AS customer_taxi_rr_intent_segments,
        segments.taxi_fe_regularity_segment AS customer_taxi_fe_regularity_segments,
        segments.taxi_fe_intent_segment AS customer_taxi_fe_intent_segments,
        segments.taxi_service_affinity AS customer_taxi_service_affinity
    FROM 
        datasets.iallocator_customer_segments AS segments
    where 
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '8' DAY, '%Y-%m-%d')
    ),

    view AS (

    SELECT
        DATE_FORMAT(FROM_UTC_TIMESTAMP(cte.run_date,'IST'),'%Y%m%d')  AS run_date,
        DATE_FORMAT(FROM_UTC_TIMESTAMP(cte.run_date,'IST') + INTERVAL '1' DAY,'%Y%m%d') AS week_start_date,
        cte.customer_id AS customer_id,
        cte.taxi_lifetime_last_ride_city AS taxi_lifetime_last_ride_city,
        cte.customer_taxi_rr_regularity_segments AS customer_taxi_rr_regularity_segments,
        cte.customer_taxi_rr_intent_segments AS customer_taxi_rr_intent_segments,
        cte.customer_taxi_fe_regularity_segments AS customer_taxi_fe_regularity_segments,
        cte.customer_taxi_fe_intent_segments AS customer_taxi_fe_intent_segments,
        cte.customer_taxi_service_affinity AS customer_taxi_service_affinity
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
    ),
    
    merge AS (
    
    SELECT
        view.run_date AS run_date,
        view.week_start_date AS week_start_date,
        view.taxi_lifetime_last_ride_city AS taxi_lifetime_last_ride_city,
        view.customer_taxi_rr_regularity_segments AS customer_taxi_rr_regularity_segments,
        view.customer_taxi_rr_intent_segments AS customer_taxi_rr_intent_segments,
        view.customer_taxi_fe_regularity_segments AS customer_taxi_fe_regularity_segments,
        view.customer_taxi_fe_intent_segments AS customer_taxi_fe_intent_segments,
        view.customer_taxi_service_affinity AS customer_taxi_service_affinity
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
        merge.week_start_date AS week_start_date,
        merge.customer_taxi_rr_regularity_segments AS customer_taxi_rr_regularity_segments,
        merge.customer_taxi_rr_intent_segments AS customer_taxi_rr_intent_segments,
        merge.customer_taxi_fe_regularity_segments AS customer_taxi_fe_regularity_segments,
        merge.customer_taxi_fe_intent_segments AS customer_taxi_fe_intent_segments,
        merge.customer_taxi_service_affinity AS customer_taxi_service_affinity
        COUNT(DISTINCT merge.customer_id) AS rsa_segment_gross_week2_retention_customers
    FROM 
        merge AS merge
    GROUP BY 
        week_start_date,
        customer_taxi_rr_regularity_segments,
        customer_taxi_rr_intent_segments,
        customer_taxi_fe_regularity_segments,
        customer_taxi_fe_intent_segments
        customer_taxi_service_affinity



-- rsa_segment_gross_week3_retention_customers


WITH iallocator_cte AS ( 
    
    SELECT
        segments.run_date AS run_date,
        segments.customer_id AS customer_id, 
        segments.taxi_lifetime_last_ride_city AS taxi_lifetime_last_ride_city,
        segments.taxi_regularity_segment AS customer_taxi_rr_regularity_segments,
        segments.taxi_intent_segment AS customer_taxi_rr_intent_segments,
        segments.taxi_fe_regularity_segment AS customer_taxi_fe_regularity_segments,
        segments.taxi_fe_intent_segment AS customer_taxi_fe_intent_segments,
        segments.taxi_service_affinity AS customer_taxi_service_affinity
    FROM 
        datasets.iallocator_customer_segments AS segments
    where 
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '15' DAY, '%Y-%m-%d')
    ),

    view AS (

    SELECT
        DATE_FORMAT(FROM_UTC_TIMESTAMP(cte.run_date,'IST'),'%Y%m%d')  AS run_date,
        DATE_FORMAT(FROM_UTC_TIMESTAMP(cte.run_date,'IST') + INTERVAL '1' DAY,'%Y%m%d') AS week_start_date,
        cte.customer_id AS customer_id,
        cte.taxi_lifetime_last_ride_city AS taxi_lifetime_last_ride_city,
        cte.customer_taxi_rr_regularity_segments AS customer_taxi_rr_regularity_segments,
        cte.customer_taxi_rr_intent_segments AS customer_taxi_rr_intent_segments,
        cte.customer_taxi_fe_regularity_segments AS customer_taxi_fe_regularity_segments,
        cte.customer_taxi_fe_intent_segments AS customer_taxi_fe_intent_segments,
        cte.customer_taxi_service_affinity AS customer_taxi_service_affinity
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
    ),
    
    merge AS (
    
    SELECT
        view.run_date AS run_date,
        view.week_start_date AS week_start_date,
        view.taxi_lifetime_last_ride_city AS taxi_lifetime_last_ride_city,
        view.customer_taxi_rr_regularity_segments AS customer_taxi_rr_regularity_segments,
        view.customer_taxi_rr_intent_segments AS customer_taxi_rr_intent_segments,
        view.customer_taxi_fe_regularity_segments AS customer_taxi_fe_regularity_segments,
        view.customer_taxi_fe_intent_segments AS customer_taxi_fe_intent_segments,
        view.customer_taxi_service_affinity AS customer_taxi_service_affinity
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
        merge.week_start_date AS week_start_date,
        merge.customer_taxi_rr_regularity_segments AS customer_taxi_rr_regularity_segments,
        merge.customer_taxi_rr_intent_segments AS customer_taxi_rr_intent_segments,
        merge.customer_taxi_fe_regularity_segments AS customer_taxi_fe_regularity_segments,
        merge.customer_taxi_fe_intent_segments AS customer_taxi_fe_intent_segments,
        merge.customer_taxi_service_affinity AS customer_taxi_service_affinity
        COUNT(DISTINCT merge.customer_id) AS rsa_segment_gross_week3_retention_customers
    FROM 
        merge AS merge
    GROUP BY 
        week_start_date,
        customer_taxi_rr_regularity_segments,
        customer_taxi_rr_intent_segments,
        customer_taxi_fe_regularity_segments,
        customer_taxi_fe_intent_segments
        customer_taxi_service_affinity



-- rsa_segment_gross_week4_retention_customers


WITH iallocator_cte AS ( 
    
    SELECT
        segments.run_date AS run_date,
        segments.customer_id AS customer_id, 
        segments.taxi_lifetime_last_ride_city AS taxi_lifetime_last_ride_city,
        segments.taxi_regularity_segment AS customer_taxi_rr_regularity_segments,
        segments.taxi_intent_segment AS customer_taxi_rr_intent_segments,
        segments.taxi_fe_regularity_segment AS customer_taxi_fe_regularity_segments,
        segments.taxi_fe_intent_segment AS customer_taxi_fe_intent_segments,
        segments.taxi_service_affinity AS customer_taxi_service_affinity
    FROM 
        datasets.iallocator_customer_segments AS segments
    where 
        segments.run_date = DATE_FORMAT(DATE_TRUNC('WEEK', DATE('{{StartDate}}')) - INTERVAL '22' DAY, '%Y-%m-%d')
    ),

    view AS (

    SELECT
        DATE_FORMAT(FROM_UTC_TIMESTAMP(cte.run_date,'IST'),'%Y%m%d')  AS run_date,
        DATE_FORMAT(FROM_UTC_TIMESTAMP(cte.run_date,'IST') + INTERVAL '1' DAY,'%Y%m%d') AS week_start_date,
        cte.customer_id AS customer_id,
        cte.taxi_lifetime_last_ride_city AS taxi_lifetime_last_ride_city,
        cte.customer_taxi_rr_regularity_segments AS customer_taxi_rr_regularity_segments,
        cte.customer_taxi_rr_intent_segments AS customer_taxi_rr_intent_segments,
        cte.customer_taxi_fe_regularity_segments AS customer_taxi_fe_regularity_segments,
        cte.customer_taxi_fe_intent_segments AS customer_taxi_fe_intent_segments,
        cte.customer_taxi_service_affinity AS customer_taxi_service_affinity
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
    ),
    
    merge AS (
    
    SELECT
        view.run_date AS run_date,
        view.week_start_date AS week_start_date,
        view.taxi_lifetime_last_ride_city AS taxi_lifetime_last_ride_city,
        view.customer_taxi_rr_regularity_segments AS customer_taxi_rr_regularity_segments,
        view.customer_taxi_rr_intent_segments AS customer_taxi_rr_intent_segments,
        view.customer_taxi_fe_regularity_segments AS customer_taxi_fe_regularity_segments,
        view.customer_taxi_fe_intent_segments AS customer_taxi_fe_intent_segments,
        view.customer_taxi_service_affinity AS customer_taxi_service_affinity
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
        merge.week_start_date AS week_start_date,
        merge.customer_taxi_rr_regularity_segments AS customer_taxi_rr_regularity_segments,
        merge.customer_taxi_rr_intent_segments AS customer_taxi_rr_intent_segments,
        merge.customer_taxi_fe_regularity_segments AS customer_taxi_fe_regularity_segments,
        merge.customer_taxi_fe_intent_segments AS customer_taxi_fe_intent_segments,
        merge.customer_taxi_service_affinity AS customer_taxi_service_affinity
        COUNT(DISTINCT merge.customer_id) AS rsa_segment_gross_week4_retention_customers
    FROM 
        merge AS merge
    GROUP BY 
        week_start_date,
        customer_taxi_rr_regularity_segments,
        customer_taxi_rr_intent_segments,
        customer_taxi_fe_regularity_segments,
        customer_taxi_fe_intent_segments
        customer_taxi_service_affinity