-- customer_rsa_segment_base_customers_w0

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
    )


    SELECT
        view.week_start_date AS week_start_date,
        view.customer_taxi_rr_regularity_segments AS customer_taxi_rr_regularity_segments,
        view.customer_taxi_rr_intent_segments AS customer_taxi_rr_intent_segments,
        view.customer_taxi_fe_regularity_segments AS customer_taxi_fe_regularity_segments,
        view.customer_taxi_fe_intent_segments AS customer_taxi_fe_intent_segments,
        view.customer_taxi_service_affinity AS customer_taxi_service_affinity,
        COUNT(DISTINCT view.customer_id) AS rsa_segment_week1_base_customers
    FROM 
        view AS view
    GROUP BY 
        week_start_date,
        customer_taxi_rr_regularity_segments,
        customer_taxi_rr_intent_segments,
        customer_taxi_fe_regularity_segments,
        customer_taxi_fe_intent_segments,
        customer_taxi_service_affinity