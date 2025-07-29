WITH iallocator_cte AS ( 
    
    SELECT
        segments.run_date AS run_date,
        segments.customer_id AS customer_id, 
        segments.taxi_lifetime_last_ride_city AS taxi_lifetime_last_ride_city,
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
        cte.taxi_lifetime_last_ride_city AS taxi_lifetime_last_ride_city,
        cte.taxi_ltr_segments AS taxi_ltr_segments,
        cte.taxi_retention_segments AS taxi_retention_segments
    FROM 
        iallocator_cte AS cte
    )


    SELECT
        view.taxi_ltr_segments AS taxi_ltr_segments,
        view.taxi_retention_segments AS taxi_retention_segments,
        COUNT(DISTINCT view.customer_id) AS rf_segment_base_customers_w0
    FROM 
        view AS view
    GROUP BY 
        taxi_ltr_segments,
        taxi_retention_segments



        -- Time ()
        -- geo ()
        -- other (other group by)