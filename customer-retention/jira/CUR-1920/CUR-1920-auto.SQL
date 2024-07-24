WITH main_query AS 
    (
        SELECT  run_date,
                customer_id,
                auto_recency_segment,
                auto_frequency_segment,
                auto_ltr_segment,
                auto_dbrr_coeff,
                auto_coeff_bucket,
                auto_ltrpw_segment,
                auto_lifetime_stage,
                auto_retention_segments,
                auto_micro_segment
                
        FROM    datasets.iallocator_customer_segments
        WHERE   run_date BETWEEN '2022-06-20' AND '2022-06-21'
    ),

count_customer AS 
    (
        SELECT  run_date, COUNT(DISTINCT customer_id) AS customer_count
        FROM    main_query
        GROUP BY 1        
    ),
auto_recency_segment AS
    (
    SELECT  'auto_recency_segment' AS segments, '1' AS priority, AVG(std_dev) AS dist_status
    FROM    
        (
        SELECT  auto_recency_segment, STDDEV(cust_perct) AS std_dev
        FROM
            (SELECT run_date, auto_recency_segment,
                    (COUNT(DISTINCT customer_id)*100.00/(SELECT customer_count FROM count_customer AS a WHERE a.run_date = c.run_date)) AS cust_perct
            FROM    main_query AS c
            GROUP BY 1, 2
            )
        GROUP BY 1
        )
    GROUP BY 1,2
    ),
auto_frequency_segment AS
    (
    SELECT  'auto_frequency_segment' AS segments, '2' AS priority, AVG(std_dev) AS dist_status
    FROM    
        (
        SELECT  auto_frequency_segment, STDDEV(cust_perct) AS std_dev
        FROM
            (SELECT  run_date, auto_frequency_segment,
                    (COUNT(DISTINCT customer_id)*100.00/(SELECT customer_count FROM count_customer AS a WHERE a.run_date = c.run_date)) AS cust_perct
            FROM    main_query AS c
            GROUP BY 1, 2
            )
        GROUP BY 1
        )
    GROUP BY 1,2
    ),
    
auto_ltr_segment AS
    (
    SELECT  'auto_ltr_segment' AS segments, '3' AS priority, AVG(std_dev) AS dist_status
    FROM    
        (
        SELECT  auto_ltr_segment, STDDEV(cust_perct) AS std_dev
        FROM
            (SELECT  run_date, auto_ltr_segment,
                    (COUNT(DISTINCT customer_id)*100.00/(SELECT customer_count FROM count_customer AS a WHERE a.run_date = c.run_date)) AS cust_perct
            FROM    main_query AS c
            GROUP BY 1, 2
            )
        GROUP BY 1
        )
    GROUP BY 1,2
    ),
  
auto_dbrr_coeff AS
    (
    SELECT  'auto_dbrr_coeff' AS segments, '4' AS priority, AVG(std_dev) AS dist_status
    FROM    
        (
        SELECT  auto_dbrr_coeff, STDDEV(cust_perct) AS std_dev
        FROM
            (SELECT  run_date, auto_dbrr_coeff,
                    (COUNT(DISTINCT customer_id)*100.00/(SELECT customer_count FROM count_customer AS a WHERE a.run_date = c.run_date)) AS cust_perct
            FROM    main_query AS c
            GROUP BY 1, 2
            )
        GROUP BY 1
        )
    GROUP BY 1,2
    ),
      
auto_coeff_bucket AS
    (
    SELECT  'auto_coeff_bucket' AS segments, '5' AS priority, AVG(std_dev) AS dist_status
    FROM    
        (
        SELECT  auto_coeff_bucket, STDDEV(cust_perct) AS std_dev
        FROM
            (SELECT  run_date, auto_coeff_bucket,
                    (COUNT(DISTINCT customer_id)*100.00/(SELECT customer_count FROM count_customer AS a WHERE a.run_date = c.run_date)) AS cust_perct
            FROM    main_query AS c
            GROUP BY 1, 2
            )
        GROUP BY 1
        )
    GROUP BY 1,2
    ),

auto_ltrpw_segment AS
    (
    SELECT  'auto_ltrpw_segment' AS segments, '6' AS priority, AVG(std_dev) AS dist_status
    FROM    
        (
        SELECT  auto_ltrpw_segment, STDDEV(cust_perct) AS std_dev
        FROM
            (SELECT  run_date, auto_ltrpw_segment,
                    (COUNT(DISTINCT customer_id)*100.00/(SELECT customer_count FROM count_customer AS a WHERE a.run_date = c.run_date)) AS cust_perct
            FROM    main_query AS c
            GROUP BY 1, 2
            )
        GROUP BY 1
        )
    GROUP BY 1,2
    ),
auto_lifetime_stage AS
    (
    SELECT  'auto_lifetime_stage' AS segments, '7' AS priority, AVG(std_dev) AS dist_status
    FROM    
        (
        SELECT  auto_lifetime_stage, STDDEV(cust_perct) AS std_dev
        FROM
            (SELECT run_date, auto_lifetime_stage,
                    (COUNT(DISTINCT customer_id)*100.00/(SELECT customer_count FROM count_customer AS a WHERE a.run_date = c.run_date)) AS cust_perct
            FROM    main_query AS c
            GROUP BY 1, 2
            )
        GROUP BY 1
        )
    GROUP BY 1,2
    ),
auto_retention_segments AS
    (
    SELECT  'auto_retention_segments' AS segments, '8' AS priority, AVG(std_dev) AS dist_status
    FROM    
        (
        SELECT  auto_retention_segments, STDDEV(cust_perct) AS std_dev
        FROM
            (SELECT  run_date, auto_retention_segments,
                    (COUNT(DISTINCT customer_id)*100.00/(SELECT customer_count FROM count_customer AS a WHERE a.run_date = c.run_date)) AS cust_perct
            FROM    main_query AS c
            GROUP BY 1, 2
            )
        GROUP BY 1
        )
    GROUP BY 1,2
    ), 
auto_micro_segment AS
    (
    SELECT  'auto_micro_segment' AS segments, '9' AS priority, AVG(std_dev) AS dist_status
    FROM    
        (
        SELECT  auto_micro_segment, STDDEV(cust_perct) AS std_dev
        FROM
            (SELECT  run_date, auto_micro_segment,
                    (COUNT(DISTINCT customer_id)*100.00/(SELECT customer_count FROM count_customer AS a WHERE a.run_date = c.run_date)) AS cust_perct
            FROM    main_query AS c
            GROUP BY 1, 2
            )
        GROUP BY 1
        )
    GROUP BY 1,2
    )

SELECT * FROM  auto_recency_segment   
UNION
SELECT * FROM auto_frequency_segment
UNION
SELECT * FROM  auto_ltr_segment   
UNION
SELECT * FROM auto_dbrr_coeff
UNION
SELECT * FROM  auto_coeff_bucket   
UNION
SELECT * FROM auto_ltrpw_segment
UNION
SELECT * FROM  auto_lifetime_stage   
UNION
SELECT * FROM auto_retention_segments
UNION
SELECT * FROM  auto_micro_segment   
ORDER BY 2