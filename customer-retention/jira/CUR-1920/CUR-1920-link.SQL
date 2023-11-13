WITH main_query AS 
    (
        SELECT  run_date,
                customer_id,
                link_recency_segment,
                link_frequency_segment,
                link_ltr_segment,
                link_dbrr_coeff,
                link_coeff_bucket,
                link_ltrpw_segment,
                link_lifetime_stage,
                link_retention_segments,
                link_micro_segment
                
        FROM    datasets.iallocator_customer_segments
        WHERE   run_date BETWEEN '2022-06-20' AND '2022-06-21'
    ),

count_customer AS 
    (
        SELECT  run_date, COUNT(DISTINCT customer_id) AS customer_count
        FROM    main_query
        GROUP BY 1        
    ),
link_recency_segment AS
    (
    SELECT  'link_recency_segment' AS segments, '1' AS priority, AVG(std_dev) AS dist_status
    FROM    
        (
        SELECT  link_recency_segment, STDDEV(cust_perct) AS std_dev
        FROM
            (SELECT run_date, link_recency_segment,
                    (COUNT(DISTINCT customer_id)*100.00/(SELECT customer_count FROM count_customer AS a WHERE a.run_date = c.run_date)) AS cust_perct
            FROM    main_query AS c
            GROUP BY 1, 2
            )
        GROUP BY 1
        )
    GROUP BY 1,2
    ),
link_frequency_segment AS
    (
    SELECT  'link_frequency_segment' AS segments, '2' AS priority, AVG(std_dev) AS dist_status
    FROM    
        (
        SELECT  link_frequency_segment, STDDEV(cust_perct) AS std_dev
        FROM
            (SELECT  run_date, link_frequency_segment,
                    (COUNT(DISTINCT customer_id)*100.00/(SELECT customer_count FROM count_customer AS a WHERE a.run_date = c.run_date)) AS cust_perct
            FROM    main_query AS c
            GROUP BY 1, 2
            )
        GROUP BY 1
        )
    GROUP BY 1,2
    ),
    
link_ltr_segment AS
    (
    SELECT  'link_ltr_segment' AS segments, '3' AS priority, AVG(std_dev) AS dist_status
    FROM    
        (
        SELECT  link_ltr_segment, STDDEV(cust_perct) AS std_dev
        FROM
            (SELECT  run_date, link_ltr_segment,
                    (COUNT(DISTINCT customer_id)*100.00/(SELECT customer_count FROM count_customer AS a WHERE a.run_date = c.run_date)) AS cust_perct
            FROM    main_query AS c
            GROUP BY 1, 2
            )
        GROUP BY 1
        )
    GROUP BY 1,2
    ),
  
link_dbrr_coeff AS
    (
    SELECT  'link_dbrr_coeff' AS segments, '4' AS priority, AVG(std_dev) AS dist_status
    FROM    
        (
        SELECT  link_dbrr_coeff, STDDEV(cust_perct) AS std_dev
        FROM
            (SELECT  run_date, link_dbrr_coeff,
                    (COUNT(DISTINCT customer_id)*100.00/(SELECT customer_count FROM count_customer AS a WHERE a.run_date = c.run_date)) AS cust_perct
            FROM    main_query AS c
            GROUP BY 1, 2
            )
        GROUP BY 1
        )
    GROUP BY 1,2
    ),
      
link_coeff_bucket AS
    (
    SELECT  'link_coeff_bucket' AS segments, '5' AS priority, AVG(std_dev) AS dist_status
    FROM    
        (
        SELECT  link_coeff_bucket, STDDEV(cust_perct) AS std_dev
        FROM
            (SELECT  run_date, link_coeff_bucket,
                    (COUNT(DISTINCT customer_id)*100.00/(SELECT customer_count FROM count_customer AS a WHERE a.run_date = c.run_date)) AS cust_perct
            FROM    main_query AS c
            GROUP BY 1, 2
            )
        GROUP BY 1
        )
    GROUP BY 1,2
    ),

link_ltrpw_segment AS
    (
    SELECT  'link_ltrpw_segment' AS segments, '6' AS priority, AVG(std_dev) AS dist_status
    FROM    
        (
        SELECT  link_ltrpw_segment, STDDEV(cust_perct) AS std_dev
        FROM
            (SELECT  run_date, link_ltrpw_segment,
                    (COUNT(DISTINCT customer_id)*100.00/(SELECT customer_count FROM count_customer AS a WHERE a.run_date = c.run_date)) AS cust_perct
            FROM    main_query AS c
            GROUP BY 1, 2
            )
        GROUP BY 1
        )
    GROUP BY 1,2
    ),
link_lifetime_stage AS
    (
    SELECT  'link_lifetime_stage' AS segments, '7' AS priority, AVG(std_dev) AS dist_status
    FROM    
        (
        SELECT  link_lifetime_stage, STDDEV(cust_perct) AS std_dev
        FROM
            (SELECT run_date, link_lifetime_stage,
                    (COUNT(DISTINCT customer_id)*100.00/(SELECT customer_count FROM count_customer AS a WHERE a.run_date = c.run_date)) AS cust_perct
            FROM    main_query AS c
            GROUP BY 1, 2
            )
        GROUP BY 1
        )
    GROUP BY 1,2
    ),
link_retention_segments AS
    (
    SELECT  'link_retention_segments' AS segments, '8' AS priority, AVG(std_dev) AS dist_status
    FROM    
        (
        SELECT  link_retention_segments, STDDEV(cust_perct) AS std_dev
        FROM
            (SELECT  run_date, link_retention_segments,
                    (COUNT(DISTINCT customer_id)*100.00/(SELECT customer_count FROM count_customer AS a WHERE a.run_date = c.run_date)) AS cust_perct
            FROM    main_query AS c
            GROUP BY 1, 2
            )
        GROUP BY 1
        )
    GROUP BY 1,2
    ), 
link_micro_segment AS
    (
    SELECT  'link_micro_segment' AS segments, '9' AS priority, AVG(std_dev) AS dist_status
    FROM    
        (
        SELECT  link_micro_segment, STDDEV(cust_perct) AS std_dev
        FROM
            (SELECT  run_date, link_micro_segment,
                    (COUNT(DISTINCT customer_id)*100.00/(SELECT customer_count FROM count_customer AS a WHERE a.run_date = c.run_date)) AS cust_perct
            FROM    main_query AS c
            GROUP BY 1, 2
            )
        GROUP BY 1
        )
    GROUP BY 1,2
    )

SELECT * FROM  link_recency_segment   
UNION
SELECT * FROM link_frequency_segment
UNION
SELECT * FROM  link_ltr_segment   
UNION
SELECT * FROM link_dbrr_coeff
UNION
SELECT * FROM  link_coeff_bucket   
UNION
SELECT * FROM link_ltrpw_segment
UNION
SELECT * FROM  link_lifetime_stage   
UNION
SELECT * FROM link_retention_segments
UNION
SELECT * FROM  link_micro_segment   
ORDER BY 2