-- (1) Age 

SELECT 
    customerId,
    year_of_birth,
    age age_value,
    CASE 
    WHEN age < 18 THEN 'Below 18'
    WHEN age >= 18 AND age <= 25 THEN '18 to 25'
    WHEN age >= 26 AND age <= 40 THEN '26 to 40'
    WHEN age >= 41 AND age <= 55 THEN '41 to 55'
    WHEN age > 55 THEN '55 +'
    WHEN age IS NULL THEN 'Unknown'
    END age
FROM 
    (
    SELECT 
        customerId,
        CAST(CAST(substr(dateofbirth,1,4) AS INT) AS VARCHAR) year_of_birth,
        CASE 
        WHEN CAST(substr(dateofbirth,1,4) AS INT)  BETWEEN 1940 AND 2015 THEN (2023 - CAST(substr(dateofbirth,1,4) AS INT))
        END age
    FROM 
        datasets.customer_single_view 
    WHERE 
        substr(dateofbirth,1,4)  BETWEEN '1900' AND '2050'
    )

-- (1) Gender

SELECT 
    customerId,
    gender
FROM 
    datasets.customer_single_view 

