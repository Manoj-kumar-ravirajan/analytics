-- 1. Question: How many days, on average, does it take each of the product groups to complete 0.1 UPA?

WITH accounts AS (
    
    SELECT
        angaza_id,
        registration_date,
        expected_repayment_days,
        free_days_included,
        product_group
    FROM 
        accounts_view
    ),

    portfolio AS (
    
    SELECT
        angaza_id,
        portfolio_date,	
        amount_toward_follow_on
    FROM 
        portfolio_view
    ),

    merge AS (
    
    SELECT
        accounts.angaza_id,
        accounts.product_group,
        (DATE_DIFF('DAY', accounts.registration_date, portfolio.portfolio_date) - accounts.free_days_included) AS unit_age_days,
        ROUND((DATE_DIFF('DAY', accounts.registration_date, portfolio.portfolio_date)  - accounts.free_days_included) / 7.0, 1) AS unit_age_weeks,
        ROUND((DATE_DIFF('DAY', accounts.registration_date, portfolio.portfolio_date)  - accounts.free_days_included) / accounts.expected_repayment_days, 1) AS upa
    FROM 
        accounts AS accounts
    JOIN 
        portfolio AS portfolio
        ON accounts.angaza_id = portfolio.angaza_id
    ),

    unit_proportion_age AS (
        SELECT
            angaza_id,
            product_group,
            MIN(unit_age_days) AS days_to_complete_10_perc_upa
        FROM 
            merge
        WHERE 
            upa >= 0.1
        GROUP BY 
            1,2
    )

    SELECT
        product_group,
        ROUND(AVG(days_to_complete_10_perc_upa), 1) AS avg_days_to_complete_10_perc_upa
    FROM 
        unit_proportion_age
    GROUP BY 
        1
    ORDER BY
        1

-- 2. Compute the Disabled rates for each portfolio date. What is the disabled rate for June 9, 2024, for all accounts in the sample?

WITH portfolio AS (
    
    SELECT
        portfolio_date,	
        COUNT(DISTINCT angaza_id) total_units,
        COUNT(DISTINCT CASE WHEN days_to_cutoff <= 0 THEN angaza_id END) AS disabled_units,
        ROUND(TRY(COUNT(DISTINCT CASE WHEN days_to_cutoff <= 0 THEN angaza_id END) * 1.0 / COUNT(DISTINCT angaza_id)), 3) AS disabled_rate
    FROM 
        portfolio_view
    )

    SELECT
        portfolio,
        total_units,
        disabled_units,
        disabled_rate
    FROM 
        portfolio
    WHERE 
        DATE_FORMAT(DATE(portfolio_date), '%Y-%m-%d') = '2024-06-09'

-- 3. What is the Repayment speed for Product 1 in Area A on July 18, 2024? Consider accounts registered in the first half of the month only.
-- Assumptions : The expected amount should only include the amount the company reasonably expects to collect — which is only from ENABLED units.

WITH accounts AS (
    
    SELECT
        angaza_id,
        area,
        daily_price,
        product_group
    FROM 
        accounts_view
    WHERE 
        DATE_FORMAT(DATE(registration_date), '%Y-%m-%d') BETWEEN '2024-07-01' AND '2024-07-15'
        AND area = 'Area A'
        AND product_group = 'Product 1'
    ),

    portfolio AS (
    
    SELECT
        accounts.angaza_id,
        accounts.area,
        accounts.product_group,
        accounts.daily_price,
        portfolio.portfolio_date,	
        portfolio.amount_toward_follow_on,
        CASE WHEN portfolio.days_to_cutoff > 0 THEN 1 ELSE 0 END AS is_enabled_day

    FROM 
        portfolio_view AS portfolio
    JOIN 
        accounts AS accounts
        ON accounts.angaza_id = portfolio.angaza_id
    WHERE 
        DATE_FORMAT(DATE(portfolio_date), '%Y-%m-%d') = '2024-07-18'
    )

    SELECT
        DATE_FORMAT(DATE(portfolio_date), '%Y-%m-%d') portfolio_date,
        product_group,
        area,
        ROUND(SUM(amount_toward_follow_on) * 1.0 / (SUM(is_enabled_day * daily_price), 0), 3) AS repayment_speed
    FROM 
        portfolio
    GROUP BY 
        1,2,3

-- 4. Compute and plot disabled rates across portfolio weeks from the week of Jan 15 to July 15, 2024, split by area. How does Area A and Area B compare?

WITH accounts AS (
    
    SELECT
        angaza_id,
        area
    FROM 
        accounts_view
    ),

    portfolio AS (
    
    SELECT
        DATE_FORMAT(DATE_TRUNC( 'WEEK' , DATE(portfolio.portfolio_date)), '%Y-%m-%d') AS portfolio_week,	
        accounts.area,
        (portfolio.angaza_id || '-' || portfolio.portfolio_date) AS uuid,
        (CASE WHEN days_to_cutoff <= 0 THEN (portfolio.angaza_id || '-' || portfolio.portfolio_date) END) AS is_disabled_day
    FROM 
        portfolio_view AS portfolio
    JOIN 
        accounts AS accounts
        ON accounts.angaza_id = portfolio.angaza_id
    WHERE 
        DATE_FORMAT(DATE(portfolio.portfolio_date), '%Y-%m-%d') BETWEEN '2024-01-15' AND '2024-07-15'
    )

    SELECT
        portfolio_week,
        area,
        COUNT(DISTINCT uuid) total_unit,
        COUNT(DISTINCT is_disabled_day) AS disabled_unit,
        TRY(COUNT(DISTINCT is_disabled_day)* 1.0 / COUNT(DISTINCT uuid)) AS disabled_rate
    FROM 
        portfolio
    GROUP BY 
        1,2
    ORDER BY 1,2


-- 5. Compute and plot repayment speed across Unit Age Weeks from week 1 to 10, split by area. How does Area A and Area B compare?

WITH accounts AS (
    
    SELECT
        angaza_id,
        registration_date,
        daily_price,
        free_days_included,
        area
    FROM 
        accounts_view
    ),

    portfolio AS (
    
    SELECT
        angaza_id,
        portfolio_date,	
        days_to_cutoff,
        amount_toward_follow_on
    FROM 
        portfolio_view
    ),

    merge AS (
    
    SELECT
        accounts.angaza_id,
        accounts.area,
        accounts.daily_price
        accounts.registration_date,
        accounts.free_days_included,
        portfolio.amount_toward_follow_on,
        (DATE_DIFF('DAY', accounts.registration_date, portfolio.portfolio_date) - accounts.free_days_included) AS unit_age_days,
        ROUND((DATE_DIFF('DAY', accounts.registration_date, portfolio.portfolio_date)  - accounts.free_days_included) / 7.0, 1) AS unit_age_weeks,
        CASE WHEN portfolio.days_to_cutoff > 0 THEN 1 ELSE 0 END AS is_enabled_day
    FROM 
        accounts AS accounts
    JOIN 
        portfolio AS portfolio
        ON accounts.angaza_id = portfolio.angaza_id
    )

    SELECT
        unit_age_weeks,
        area,
        SUM(amount_toward_follow_on) total_collected,
        SUM(is_enabled_day * daily_price) total_expected,
        ROUND( TRY(SUM(amount_toward_follow_on) * 1.0 / SUM(is_enabled_day * daily_price)) , 3) AS repayment_speed
    FROM 
        merge
    WHERE 
        unit_age_weeks BETWEEN 1 AND 10
    GROUP BY 
        1,2
    ORDER BY 
        1,2