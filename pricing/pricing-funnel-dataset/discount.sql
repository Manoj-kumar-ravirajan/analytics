WITH city_cluster_hex_mapping AS (
    
        SELECT
            cch.hex_id AS hex_id,
            cch.cluster AS cluster
        FROM
            datasets.city_cluster_hex cch
        WHERE
            cch.resolution = 8
    ),
    
    additive_surge AS (
        
        SELECT
            srg_bkp.yyyymmdd AS yyyymmdd,
            srg_bkp.service_detail_id AS service_detail_id,
            srg_bkp.fare_estimate_id AS fare_estimate_id,
            srg_bkp.price_increase_strategy AS additive_strategy
        FROM
            pricing.iprice_customer_surge_breakup srg_bkp
        WHERE
            srg_bkp.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d') 
            AND srg_bkp.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
            AND srg_bkp.surge_category = 'additive'
    ),
    
    fare_estimates AS (

        SELECT
            'Route' AS geo_level,
            'Cluster' AS geo_value,
            fe_ench.epoch AS epoch,
            fe_ench.yyyymmdd AS yyyymmdd,
            fe_ench.city AS city_name,
            fe_ench.service_name AS service_name,
            fe_ench.service_detail_id AS service_detail_id,
            fe_ench.pickup_location_hex_8 AS pickup_location_hex_8,
            fe_ench.drop_location_hex_8 AS drop_location_hex_8,
            COALESCE(fe_ench.price_increase_strategy,'no_dynamic_surge') AS surge_strategy,
            COALESCE(fe_ench.is_gradient_applied,false) AS gradient_applied,
            fe_ench.fare_estimate_id AS fare_estimate_id,
            fe_ench.discount_amount AS discount_amount
        FROM
            pricing.fare_estimates_enriched fe_ench
        WHERE
            fe_ench.yyyymmdd >= DATE_FORMAT(DATE('{{StartDate}}'), '%Y%m%d') 
            AND fe_ench.yyyymmdd <= DATE_FORMAT(DATE('{{EndDate}}'), '%Y%m%d')
            AND fe_ench.service_name IN ('Link', 'Auto', 'Auto Pool', 'CabEconomy')
    ),
    
    combined_df AS (
        
        SELECT 
            fe.geo_level AS geo_level,
            fe.geo_value AS geo_value,
            fe.epoch AS epoch,
            fe.yyyymmdd AS yyyymmdd,
            fe.city_name AS city_name,
            fe.service_name AS service_name,
            fe.service_detail_id AS service_detail_id,
            pic.cluster AS pickup_location,
            drp.cluster AS drop_location,
            fe.surge_strategy AS surge_strategy,
            fe.gradient_applied AS gradient_applied,
            COALESCE(add_srg.additive_strategy,'no_additive_surge') AS additive_strategy,
            fe.fare_estimate_id AS fare_estimate_id,
            fe.discount_amount AS discount_amount
        FROM
            fare_estimates fe
        LEFT JOIN
            city_cluster_hex_mapping pic
            ON fe.pickup_location_hex_8 = pic.hex_id
        LEFT JOIN 
            city_cluster_hex_mapping drp
            ON fe.drop_location_hex_8 = drp.hex_id
        LEFT JOIN
            additive_surge add_srg
            ON fe.yyyymmdd = add_srg.yyyymmdd
            AND fe.service_detail_id = add_srg.service_detail_id
            AND fe.fare_estimate_id = add_srg.fare_estimate_id
    )
    
SELECT 
    cdf.geo_level AS geo_level,
    cdf.geo_value AS geo_value,
    cdf.city_name AS city_name,
    cdf.service_name AS service_name,
    cdf.service_detail_id AS service_detail_id,
    cdf.pickup_location AS pickup_location,
    cdf.drop_location AS drop_location,
    cdf.surge_strategy AS surge_strategy,
    cdf.gradient_applied AS gradient_applied,
    cdf.additive_strategy AS additive_strategy,
    SUM(cdf.discount_amount) AS discount
FROM
    combined_df cdf
GROUP BY
    geo_level,
    geo_value,
    city_name,
    service_name,
    service_detail_id,
    pickup_location,
    drop_location,
    surge_strategy,
    gradient_applied,
    additive_strategy