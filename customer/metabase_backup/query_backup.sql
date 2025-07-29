-- Prefetched Latency Tracker - OTWS1


WITH request as (

    SELECT     
        DISTINCT
        yyyymmdd,
        hh hour,
        city,
        CASE WHEN appversion < '8.30.0' THEN '< 8.30.0' ELSE '>= 8.30.0' END appversion,
        isprefetched,
        responsestatus,
        userId,
        slotname,
        format,
        COALESCE(aduuid, adid) aduuid,
        epoch
    FROM 
        canonical.iceberg_log_telemetry_ads_impressions_immutable
    
    WHERE  
        yyyymmdd >= date_format({{start_date}},'%Y%m%d')
        and yyyymmdd <= date_format({{end_date}},'%Y%m%d')
        and responseType = 'GAMBanner'
        -- and city = 'Bangalore'
        and slotname = 'PostOrderOnTheWay1'
        and eventName = 'Ad_Request'
        -- and appversion = '8.30.0'
    ),
    
    response as (

    SELECT     
        DISTINCT
        yyyymmdd,
        hh hour,
        city,
        -- CASE WHEN appversion < '8.30.0' THEN 'Below 8.30.0' ELSE '>= 8.30.0 ' END appversion,
        isprefetched,
        responsestatus,
        userId,
        slotname,
        format,
        COALESCE(aduuid, adid) aduuid,
        epoch
    FROM 
        canonical.iceberg_log_telemetry_ads_impressions_immutable
    
    WHERE  
        yyyymmdd >= date_format({{start_date}},'%Y%m%d')
        and yyyymmdd <= date_format({{end_date}},'%Y%m%d')
        and responseType = 'GAMBanner'
        -- and city = 'Bangalore'
        and slotname = 'PostOrderOnTheWay1'
        and eventName = 'Ad_Response'
        -- and appversion = '8.30.0'
    ),
    
    impression as (

    SELECT     
        DISTINCT
        yyyymmdd,
        hh hour,
        city,
        -- CASE WHEN appversion < '8.30.0' THEN 'Below 8.30.0' ELSE '>= 8.30.0 ' END appversion,
        -- isprefetched,
        -- responsestatus,
        userId,
        slotname,
        format,
        COALESCE(aduuid, adid) aduuid,
        epoch
    FROM 
        canonical.iceberg_log_telemetry_ads_impressions_immutable
    
    WHERE  
        yyyymmdd >= date_format({{start_date}},'%Y%m%d')
        and yyyymmdd <= date_format({{end_date}},'%Y%m%d')
        and responseType = 'GAMBanner'
        -- and city = 'Bangalore'
        and slotname = 'PostOrderOnTheWay1'
        and eventName = 'Ad_Impression'
        -- and appversion = '8.30.0'
    ),
    
    vbl_impression as (

    SELECT     
        DISTINCT
        yyyymmdd,
        hh hour,
        city,
        -- CASE WHEN appversion < '8.30.0' THEN 'Below 8.30.0' ELSE '>= 8.30.0 ' END appversion,
        -- isprefetched,
        -- responsestatus,
        userId,
        slotname,
        format,
        COALESCE(aduuid, adid) aduuid,
        epoch
    FROM 
        canonical.iceberg_log_telemetry_ads_impressions_immutable
    
    WHERE  
        yyyymmdd >= date_format({{start_date}},'%Y%m%d')
        and yyyymmdd <= date_format({{end_date}},'%Y%m%d')
        and responseType = 'GAMBanner'
        -- and city = 'Bangalore'
        and slotname = 'PostOrderOnTheWay1'
        and eventName = 'Ad_Viewable_Impression'
        -- and appversion = '8.30.0'
    ),
    
    click as (

    SELECT     
        DISTINCT
        yyyymmdd,
        hh hour,
        city,
        -- CASE WHEN appversion < '8.30.0' THEN 'Below 8.30.0' ELSE '>= 8.30.0 ' END appversion,
        -- isprefetched,
        -- responsestatus,
        userId,
        slotname,
        format,
        COALESCE(aduuid, adid) aduuid,
        epoch
    FROM 
        canonical.iceberg_log_telemetry_ads_impressions_immutable
    
    WHERE  
        yyyymmdd >= date_format({{start_date}},'%Y%m%d')
        and yyyymmdd <= date_format({{end_date}},'%Y%m%d')
        and responseType = 'GAMBanner'
        -- and city = 'Bangalore'
        and slotname = 'PostOrderOnTheWay1'
        and eventName = 'Ad_Click'
        -- and appversion = '8.30.0'
    ),
    
    postorderstatus_ontheway AS (
    
    SELECT
        yyyymmdd,
        hh,
        user_id,
        order_id,
        MIN(epoch) epoch,
        COUNT(DISTINCT unique_visit) unique_visit
    FROM
        (
        SELECT
            *,
            user_id || '-' || order_id || '-' || cast(rn as varchar) as unique_visit
        FROM
            (
            SELECT 
                *,
                ROW_NUMBER() OVER(PARTITION BY yyyymmdd, user_id, order_id ORDER BY epoch) rn
            FROM
                (
                SELECT
                    yyyymmdd,
                    hh,
                    profile_identity user_id,
                    JSON_EXTRACT_SCALAR(event_props, '$.orderId') order_id,
                    epoch
                FROM 
                    clevertap.customer_postorderstatus_ontheway_immutable
                WHERE 
                    yyyymmdd >= date_format({{start_date}},'%Y%m%d')
                    AND yyyymmdd <= date_format({{end_date}},'%Y%m%d')
                    AND profile_platform = 'Android'
                )
            )
            -- WHERE 
            --     order_id IS NOT NULL
                -- AND rn = 1
        )
    GROUP BY 1,2,3,4
    ),
    
    merge AS (
    
    SELECT
        request.yyyymmdd,
        request.slotname,
        request.appversion,
        UPPER(COALESCE(request.isprefetched, 'false')) isprefetched,
        request.aduuid req_aduuid,
        UPPER(COALESCE(response.responsestatus, 'false')) responsestatus,
        response.aduuid res_aduuid, 
        impression.aduuid imp_aduuid,
        vbl_impression.aduuid vbl_imp_aduuid,
        click.aduuid click_aduuid,
        request.userId || CAST(unique_visit AS VARCHAR) unique_visit,
        -- request.epoch req_epoch,
        -- response.epoch res_epoch,
        -- impression.epoch imp_epoch,
        -- otw.epoch pv_epoch,
        -- vbl_impression.epoch vbl_imp_epoch,
        (response.epoch/1000.0 - request.epoch/1000.0) Req2Res_time_spent_in_sec,
        (response.epoch - request.epoch) Req2Res_time_spent_in_ms,
        (impression.epoch/1000.0 - otw.epoch/1000.0) Pv2Imp_time_spent_in_sec,
        (impression.epoch - otw.epoch) Pv2Imp_time_spent_in_ms
        
    FROM 
        request
    LEFT JOIN 
        response
        ON request.yyyymmdd = response.yyyymmdd
        AND request.hour = response.hour
        AND request.userId = response.userId
        AND request.slotname = response.slotname
        AND request.aduuid = response.aduuid
        AND request.epoch <= response.epoch
    LEFT JOIN 
        impression
        ON response.yyyymmdd = impression.yyyymmdd
        AND response.hour = impression.hour
        AND response.userId = impression.userId
        AND response.slotname = impression.slotname
        AND response.aduuid = impression.aduuid
        AND response.epoch <= impression.epoch
    LEFT JOIN 
        vbl_impression
        ON impression.yyyymmdd = vbl_impression.yyyymmdd
        AND impression.hour = vbl_impression.hour
        AND impression.userId = vbl_impression.userId
        AND impression.slotname = vbl_impression.slotname
        AND impression.aduuid = vbl_impression.aduuid
        AND impression.epoch <= vbl_impression.epoch
    LEFT JOIN 
        click
        ON impression.yyyymmdd = click.yyyymmdd
        AND impression.hour = click.hour
        AND impression.userId = click.userId
        AND impression.slotname = click.slotname
        AND impression.aduuid = click.aduuid
        AND impression.epoch <= click.epoch
        
    LEFT JOIN 
        postorderstatus_ontheway otw
        ON response.yyyymmdd = otw.yyyymmdd
        AND response.userId = otw.user_id
        AND response.hour = otw.hh 
        -- AND response.epoch <= otw.epoch
        AND otw.epoch <= impression.epoch
    )   
    
    
    SELECT
        yyyymmdd,
        slotname,
        appversion,
        isprefetched,
        -- responsestatus,
        COUNT(DISTINCT req_aduuid) req_aduuid,
        COUNT(DISTINCT res_aduuid) res_aduuid,
        COUNT(DISTINCT unique_visit) unique_visit,
        COUNT(DISTINCT imp_aduuid) imp_aduuid,
        COUNT(DISTINCT vbl_imp_aduuid) vbl_imp_aduuid,
        COUNT(DISTINCT click_aduuid) click_aduuid,
        approx_percentile(Req2Res_time_spent_in_sec, 0.50) Req2Res_time_spent_in_sec,
        approx_percentile(Req2Res_time_spent_in_ms, 0.50) Req2Res_time_spent_in_ms,
        approx_percentile(Pv2Imp_time_spent_in_sec, 0.05) Pv2Imp_time_spent_in_sec,
        approx_percentile(Pv2Imp_time_spent_in_ms, 0.05) Pv2Imp_time_spent_in_ms
    FROM 
        merge
    GROUP BY 1,2,3,4
    ORDER BY 1,2,3,4



    -- OnlineSales Latency Tracker - OTWS1


    WITH request as (

    SELECT     
        DISTINCT
        yyyymmdd,
        hh hour,
        city,
        CASE WHEN appversion < '8.30.0' THEN '< 8.30.0' ELSE '>= 8.30.0' END appversion,
        isprefetched,
        responsestatus,
        userId,
        slotname,
        responseType,
        format,
        COALESCE(aduuid, adid) aduuid,
        epoch
    FROM 
        canonical.iceberg_log_telemetry_ads_impressions_immutable
    
    WHERE  
        yyyymmdd >= date_format({{start_date}},'%Y%m%d')
        and yyyymmdd <= date_format({{end_date}},'%Y%m%d')
        and (responseType = 'GAMBanner' OR format = 'ONLINE_SALES')
        and city = 'Bangalore'
        and slotname = 'PostOrderOnTheWay1'
        and eventName = 'Ad_Request'
        and appversion >= '8.30.0'
    ),
    
    response as (

    SELECT     
        DISTINCT
        yyyymmdd,
        hh hour,
        city,
        -- CASE WHEN appversion < '8.30.0' THEN 'Below 8.30.0' ELSE '>= 8.30.0 ' END appversion,
        isprefetched,
        responsestatus,
        userId,
        slotname,
        format,
        COALESCE(aduuid, adid) aduuid,
        epoch
    FROM 
        canonical.iceberg_log_telemetry_ads_impressions_immutable
    
    WHERE  
        yyyymmdd >= date_format({{start_date}},'%Y%m%d')
        and yyyymmdd <= date_format({{end_date}},'%Y%m%d')
        and (responseType = 'GAMBanner' OR format = 'ONLINE_SALES')
        and city = 'Bangalore'
        and slotname = 'PostOrderOnTheWay1'
        and eventName = 'Ad_Response'
        and appversion >= '8.30.0'
    ),
    
    impression as (

    SELECT     
        DISTINCT
        yyyymmdd,
        hh hour,
        city,
        -- CASE WHEN appversion < '8.30.0' THEN 'Below 8.30.0' ELSE '>= 8.30.0 ' END appversion,
        -- isprefetched,
        -- responsestatus,
        userId,
        slotname,
        format,
        COALESCE(aduuid, adid) aduuid,
        epoch
    FROM 
        canonical.iceberg_log_telemetry_ads_impressions_immutable
    
    WHERE  
        yyyymmdd >= date_format({{start_date}},'%Y%m%d')
        and yyyymmdd <= date_format({{end_date}},'%Y%m%d')
        and (responseType = 'GAMBanner' OR format = 'ONLINE_SALES')
        and city = 'Bangalore'
        and slotname = 'PostOrderOnTheWay1'
        and eventName = 'Ad_Impression'
        and appversion >= '8.30.0'
    ),
    
    vbl_impression as (

    SELECT     
        DISTINCT
        yyyymmdd,
        hh hour,
        city,
        -- CASE WHEN appversion < '8.30.0' THEN 'Below 8.30.0' ELSE '>= 8.30.0 ' END appversion,
        -- isprefetched,
        -- responsestatus,
        userId,
        slotname,
        format,
        COALESCE(aduuid, adid) aduuid,
        epoch
    FROM 
        canonical.iceberg_log_telemetry_ads_impressions_immutable
    
    WHERE  
        yyyymmdd >= date_format({{start_date}},'%Y%m%d')
        and yyyymmdd <= date_format({{end_date}},'%Y%m%d')
        and (responseType = 'GAMBanner' OR format = 'ONLINE_SALES')
        and city = 'Bangalore'
        and slotname = 'PostOrderOnTheWay1'
        and eventName = 'Ad_Viewable_Impression'
        and appversion >= '8.30.0'
    ),
    
    click as (

    SELECT     
        DISTINCT
        yyyymmdd,
        hh hour,
        city,
        -- CASE WHEN appversion < '8.30.0' THEN 'Below 8.30.0' ELSE '>= 8.30.0 ' END appversion,
        -- isprefetched,
        -- responsestatus,
        userId,
        slotname,
        format,
        COALESCE(aduuid, adid) aduuid,
        epoch
    FROM 
        canonical.iceberg_log_telemetry_ads_impressions_immutable
    
    WHERE  
        yyyymmdd >= date_format({{start_date}},'%Y%m%d')
        and yyyymmdd <= date_format({{end_date}},'%Y%m%d')
        and (responseType = 'GAMBanner' OR format = 'ONLINE_SALES')
        and city = 'Bangalore'
        and slotname = 'PostOrderOnTheWay1'
        and eventName = 'Ad_Click'
        and appversion >= '8.30.0'
    ),
    
    postorderstatus_ontheway AS (
    
    SELECT
        yyyymmdd,
        hh,
        user_id,
        order_id,
        MIN(epoch) epoch,
        COUNT(DISTINCT unique_visit) unique_visit
    FROM
        (
        SELECT
            *,
            user_id || '-' || order_id || '-' || cast(rn as varchar) as unique_visit
        FROM
            (
            SELECT 
                *,
                ROW_NUMBER() OVER(PARTITION BY yyyymmdd, user_id, order_id ORDER BY epoch) rn
            FROM
                (
                SELECT
                    yyyymmdd,
                    hh,
                    profile_identity user_id,
                    JSON_EXTRACT_SCALAR(event_props, '$.orderId') order_id,
                    epoch
                FROM 
                    clevertap.customer_postorderstatus_ontheway_immutable
                WHERE 
                    yyyymmdd >= date_format({{start_date}},'%Y%m%d')
                    AND yyyymmdd <= date_format({{end_date}},'%Y%m%d')
                    AND profile_platform = 'Android'
                )
            )
            -- WHERE 
            --     order_id IS NOT NULL
                -- AND rn = 1
        )
    GROUP BY 1,2,3,4
    ),
    
    user_selector AS (
    
    SELECT
        'TEST' group_tc,
        userid
    FROM    
        canonical.user_selector
    WHERE  
        yyyymmdd = '20240906'
        AND selectorid = '66dae25cac2f570001f2e935'
    
    ),
    
    merge AS (
    
    SELECT
        COALESCE(user_selector.group_tc, 'CONTROL') group_tc,
        request.yyyymmdd,
        request.slotname,
        request.appversion,
        request.responseType,
        request.format,
        UPPER(COALESCE(request.isprefetched, 'false')) isprefetched,
        request.aduuid req_aduuid,
        UPPER(COALESCE(response.responsestatus, 'false')) responsestatus,
        response.aduuid res_aduuid, 
        impression.aduuid imp_aduuid,
        vbl_impression.aduuid vbl_imp_aduuid,
        click.aduuid click_aduuid,
        request.userId || CAST(unique_visit AS VARCHAR) unique_visit,
        -- request.epoch req_epoch,
        -- response.epoch res_epoch,
        -- impression.epoch imp_epoch,
        -- otw.epoch pv_epoch,
        -- vbl_impression.epoch vbl_imp_epoch,
        (response.epoch/1000.0 - request.epoch/1000.0) Req2Res_time_spent_in_sec,
        (response.epoch - request.epoch) Req2Res_time_spent_in_ms,
        (impression.epoch/1000.0 - otw.epoch/1000.0) Pv2Imp_time_spent_in_sec,
        (impression.epoch - otw.epoch) Pv2Imp_time_spent_in_ms
        
    FROM 
        request
    LEFT JOIN 
        user_selector
        ON request.userId = user_selector.userid
        
    LEFT JOIN 
        response
        ON request.yyyymmdd = response.yyyymmdd
        AND request.hour = response.hour
        AND request.userId = response.userId
        AND request.slotname = response.slotname
        AND request.aduuid = response.aduuid
        AND request.epoch <= response.epoch
    LEFT JOIN 
        impression
        ON response.yyyymmdd = impression.yyyymmdd
        AND response.hour = impression.hour
        AND response.userId = impression.userId
        AND response.slotname = impression.slotname
        AND response.aduuid = impression.aduuid
        AND response.epoch <= impression.epoch
    LEFT JOIN 
        vbl_impression
        ON impression.yyyymmdd = vbl_impression.yyyymmdd
        AND impression.hour = vbl_impression.hour
        AND impression.userId = vbl_impression.userId
        AND impression.slotname = vbl_impression.slotname
        AND impression.aduuid = vbl_impression.aduuid
        AND impression.epoch <= vbl_impression.epoch
    LEFT JOIN 
        click
        ON impression.yyyymmdd = click.yyyymmdd
        AND impression.hour = click.hour
        AND impression.userId = click.userId
        AND impression.slotname = click.slotname
        AND impression.aduuid = click.aduuid
        AND impression.epoch <= click.epoch
        
    LEFT JOIN 
        postorderstatus_ontheway otw
        ON response.yyyymmdd = otw.yyyymmdd
        AND response.userId = otw.user_id
        AND response.hour = otw.hh 
        -- AND response.epoch <= otw.epoch
        AND otw.epoch <= impression.epoch
    )   
    
    
    SELECT
        yyyymmdd,
        group_tc,
        slotname,
        -- appversion,
        -- isprefetched,
        -- responsestatus,
        COUNT(DISTINCT req_aduuid) req_aduuid,
        COUNT(DISTINCT res_aduuid) res_aduuid,
        COUNT(DISTINCT unique_visit) unique_visit,
        COUNT(DISTINCT imp_aduuid) imp_aduuid,
        COUNT(DISTINCT vbl_imp_aduuid) vbl_imp_aduuid,
        COUNT(DISTINCT click_aduuid) click_aduuid,
        approx_percentile(Req2Res_time_spent_in_sec, 0.50) Req2Res_time_spent_in_sec,
        approx_percentile(Req2Res_time_spent_in_ms, 0.50) Req2Res_time_spent_in_ms,
        approx_percentile(Pv2Imp_time_spent_in_sec, 0.05) Pv2Imp_time_spent_in_sec,
        approx_percentile(Pv2Imp_time_spent_in_ms, 0.05) Pv2Imp_time_spent_in_ms
    FROM 
        merge
    WHERE 
        (responseType = 'GAMBanner' AND isprefetched = 'TRUE' AND group_tc = 'CONTROL')
        OR 
        (responseType = 'System Banner' AND format = 'ONLINE_SALES' AND group_tc = 'TEST')
        
        
    GROUP BY 1,2,3
    ORDER BY 1,2,3


-- Ad-hoc (OTW)


WITH request as (

    SELECT     
        DISTINCT
        yyyymmdd,
        hh hour,
        city,
        CASE WHEN appversion <= '8.28.0' THEN '8.28.0 >=' ELSE appversion END appversion,
        isprefetched,
        userId,
        slotname,
        format,
        COALESCE(aduuid, adid) aduuid,
        epoch 
    FROM 
        canonical.iceberg_log_telemetry_ads_impressions_immutable
    
    WHERE  
        yyyymmdd >= date_format({{start_date}},'%Y%m%d')
        and yyyymmdd <= date_format({{end_date}},'%Y%m%d')
        and responseType = 'GAMBanner'
        and [[ {{city}} --]] city IS NOT NULL
        and regexp_like(lower(slotname), 'ontheway')
        and eventName = 'Ad_Request'
    ),
    
    response as (

    SELECT     
        DISTINCT
        yyyymmdd,
        hh hour,
        city,
        -- appversion,
        -- isprefetched,
        userId,
        slotname,
        format,
        COALESCE(aduuid, adid) aduuid,
        epoch 
    FROM 
        canonical.iceberg_log_telemetry_ads_impressions_immutable
    
    WHERE  
        yyyymmdd >= date_format({{start_date}},'%Y%m%d')
        and yyyymmdd <= date_format({{end_date}},'%Y%m%d')
        and responseType = 'GAMBanner'
        and [[ {{city}} --]] city IS NOT NULL
        and regexp_like(lower(slotname), 'ontheway')
        and eventName = 'Ad_Response'
    ),
    
    impression as (

    SELECT     
        DISTINCT
        yyyymmdd,
        hh hour,
        city,
        -- appversion,
        -- isprefetched,
        userId,
        slotname,
        format,
        COALESCE(aduuid, adid) aduuid,
        epoch 
    FROM 
        canonical.iceberg_log_telemetry_ads_impressions_immutable
    
    WHERE  
        yyyymmdd >= date_format({{start_date}},'%Y%m%d')
        and yyyymmdd <= date_format({{end_date}},'%Y%m%d')
        and responseType = 'GAMBanner'
        and [[ {{city}} --]] city IS NOT NULL
        and regexp_like(lower(slotname), 'ontheway')
        and eventName = 'Ad_Impression'
    ),
    
    viewable_impression as (

    SELECT     
        DISTINCT
        yyyymmdd,
        hh hour,
        city,
        -- appversion,
        -- isprefetched,
        userId,
        slotname,
        format,
        COALESCE(aduuid, adid) aduuid,
        epoch 
    FROM 
        canonical.iceberg_log_telemetry_ads_impressions_immutable
    
    WHERE  
        yyyymmdd >= date_format({{start_date}},'%Y%m%d')
        and yyyymmdd <= date_format({{end_date}},'%Y%m%d')
        and responseType = 'GAMBanner'
        and [[ {{city}} --]] city IS NOT NULL
        and regexp_like(lower(slotname), 'ontheway')
        and eventName = 'Ad_Viewable_Impression'
    ),
    
    click as (

    SELECT     
        DISTINCT
        yyyymmdd,
        hh hour,
        city,
        -- appversion,
        -- isprefetched,
        userId,
        slotname,
        format,
        COALESCE(aduuid, adid) aduuid,
        epoch 
    FROM 
        canonical.iceberg_log_telemetry_ads_impressions_immutable
    
    WHERE  
        yyyymmdd >= date_format({{start_date}},'%Y%m%d')
        and yyyymmdd <= date_format({{end_date}},'%Y%m%d')
        and responseType = 'GAMBanner'
        and [[ {{city}} --]] city IS NOT NULL
        and regexp_like(lower(slotname), 'ontheway')
        and eventName = 'Ad_Click'
    ),
    
    postorderstatus_ontheway AS (
    
    SELECT
        yyyymmdd,
        hh,
        user_id,
        -- order_id,
        epoch
    FROM
        (
        SELECT 
            *,
            ROW_NUMBER() OVER(PARTITION BY yyyymmdd, user_id, order_id ORDER BY epoch) rn
        FROM
            (
            SELECT
                yyyymmdd,
                hh,
                profile_identity user_id,
                JSON_EXTRACT_SCALAR(event_props, '$.orderId') order_id,
                epoch
            FROM 
                clevertap.customer_postorderstatus_ontheway_immutable
            WHERE 
                yyyymmdd >= date_format({{start_date}},'%Y%m%d')
                AND yyyymmdd <= date_format({{end_date}},'%Y%m%d')
                AND profile_platform = 'Android'
            )
        )
        WHERE 
            rn = 1
    )
    
    
    SELECT
        yyyymmdd,
        appversion,
        isprefetched,
        slotname,
        count(distinct aduuid) ad_response,
        count(distinct ad_response) ad_response,
        count(distinct ad_impression) ad_impression,
        count(distinct ad_viewable_impression) ad_viewable_impression,
        count(distinct ad_click) ad_click,
        approx_percentile(Req2Res_time_spent_in_sec, 0.5) Req2Res_time_spent_in_sec,
        approx_percentile(Req2Res_time_spent_in_ms, 0.5) Req2Res_time_spent_in_ms,
        approx_percentile(Pv2Imp_time_spent_in_sec, 0.1) Pv2Imp_time_spent_in_sec,
        approx_percentile(Pv2Imp_time_spent_in_ms, 0.1) Pv2Imp_time_spent_in_ms
    from 
    (
    SELECT
        request.*,
        -- otw.epoch screen_epoch,
        response.epoch response,
        otw.epoch page_visit,
        impression.epoch impression,
        (response.epoch/1000.0 - request.epoch/1000.0) Req2Res_time_spent_in_sec,
        (response.epoch - request.epoch) Req2Res_time_spent_in_ms,
        (impression.epoch/1000.0 - otw.epoch/1000.0) Pv2Imp_time_spent_in_sec,
        (impression.epoch - otw.epoch) Pv2Imp_time_spent_in_ms
        -- ROW_NUMBER() OVER(PARTITION BY request.aduuid ORDER BY response.epoch desc) rn
        
        ,response.aduuid ad_response
        ,impression.aduuid ad_impression
        ,viewable_impression.aduuid ad_viewable_impression
        ,click.aduuid ad_click
        
    FROM 
        request
    LEFT JOIN 
        response
        ON request.yyyymmdd = response.yyyymmdd
        AND request.city = response.city
        AND request.userId = response.userId
        AND request.slotname = response.slotname
        AND request.format = response.format
        AND request.aduuid = response.aduuid
        AND request.epoch <= response.epoch
        
    LEFT JOIN 
        postorderstatus_ontheway otw
        ON response.yyyymmdd = otw.yyyymmdd
        AND response.userId = otw.user_id
        AND response.hour = otw.hh 
        AND response.epoch <= otw.epoch
    
    LEFT JOIN 
        impression
        ON impression.yyyymmdd = response.yyyymmdd
        AND impression.city = response.city
        AND impression.userId = response.userId
        AND impression.slotname = response.slotname
        AND impression.format = response.format
        AND impression.aduuid = impression.aduuid
        AND otw.epoch <= impression.epoch
        
    LEFT JOIN 
        viewable_impression
        ON viewable_impression.yyyymmdd = response.yyyymmdd
        AND viewable_impression.city = response.city
        AND viewable_impression.userId = response.userId
        AND viewable_impression.slotname = response.slotname
        AND viewable_impression.format = response.format
        AND viewable_impression.aduuid = response.aduuid
        
    LEFT JOIN 
        click
        ON click.yyyymmdd = response.yyyymmdd
        AND click.city = response.city
        AND click.userId = response.userId
        AND click.slotname = response.slotname
        AND click.format = response.format
        AND click.aduuid = response.aduuid
    )
    GROUP BY 1,2,3,4
    ORDER BY 1, 2 DESC, 3, 4


    -- Ad-hoc (Arrived)

    WITH request as (

    SELECT     
        DISTINCT
        yyyymmdd,
        hh hour,
        city,
        CASE WHEN appversion <= '8.28.0' THEN '8.28.0 >=' ELSE appversion END appversion,
        isprefetched,
        userId,
        slotname,
        format,
        COALESCE(aduuid, adid) aduuid,
        epoch 
    FROM 
        canonical.iceberg_log_telemetry_ads_impressions_immutable
    
    WHERE  
        yyyymmdd >= date_format({{start_date}},'%Y%m%d')
        and yyyymmdd <= date_format({{end_date}},'%Y%m%d')
        and responseType = 'GAMBanner'
        and [[ {{city}} --]] city IS NOT NULL
        and regexp_like(lower(slotname), 'arrived')
        and eventName = 'Ad_Request'
    ),
    
    response as (

    SELECT     
        DISTINCT
        yyyymmdd,
        hh hour,
        city,
        -- appversion,
        -- isprefetched,
        userId,
        slotname,
        format,
        COALESCE(aduuid, adid) aduuid,
        epoch 
    FROM 
        canonical.iceberg_log_telemetry_ads_impressions_immutable
    
    WHERE  
        yyyymmdd >= date_format({{start_date}},'%Y%m%d')
        and yyyymmdd <= date_format({{end_date}},'%Y%m%d')
        and responseType = 'GAMBanner'
        and [[ {{city}} --]] city IS NOT NULL
        and regexp_like(lower(slotname), 'arrived')
        and eventName = 'Ad_Response'
    ),
    
    impression as (

    SELECT     
        DISTINCT
        yyyymmdd,
        hh hour,
        city,
        -- appversion,
        -- isprefetched,
        userId,
        slotname,
        format,
        COALESCE(aduuid, adid) aduuid,
        epoch 
    FROM 
        canonical.iceberg_log_telemetry_ads_impressions_immutable
    
    WHERE  
        yyyymmdd >= date_format({{start_date}},'%Y%m%d')
        and yyyymmdd <= date_format({{end_date}},'%Y%m%d')
        and responseType = 'GAMBanner'
        and [[ {{city}} --]] city IS NOT NULL
        and regexp_like(lower(slotname), 'arrived')
        and eventName = 'Ad_Impression'
    ),
    
    viewable_impression as (

    SELECT     
        DISTINCT
        yyyymmdd,
        hh hour,
        city,
        -- appversion,
        -- isprefetched,
        userId,
        slotname,
        format,
        COALESCE(aduuid, adid) aduuid,
        epoch 
    FROM 
        canonical.iceberg_log_telemetry_ads_impressions_immutable
    
    WHERE  
        yyyymmdd >= date_format({{start_date}},'%Y%m%d')
        and yyyymmdd <= date_format({{end_date}},'%Y%m%d')
        and responseType = 'GAMBanner'
        and [[ {{city}} --]] city IS NOT NULL
        and regexp_like(lower(slotname), 'arrived')
        and eventName = 'Ad_Viewable_Impression'
    ),
    
    click as (

    SELECT     
        DISTINCT
        yyyymmdd,
        hh hour,
        city,
        -- appversion,
        -- isprefetched,
        userId,
        slotname,
        format,
        COALESCE(aduuid, adid) aduuid,
        epoch 
    FROM 
        canonical.iceberg_log_telemetry_ads_impressions_immutable
    
    WHERE  
        yyyymmdd >= date_format({{start_date}},'%Y%m%d')
        and yyyymmdd <= date_format({{end_date}},'%Y%m%d')
        and responseType = 'GAMBanner'
        and [[ {{city}} --]] city IS NOT NULL
        and regexp_like(lower(slotname), 'arrived')
        and eventName = 'Ad_Click'
    ),
    
    postorderstatus_ontheway AS (
    
    SELECT
        yyyymmdd,
        hh,
        user_id,
        -- order_id,
        epoch
    FROM
        (
        SELECT 
            *,
            ROW_NUMBER() OVER(PARTITION BY yyyymmdd, user_id, order_id ORDER BY epoch) rn
        FROM
            (
            SELECT
                yyyymmdd,
                hh,
                profile_identity user_id,
                JSON_EXTRACT_SCALAR(event_props, '$.orderId') order_id,
                epoch
            FROM 
                clevertap.customer_postorderstatus_arrived_immutable
            WHERE 
                yyyymmdd >= date_format({{start_date}},'%Y%m%d')
                AND yyyymmdd <= date_format({{end_date}},'%Y%m%d')
                AND profile_platform = 'Android'
            )
        )
        WHERE 
            rn = 1
    )
    
    
    SELECT
        yyyymmdd,
        appversion,
        isprefetched,
        slotname,
        count(distinct aduuid) ad_response,
        count(distinct ad_response) ad_response,
        count(distinct ad_impression) ad_impression,
        count(distinct ad_viewable_impression) ad_viewable_impression,
        count(distinct ad_click) ad_click,
        approx_percentile(Req2Res_time_spent_in_sec, 0.5) Req2Res_time_spent_in_sec,
        approx_percentile(Req2Res_time_spent_in_ms, 0.5) Req2Res_time_spent_in_ms,
        approx_percentile(Pv2Imp_time_spent_in_sec, 0.1) Pv2Imp_time_spent_in_sec,
        approx_percentile(Pv2Imp_time_spent_in_ms, 0.1) Pv2Imp_time_spent_in_ms
    from 
    (
    SELECT
        request.*,
        -- otw.epoch screen_epoch,
        response.epoch response,
        otw.epoch page_visit,
        impression.epoch impression,
        (response.epoch/1000.0 - request.epoch/1000.0) Req2Res_time_spent_in_sec,
        (response.epoch - request.epoch) Req2Res_time_spent_in_ms,
        (impression.epoch/1000.0 - otw.epoch/1000.0) Pv2Imp_time_spent_in_sec,
        (impression.epoch - otw.epoch) Pv2Imp_time_spent_in_ms
        -- ROW_NUMBER() OVER(PARTITION BY request.aduuid ORDER BY response.epoch desc) rn
        
        ,response.aduuid ad_response
        ,impression.aduuid ad_impression
        ,viewable_impression.aduuid ad_viewable_impression
        ,click.aduuid ad_click
        
    FROM 
        request
    LEFT JOIN 
        response
        ON request.yyyymmdd = response.yyyymmdd
        AND request.city = response.city
        AND request.userId = response.userId
        AND request.slotname = response.slotname
        AND request.format = response.format
        AND request.aduuid = response.aduuid
        AND request.epoch <= response.epoch
        
    LEFT JOIN 
        postorderstatus_ontheway otw
        ON response.yyyymmdd = otw.yyyymmdd
        AND response.userId = otw.user_id
        AND response.hour = otw.hh 
        AND response.epoch <= otw.epoch
    
    LEFT JOIN 
        impression
        ON impression.yyyymmdd = response.yyyymmdd
        AND impression.city = response.city
        AND impression.userId = response.userId
        AND impression.slotname = response.slotname
        AND impression.format = response.format
        AND impression.aduuid = impression.aduuid
        AND otw.epoch <= impression.epoch
        
    LEFT JOIN 
        viewable_impression
        ON viewable_impression.yyyymmdd = response.yyyymmdd
        AND viewable_impression.city = response.city
        AND viewable_impression.userId = response.userId
        AND viewable_impression.slotname = response.slotname
        AND viewable_impression.format = response.format
        AND viewable_impression.aduuid = response.aduuid
        
    LEFT JOIN 
        click
        ON click.yyyymmdd = response.yyyymmdd
        AND click.city = response.city
        AND click.userId = response.userId
        AND click.slotname = response.slotname
        AND click.format = response.format
        AND click.aduuid = response.aduuid
    )
    GROUP BY 1,2,3,4
    ORDER BY 1, 2 DESC, 3, 4



    -- Ad-hoc (Started)


    WITH request as (

    SELECT     
        DISTINCT
        yyyymmdd,
        hh hour,
        city,
        CASE WHEN appversion <= '8.28.0' THEN '8.28.0 >=' ELSE appversion END appversion,
        isprefetched,
        userId,
        slotname,
        format,
        COALESCE(aduuid, adid) aduuid,
        epoch 
    FROM 
        canonical.iceberg_log_telemetry_ads_impressions_immutable
    
    WHERE  
        yyyymmdd >= date_format({{start_date}},'%Y%m%d')
        and yyyymmdd <= date_format({{end_date}},'%Y%m%d')
        and responseType = 'GAMBanner'
        and [[ {{city}} --]] city IS NOT NULL
        and regexp_like(lower(slotname), 'started')
        and eventName = 'Ad_Request'
    ),
    
    response as (

    SELECT     
        DISTINCT
        yyyymmdd,
        hh hour,
        city,
        -- appversion,
        -- isprefetched,
        userId,
        slotname,
        format,
        COALESCE(aduuid, adid) aduuid,
        epoch 
    FROM 
        canonical.iceberg_log_telemetry_ads_impressions_immutable
    
    WHERE  
        yyyymmdd >= date_format({{start_date}},'%Y%m%d')
        and yyyymmdd <= date_format({{end_date}},'%Y%m%d')
        and responseType = 'GAMBanner'
        and [[ {{city}} --]] city IS NOT NULL
        and regexp_like(lower(slotname), 'started')
        and eventName = 'Ad_Response'
    ),
    
    impression as (

    SELECT     
        DISTINCT
        yyyymmdd,
        hh hour,
        city,
        -- appversion,
        -- isprefetched,
        userId,
        slotname,
        format,
        COALESCE(aduuid, adid) aduuid,
        epoch 
    FROM 
        canonical.iceberg_log_telemetry_ads_impressions_immutable
    
    WHERE  
        yyyymmdd >= date_format({{start_date}},'%Y%m%d')
        and yyyymmdd <= date_format({{end_date}},'%Y%m%d')
        and responseType = 'GAMBanner'
        and [[ {{city}} --]] city IS NOT NULL
        and regexp_like(lower(slotname), 'started')
        and eventName = 'Ad_Impression'
    ),
    
    viewable_impression as (

    SELECT     
        DISTINCT
        yyyymmdd,
        hh hour,
        city,
        -- appversion,
        -- isprefetched,
        userId,
        slotname,
        format,
        COALESCE(aduuid, adid) aduuid,
        epoch 
    FROM 
        canonical.iceberg_log_telemetry_ads_impressions_immutable
    
    WHERE  
        yyyymmdd >= date_format({{start_date}},'%Y%m%d')
        and yyyymmdd <= date_format({{end_date}},'%Y%m%d')
        and responseType = 'GAMBanner'
        and [[ {{city}} --]] city IS NOT NULL
        and regexp_like(lower(slotname), 'started')
        and eventName = 'Ad_Viewable_Impression'
    ),
    
    click as (

    SELECT     
        DISTINCT
        yyyymmdd,
        hh hour,
        city,
        -- appversion,
        -- isprefetched,
        userId,
        slotname,
        format,
        COALESCE(aduuid, adid) aduuid,
        epoch 
    FROM 
        canonical.iceberg_log_telemetry_ads_impressions_immutable
    
    WHERE  
        yyyymmdd >= date_format({{start_date}},'%Y%m%d')
        and yyyymmdd <= date_format({{end_date}},'%Y%m%d')
        and responseType = 'GAMBanner'
        and [[ {{city}} --]] city IS NOT NULL
        and regexp_like(lower(slotname), 'started')
        and eventName = 'Ad_Click'
    ),
    
    postorderstatus_ontheway AS (
    
    SELECT
        yyyymmdd,
        hh,
        user_id,
        -- order_id,
        epoch
    FROM
        (
        SELECT 
            *,
            ROW_NUMBER() OVER(PARTITION BY yyyymmdd, user_id, order_id ORDER BY epoch) rn
        FROM
            (
            SELECT
                yyyymmdd,
                hh,
                profile_identity user_id,
                JSON_EXTRACT_SCALAR(event_props, '$.orderId') order_id,
                epoch
            FROM 
                clevertap.customer_postorderstatus_started_immutable
            WHERE 
                yyyymmdd >= date_format({{start_date}},'%Y%m%d')
                AND yyyymmdd <= date_format({{end_date}},'%Y%m%d')
                AND profile_platform = 'Android'
            )
        )
        WHERE 
            rn = 1
    )
    
    
    SELECT
        yyyymmdd,
        appversion,
        isprefetched,
        slotname,
        count(distinct aduuid) ad_response,
        count(distinct ad_response) ad_response,
        count(distinct ad_impression) ad_impression,
        count(distinct ad_viewable_impression) ad_viewable_impression,
        count(distinct ad_click) ad_click,
        approx_percentile(Req2Res_time_spent_in_sec, 0.5) Req2Res_time_spent_in_sec,
        approx_percentile(Req2Res_time_spent_in_ms, 0.5) Req2Res_time_spent_in_ms,
        approx_percentile(Pv2Imp_time_spent_in_sec, 0.1) Pv2Imp_time_spent_in_sec,
        approx_percentile(Pv2Imp_time_spent_in_ms, 0.1) Pv2Imp_time_spent_in_ms
    from 
    (
    SELECT
        request.*,
        -- otw.epoch screen_epoch,
        response.epoch response,
        otw.epoch page_visit,
        impression.epoch impression,
        (response.epoch/1000.0 - request.epoch/1000.0) Req2Res_time_spent_in_sec,
        (response.epoch - request.epoch) Req2Res_time_spent_in_ms,
        (impression.epoch/1000.0 - otw.epoch/1000.0) Pv2Imp_time_spent_in_sec,
        (impression.epoch - otw.epoch) Pv2Imp_time_spent_in_ms
        -- ROW_NUMBER() OVER(PARTITION BY request.aduuid ORDER BY response.epoch desc) rn
        
        ,response.aduuid ad_response
        ,impression.aduuid ad_impression
        ,viewable_impression.aduuid ad_viewable_impression
        ,click.aduuid ad_click
        
    FROM 
        request
    LEFT JOIN 
        response
        ON request.yyyymmdd = response.yyyymmdd
        AND request.city = response.city
        AND request.userId = response.userId
        AND request.slotname = response.slotname
        AND request.format = response.format
        AND request.aduuid = response.aduuid
        AND request.epoch <= response.epoch
        
    LEFT JOIN 
        postorderstatus_ontheway otw
        ON response.yyyymmdd = otw.yyyymmdd
        AND response.userId = otw.user_id
        AND response.hour = otw.hh 
        AND response.epoch <= otw.epoch
    
    LEFT JOIN 
        impression
        ON impression.yyyymmdd = response.yyyymmdd
        AND impression.city = response.city
        AND impression.userId = response.userId
        AND impression.slotname = response.slotname
        AND impression.format = response.format
        AND impression.aduuid = impression.aduuid
        AND otw.epoch <= impression.epoch
        
    LEFT JOIN 
        viewable_impression
        ON viewable_impression.yyyymmdd = response.yyyymmdd
        AND viewable_impression.city = response.city
        AND viewable_impression.userId = response.userId
        AND viewable_impression.slotname = response.slotname
        AND viewable_impression.format = response.format
        AND viewable_impression.aduuid = response.aduuid
        
    LEFT JOIN 
        click
        ON click.yyyymmdd = response.yyyymmdd
        AND click.city = response.city
        AND click.userId = response.userId
        AND click.slotname = response.slotname
        AND click.format = response.format
        AND click.aduuid = response.aduuid
    )
    GROUP BY 1,2,3,4
    ORDER BY 1, 2 DESC, 3, 4



    -- Daily AO-FE-RR-NET Unique Customer Trend | PAN India


    WITH appOpen AS (

    select
        DATE_FORMAT(DATE_TRUNC( {{time_period}} , DATE_PARSE(yyyymmdd, '%Y%m%d')), '%Y%m%d') yyyymmdd,
        COUNT(DISTINCT user_id) ao_customers
    from 
        clevertap.clevertap_customer_order_activity
    where 
        yyyymmdd BETWEEN DATE_FORMAT({{start_date}},'%Y%m%d') 
        and DATE_FORMAT({{end_date}},'%Y%m%d')
        -- AND order_activity_source = 'appOpen'
    group by 1
    ),
    
    fe_customer as (
    select
        DATE_FORMAT(DATE_TRUNC( {{time_period}} , DATE_PARSE(yyyymmdd, '%Y%m%d')), '%Y%m%d') yyyymmdd,
        COUNT(DISTINCT user_id) fe_customers
    from 
        pricing.fare_estimates_enriched
    where 
        yyyymmdd BETWEEN DATE_FORMAT({{start_date}},'%Y%m%d') 
        and DATE_FORMAT({{end_date}},'%Y%m%d')
        and service_name in ('Auto','Auto Lite','Auto NCR','Auto Pool','AutoPremium','CityAuto','Link','Bike Lite','Bike Pink','CabEconomy','Bike Metro', 'CabPremium')
    group by 1
    ),
    
    rr_customer as (
    select
        DATE_FORMAT(DATE_TRUNC( {{time_period}} , DATE_PARSE(yyyymmdd, '%Y%m%d')), '%Y%m%d') yyyymmdd,
        COUNT(DISTINCT customer_id) rr_customers,
        COUNT(DISTINCT CASE WHEN order_status = 'dropped' THEN customer_id END) net_customers
    from 
        orders.order_logs_fact
    where 
        yyyymmdd BETWEEN DATE_FORMAT({{start_date}},'%Y%m%d') 
        and DATE_FORMAT({{end_date}},'%Y%m%d')
        and service_obj_service_name in ('Auto','Auto Lite','Auto NCR','Auto Pool','AutoPremium','CityAuto','Link','Bike Lite','Bike Pink','CabEconomy','Bike Metro', 'CabPremium')
    group by 1
    )
    
    
    
    select
        appOpen.*,
        fe_customers,
        rr_customers,
        net_customers
    from 
        appOpen 
    left join 
        fe_customer
        on appOpen.yyyymmdd = fe_customer.yyyymmdd
    left join 
        rr_customer
        on appOpen.yyyymmdd = rr_customer.yyyymmdd
    order by 1



    -- Daily AO-FE-RR-NET Unique Customer Trend | City

    WITH city AS (

    SELECT 
        city_display_name 
    FROM 
        datasets.service_mapping 
    WHERE 
        [[ {{city}} --]] city_display_name = 'Bangalore'
    GROUP BY 1
    ),

    appOpen AS (

    select
        DATE_FORMAT(DATE_TRUNC( {{time_period}} , DATE_PARSE(yyyymmdd, '%Y%m%d')), '%Y%m%d') yyyymmdd,
        COUNT(DISTINCT user_id) ao_customers
    from 
        clevertap.clevertap_customer_order_activity
    where 
        yyyymmdd BETWEEN DATE_FORMAT({{start_date}},'%Y%m%d') 
        and DATE_FORMAT({{end_date}},'%Y%m%d')
        -- AND order_activity_source = 'appOpen'
        and current_city IN (select city_display_name from city)
    group by 1
    ),
    
    fe_customer as (
    select
        DATE_FORMAT(DATE_TRUNC( {{time_period}} , DATE_PARSE(yyyymmdd, '%Y%m%d')), '%Y%m%d') yyyymmdd,
        COUNT(DISTINCT user_id) fe_customers
    from 
        pricing.fare_estimates_enriched
    where 
        yyyymmdd BETWEEN DATE_FORMAT({{start_date}},'%Y%m%d') 
        and DATE_FORMAT({{end_date}},'%Y%m%d')
        and city IN (select city_display_name from city)
        and service_name in ('Auto','Auto Lite','Auto NCR','Auto Pool','AutoPremium','CityAuto','Link','Bike Lite','Bike Pink','CabEconomy','Bike Metro', 'CabPremium')
    group by 1
    ),
    
    rr_customer as (
    select
        DATE_FORMAT(DATE_TRUNC( {{time_period}} , DATE_PARSE(yyyymmdd, '%Y%m%d')), '%Y%m%d') yyyymmdd,
        COUNT(DISTINCT customer_id) rr_customers,
        COUNT(DISTINCT CASE WHEN order_status = 'dropped' THEN customer_id END) net_customers
    from 
        orders.order_logs_fact
    where 
        yyyymmdd BETWEEN DATE_FORMAT({{start_date}},'%Y%m%d') 
        and DATE_FORMAT({{end_date}},'%Y%m%d')
        and city_name IN (select city_display_name from city)
        and service_obj_service_name in ('Auto','Auto Lite','Auto NCR','Auto Pool','AutoPremium','CityAuto','Link','Bike Lite','Bike Pink','CabEconomy','Bike Metro', 'CabPremium')
    group by 1
    )
    
    
    
    select
        appOpen.*,
        fe_customers,
        rr_customers,
        net_customers
    from 
        appOpen 
    left join 
        fe_customer
        on appOpen.yyyymmdd = fe_customer.yyyymmdd
    left join 
        rr_customer
        on appOpen.yyyymmdd = rr_customer.yyyymmdd
    order by 1


    -- Ad-Monet | Opportunity sizing Ratings

    WITH order_base AS (
    
    SELECT
        DATE_FORMAT(DATE_TRUNC( {{time_period}} , DATE_PARSE(yyyymmdd, '%Y%m%d')), '%Y%m%d') order_date,
        service_obj_service_name service,
        order_id
    FROM 
        orders.order_logs_snapshot 
    WHERE 
       yyyymmdd >= date_format({{start_date}},'%Y%m%d') 
       AND yyyymmdd <= date_format({{end_date}},'%Y%m%d')
       AND order_status = 'dropped'
       AND service_obj_service_name in ('Auto','Auto Lite','Auto NCR','Auto Pool','AutoPremium','CityAuto','Link','Bike Lite','Bike Pink','CabEconomy','Bike Metro', 'CabPremium')
    ),
    
    postorderstatus_dropped AS (
    
    SELECT 
        DATE_FORMAT(DATE_TRUNC( {{time_period}} , DATE_PARSE(yyyymmdd, '%Y%m%d')), '%Y%m%d') order_date,
        profile_identity,
        CAST(CAST(event_props_ct_session_id AS DECIMAL) AS VARCHAR) CTSessionId,
        JSON_EXTRACT_SCALAR(event_props, '$.orderId') orderId
    FROM 
        clevertap.customer_postorderstatus_dropped_immutable 
    WHERE 
        yyyymmdd >= date_format({{start_date}},'%Y%m%d') 
        AND yyyymmdd <= date_format({{end_date}},'%Y%m%d')
    ),
    
    riderrating AS (
    
    SELECT 
        DATE_FORMAT(DATE_TRUNC( {{time_period}} , DATE_PARSE(yyyymmdd, '%Y%m%d')), '%Y%m%d') order_date,
        profile_identity,
        CAST(CAST(JSON_EXTRACT_SCALAR(event_props, '$.CTSessionId') AS DECIMAL) AS VARCHAR) CTSessionId,
        MAX(CASE WHEN JSON_EXTRACT_SCALAR(event_props, '$.context') = 'rating_page_opened' THEN 'rating_page_opened' END) AS rating_page_opened,
        MAX(CASE WHEN JSON_EXTRACT_SCALAR(event_props, '$.context') = 'ratingskip' THEN 'ratingskip' END) AS ratingskip,
        MAX(CASE WHEN JSON_EXTRACT_SCALAR(event_props, '$.context') = 'submit' THEN 'submit' END) AS submit
    FROM 
        clevertap.customer_riderrating_immutable 
    WHERE 
        yyyymmdd >= date_format({{start_date}},'%Y%m%d') 
        AND yyyymmdd <= date_format({{end_date}},'%Y%m%d')
        AND JSON_EXTRACT_SCALAR(event_props, '$.context') IN ('rating_page_opened', 'ratingskip', 'submit')
    GROUP BY 1,2,3
    ),
    
    merge AS (

    SELECT
        a.*,
        b.orderId,
        b.CTSessionId,
        c.rating_page_opened,
        c.ratingskip,
        c.submit
    FROM 
        order_base AS a
    LEFT JOIN 
        postorderstatus_dropped AS b
        ON a.order_date = b.order_date
        AND a.order_id = b.orderId
    LEFT JOIN 
        riderrating AS c
        ON a.order_date = b.order_date
        AND b.profile_identity = c.profile_identity
        AND b.CTSessionId = c.CTSessionId
    )
    
    SELECT
        order_date,
        net_orders,
        rating_page_opened,
        TRY(rating_page_opened*100.00/net_orders) AS "% rating_page_opened",
        ratingskip rating_skipped,
        TRY(ratingskip*100.00/net_orders) AS "% rating_skipped",
        submit,
        TRY(submit*100.00/net_orders) AS "% rating_submit"
    FROM
    
        (
        SELECT
            order_date,
            COUNT(DISTINCT order_id) net_orders,
            COUNT(DISTINCT CASE WHEN rating_page_opened IS NOT NULL THEN orderId END) rating_page_opened,
            COUNT(DISTINCT CASE WHEN ratingskip IS NOT NULL THEN orderId END) ratingskip,
            COUNT(DISTINCT CASE WHEN submit IS NOT NULL THEN orderId END) submit
        FROM 
            merge
        GROUP BY 1
        )
    ORDER BY 1 DESC


    -- ScratchCard Push Notification Impact Analysis

    WITH order_base AS (
    
    SELECT
        DATE_FORMAT(DATE_TRUNC( 'day' , DATE_PARSE(yyyymmdd, '%Y%m%d')), '%Y%m%d') order_date,
        customer_id,
        order_id,
        started_epoch,
        dropped_epoch
    FROM 
        orders.order_logs_fact
    WHERE 
       yyyymmdd >= date_format({{start_date}},'%Y%m%d') 
       AND yyyymmdd <= date_format({{end_date}},'%Y%m%d')
       AND order_status = 'dropped'
       AND service_obj_service_name in ('Auto','Auto Lite','Auto NCR','Auto Pool','AutoPremium','CityAuto','Link','Bike Lite','Bike Pink','CabEconomy','Bike Metro', 'CabPremium')
    --   AND city_name = 'Bangalore'
    --   AND customer_id = '6288c6694281e53bbe33d133'
    ),
    
    pos_started AS (
    
    SELECT
        DATE_FORMAT(DATE_TRUNC( 'day' , DATE_PARSE(yyyymmdd, '%Y%m%d')), '%Y%m%d') order_date,
        profile_identity userid,
        JSON_EXTRACT_SCALAR(event_props, '$.orderId') orderId,
        MIN(epoch) first_pv,
        APPROX_PERCENTILE(epoch, 0.5) middle_pv,
        MAX(epoch) last_pv
    FROM 
        clevertap.customer_postorderstatus_started_immutable
    WHERE 
        yyyymmdd >= date_format({{start_date}},'%Y%m%d') 
        AND yyyymmdd <= date_format({{end_date}},'%Y%m%d')
        -- AND profile_identity = '6288c6694281e53bbe33d133'
    GROUP BY 1,2,3
    ),
    
    unlocked AS (
    
    SELECT 
        DATE_FORMAT(DATE_TRUNC( 'day' , DATE_PARSE(yyyymmdd, '%Y%m%d')), '%Y%m%d') order_date,
        userid,
        rewards__transactionid rewards_transaction_id,
        rewards__type rewards_type,
        epoch unlocked_epoch
    FROM 
        canonical.iceberg_domain_pricing_scratchcards_immutable
    WHERE 
        yyyymmdd >= date_format({{start_date}}, '%Y%m%d') 
        AND yyyymmdd <= date_format({{end_date}}, '%Y%m%d') 
        AND rewards__state = 'UNLOCKED'
        -- AND userid =  '6288c6694281e53bbe33d133'
    ),
    
    push_impression AS (
    
    SELECT 
        DATE_FORMAT(DATE_TRUNC( 'day' , DATE_PARSE(yyyymmdd, '%Y%m%d')), '%Y%m%d') order_date,
        profile_identity userid,
        event_props_wzrk_nms wzrk_nms,
        epoch pn_epoch
    FROM 
        clevertap.customer_push_impressions_immutable 
    WHERE 
        yyyymmdd >= date_format({{start_date}}, '%Y%m%d') 
        AND yyyymmdd <= date_format({{end_date}}, '%Y%m%d') 
        AND event_props_wzrk_cid = 'promotional'
        AND event_props_wzrk_dl = 'rapido://rewards'
        -- AND profile_identity = '6288c6694281e53bbe33d133'
    ),
    
    click AS (
    
    SELECT 
        DATE_FORMAT(DATE_TRUNC( 'day' , DATE_PARSE(yyyymmdd, '%Y%m%d')), '%Y%m%d') order_date,
        profile_identity userid,
        epoch click_epoch
    FROM 
        clevertap.customer_notification_clicked_immutable
    WHERE 
        yyyymmdd >= date_format({{start_date}}, '%Y%m%d') 
        AND yyyymmdd <= date_format({{end_date}}, '%Y%m%d') 
        AND event_props_wzrk_cid = 'promotional'
        AND event_props_wzrk_dl = 'rapido://rewards'
        -- AND profile_identity = '6288c6694281e53bbe33d133'
    ),
    
    merge AS (
    
    SELECT
        order_base.*,
        CASE 
        WHEN rewards_transaction_id IS NULL THEN 'NON SC SCENARIO'
        WHEN rewards_transaction_id IS NOT NULL AND pn_epoch IS NOT NULL THEN 'SC SCENARIO - PN'
        WHEN rewards_transaction_id IS NOT NULL AND pn_epoch IS NULL THEN 'SC SCENARIO - NO PN'
        ELSE 'CHECK' END AS scenario,
        pos_started.orderId,
        pos_started.first_pv,
        pos_started.middle_pv,
        pos_started.last_pv,
        unlocked.rewards_transaction_id,
        unlocked.rewards_type,
        unlocked.unlocked_epoch,
        push_impression.pn_epoch,
        click.click_epoch,
        LEAST((last_pv/1000 - first_pv/1000), (last_pv/1000 - middle_pv/1000), (middle_pv/1000 - first_pv/1000)) screen_time
        
    FROM 
        order_base
    LEFT JOIN
        pos_started 
        ON order_base.order_date = pos_started.order_date
        AND order_base.order_id = pos_started.orderId
        AND order_base.started_epoch <= pos_started.first_pv
        AND pos_started.last_pv <= order_base.dropped_epoch
    LEFT JOIN
        unlocked
        ON order_base.order_date = unlocked.order_date
        AND order_base.customer_id = unlocked.userid
        AND order_base.order_id = unlocked.rewards_transaction_id
        AND order_base.started_epoch <= unlocked.unlocked_epoch
        AND unlocked.unlocked_epoch <= order_base.dropped_epoch
    LEFT JOIN 
        push_impression
        ON unlocked.order_date = push_impression.order_date
        AND unlocked.userid = push_impression.userid
        AND unlocked.unlocked_epoch <= push_impression.pn_epoch
        AND push_impression.pn_epoch <= order_base.dropped_epoch
    LEFT JOIN 
        click
        ON push_impression.order_date = click.order_date
        AND push_impression.userid = click.userid
        AND push_impression.pn_epoch <= click.click_epoch
        AND click.click_epoch <= order_base.dropped_epoch
    )
        
    SELECT
        order_date,
        scenario,
        COUNT(DISTINCT order_id) net_orders,
        COUNT(DISTINCT orderId) pv_atleast_once,
        TRY(COUNT(DISTINCT orderId)*100.00/COUNT(DISTINCT order_id)) "% pv atleast once",
        APPROX_PERCENTILE(CASE WHEN screen_time > 0 THEN screen_time END, 0.70) screen_time
    FROM 
        merge
    GROUP BY 1,2
    ORDER BY 1,2
        


-- ScratchCard Push Notification Impact Analysis - Trend

WITH order_base AS (
    
    SELECT
        DATE_FORMAT(DATE_TRUNC( 'day' , DATE_PARSE(yyyymmdd, '%Y%m%d')), '%Y%m%d') order_date,
        customer_id,
        order_id,
        started_epoch,
        dropped_epoch
    FROM 
        orders.order_logs_fact
    WHERE 
       yyyymmdd >= date_format({{start_date}},'%Y%m%d') 
       AND yyyymmdd <= date_format({{end_date}},'%Y%m%d')
       AND order_status = 'dropped'
       AND service_obj_service_name in ('Auto','Auto Lite','Auto NCR','Auto Pool','AutoPremium','CityAuto','Link','Bike Lite','Bike Pink','CabEconomy','Bike Metro', 'CabPremium')
    --   AND city_name = 'Bangalore'
    --   AND customer_id = '6288c6694281e53bbe33d133'
    ),
    
    pos_started AS (
    
    SELECT
        DATE_FORMAT(DATE_TRUNC( 'day' , DATE_PARSE(yyyymmdd, '%Y%m%d')), '%Y%m%d') order_date,
        profile_identity userid,
        JSON_EXTRACT_SCALAR(event_props, '$.orderId') orderId,
        MIN(epoch) first_pv,
        APPROX_PERCENTILE(epoch, 0.5) middle_pv,
        MAX(epoch) last_pv
    FROM 
        clevertap.customer_postorderstatus_started_immutable
    WHERE 
        yyyymmdd >= date_format({{start_date}},'%Y%m%d') 
        AND yyyymmdd <= date_format({{end_date}},'%Y%m%d')
        -- AND profile_identity = '6288c6694281e53bbe33d133'
    GROUP BY 1,2,3
    ),
    
    unlocked AS (
    
    SELECT 
        DATE_FORMAT(DATE_TRUNC( 'day' , DATE_PARSE(yyyymmdd, '%Y%m%d')), '%Y%m%d') order_date,
        userid,
        rewards__transactionid rewards_transaction_id,
        rewards__type rewards_type,
        epoch unlocked_epoch
    FROM 
        canonical.iceberg_domain_pricing_scratchcards_immutable
    WHERE 
        yyyymmdd >= date_format({{start_date}}, '%Y%m%d') 
        AND yyyymmdd <= date_format({{end_date}}, '%Y%m%d') 
        AND rewards__state = 'UNLOCKED'
        -- AND userid =  '6288c6694281e53bbe33d133'
    ),
    
    push_impression AS (
    
    SELECT 
        DATE_FORMAT(DATE_TRUNC( 'day' , DATE_PARSE(yyyymmdd, '%Y%m%d')), '%Y%m%d') order_date,
        profile_identity userid,
        event_props_wzrk_nms wzrk_nms,
        epoch pn_epoch
    FROM 
        clevertap.customer_push_impressions_immutable 
    WHERE 
        yyyymmdd >= date_format({{start_date}}, '%Y%m%d') 
        AND yyyymmdd <= date_format({{end_date}}, '%Y%m%d') 
        AND event_props_wzrk_cid = 'promotional'
        AND event_props_wzrk_dl = 'rapido://rewards'
        -- AND profile_identity = '6288c6694281e53bbe33d133'
    ),
    
    click AS (
    
    SELECT 
        DATE_FORMAT(DATE_TRUNC( 'day' , DATE_PARSE(yyyymmdd, '%Y%m%d')), '%Y%m%d') order_date,
        profile_identity userid,
        epoch click_epoch
    FROM 
        clevertap.customer_notification_clicked_immutable
    WHERE 
        yyyymmdd >= date_format({{start_date}}, '%Y%m%d') 
        AND yyyymmdd <= date_format({{end_date}}, '%Y%m%d') 
        AND event_props_wzrk_cid = 'promotional'
        AND event_props_wzrk_dl = 'rapido://rewards'
        -- AND profile_identity = '6288c6694281e53bbe33d133'
    ),
    
    merge AS (
    
    SELECT
        order_base.*,
        CASE 
        WHEN rewards_transaction_id IS NULL THEN 'NON SC SCENARIO'
        WHEN rewards_transaction_id IS NOT NULL AND pn_epoch IS NOT NULL THEN 'SC SCENARIO - PN'
        WHEN rewards_transaction_id IS NOT NULL AND pn_epoch IS NULL THEN 'SC SCENARIO - NO PN'
        ELSE 'CHECK' END AS scenario,
        pos_started.orderId,
        pos_started.first_pv,
        pos_started.middle_pv,
        pos_started.last_pv,
        unlocked.rewards_transaction_id,
        unlocked.rewards_type,
        unlocked.unlocked_epoch,
        push_impression.pn_epoch,
        click.click_epoch,
        LEAST((last_pv/1000 - first_pv/1000), (last_pv/1000 - middle_pv/1000), (middle_pv/1000 - first_pv/1000)) screen_time
        
    FROM 
        order_base
    LEFT JOIN
        pos_started 
        ON order_base.order_date = pos_started.order_date
        AND order_base.order_id = pos_started.orderId
        AND order_base.started_epoch <= pos_started.first_pv
        AND pos_started.last_pv <= order_base.dropped_epoch
    LEFT JOIN
        unlocked
        ON order_base.order_date = unlocked.order_date
        AND order_base.customer_id = unlocked.userid
        AND order_base.order_id = unlocked.rewards_transaction_id
        AND order_base.started_epoch <= unlocked.unlocked_epoch
        AND unlocked.unlocked_epoch <= order_base.dropped_epoch
    LEFT JOIN 
        push_impression
        ON unlocked.order_date = push_impression.order_date
        AND unlocked.userid = push_impression.userid
        AND unlocked.unlocked_epoch <= push_impression.pn_epoch
        AND push_impression.pn_epoch <= order_base.dropped_epoch
    LEFT JOIN 
        click
        ON push_impression.order_date = click.order_date
        AND push_impression.userid = click.userid
        AND push_impression.pn_epoch <= click.click_epoch
        AND click.click_epoch <= order_base.dropped_epoch
    )
        
    SELECT
        order_date,
        scenario,
        COUNT(DISTINCT order_id) net_orders,
        COUNT(DISTINCT orderId) pv_atleast_once,
        TRY(COUNT(DISTINCT orderId)*100.00/COUNT(DISTINCT order_id)) "% pv atleast once",
        APPROX_PERCENTILE(CASE WHEN screen_time > 0 THEN screen_time END, 0.70) screen_time
    FROM 
        merge
    GROUP BY 1,2
    ORDER BY 1,2
        

-- Scratch Cards | Inventory Utilization ?


with usage_data as (

    select 
        id reward_id,
        -- JSON_EXTRACT_SCALAR(data, '$.data.status') status,
        MAX(CAST(JSON_EXTRACT_SCALAR(data, '$.data.maxUsage') AS INT)) maxUsage,
        MAX(CAST(JSON_EXTRACT_SCALAR(data, '$.data.currentUsage') AS INT)) currentUsage
        -- *
    from 
        raw_internal.kafka_info_offers_external_reward_v1
    where 
        yyyymmdd >= date_format({{start_date}},'%Y%m%d')
        and yyyymmdd <= date_format({{end_date}},'%Y%m%d') 
    group by 1
    having MAX(CAST(JSON_EXTRACT_SCALAR(data, '$.data.currentUsage') AS INT)) > 0
    ),
    
    partner_name as (
    select 
        json_extract_scalar(data__rewards, '$[0].externalReward.id')  AS reward_id,
        json_extract_scalar(data__rewards, '$[0].externalReward.partnerName')  AS partner_name,
        -- data__name as campaign_name, 
        data__lastmodifiedby last_modifiedby
    from 
        hive.canonical.iceberg_domain_scratchcards_inventory_events_immutable 
    where 
        yyyymmdd >= '20240401'
        and yyyymmdd <= date_format(current_date, '%Y%m%d')
    group by 1,2,3
    )
    
    select
        partner_name,
        usage_data.*,
        try(currentUsage*100.0/maxUsage) utilization,
        last_modifiedby
    from 
        usage_data
    inner join 
        partner_name
        on usage_data.reward_id = partner_name.reward_id
    order by 1,3,2



    -- Inventory Performance


    with raw as (
    select 
        yyyymmdd,
        json_extract_scalar(data__rewards, '$[0].externalReward.id') AS reward_partner_id,
        json_extract_scalar(data__rewards, '$[0].externalReward.partnerName') AS reward_partner_name,
        json_extract_scalar(data__rewards, '$[0].externalReward.inventoryName') AS inventoryName
    from  
        hive.canonical.iceberg_domain_scratchcards_inventory_events_immutable
    where  
        yyyymmdd >= [[ date_format({{start_date}}, '%Y%m%d') --]] DATE_FORMAT(CURRENT_DATE - interval '1' day , '%Y%m%d') 
        and yyyymmdd <= [[ date_format({{end_date}}, '%Y%m%d') --]] DATE_FORMAT(CURRENT_DATE - interval '1' day , '%Y%m%d') 
    group by 1,2,3,4
    ),
    
    unlocked_raw AS (

    select 
        kpsi.yyyymmdd,
        kpsi.userid as user_id,
        raw.reward_partner_name,
        raw.inventoryName,
        kpsi.rewards__transactionid as rewards_transaction_id
    from 
        raw
    left join 
        hive.canonical.iceberg_domain_pricing_scratchcards_immutable kpsi
        on rewards__externalrewardmetadata__externalrewardid = raw.reward_partner_id
    WHERE 
        kpsi.yyyymmdd >= [[ date_format({{start_date}}, '%Y%m%d') --]] DATE_FORMAT(CURRENT_DATE - interval '1' day , '%Y%m%d') 
        AND kpsi.yyyymmdd <= [[ date_format({{end_date}}, '%Y%m%d') --]] DATE_FORMAT(CURRENT_DATE - interval '1' day , '%Y%m%d') 
        AND rewards__state = 'UNLOCKED'
        AND rewards__type = 'EXTERNAL_REWARD'
    ),
    
    unlocked as (
    
    select 
        min(yyyymmdd) || '-' || max(yyyymmdd) date_range,
        reward_partner_name,
        inventoryName,
        count(distinct rewards_transaction_id) as unlocked_cards
    from 
        unlocked_raw
    group by 2,3
    ),
    
    scratched_raw as (
    select 
        kpsi.yyyymmdd,
        raw.reward_partner_name,
        raw.inventoryName,
        kpsi.rewards__transactionid as rewards_transaction_id
    from 
        raw
    left join 
        hive.canonical.iceberg_domain_pricing_scratchcards_immutable kpsi
        on rewards__externalrewardmetadata__externalrewardid = raw.reward_partner_id
    WHERE 
        kpsi.yyyymmdd >= [[ date_format({{start_date}}, '%Y%m%d') --]] DATE_FORMAT(CURRENT_DATE - interval '1' day , '%Y%m%d') 
        AND kpsi.yyyymmdd <= [[ date_format({{end_date}}, '%Y%m%d') --]] DATE_FORMAT(CURRENT_DATE - interval '1' day , '%Y%m%d') 
        AND rewards__type = 'EXTERNAL_REWARD'
        AND rewards__state = 'SCRATCHED' 
        
    ),
    
    total_scratched as (
    
    select 
        reward_partner_name,
        inventoryName,
        count(distinct rewards_transaction_id) as scratched_cards
    from 
        scratched_raw
    group by 1,2
    )
    
    select
        unlocked.*,
        scratched_cards,
        try(scratched_cards*100.00/unlocked_cards) "% scratch rate"
    from 
        unlocked
    left join 
        total_scratched
        on unlocked.reward_partner_name = total_scratched.reward_partner_name
        and unlocked.inventoryName = total_scratched.inventoryName

    -- Daily Reporting | Scratch Cards | Central Report?

    with raw as (
select 
distinct yyyymmdd,
json_extract_scalar(data__rewards, '$[0].externalReward.id') AS reward_partner_id,
        json_extract_scalar(data__rewards, '$[0].externalReward.partnerName') AS reward_partner_name,
        json_extract_scalar(data__rewards, '$[0].externalReward.inventoryName') AS inventoryName
from  hive.canonical.iceberg_domain_scratchcards_inventory_events_immutable
where  
yyyymmdd >= '20240401'

)




,unlocked_raw AS (

    select 
distinct
        kpsi.yyyymmdd,
        kpsi.userid as user_id,
        raw.reward_partner_name,
        raw.inventoryName,
        kpsi.rewards__transactionid as rewards_transaction_id
from raw
left join 
hive.canonical.iceberg_domain_pricing_scratchcards_immutable kpsi
on rewards__externalrewardmetadata__externalrewardid = raw.reward_partner_id
WHERE 
        kpsi.yyyymmdd >= [[ date_format({{start_date}}, '%Y%m%d') --]] DATE_FORMAT(CURRENT_DATE - interval '1' day , '%Y%m%d') 
    AND kpsi.yyyymmdd <= [[ date_format({{end_date}}, '%Y%m%d') --]] DATE_FORMAT(CURRENT_DATE - interval '1' day , '%Y%m%d') 
    AND rewards__state = 'UNLOCKED'
        AND rewards__type = 'EXTERNAL_REWARD'
    )
    
    ,unlocked as (
    
    select yyyymmdd, 
    inventoryName,
    -- temp_reward_partner_name,
    -- 'unlocked on ' || date_format(date_parse(yyyymmdd, '%Y%m%d'), '%d-%m-%Y') AS status,
    count(distinct rewards_transaction_id) as unlocked_cards
    from unlocked_raw
    group by 1,2
    )
   
   
   ,scratched_raw as (
    select 
distinct
        kpsi.yyyymmdd,
        -- kpsi.userid as user_id,
        raw.reward_partner_name,
        raw.inventoryName,
        kpsi.rewards__transactionid as rewards_transaction_id
from raw
left join 
hive.canonical.iceberg_domain_pricing_scratchcards_immutable kpsi
on rewards__externalrewardmetadata__externalrewardid = raw.reward_partner_id
WHERE 
        kpsi.yyyymmdd >= [[ date_format({{start_date}}, '%Y%m%d') --]] DATE_FORMAT(CURRENT_DATE - interval '1' day , '%Y%m%d') 
    AND kpsi.yyyymmdd <= [[ date_format({{end_date}}, '%Y%m%d') --]] DATE_FORMAT(CURRENT_DATE - interval '1' day , '%Y%m%d') 
        AND rewards__type = 'EXTERNAL_REWARD'
        AND rewards__state = 'SCRATCHED' 
        
    )


     ,same_day_scratched as (
    SELECT distinct
        yyyymmdd,
        inventoryName,
        -- temp_reward_partner_name,
        -- 'scratched on ' || date_format(date_parse(yyyymmdd, '%Y%m%d'), '%d-%m-%Y') AS status,
        -- user_id,
        count(distinct rewards_transaction_id) as same_day_unlocked_scratched
        -- JSON_EXTRACT_SCALAR(rewards, '$.externalReward.partnerName') partner_name,
        -- JSON_EXTRACT_SCALAR(rewards, '$.externalRewardMetaData.externalRewardId') externalRewardId

    FROM scratched_raw
    WHERE rewards_transaction_id in (select distinct rewards_transaction_id from unlocked_raw)
        
    group by 1, 2 
    )

    ,total_scratched as (
    SELECT distinct
        yyyymmdd,
        -- 'scratched on ' || date_format(date_parse(yyyymmdd, '%Y%m%d'), '%d-%m-%Y') AS status,
        -- user_id,
        inventoryName,
        -- temp_reward_partner_name,
        count(distinct rewards_transaction_id) as scratched_cards
        -- JSON_EXTRACT_SCALAR(rewards, '$.externalReward.partnerName') partner_name,
        -- JSON_EXTRACT_SCALAR(rewards, '$.externalRewardMetaData.externalRewardId') externalRewardId

    FROM scratched_raw 
        
    group by 1 , 2
    )

select 
ul.yyyymmdd as day,
ul.inventoryName,
-- ul.temp_reward_partner_name,
ul.unlocked_cards,
coalesce(ts.scratched_cards,0) as total_scratched_cards_for_the_day,
coalesce(cast(ts.scratched_cards as double),0) / coalesce(cast(ul.unlocked_cards as double),0) as total_scratch_rate
-- coalesce(sds.same_day_unlocked_scratched,0) as scratched_cards_unlocked_on_same_day,
-- coalesce(cast(sds.same_day_unlocked_scratched as double),0) / coalesce(cast(ul.unlocked_cards as double),0) as same_day_scratch_rate

from unlocked ul 
left join total_scratched ts 
    on ul.yyyymmdd = ts.yyyymmdd 
    -- and ts.temp_reward_partner_name = ul.temp_reward_partner_name
    and ts.inventoryName = ul.inventoryName
left join same_day_scratched sds 
    on ul.yyyymmdd = sds.yyyymmdd 
    -- and sds.temp_reward_partner_name = ul.temp_reward_partner_name
    and sds.inventoryName = ul.inventoryName
order by 2,1

-- date, reward_partner_name, inventory_name, campaign_name, start / end dates of campaign_name, validity, timings 
-- reward_partner_name vs metrics - day level trend 


-- SC type - coins and SC 
-- coins unlocked at order_id - amount>0 is earned and amount = 0 is not earned and coins type = rapido coins - check 
-- Scratch Cards: type = external reward 
-- sc unlocked with order_id ('better luck next time' (not earned) will show if inventory is exhausted before unlock stage irrespective of order completion ) - amount =0 and we have to segregate with order_id and status = scratched 
-- earned - amount 
-- Right now, sc with 'better luck next time' is not being considered in scratch rate 
-- Include rewards_transaction_id and exclude the partner_id and status = scratched 



-- Trends | Unlocked Cards | Total Scratch Rate

with raw as (
select 
distinct yyyymmdd,
replace(cast (json_extract(data__rewards, '$[0].externalReward.id') as varchar), '"','')  AS reward_partner_id,
replace(cast (json_extract(data__rewards, '$[0].externalReward.partnerName') as varchar), '"','')  AS reward_partner_name
from  hive.canonical.iceberg_domain_scratchcards_inventory_events_immutable
where  
yyyymmdd >= [[ date_format({{start_date}}, '%Y%m%d') --]] DATE_FORMAT(CURRENT_DATE - interval '1' day , '%Y%m%d') 
    AND yyyymmdd <= [[ date_format({{end_date}}, '%Y%m%d') --]] DATE_FORMAT(CURRENT_DATE - interval '1' day , '%Y%m%d') 

)




,unlocked_raw AS (

    select 
distinct
        kpsi.yyyymmdd,
        kpsi.userid as user_id,
        raw.reward_partner_name,
        kpsi.rewards__transactionid as rewards_transaction_id
from raw
left join 
hive.canonical.iceberg_domain_pricing_scratchcards_immutable kpsi
on rewards__externalrewardmetadata__externalrewardid = raw.reward_partner_id
WHERE 
        kpsi.yyyymmdd >= [[ date_format({{start_date}}, '%Y%m%d') --]] DATE_FORMAT(CURRENT_DATE - interval '1' day , '%Y%m%d') 
    AND kpsi.yyyymmdd <= [[ date_format({{end_date}}, '%Y%m%d') --]] DATE_FORMAT(CURRENT_DATE - interval '1' day , '%Y%m%d') 
    AND rewards__state = 'UNLOCKED'
        AND rewards__type = 'EXTERNAL_REWARD'
    )
    
    ,unlocked as (
    
    select yyyymmdd, 
    reward_partner_name,
    -- temp_reward_partner_name,
    -- 'unlocked on ' || date_format(date_parse(yyyymmdd, '%Y%m%d'), '%d-%m-%Y') AS status,
    count(distinct rewards_transaction_id) as unlocked_cards
    from unlocked_raw
    group by 1,2
    )
   
   
   ,scratched_raw as (
    select 
distinct
        kpsi.yyyymmdd,
        -- kpsi.userid as user_id,
        raw.reward_partner_name,
        kpsi.rewards__transactionid as rewards_transaction_id
from raw
left join 
hive.canonical.iceberg_domain_pricing_scratchcards_immutable kpsi
on rewards__externalrewardmetadata__externalrewardid = raw.reward_partner_id
WHERE 
        kpsi.yyyymmdd >= [[ date_format({{start_date}}, '%Y%m%d') --]] DATE_FORMAT(CURRENT_DATE - interval '1' day , '%Y%m%d') 
    AND kpsi.yyyymmdd <= [[ date_format({{end_date}}, '%Y%m%d') --]] DATE_FORMAT(CURRENT_DATE - interval '1' day , '%Y%m%d') 
        AND rewards__type = 'EXTERNAL_REWARD'
        AND rewards__state = 'SCRATCHED' 
        
    )


     ,same_day_scratched as (
    SELECT distinct
        yyyymmdd,
        reward_partner_name,
        -- temp_reward_partner_name,
        -- 'scratched on ' || date_format(date_parse(yyyymmdd, '%Y%m%d'), '%d-%m-%Y') AS status,
        -- user_id,
        count(distinct rewards_transaction_id) as same_day_unlocked_scratched
        -- JSON_EXTRACT_SCALAR(rewards, '$.externalReward.partnerName') partner_name,
        -- JSON_EXTRACT_SCALAR(rewards, '$.externalRewardMetaData.externalRewardId') externalRewardId

    FROM scratched_raw
    WHERE rewards_transaction_id in (select distinct rewards_transaction_id from unlocked_raw)
        
    group by 1, 2 
    )

    ,total_scratched as (
    SELECT distinct
        yyyymmdd,
        -- 'scratched on ' || date_format(date_parse(yyyymmdd, '%Y%m%d'), '%d-%m-%Y') AS status,
        -- user_id,
        reward_partner_name,
        -- temp_reward_partner_name,
        count(distinct rewards_transaction_id) as scratched_cards
        -- JSON_EXTRACT_SCALAR(rewards, '$.externalReward.partnerName') partner_name,
        -- JSON_EXTRACT_SCALAR(rewards, '$.externalRewardMetaData.externalRewardId') externalRewardId

    FROM scratched_raw 
        
    group by 1 , 2
    )

select 
ul.yyyymmdd as day,
ul.reward_partner_name,
-- ul.temp_reward_partner_name,
ul.unlocked_cards,
coalesce(ts.scratched_cards,0) as total_scratched_cards_for_the_day,
coalesce(cast(ts.scratched_cards as double),0) / coalesce(cast(ul.unlocked_cards as double),0) as total_scratch_rate,
coalesce(sds.same_day_unlocked_scratched,0) as scratched_cards_unlocked_on_same_day,
coalesce(cast(sds.same_day_unlocked_scratched as double),0) / coalesce(cast(ul.unlocked_cards as double),0) as same_day_scratch_rate

from unlocked ul 
left join total_scratched ts 
    on ul.yyyymmdd = ts.yyyymmdd 
    -- and ts.temp_reward_partner_name = ul.temp_reward_partner_name
    and ts.reward_partner_name = ul.reward_partner_name
left join same_day_scratched sds 
    on ul.yyyymmdd = sds.yyyymmdd 
    -- and sds.temp_reward_partner_name = ul.temp_reward_partner_name
    and sds.reward_partner_name = ul.reward_partner_name
order by 1,2

-- date, reward_partner_name, inventory_name, campaign_name, start / end dates of campaign_name, validity, timings 
-- reward_partner_name vs metrics - day level trend 


-- SC type - coins and SC 
-- coins unlocked at order_id - amount>0 is earned and amount = 0 is not earned and coins type = rapido coins - check 
-- Scratch Cards: type = external reward 
-- sc unlocked with order_id ('better luck next time' (not earned) will show if inventory is exhausted before unlock stage irrespective of order completion ) - amount =0 and we have to segregate with order_id and status = scratched 
-- earned - amount 
-- Right now, sc with 'better luck next time' is not being considered in scratch rate 
-- Include rewards_transaction_id and exclude the partner_id and status = scratched 



-- Scratch Cards | Inventory Data?

-- date, reward_partner_name, inventory_name, campaign_name, start / end dates of campaign_name, validity, timings 


select 
    json_extract_scalar(data__rewards, '$[0].type') reward_type,
    yyyymmdd,
    json_extract_scalar(data__rewards, '$[0].externalReward.id')  AS reward_partner_id,
    json_extract_scalar(data__rewards, '$[0].externalReward.partnerName')  AS partner_name,
    data__name as campaign_name, 
    data__status as status,
    data__startdate || ' - ' || data__enddate as campaign_range,
    data__timings as timings,
    count(distinct data__city__id) cities,
    -- json_extract_scalar(data__rewards, '$[0].probability')  AS probability,
    -- data__usagelimit usagelimit,
    -- json_extract_scalar(data__rewards, '$[0].validity.value') || ' ' || json_extract_scalar(data__rewards, '$[0].validity.unit') AS validity_period,
    data__type type, 
    eventtype,
    data__lastmodifiedby last_modifiedby
from 
    hive.canonical.iceberg_domain_scratchcards_inventory_events_immutable 
where 
    yyyymmdd >= [[ date_format({{start_date}},'%Y%m%d')--]] '20240401' 
    and yyyymmdd <= [[ date_format({{end_date}},'%Y%m%d') --]] date_format(current_date, '%Y%m%d')
    [[and {{Campaign_name}}]]
group by 1,2,3,4,5,6,7,8,10,11,12
order by 1,2


-- Ads Latency Tracker - PostOrderStatus onTheWay

WITH service_mapping as (

    SELECT
        city_display_name city,
        service_level service        
    FROM 
        datasets.service_mapping
    WHERE 
        [[ {{city}} --]] city_display_name = 'Bangalore' 
        AND [[ {{service}} --]] service_level IN ('Link', 'Auto', 'CabEconomy')
    ),
    
    postorderstatus_ontheway AS (
    
    SELECT
        yyyymmdd,
        hh,
        'PostOrderStatus onTheWay' screen,
        user_id,
        -- order_id,
        epoch
    FROM
        (
        SELECT 
            *,
            ROW_NUMBER() OVER(PARTITION BY yyyymmdd, user_id, order_id ORDER BY epoch DESC) rn
        FROM
            (
            SELECT
                yyyymmdd,
                hh,
                profile_identity user_id,
                JSON_EXTRACT_SCALAR(event_props, '$.orderId') order_id,
                epoch
            FROM 
                clevertap.customer_postorderstatus_ontheway_immutable
            WHERE 
                yyyymmdd >= date_format({{start_date}},'%Y%m%d')
                AND yyyymmdd <= date_format({{end_date}},'%Y%m%d')
                AND profile_platform = 'Android'
                AND JSON_EXTRACT_SCALAR(event_props, '$.currentCity') IN (SELECT city FROM service_mapping)
            )
        )
        WHERE 
            rn = 1
    ),
    
    request as (

    SELECT     
        DISTINCT
        yyyymmdd,
        hh hour,
        city,
        service,
        userId,
        pagename,
        eventName,
        responseType,
        pagename,
        slotname,
        format,
        adid,
        epoch 
    FROM 
        canonical.iceberg_log_telemetry_ads_impressions_immutable_full
    
    WHERE  
        yyyymmdd >= date_format({{start_date}},'%Y%m%d')
        and yyyymmdd <= date_format({{end_date}},'%Y%m%d')
        and responseType = 'GAMBanner'
        -- and userId = '659f595c6d2e761d5ded3b0e'
        and city IN (SELECT city FROM service_mapping)
        and service IN (SELECT service FROM service_mapping)
        and regexp_like(lower(slotname), 'ontheway')
        and eventName = 'Ad_Request'
    ),
    
    response as (

    SELECT     
        DISTINCT
        yyyymmdd,
        hh hour,
        city,
        service,
        userId,
        pagename,
        eventName,
        responseType,
        pagename,
        slotname,
        format,
        adid,
        epoch 
    FROM 
        canonical.iceberg_log_telemetry_ads_impressions_immutable_full
    
    WHERE  
        yyyymmdd >= date_format({{start_date}},'%Y%m%d')
        and yyyymmdd <= date_format({{end_date}},'%Y%m%d')
        and responseType = 'GAMBanner'
        -- and userId = '659f595c6d2e761d5ded3b0e'
        and city IN (SELECT city FROM service_mapping)
        and service IN (SELECT service FROM service_mapping)
        and regexp_like(lower(slotname), 'ontheway')
        and eventName = 'Ad_Response'
    ),
    
    impression as (

    SELECT     
        DISTINCT
        yyyymmdd,
        hh hour,
        city,
        service,
        userId,
        pagename,
        eventName,
        responseType,
        pagename,
        slotname,
        format,
        adid,
        epoch 
    FROM 
        canonical.iceberg_log_telemetry_ads_impressions_immutable_full
    
    WHERE  
        yyyymmdd >= date_format({{start_date}},'%Y%m%d')
        and yyyymmdd <= date_format({{end_date}},'%Y%m%d')
        and responseType = 'GAMBanner'
        -- and userId = '659f595c6d2e761d5ded3b0e'
        and city IN (SELECT city FROM service_mapping)
        and service IN (SELECT service FROM service_mapping)
        and regexp_like(lower(slotname), 'ontheway')
        and eventName = 'Ad_Impression'
    )
    
    SELECT
        screen,
        slotname,
        format,
        yyyymmdd,
        count(distinct adid) ad_requests,
        approx_percentile(Req2Res_time_spent_in_sec, 0.5) Req2Res_time_spent_in_sec,
        approx_percentile(Req2Res_time_spent_in_ms, 0.5) Req2Res_time_spent_in_ms,
        approx_percentile(Res2Imp_time_spent_in_sec, 0.5) Res2Imp_time_spent_in_sec,
        approx_percentile(Res2Imp_time_spent_in_ms, 0.5) Res2Imp_time_spent_in_ms
    from 
    (
    SELECT
        screen,
        request.*,
        -- otw.epoch screen_epoch,
        response.epoch response,
        impression.epoch impression,
        (response.epoch/1000.0 - request.epoch/1000.0) Req2Res_time_spent_in_sec,
        (response.epoch - request.epoch) Req2Res_time_spent_in_ms,
        (impression.epoch/1000.0 - response.epoch/1000.0) Res2Imp_time_spent_in_sec,
        (impression.epoch - response.epoch) Res2Imp_time_spent_in_ms,
        ROW_NUMBER() OVER(PARTITION BY request.adid ORDER BY response.epoch desc) rn
        
    FROM 
        postorderstatus_ontheway otw
    JOIN
        request
        ON otw.yyyymmdd = request.yyyymmdd
        AND otw.user_id = request.userId
        AND otw.hh = request.hour
        -- AND otw.epoch <= request.epoch
    JOIN 
        response
        ON request.yyyymmdd = response.yyyymmdd
        AND request.city = response.city
        AND request.service = response.service
        AND request.userId = response.userId
        AND request.slotname = response.slotname
        AND request.format = response.format
        AND request.adid = response.adid
    LEFT JOIN 
        impression
        ON impression.yyyymmdd = response.yyyymmdd
        AND impression.city = response.city
        AND impression.service = response.service
        AND impression.userId = response.userId
        AND impression.slotname = response.slotname
        AND impression.format = response.format
        AND impression.adid = response.adid
    )
    GROUP BY 1,2,3,4
    ORDER BY 1,2,3,4


    -- Ads Latency Tracker - PostOrderStatus Arrived


    WITH service_mapping as (

    SELECT
        city_display_name city,
        service_level service        
    FROM 
        datasets.service_mapping
    WHERE 
        [[ {{city}} --]] city_display_name = 'Bangalore' 
        AND [[ {{service}} --]] service_level IN ('Link', 'Auto', 'CabEconomy')
    ),
    
    postorderstatus_arrived AS (
    
    SELECT
        yyyymmdd,
        hh,
        'PostOrderStatus Arrived' screen,
        user_id,
        -- order_id,
        epoch
    FROM
        (
        SELECT 
            *,
            ROW_NUMBER() OVER(PARTITION BY yyyymmdd, user_id, order_id ORDER BY epoch DESC) rn
        FROM
            (
            SELECT
                yyyymmdd,
                hh,
                profile_identity user_id,
                JSON_EXTRACT_SCALAR(event_props, '$.orderId') order_id,
                epoch
            FROM 
                clevertap.customer_postorderstatus_arrived_immutable
            WHERE 
                yyyymmdd >= date_format({{start_date}},'%Y%m%d')
                AND yyyymmdd <= date_format({{end_date}},'%Y%m%d')
                AND profile_platform = 'Android'
                AND JSON_EXTRACT_SCALAR(event_props, '$.currentCity') IN (SELECT city FROM service_mapping)
            )
        )
        WHERE 
            rn = 1
    ),
    
    request as (

    SELECT     
        DISTINCT
        yyyymmdd,
        hh hour,
        city,
        service,
        userId,
        pagename,
        eventName,
        responseType,
        pagename,
        slotname,
        format,
        adid,
        epoch 
    FROM 
        canonical.iceberg_log_telemetry_ads_impressions_immutable_full
    
    WHERE  
        yyyymmdd >= date_format({{start_date}},'%Y%m%d')
        and yyyymmdd <= date_format({{end_date}},'%Y%m%d')
        and responseType = 'GAMBanner'
        -- and userId = '659f595c6d2e761d5ded3b0e'
        and city IN (SELECT city FROM service_mapping)
        and service IN (SELECT service FROM service_mapping)
        and regexp_like(lower(slotname), 'arrived')
        and eventName = 'Ad_Request'
    ),
    
    response as (

    SELECT     
        DISTINCT
        yyyymmdd,
        hh hour,
        city,
        service,
        userId,
        pagename,
        eventName,
        responseType,
        pagename,
        slotname,
        format,
        adid,
        epoch 
    FROM 
        canonical.iceberg_log_telemetry_ads_impressions_immutable_full
    
    WHERE  
        yyyymmdd >= date_format({{start_date}},'%Y%m%d')
        and yyyymmdd <= date_format({{end_date}},'%Y%m%d')
        and responseType = 'GAMBanner'
        -- and userId = '659f595c6d2e761d5ded3b0e'
        and city IN (SELECT city FROM service_mapping)
        and service IN (SELECT service FROM service_mapping)
        and regexp_like(lower(slotname), 'arrived')
        and eventName = 'Ad_Response'
    ),
    
    impression as (

    SELECT     
        DISTINCT
        yyyymmdd,
        hh hour,
        city,
        service,
        userId,
        pagename,
        eventName,
        responseType,
        pagename,
        slotname,
        format,
        adid,
        epoch 
    FROM 
        canonical.iceberg_log_telemetry_ads_impressions_immutable_full
    
    WHERE  
        yyyymmdd >= date_format({{start_date}},'%Y%m%d')
        and yyyymmdd <= date_format({{end_date}},'%Y%m%d')
        and responseType = 'GAMBanner'
        -- and userId = '659f595c6d2e761d5ded3b0e'
        and city IN (SELECT city FROM service_mapping)
        and service IN (SELECT service FROM service_mapping)
        and regexp_like(lower(slotname), 'arrived')
        and eventName = 'Ad_Impression'
    )
    
    SELECT
        screen,
        slotname,
        format,
        yyyymmdd,
        count(distinct adid) ad_requests,
        approx_percentile(Req2Res_time_spent_in_sec, 0.5) Req2Res_time_spent_in_sec,
        approx_percentile(Req2Res_time_spent_in_ms, 0.5) Req2Res_time_spent_in_ms,
        approx_percentile(Res2Imp_time_spent_in_sec, 0.5) Res2Imp_time_spent_in_sec,
        approx_percentile(Res2Imp_time_spent_in_ms, 0.5) Res2Imp_time_spent_in_ms
    from 
    (
    SELECT
        screen,
        request.*,
        -- arr.epoch screen_epoch,
        response.epoch response,
        impression.epoch impression,
        (response.epoch/1000.0 - request.epoch/1000.0) Req2Res_time_spent_in_sec,
        (response.epoch - request.epoch) Req2Res_time_spent_in_ms,
        (impression.epoch/1000.0 - response.epoch/1000.0) Res2Imp_time_spent_in_sec,
        (impression.epoch - response.epoch) Res2Imp_time_spent_in_ms,
        ROW_NUMBER() OVER(PARTITION BY request.adid ORDER BY response.epoch desc) rn
        
    FROM 
        postorderstatus_arrived arr
    JOIN
        request
        ON arr.yyyymmdd = request.yyyymmdd
        AND arr.user_id = request.userId
        AND arr.hh = request.hour
        -- AND arr.epoch <= request.epoch
    JOIN 
        response
        ON request.yyyymmdd = response.yyyymmdd
        AND request.city = response.city
        AND request.service = response.service
        AND request.userId = response.userId
        AND request.slotname = response.slotname
        AND request.format = response.format
        AND request.adid = response.adid
    LEFT JOIN 
        impression
        ON impression.yyyymmdd = response.yyyymmdd
        AND impression.city = response.city
        AND impression.service = response.service
        AND impression.userId = response.userId
        AND impression.slotname = response.slotname
        AND impression.format = response.format
        AND impression.adid = response.adid
    )
    GROUP BY 1,2,3,4
    ORDER BY 1,2,3,4


    -- Ads Latency Tracker - PostOrderStatus Started


    WITH service_mapping as (

    SELECT
        city_display_name city,
        service_level service        
    FROM 
        datasets.service_mapping
    WHERE 
        [[ {{city}} --]] city_display_name = 'Bangalore' 
        AND [[ {{service}} --]] service_level IN ('Link', 'Auto', 'CabEconomy')
    ),
    
    postorderstatus_started AS (
    
    SELECT
        yyyymmdd,
        hh,
        'PostOrderStatus Started' screen,
        user_id,
        -- order_id,
        epoch
    FROM
        (
        SELECT 
            *,
            ROW_NUMBER() OVER(PARTITION BY yyyymmdd, user_id, order_id ORDER BY epoch DESC) rn
        FROM
            (
            SELECT
                yyyymmdd,
                hh,
                profile_identity user_id,
                JSON_EXTRACT_SCALAR(event_props, '$.orderId') order_id,
                epoch
            FROM 
                clevertap.customer_postorderstatus_started_immutable
            WHERE 
                yyyymmdd >= date_format({{start_date}},'%Y%m%d')
                AND yyyymmdd <= date_format({{end_date}},'%Y%m%d')
                AND profile_platform = 'Android'
                AND JSON_EXTRACT_SCALAR(event_props, '$.currentCity') IN (SELECT city FROM service_mapping)
            )
        )
        WHERE 
            rn = 1
    ),
    
    request as (

    SELECT     
        DISTINCT
        yyyymmdd,
        hh hour,
        city,
        service,
        userId,
        pagename,
        eventName,
        responseType,
        pagename,
        slotname,
        format,
        adid,
        epoch 
    FROM 
        canonical.iceberg_log_telemetry_ads_impressions_immutable_full
    
    WHERE  
        yyyymmdd >= date_format({{start_date}},'%Y%m%d')
        and yyyymmdd <= date_format({{end_date}},'%Y%m%d')
        and responseType = 'GAMBanner'
        -- and userId = '659f595c6d2e761d5ded3b0e'
        and city IN (SELECT city FROM service_mapping)
        and service IN (SELECT service FROM service_mapping)
        and regexp_like(lower(slotname), 'started')
        and eventName = 'Ad_Request'
    ),
    
    response as (

    SELECT     
        DISTINCT
        yyyymmdd,
        hh hour,
        city,
        service,
        userId,
        pagename,
        eventName,
        responseType,
        pagename,
        slotname,
        format,
        adid,
        epoch 
    FROM 
        canonical.iceberg_log_telemetry_ads_impressions_immutable_full
    
    WHERE  
        yyyymmdd >= date_format({{start_date}},'%Y%m%d')
        and yyyymmdd <= date_format({{end_date}},'%Y%m%d')
        and responseType = 'GAMBanner'
        -- and userId = '659f595c6d2e761d5ded3b0e'
        and city IN (SELECT city FROM service_mapping)
        and service IN (SELECT service FROM service_mapping)
        and regexp_like(lower(slotname), 'started')
        and eventName = 'Ad_Response'
    ),
    
    impression as (

    SELECT     
        DISTINCT
        yyyymmdd,
        hh hour,
        city,
        service,
        userId,
        pagename,
        eventName,
        responseType,
        pagename,
        slotname,
        format,
        adid,
        epoch 
    FROM 
        canonical.iceberg_log_telemetry_ads_impressions_immutable_full
    
    WHERE  
        yyyymmdd >= date_format({{start_date}},'%Y%m%d')
        and yyyymmdd <= date_format({{end_date}},'%Y%m%d')
        and responseType = 'GAMBanner'
        -- and userId = '659f595c6d2e761d5ded3b0e'
        and city IN (SELECT city FROM service_mapping)
        and service IN (SELECT service FROM service_mapping)
        and regexp_like(lower(slotname), 'started')
        and eventName = 'Ad_Impression'
    )
    
    SELECT
        screen,
        slotname,
        format,
        yyyymmdd,
        count(distinct adid) ad_requests,
        approx_percentile(Req2Res_time_spent_in_sec, 0.5) Req2Res_time_spent_in_sec,
        approx_percentile(Req2Res_time_spent_in_ms, 0.5) Req2Res_time_spent_in_ms,
        approx_percentile(Res2Imp_time_spent_in_sec, 0.5) Res2Imp_time_spent_in_sec,
        approx_percentile(Res2Imp_time_spent_in_ms, 0.5) Res2Imp_time_spent_in_ms
    from 
    (
    SELECT
        screen,
        request.*,
        -- started.epoch screen_epoch,
        response.epoch response,
        impression.epoch impression,
        (response.epoch/1000.0 - request.epoch/1000.0) Req2Res_time_spent_in_sec,
        (response.epoch - request.epoch) Req2Res_time_spent_in_ms,
        (impression.epoch/1000.0 - response.epoch/1000.0) Res2Imp_time_spent_in_sec,
        (impression.epoch - response.epoch) Res2Imp_time_spent_in_ms,
        ROW_NUMBER() OVER(PARTITION BY request.adid ORDER BY response.epoch desc) rn
        
    FROM 
        postorderstatus_started started
    JOIN
        request
        ON started.yyyymmdd = request.yyyymmdd
        AND started.user_id = request.userId
        AND started.hh = request.hour
        -- AND started.epoch <= request.epoch
    JOIN 
        response
        ON request.yyyymmdd = response.yyyymmdd
        AND request.city = response.city
        AND request.service = response.service
        AND request.userId = response.userId
        AND request.slotname = response.slotname
        AND request.format = response.format
        AND request.adid = response.adid
    LEFT JOIN 
        impression
        ON impression.yyyymmdd = response.yyyymmdd
        AND impression.city = response.city
        AND impression.service = response.service
        AND impression.userId = response.userId
        AND impression.slotname = response.slotname
        AND impression.format = response.format
        AND impression.adid = response.adid
    )
    GROUP BY 1,2,3,4
    ORDER BY 1,2,3,4


    -- 