with base as 
(
select
    week(cast(run_date as date))+1 week_number,
    customer_id,
    taxi_l1_segment l1_segments,
    taxi_lifetime_stage
from
    datasets.iallocator_customer_segments
where
    run_date = date_format( {{start_date}} -interval '1' day,'%Y-%m-%d') 
    and taxi_lifetime_rides >0
    and taxi_lifetime_last_ride_city = 'Kolkata'
    --AND taxi_lifetime_last_ride_city in ('Kolkata','Jaipur','Hyderabad')
    
),

coin_credited AS
(
SELECT entity_id orderid,
      concat(owner_id, '-', order_unique_id) unique_id,
      SUM(CASE WHEN cast(offer_type AS varchar) != 'scratchCardOffer' THEN cast(amount AS INT) END) custCashBackAmount
FROM
 (SELECT * FROM
    (SELECT id,
            yyyymmdd,
            coin_wallet_amount,
            transaction_type,
            subtype,
            entity_id,
            owner_id,
            order_unique_id,
            coin_wallet_changes,
            coin_wallet_offer_type,
            subsub_type
     FROM payments.transactions_snapshot
     WHERE yyyymmdd BETWEEN date_format({{start_date}}, '%Y%m%d') AND date_format({{end_date}}, '%Y%m%d')
       AND owner_type = 'customer'
       AND transaction_status = 'done'
       AND cardinality(coin_id) != 0 )
  CROSS JOIN unnest(coin_wallet_offer_type, coin_wallet_amount) AS tbl1(offer_type, amount)
  )
WHERE transaction_type = 'credit'
    AND subtype IN ('locationOffer', 'rideOffer', 'customerCashBack', 'scratchCard')
    AND cast(offer_type AS varchar) IN ('locationOffer', 'rideOffer', 'scratchCardOffer')
GROUP BY 1, 2
),
    
coin_expired AS
(
SELECT entity_id orderid,
      concat(owner_id, '-', order_unique_id) unique_id,
      SUM(CASE WHEN cast(offer_type AS varchar) != 'scratchCardOffer' THEN cast(amount AS INT) END) custexpiryAmount
FROM
 (SELECT * FROM
    (SELECT id,
            yyyymmdd,
            coin_wallet_amount,
            transaction_type,
            subtype,
            entity_id,
            owner_id,
            order_unique_id,
            coin_wallet_changes,
            coin_wallet_offer_type,
            subsub_type
     FROM payments.transactions_snapshot
     WHERE --yyyymmdd BETWEEN date_format({{start_date}}, '%Y%m%d') AND date_format({{end_date}}, '%Y%m%d')
       yyyymmdd BETWEEN date_format({{start_date}}, '%Y%m%d') AND date_format({{end_date}} + interval '21' day, '%Y%m%d')
       
       AND owner_type = 'customer'
       AND transaction_status = 'done'
       AND cardinality(coin_id) != 0 )
  CROSS JOIN unnest(coin_wallet_offer_type, coin_wallet_amount) AS tbl1(offer_type, amount)
  )
WHERE transaction_type = 'debit' AND subtype = 'coinExpired'
    AND CAST(offer_type AS varchar) IN ('locationOffer', 'rideOffer', 'scratchCardOffer')
GROUP BY 1, 2
),
    
transaction_query AS
(   
SELECT 
    a.orderid,
    a.unique_id,
    coalesce(a.custCashBackAmount,0) custCashBackAmount,
    coalesce(b.custexpiryAmount,0) custexpiryAmount
FROM coin_credited a
LEFT JOIN coin_expired b on a.orderid = b.orderid 
),

orders_1 AS
    (SELECT week(cast(date_parse(yyyymmdd, '%Y%m%d') AS date)) week_number,
          date_format(date_parse(yyyymmdd, '%Y%m%d'), '%Y-%m-%d') order_date,
          customer_id,
          order_id order_id,
        --   city_name,
          service_obj_service_name service_name,
          cast(discount AS INT) discount
    FROM "orders"."order_logs_snapshot"
    WHERE yyyymmdd BETWEEN date_format({{start_date}}, '%Y%m%d') AND date_format({{end_date}}, '%Y%m%d')
        AND order_status = 'dropped'
        AND service_obj_service_name IN ('Auto','Link')
        AND (spd_fraud_flag != true OR spd_fraud_flag IS NULL)
        AND city_name = 'Kolkata'
        --AND city_name in ('Kolkata','Jaipur','Hyderabad')
        
    ),
orders_cb AS
(
SELECT a.*,
      coalesce(b.custCashBackAmount, 0) AS custCashBackAmount,
      coalesce(b.custexpiryAmount, 0) AS custexpiryAmount,
      coalesce(b.custCashBackAmount - b.custexpiryAmount, 0) AS coin_utilization
FROM orders_1 a
LEFT JOIN transaction_query b on a.order_id = b.orderid
),

discount_view AS
(
SELECT week_number,customer_id, order_id,--city_name,
    service_name, discount+coin_utilization as discount_amount
FROM orders_cb
),

order_raw as (
    SELECT
    -- s.city_name,
    s.customer_id,
    date_trunc('week', date_parse(yyyymmdd ,'%Y%m%d')) as week_start_date,
    
        --NET ORDERS
        count(distinct case when service_obj_service_name = 'Link'  and (offer_details_offer_type != 'subscription' or offer_details_offer_type is null) then s.order_id end) net_orders_link,
        count(distinct case when service_obj_service_name = 'Auto' and (offer_details_offer_type != 'subscription' or offer_details_offer_type is null) then s.order_id end) net_orders_auto,
        count(distinct case when service_obj_service_name IN ('Link','Auto')  and (offer_details_offer_type != 'subscription' or offer_details_offer_type is null) then s.order_id end) net_orders_taxi,
        count(distinct case when service_obj_service_name = 'Auto' and offer_details_offer_type = 'subscription' then s.order_id end) net_orders_auto_subs,
        count(distinct case when service_obj_service_name = 'Link' and offer_details_offer_type = 'subscription' then s.order_id end) net_orders_link_subs,
        count(distinct case when service_obj_service_name IN ('Link','Auto') and offer_details_offer_type = 'subscription' then s.order_id end) taxi_subs_rides,
        
        --DISCOUNTED RIDS 
        count(distinct case when service_obj_service_name = 'Link'  and (offer_details_offer_type != 'subscription' or offer_details_offer_type is null) and discount_amount>0 then s.order_id end) disc_net_orders_link,
        count(distinct case when service_obj_service_name = 'Auto'  and (offer_details_offer_type != 'subscription' or offer_details_offer_type is null) and discount_amount>0 then s.order_id end) disc_net_orders_auto,
        count(distinct case when service_obj_service_name IN ('Auto','Link')  and (offer_details_offer_type != 'subscription' or offer_details_offer_type is null) and discount_amount>0 then s.order_id end) disc_net_orders_taxi,
        
        --RIDES NON-SUBS
        count(distinct case when service_obj_service_name = 'Link' and distance_final_distance <= 6 and (offer_details_offer_type != 'subscription' or offer_details_offer_type is null) then s.order_id end) link_short_rides,
        count(distinct case when service_obj_service_name = 'Link' and distance_final_distance >6   and (offer_details_offer_type != 'subscription' or offer_details_offer_type is null) then s.order_id end) link_long_rides,
        count(distinct case when service_obj_service_name = 'Auto' and distance_final_distance <= 6 and (offer_details_offer_type != 'subscription' or offer_details_offer_type is null) then s.order_id end) auto_short_rides,
        count(distinct case when service_obj_service_name = 'Auto' and distance_final_distance >6   and (offer_details_offer_type != 'subscription' or offer_details_offer_type is null) then s.order_id end) auto_long_rides,
        count(distinct case when distance_final_distance >6   and (offer_details_offer_type != 'subscription' or offer_details_offer_type is null) then s.order_id end) taxi_long_rides,
        count(distinct case when distance_final_distance <=6  and (offer_details_offer_type != 'subscription' or offer_details_offer_type is null) then s.order_id end) taxi_short_rides,
        
        --DISCOUNT NON-SUBS
        sum( case when service_obj_service_name = 'Link' and distance_final_distance <= 6  and (offer_details_offer_type != 'subscription' or offer_details_offer_type is null) then discount_amount end) link_short_discount,
        sum( case when service_obj_service_name = 'Link' and distance_final_distance >6    and (offer_details_offer_type != 'subscription' or offer_details_offer_type is null) then discount_amount end) link_long_discount,
        sum( case when service_obj_service_name = 'Auto' and distance_final_distance <= 6  and (offer_details_offer_type != 'subscription' or offer_details_offer_type is null) then discount_amount end) auto_short_discount,
        sum( case when service_obj_service_name = 'Auto' and distance_final_distance >6    and (offer_details_offer_type != 'subscription' or offer_details_offer_type is null) then discount_amount end) auto_long_discount,
        sum( case when distance_final_distance >6  and (offer_details_offer_type != 'subscription' or offer_details_offer_type is null) then discount_amount end) taxi_long_discount,
        sum( case when distance_final_distance <=6 and (offer_details_offer_type != 'subscription' or offer_details_offer_type is null)  then discount_amount end) taxi_short_discount,
        sum( case when service_obj_service_name = 'Link'  and (offer_details_offer_type != 'subscription' or offer_details_offer_type is null) then discount_amount end) discount_link,
        sum( case when service_obj_service_name = 'Auto' and (offer_details_offer_type != 'subscription' or offer_details_offer_type is null) then discount_amount end) discount_auto,
        sum( case when service_obj_service_name IN ('Auto','Link') and (offer_details_offer_type != 'subscription' or offer_details_offer_type is null) then discount_amount end) discount_taxi,
        
        --DISCOUNT SUBS
        sum( case when service_obj_service_name = 'Link' and distance_final_distance <= 6  and offer_details_offer_type = 'subscription' then discount_amount end) link_subs_short_discount,
        sum( case when service_obj_service_name = 'Link' and distance_final_distance >6    and offer_details_offer_type = 'subscription' then discount_amount end) link_subs_long_discount,
        sum( case when service_obj_service_name = 'Auto' and distance_final_distance <= 6  and offer_details_offer_type = 'subscription' then discount_amount end) auto_subs_short_discount,
        sum( case when service_obj_service_name = 'Auto' and distance_final_distance >6    and offer_details_offer_type = 'subscription' then discount_amount end) auto_subs_long_discount,
        sum( case when distance_final_distance <= 6  and offer_details_offer_type = 'subscription' then discount_amount end) taxi_subs_short_discount,
        sum( case when distance_final_distance >6    and offer_details_offer_type = 'subscription' then discount_amount end) taxi_subs_long_discount,
        sum( case when service_obj_service_name = 'Link' and offer_details_offer_type = 'subscription' then discount_amount end) link_subs_discount,
        sum( case when service_obj_service_name = 'Auto' and offer_details_offer_type = 'subscription' then discount_amount end) auto_subs_discount,
        sum( case when offer_details_offer_type = 'subscription' then discount_amount end) taxi_subs_discount,
        
        --SUB-TOTAL NON-SUBS
        sum( case when service_obj_service_name = 'Link' and distance_final_distance <= 6  and (offer_details_offer_type != 'subscription' or offer_details_offer_type is null) then sub_total end) link_short_subtotal,
        sum( case when service_obj_service_name = 'Link' and distance_final_distance >6    and (offer_details_offer_type != 'subscription' or offer_details_offer_type is null) then sub_total end) link_long_subtotal,
        sum( case when service_obj_service_name = 'Auto' and distance_final_distance <= 6  and (offer_details_offer_type != 'subscription' or offer_details_offer_type is null) then sub_total end) auto_short_subtotal,
        sum( case when service_obj_service_name = 'Auto' and distance_final_distance >6    and (offer_details_offer_type != 'subscription' or offer_details_offer_type is null) then sub_total end) auto_long_subtotal,
        sum( case when distance_final_distance >6   and (offer_details_offer_type != 'subscription' or offer_details_offer_type is null) then sub_total end) taxi_long_subtotal,
        sum( case when distance_final_distance <=6  and (offer_details_offer_type != 'subscription' or offer_details_offer_type is null) then sub_total end) taxi_short_subtotal,
        sum( case when service_obj_service_name = 'Link' and (offer_details_offer_type != 'subscription' or offer_details_offer_type is null) then sub_total end) link_subtotal,
        sum( case when service_obj_service_name = 'Auto' and (offer_details_offer_type != 'subscription' or offer_details_offer_type is null) then sub_total end) auto_subtotal,
        sum( case when service_obj_service_name IN ('Auto','Link') and (offer_details_offer_type != 'subscription' or offer_details_offer_type is null) then sub_total end) taxi_subtotal,
        
        --SUB-TOTAL SUBS
        sum( case when service_obj_service_name = 'Link' and distance_final_distance <= 6  and offer_details_offer_type = 'subscription' then sub_total end) link_subs_short_subtotal,
        sum( case when service_obj_service_name = 'Link' and distance_final_distance >6    and offer_details_offer_type = 'subscription' then sub_total end) link_subs_long_subtotal,
        sum( case when service_obj_service_name = 'Auto' and distance_final_distance <= 6  and offer_details_offer_type = 'subscription' then sub_total end) auto_subs_short_subtotal,
        sum( case when service_obj_service_name = 'Auto' and distance_final_distance >6    and offer_details_offer_type = 'subscription' then sub_total end) auto_subs_long_subtotal,
        sum( case when distance_final_distance >6   and offer_details_offer_type = 'subscription' then sub_total end) taxi_subs_long_subtotal,
        sum( case when distance_final_distance <=6  and offer_details_offer_type = 'subscription' then sub_total end) taxi_subs_short_subtotal,
        sum( case when service_obj_service_name = 'Link' and offer_details_offer_type = 'subscription' then sub_total end) link_subs_subtotal,
        sum( case when service_obj_service_name = 'Auto' and offer_details_offer_type = 'subscription' then sub_total end) auto_subs_subtotal,
        sum( case when service_obj_service_name IN ('Auto','Link') and offer_details_offer_type = 'subscription' then sub_total end) taxi_subs_subtotal
        
    FROM orders.order_logs_snapshot s
    LEFT JOIN discount_view d ON s.customer_id = d.customer_id AND s.order_id = d.order_id --AND s.city_name = d.city_name 
    AND s. service_obj_service_name = d.service_name
    WHERE 
    yyyymmdd >= date_format({{start_date}},'%Y%m%d') AND yyyymmdd <= date_format({{end_date}},'%Y%m%d') 
    AND (spd_fraud_flag != true OR spd_fraud_flag is null)
    and service_obj_service_name in ('Auto','Link')
    and order_status = 'dropped'
    -- and s.city_name = 'Bhopal'
    AND s.city_name = 'Kolkata'
    --AND s.city_name in ('Kolkata','Jaipur','Hyderabad')
    group by 1,2

),

final_view as
(
select 
    week_number,
    case when taxi_lifetime_stage in ('HANDHOLDING','INACTIVE','DORMANT') then taxi_lifetime_stage else l1_segments end l1_segments,
    taxi_lifetime_stage, 
        
        sum(net_orders_link) link_rides,
        sum(net_orders_auto) auto_rides,
        sum(net_orders_taxi) taxi_rides,
        
        sum(net_orders_link_subs) link_subs_ride,
        sum(net_orders_auto_subs) auto_subs_ride,
        sum(taxi_subs_rides) taxi_subs_rides,
        -- sum(disc_auto_subs) auto_subs_burn,
        -- sum(disc_link_subs) link_subs_burn,
        
        sum(link_short_rides) link_short_rides,
        sum(link_long_rides) link_long_rides,
        sum(auto_short_rides) auto_short_rides,
        sum(auto_long_rides) auto_long_rides,
        sum(taxi_short_rides) taxi_short_rides,
        sum(taxi_long_rides) taxi_long_rides,
        --Discount
        sum(discount_link) link_discount,
        sum(discount_auto) auto_discount,
        sum(discount_taxi) taxi_discount,
        sum(link_short_discount) link_short_discount,
        sum(link_long_discount) link_long_discount,
        sum(auto_short_discount) auto_short_discount,
        sum(auto_long_discount) auto_long_discount,
        sum(taxi_short_discount) taxi_short_discount,
        sum(taxi_long_discount) taxi_long_discount,
        sum(link_subs_discount) link_subs_discount,
        sum(auto_subs_discount) auto_subs_discount,
        sum(taxi_subs_discount) taxi_subs_discount,
        sum(link_subs_short_discount) link_subs_short_discount,
        sum(link_subs_long_discount) link_subs_long_discount,
        sum(auto_subs_short_discount) auto_subs_short_discount,
        sum(auto_subs_long_discount) auto_subs_long_discount,
        sum(taxi_subs_short_discount) taxi_subs_short_discount,
        sum(taxi_subs_long_discount) taxi_subs_long_discount,
        --Subtotal
        sum(link_subtotal) link_subtotal,
        sum(auto_subtotal) auto_subtotal,
        sum(taxi_subtotal) taxi_subtotal,
        sum(link_short_subtotal) link_short_subtotal,
        sum(link_long_subtotal) link_long_subtotal,
        sum(auto_short_subtotal) auto_short_subtotal,
        sum(auto_long_subtotal) auto_long_subtotal,
        sum(taxi_short_subtotal) taxi_short_subtotal,
        sum(taxi_long_subtotal) taxi_long_subtotal,
        sum(link_subs_subtotal) link_subs_subtotal,
        sum(auto_subs_subtotal) auto_subs_subtotal,
        sum(taxi_subs_subtotal) taxi_subs_subtotal,
        sum(link_subs_short_subtotal) link_subs_short_subtotal,
        sum(link_subs_long_subtotal) link_subs_long_subtotal,
        sum(auto_subs_short_subtotal) auto_subs_short_subtotal,
        sum(auto_subs_long_subtotal) auto_subs_long_subtotal,
        sum(taxi_subs_short_subtotal) taxi_subs_short_subtotal,
        sum(taxi_subs_long_subtotal) taxi_subs_long_subtotal,
        
        
        sum(case 
            when  l1_segments like '%BOTH%' and  l1_segments like '%SHORT%' then taxi_short_rides
            when  l1_segments like '%BOTH%' and  l1_segments like '%LONG%' then taxi_long_rides 
            when  l1_segments like '%LINK%' and  l1_segments like '%SHORT%' then link_short_rides
            when  l1_segments like '%LINK%' and  l1_segments like '%LONG%' then link_long_rides
            when  l1_segments like '%AUTO%' and  l1_segments like '%SHORT%' then auto_short_rides
            when  l1_segments like '%AUTO%' and  l1_segments like '%LONG%' then auto_long_rides
        ELSE  net_orders_auto + net_orders_link
        end) 
        primary_needs,
    
        sum(case 
            when  l1_segments like '%BOTH%' and  l1_segments like '%SHORT%' then taxi_short_discount
            when  l1_segments like '%BOTH%' and  l1_segments like '%LONG%' then taxi_long_discount 
            when  l1_segments like '%LINK%' and  l1_segments like '%SHORT%' then link_short_discount
            when  l1_segments like '%LINK%' and  l1_segments like '%LONG%' then link_long_discount
            when  l1_segments like '%AUTO%' and  l1_segments like '%SHORT%' then auto_short_discount
            when  l1_segments like '%AUTO%' and  l1_segments like '%LONG%' then auto_long_discount
        ELSE  discount_auto + discount_link
        end) primary_needs_burn,
        
        
        sum(case 
            when  l1_segments like '%BOTH%' and  l1_segments like '%SHORT%' then taxi_short_subtotal
            when  l1_segments like '%BOTH%' and  l1_segments like '%LONG%' then taxi_long_subtotal 
            when  l1_segments like '%LINK%' and  l1_segments like '%SHORT%' then link_short_subtotal
            when  l1_segments like '%LINK%' and  l1_segments like '%LONG%' then link_long_subtotal
            when  l1_segments like '%AUTO%' and  l1_segments like '%SHORT%' then auto_short_subtotal
            when  l1_segments like '%AUTO%' and  l1_segments like '%LONG%' then auto_long_subtotal
        ELSE  auto_subtotal + link_subtotal
        end) primary_needs_subtotal
        
        

from base right join order_raw 
on order_raw.customer_id = base.customer_id
group by 1,2,3 

),

view as
(
SELECT 

*,
    case 
        when  l1_segments like '%BOTH%' and  l1_segments like '%SHORT%' then taxi_short_rides
        when  l1_segments like '%BOTH%' and  l1_segments like '%LONG%' then taxi_long_rides 
        when  l1_segments like '%LINK%' and  l1_segments like '%SHORT%' then link_short_rides
        when  l1_segments like '%LINK%' and  l1_segments like '%LONG%' then link_long_rides
        when  l1_segments like '%AUTO%' and  l1_segments like '%SHORT%' then auto_short_rides
        when  l1_segments like '%AUTO%' and  l1_segments like '%LONG%' then auto_long_rides
    ELSE  taxi_rides
    end primary_needs,

    taxi_rides - (case 
        when  l1_segments like '%BOTH%' and  l1_segments like '%SHORT%' then taxi_short_rides
        when  l1_segments like '%BOTH%' and  l1_segments like '%LONG%' then taxi_long_rides 
        when  l1_segments like '%LINK%' and  l1_segments like '%SHORT%' then link_short_rides
        when  l1_segments like '%LINK%' and  l1_segments like '%LONG%' then link_long_rides
        when  l1_segments like '%AUTO%' and  l1_segments like '%SHORT%' then auto_short_rides
        when  l1_segments like '%AUTO%' and  l1_segments like '%LONG%' then auto_long_rides
    ELSE  taxi_rides
    end) secondary_needs,

    case 
        when  l1_segments like '%BOTH%' and  l1_segments like '%SHORT%' then taxi_short_discount
        when  l1_segments like '%BOTH%' and  l1_segments like '%LONG%' then taxi_long_discount 
        when  l1_segments like '%LINK%' and  l1_segments like '%SHORT%' then link_short_discount
        when  l1_segments like '%LINK%' and  l1_segments like '%LONG%' then link_long_discount
        when  l1_segments like '%AUTO%' and  l1_segments like '%SHORT%' then auto_short_discount
        when  l1_segments like '%AUTO%' and  l1_segments like '%LONG%' then auto_long_discount
    ELSE  taxi_discount
    end primary_needs_burn,
    
    taxi_discount - (case 
        when  l1_segments like '%BOTH%' and  l1_segments like '%SHORT%' then taxi_short_discount
        when  l1_segments like '%BOTH%' and  l1_segments like '%LONG%' then taxi_long_discount 
        when  l1_segments like '%LINK%' and  l1_segments like '%SHORT%' then link_short_discount
        when  l1_segments like '%LINK%' and  l1_segments like '%LONG%' then link_long_discount
        when  l1_segments like '%AUTO%' and  l1_segments like '%SHORT%' then auto_short_discount
        when  l1_segments like '%AUTO%' and  l1_segments like '%LONG%' then auto_long_discount
    ELSE  taxi_discount
    end) secondary_needs_burn,
    
    case 
        when  l1_segments like '%BOTH%' and  l1_segments like '%SHORT%' then taxi_short_subtotal
        when  l1_segments like '%BOTH%' and  l1_segments like '%LONG%' then taxi_long_subtotal 
        when  l1_segments like '%LINK%' and  l1_segments like '%SHORT%' then link_short_subtotal
        when  l1_segments like '%LINK%' and  l1_segments like '%LONG%' then link_long_subtotal
        when  l1_segments like '%AUTO%' and  l1_segments like '%SHORT%' then auto_short_subtotal
        when  l1_segments like '%AUTO%' and  l1_segments like '%LONG%' then auto_long_subtotal
    ELSE  taxi_subtotal
    end primary_needs_subtotal,
    
    taxi_subtotal - (case 
        when  l1_segments like '%BOTH%' and  l1_segments like '%SHORT%' then taxi_short_subtotal
        when  l1_segments like '%BOTH%' and  l1_segments like '%LONG%' then taxi_long_subtotal 
        when  l1_segments like '%LINK%' and  l1_segments like '%SHORT%' then link_short_subtotal
        when  l1_segments like '%LINK%' and  l1_segments like '%LONG%' then link_long_subtotal
        when  l1_segments like '%AUTO%' and  l1_segments like '%SHORT%' then auto_short_subtotal
        when  l1_segments like '%AUTO%' and  l1_segments like '%LONG%' then auto_long_subtotal
    ELSE  taxi_subtotal
    end) secondary_needs_subtotal

FROM final_view
order by 1,2,3
)
SELECT *
FROM view 