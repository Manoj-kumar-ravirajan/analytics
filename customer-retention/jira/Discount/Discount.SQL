 with coin_tbl as
    (
    select date_trunc('week', date_parse(order_date, '%Y-%m-%d')) as Week, -- day_of_week(current_date - interval '1' day) as dow,
        -- DATE_TRUNC('month', DATE(order_date)) as order_date,
        city_name, -- service_name,
        sum(subscription_debit) as subscription_debit,
        sum(adjustment_debit) as adjustments,
        sum(customercashback_creditcoinlocationoffer_credit + customercashback_creditcoinrideoffer_credit + scratchcard_creditcoinscratchcard_credit) as coin_credited,
        sum(utilization_dashboardcoinscredit + utilization_giftoffer + utilization_referraloffer + utilization_walletrechargeoffer) as coin_utilized,
        sum(coinexpired_locationoffer_debit + coinexpired_rideoffer_debit + coinexpired_scratchcardoffer_debit) as coin_expired,
        sum(customerrefund_credit) as refund
    from
        (select date_format(date_parse(yyyymmdd, '%Y%m%d'), '%Y-%m-%d') as order_date,
                service_name,
                city_name,
                coalesce(subscription_debit, 0) as subscription_debit,
                coalesce(adjustment_debit, 0) as adjustment_debit,
                coalesce(customercashback_creditcoinlocationoffer_credit, 0) as customercashback_creditcoinlocationoffer_credit,
                coalesce(customercashback_creditcoinrideoffer_credit, 0) as customercashback_creditcoinrideoffer_credit,
                coalesce(scratchcard_creditcoinscratchcard_credit, 0) as scratchcard_creditcoinscratchcard_credit,
                coalesce(utilization_dashboardcoinscredit, 0) as utilization_dashboardcoinscredit,
                coalesce(utilization_giftoffer, 0) as utilization_giftoffer,
                coalesce(utilization_referraloffer, 0) as utilization_referraloffer,
                coalesce(utilization_walletrechargeoffer, 0) as utilization_walletrechargeoffer,
                coalesce(coinexpired_locationoffer_debit, 0) as coinexpired_locationoffer_debit,
                coalesce(coinexpired_rideoffer_debit, 0) as coinexpired_rideoffer_debit,
                coalesce(coinexpired_scratchcardoffer_debit, 0) as coinexpired_scratchcardoffer_debit,
                coalesce(customerrefund_credit, 0) as customerrefund_credit
        from datasets.customer_transactions_fin_kpi
        where yyyymmdd >= date_format(date_trunc('week', date_add('week', -4, current_date)), '%Y%m%d')
        and yyyymmdd <= date_format(current_date - interval '1' day, '%Y%m%d')
        and service_name IN ('Link')
        and city_name in ('bangalore',
                            'chandigarh',
                            'chennai',
                            'delhi',
                            'guwahati',
                            'hyderabad',
                            'jaipur',
                            'kolkata',
                            'lucknow',
                            'patna',
                            'vijayawada',
                            'vishakapatnam')
        and day_of_week(date_parse(yyyymmdd, '%Y%m%d')) <= day_of_week(current_date - interval '1' day) )
    group by 1,
            2 -- ,3
    ),

disc_ord as
  (select OrderWeek,
          City,
          net_rides,
          discount_rides,
          pass_rides,
          "Discount Rides %",
          "Pass Rides %",
          (discounts + coalesce(refund, 0) + (coalesce(coin_credited, 0) - coalesce(coin_expired, 0)) + coalesce(coin_utilized, 0) - coalesce(adjustments, 0)) / subtotal as "Discount %",
          (discounts + coalesce(refund, 0) + (coalesce(coin_credited, 0) - coalesce(coin_expired, 0)) + coalesce(coin_utilized, 0) - coalesce(adjustments, 0)) / net_rides as DPR,
          row
   from
     (select date_trunc('week', date_parse(yyyymmdd, '%Y%m%d')) as OrderWeek,
             city_name as City,
             count(distinct order_id) as net_rides,
             count(distinct case  when discount > 0 then order_id end) as discount_rides,
             count(distinct case when offer_details_offer_type = 'subscription' then order_id end) as pass_rides,
             cast(count(distinct case when discount > 0 then order_id end) as double) / count(distinct order_id) as "Discount Rides %",
             cast(count(distinct case when offer_details_offer_type = 'subscription' then order_id end) as double) / count(distinct order_id) as "Pass Rides %", 
             -- cast(sum(case when discount > 0 then discount end) as double) / sum(sub_total) as "Discount %",
            cast(sum(case when discount > 0 then discount  end) as double) as discounts,
            sum(sub_total) as subtotal, -- cast(sum(case when discount > 0 then discount end) as double) / count(distinct order_id) as DPR,
            rank() over( order by date_trunc('week', date_parse(yyyymmdd, '%Y%m%d')) desc) as row
      from orders.order_logs_snapshot
      where yyyymmdd >= date_format(date_trunc('week', date_add('week', -4, current_date)), '%Y%m%d')
        and yyyymmdd <= date_format(current_date - interval '1' day, '%Y%m%d')
        and order_status = 'dropped'
        and order_type = 'app'
        and day_of_week(date_parse(yyyymmdd, '%Y%m%d')) <= day_of_week(current_date - interval '1' day)
        and city_name in ('Bangalore',
                          'Chandigarh',
                          'Chennai',
                          'Delhi',
                          'Guwahati',
                          'Hyderabad',
                          'Jaipur',
                          'Kolkata',
                          'Lucknow',
                          'Patna',
                          'Vijayawada',
                          'Vishakapatnam')
      group by 1,
               2) ord
   join coin_tbl on ord.OrderWeek = coin_tbl.Week and lower(ord.City) = lower(coin_tbl.city_name)),




disc_pass_rides as
  (select date_format(OrderWeek, '%M %d, %Y') || ' - ' || date_format(date_add('day', day_of_week(current_date - interval '2' day), OrderWeek), '%M %d, %Y') as OrderWeek,
          City,
          "Net Rides",
          "Discount Rides %",
          "Disc Rides % Compare to Benchmark",
          "Pass Rides %",
          "Pass Rides % Compare to Benchmark",
          "Discount %",
          "Disc % Compare to Benchmark",
          DPR,
          "DPR Compare to Benchmark"
   from
     (select OrderWeek,
             City,
             row,
             net_rides as "Net Rides",
             Discount_rides as "Discount Rides",
             cast(round("Discount Rides %"*100.00, 1) as varchar) || '%' as "Discount Rides %",
             cast(round("Pass Rides %"*100.00, 1) as varchar) || '%' as "Pass Rides %",
             "Discount Rides %" - avg(case
                                          when row >1 then "Discount Rides %"
                                      end) over(partition by city
                                                order by OrderWeek) as "Disc Rides % Compare to Benchmark",
             "Pass Rides %" - avg(case
                                      when row >1 then "Pass Rides %"
                                  end) over(partition by city
                                            order by OrderWeek) as "Pass Rides % Compare to Benchmark",
             cast(round("Discount %"*100.00, 1) as varchar) || '%' as "Discount %",
             DPR,
             "Discount %" - avg(case
                                    when row >1 then cast("Discount %" as double)
                                end) over(partition by city
                                          order by OrderWeek) as "Disc % Compare to Benchmark",
             (DPR - avg(case
                            when row >1 then DPR
                        end) over(partition by city
                                  order by OrderWeek))/avg(case
                                                               when row >1 then DPR
                                                           end) over(partition by city
                                                                     order by OrderWeek) as "DPR Compare to Benchmark"
      from disc_ord)
   where row = 1
   order by 1)
select OrderWeek as Week,
       disc_pass_rides.City,
       "Net Rides",
       "Discount %",
       "Disc % Compare to Benchmark",
       DPR,
       "DPR Compare to Benchmark",
       "Discount Rides %",
       "Disc Rides % Compare to Benchmark",
       "Pass Rides %" -- , "Pass Rides % Compare to Benchmark"
from -- discount
 -- join
 disc_pass_rides -- on discount.week = disc_pass_rides.OrderWeek and lower(discount.city) = lower(disc_pass_rides.city)
order by 2