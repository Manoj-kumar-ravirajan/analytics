With fin_tbl as(
    SELECT
        orderdate,
        city,
        CASE WHEN service = 'link' THEN 'Link' ELSE service END AS service,
        SUM(discounts + wallet_cashback) AS discounts
    FROM datasets.financial_kpi
    WHERE orderdate >= [[DATE_FORMAT({{start_date}}, '%Y-%m-%d') --]] DATE_FORMAT(DATE_ADD('week', -7, DATE_TRUNC('week', CURRENT_DATE)), '%Y-%m-%d') 
        AND orderdate <= [[DATE_FORMAT({{end_date}}, '%Y-%m-%d') --]] DATE_FORMAT(CURRENT_DATE - interval '1' day, '%Y-%m-%d')
        [[AND {{city}}]]
        [[AND {{service}}]]
        AND service IN ('Link', 'link', 'Auto')
    GROUP BY 1, 2, 3
),

coin_tbl as(
select 
    order_date,
    city_name,
    service_name,
    sum(subscription_debit) as subscription_debit,
    sum(adjustment_debit) as adjustments,
    sum(customercashback_creditcoinlocationoffer_credit + customercashback_creditcoinrideoffer_credit + scratchcard_creditcoinscratchcard_credit 
            + customercashback_credit + locationoffer_creditcoin_credit + locationoffer_creditcoinlocationoffer_credit + rideoffer_creditcoin_credit + rideoffer_creditcoinrideoffer_credit) as coin_credited,
    sum(utilization_dashboardcoinscredit + utilization_giftoffer + utilization_walletrechargeoffer) as coin_utilized,
    sum(coinexpired_locationoffer_debit + coinexpired_rideoffer_debit + coinexpired_scratchcardoffer_debit + coinexpired_debit) as coin_expired,
    sum(customerrefund_credit) as refund
from(
    select 
        date_format(date_parse(yyyymmdd, '%Y%m%d'), '%Y-%m-%d') as order_date,
        service_name, city_name,
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
        coalesce(customerrefund_credit, 0) as customerrefund_credit,
        
        coalesce(coinexpired_debit,0) as coinexpired_debit,
        coalesce(customercashback_credit,0) as customercashback_credit,
        coalesce(locationoffer_creditcoin_credit,0) as locationoffer_creditcoin_credit,
        coalesce(locationoffer_creditcoinlocationoffer_credit,0) as locationoffer_creditcoinlocationoffer_credit,
        coalesce(rideoffer_creditcoin_credit,0) as rideoffer_creditcoin_credit,
        coalesce(rideoffer_creditcoinrideoffer_credit,0) as rideoffer_creditcoinrideoffer_credit
    from datasets.customer_transactions_fin_kpi
    where yyyymmdd >= [[DATE_FORMAT({{start_date}}, '%Y%m%d') --]] DATE_FORMAT(DATE_ADD('week', -7, DATE_TRUNC('week', CURRENT_DATE)), '%Y%m%d') 
        AND yyyymmdd <= [[DATE_FORMAT({{end_date}}, '%Y%m%d') --]] DATE_FORMAT(CURRENT_DATE - interval '1' day, '%Y%m%d')
)
group by 1,2,3
),

temp_tbl as(
select 
    orderdate,
    city,
    service,
    sum(discounts + coalesce(refund,0) + (coalesce(coin_credited,0) - coalesce(coin_expired,0)) + coalesce(coin_utilized,0) - coalesce(adjustments,0)) as DiscountAndCashback
from fin_tbl
left join coin_tbl on orderdate = order_date and lower(city) = lower(city_name) and service = service_name
group by 1,2,3
order by 1,2,3
)

select  
    CONCAT('WK', '-', DATE_FORMAT(DATE_PARSE(orderdate, '%Y-%m-%d'), '%v')) AS WeekNum,
    MIN(orderdate) as StartDate,
    city AS City,
    service AS Service,
    round(sum(DiscountAndCashback)) as DiscountAndCashback
from temp_tbl
group by 1,3,4
order by 2 desc, 5 desc