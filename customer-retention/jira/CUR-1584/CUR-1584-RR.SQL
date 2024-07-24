SELECT  
      day,
      customerid, 
      COUNT(customerid) AS customers
FROM  datasets.customer_rf_daily_kpi
WHERE  
    day between date_format({{offer_start_date}}, '%Y-%m-%d') AND date_format({{offer_end_date}}, '%Y-%m-%d')
    AND service_name in ('Link','Auto')
GROUP BY 1,2
HAVING sum(rr_sessions_unique_daily) > 0