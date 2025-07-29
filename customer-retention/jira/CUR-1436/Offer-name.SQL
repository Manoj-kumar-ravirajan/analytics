offer_details as 
    (select 
       yyyymmdd, 
        startdate, endDate, 
        userselectors, 
        id, 
        offer_name,
        description
    from 
    (
    select 
        yyyymmdd, 
        startdate, endDate, 
        userselectors, 
        id, 
        NAME AS offer_name,
        description,
        row_number() over(partition by id order by yyyymmdd desc) offer_rnk
    from 
        raw.kafka_offers_immutable 
    where
        yyyymmdd between date_format({{start_date}} - interval '14' day, '%Y%m%d') and date_format({{endDate}}, '%Y%m%d')
    )
    where offer_rnk = 1
)