SELECT 
    city_id, 
    city_display_name AS city_name,
    service_level,
    service_id
FROM datasets.service_mapping  
WHERE [[{{city_display_name}}]] AND  [[{{service_level}}]]
-- service_level in ('Link','Auto')
