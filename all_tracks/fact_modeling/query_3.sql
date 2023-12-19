INSERT INTO surbhiwahie.user_devices_cumulated

with yesterday  as (

select * from surbhiwahie.user_devices_cumulated 
where date = DATE('2022-12-31')
),
today as (

select 
user_id, 
CAST(date_trunc('day', event_time) as DATE) as event_date,
device_id,
count(1)   from bootcamp.web_events
where date_trunc('day', event_time) = 
DATE('2023-01-01')
group by 
user_id, 
CAST(date_trunc('day', event_time) as DATE) ,
device_id

)
select 
COALESCE(y.user_id, t.user_id) as user_id,
d.browser_type,
CASE WHEN  dates_active is NOT NULL THEN ARRAY[t.event_date] || y.dates_active
ELSE ARRAY[t.event_date] END
 as dates_active,
 DATE('2023-01-01') as date
from yesterday y full outer join  today t
on y.user_id = t.user_id
left join bootcamp.devices d 
on d.device_id = t.device_id
