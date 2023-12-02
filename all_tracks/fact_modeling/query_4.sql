with today as (

select * from surbhiwahie.user_devices_cumulated
where date = DATE('2023-01-07')

),
date_list_int as(

select 
user_id, 
browser_type,
CAST(SUM(CASE WHEN CONTAINS(dates_active,sequence_date) THEN 
pow(2,31 - DATE_DIFF('day', sequence_date, date))
else 0 
end ) as BIGINT) as  history_int

 from today
CROSS JOIN UNNEST (SEQUENCE(DATE('2022-01-01'),
DATE('2023-01-07'))) as t(sequence_date)
 group by user_id , browser_type)
 
 select *,
TO_BASE(history_int, 2) as history_in_binary
  from date_list_int
