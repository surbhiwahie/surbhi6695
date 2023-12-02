Create table surbhiwahie.daily_web_metrics
STEP#1:

create table surbhiwahie.daily_web_metrics (
    host VARCHAR,
    metric_name VARCHAR,
    metric_value INTEGER,
    date date
)
WITH
  (
    FORMAT = 'PARQUET',
    partitioning = ARRAY['metric_name', 'date']
  )


STEP#2: Now Insert data into the table daily_web_metrics

insert into surbhiwahie.daily_web_metrics
Select 
host ,
'visited_home_page' as metric_name,
COUNT(CASE WHEN url= '/' THEN 1 END ) as metric_value,
 CAST(event_time AS DATE) as date
From bootcamp.web_events
GROUP BY 
user_id, CAST(event_time AS DATE)



Also, insert some more value eg.: sign up page

insert into surbhiwahie.daily_web_metrics
Select 
host ,
'visited_signup' as metric_name,
COUNT(CASE WHEN url= '/signup' THEN 1 END ) as metric_value,
 CAST(event_time AS DATE) as date
From bootcamp.web_events
GROUP BY 
user_id, CAST(event_time AS DATE)
order by metric_value desc



--------------------------------------------------

INSERT INTO   surbhiwahie.host_activity_reduced
WITH yesterday as (
Select * from surbhiwahie.host_activity_reduced
Where month_start = DATE('2023-08-01')

),
today as (
Select * from surbhiwahie.daily_web_metrics
WHERE date = DATE('2023-08-02')
)

Select 
COALESCE(t.host, y.host) as host,
COALESCE(t.metric_name, y.metric_name) as 
metric_name,
  COALESCE(
    y.metric_array,
    REPEAT(
      NULL,
      CAST(
        DATE_DIFF('day', DATE('2023-08-01'), t.date) AS INTEGER
      )
    )
  ) || ARRAY[t.metric_value] AS metric_array
,
CAST('2023-08-01' as date) AS month_start
from today t 
FULL OUTER JOIN yesterday y 
On t.host = y.host
And t.metric_name = y.metric_name


-------------------------------------------

With aggregated as(
Select 
host,
metric_name,
month_start,
ARRAY[SUM(metric_array[1]), SUM(metric_array[2]) , SUM(metric_array[3]) ]
 as agg_array,
From surbhiwahie.host_activity_reduced
Group by 1,2,3
)


Select 
host,
metric_name,
DATE_ADD('day',  index - 1, DATE(month_start)) as date,
value
from aggregated 
CROSS JOIN UNNEST (agg_array) with ORDINALITY as t(value, index)
Limit 100




