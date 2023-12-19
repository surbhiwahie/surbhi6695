
CREATE TABLE
  surbhiwahie.host_activity_reduced (
    host VARCHAR,
    metric_name VARCHAR,
    metric_array ARRAY(INTEGER),
    month_start DATE
  )
WITH
  (
    FORMAT = 'PARQUET',
    partitioning = ARRAY['metric_name', 'month_start']
  )
