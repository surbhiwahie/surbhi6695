
CREATE TABLE
  surbhiwahie.user_devices_cumulated (
    user_id BIGINT,
    browser_type VARCHAR,
    dates_active ARRAY(DATE),
    date DATE
  )
WITH
  (FORMAT = 'PARQUET', partitioning = ARRAY['date'])
