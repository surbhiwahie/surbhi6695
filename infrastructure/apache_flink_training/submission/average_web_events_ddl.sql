-- Define a table to store the average number of web events in a session by hostname

CREATE TABLE public.average_web_events_byhost (
    host VARCHAR,
    average_events FLOAT
) WITH (
    FORMAT = 'parquet'
)
