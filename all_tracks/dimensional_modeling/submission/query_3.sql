CREATE TABLE surbhiwahie.actors_history_scd(
actor VARCHAR,
is_active BOOLEAN,
start_date INTEGER,
end_date INTEGER,
current_year INTEGER
)

WITH (
format = 'PARQUET',
partitioning = ARRAY['current_year']
)
