INSERT INTO surbhiwahie.hosts_cumulated
SELECT
    host,
    ARRAY_AGG(DISTINCT CAST(event_time AS DATE)) AS host_activity_datelist,
    CURRENT_DATE AS date
FROM
    bootcamp.web_events
GROUP BY
    host

