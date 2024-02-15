-- Postgres
SELECT *
FROM average_web_events_byhost
WHERE host IN (
               'zachwilson.techcreator.io', 'zachwilson.tech', 'lulu.techcreator.io'
    )
ORDER BY average_events DESC 
