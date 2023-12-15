-- Inserting into the table tracking the average number of hits per 5 minute session
INSERT INTO surbhiwahie.average_web_events_byhost
select host,
       AVG(num_hits) as avg_hits_per_5min_session
FROM surbhiwahie.processed_events_aggregated_hw
group by host
