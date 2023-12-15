# Query #4

WITH player_ids AS (
    SELECT DISTINCT
        player_id,
        player_name
    FROM bootcamp.nba_game_details
)

SELECT
    pids.player_name,
    season,
    gs.points
FROM surbhiwahie.nba_players_grouping_set AS gs
LEFT JOIN player_ids AS pids
    ON gs.player_name = pids.player_name
WHERE
    gs.player_name IS NOT NULL
    AND gs.team_id IS NULL
    AND season IS NOT NULL
ORDER BY gs.points DESC
