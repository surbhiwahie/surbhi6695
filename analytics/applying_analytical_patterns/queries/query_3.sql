WITH
teams AS (
    SELECT DISTINCT
        player_id,
        player_name,
        team_id,
        team_abbreviation,
        team_city
    FROM bootcamp.nba_game_details
)

SELECT
    pids.player_name,
    pids.team_abbreviation,
    gs.points
FROM surbhiwahie.nba_players_grouping_set AS gs
LEFT JOIN teams AS pids
    ON gs.player_name = pids.player_name
    and gs.team_id = pids.team_id
WHERE
    gs.player_name IS NOT NULL
    AND gs.team_id IS NOT NULL
    AND season IS NULL
ORDER BY gs.points DESC

--# LeBron James	CLE	28314
