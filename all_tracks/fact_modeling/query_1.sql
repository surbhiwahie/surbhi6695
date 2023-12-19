WITH deduplicated_data AS (
    SELECT game_id, team_id, player_id,
           ROW_NUMBER() OVER (PARTITION BY game_id, team_id, player_id ORDER BY game_id) AS row_num
    FROM bootcamp.nba_game_details
)
SELECT game_id, team_id, player_id
FROM deduplicated_data
WHERE row_num = 1
