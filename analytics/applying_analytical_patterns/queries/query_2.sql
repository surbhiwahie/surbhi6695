 
CREATE TABLE surbhiwahie.nba_players_grouping_set (
 player_name VARCHAR,
 team_id BIGINT,
season INTEGER, 
points DOUBLE,
 wins Integer)
 
WITH (FORMAT = 'parquet')


-------------------------------------------------------

WITH games AS (
    SELECT  
    distinct
    season, 
    game_id, 
        CASE
            WHEN home_team_wins = 1 THEN home_team_id
            ELSE visitor_team_id
        END AS winning_team
    FROM bootcamp.nba_games
),
game_details AS (
  
  SELECT *,
    CASE
      WHEN (ROW_NUMBER() OVER (PARTITION BY game_id, team_id, player_id)) = 1 THEN 1
      ELSE 0
    END as row_valid
  FROM bootcamp.nba_game_details
)

SELECT gd.player_name, gd.team_id, g.season,
	SUM(gd.pts) as points,
	COUNT(CASE
			WHEN g.winning_team = gd.team_id THEN 1
		END) as wins
FROM game_details gd
INNER JOIN games g 
	ON gd.game_id = g.game_id
	where gd.row_valid = 1
GROUP BY GROUPING SETS (
	(gd.player_name, g.season),
	(gd.player_name, gd.team_id),
	gd.team_id
	)
