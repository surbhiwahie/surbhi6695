WITH
  Combined AS (
    SELECT
      gd.player_id,
      gd.team_id,
      g.season,
      SUM(pts) AS total_points,
      SUM(
        CASE
          WHEN team_id = home_team_id
          AND home_team_wins = 1 THEN 1
          WHEN team_id = home_team_id
          AND home_team_wins = 0 THEN 0
          WHEN team_id != home_team_id
          AND home_team_wins = 0 THEN 1
          ELSE 0
        END
      ) AS total_wins
    FROM
      bootcamp.nba_game_details gd
      JOIN bootcamp.nba_games g ON gd.game_id = g.game_id
    GROUP BY
      GROUPING SETS (
        (gd.player_id, gd.team_id),
        (gd.player_id, g.season),
        (gd.team_id)
      )
  )
SELECT 
team_id, total_points, total_wins
FROM
  Combined
  where   total_wins in (
  select max(total_wins) from Combined where  player_id is null and season is null)
