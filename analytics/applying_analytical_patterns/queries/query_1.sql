CREATE TABLE surbhiwahie.state_change_accounting
(
player_name VARCHAR,
first_active_season INTEGER,
last_active_season INTEGER,
seasons_active ARRAY(INTEGER),
state_change  VARCHAR,
Season INTEGER
)
WITH (
format = 'PARQUET' ,
partitioning = ARRAY['season']
)



-----------------------------------------

INSERT INTO surbhiwahie.state_change_accounting

WITH current_year AS (
  SELECT 1997 as year
),
last_season AS (
select *
from surbhiwahie.state_change_accounting
WHERE season = (SELECT year-1 FROM current_year)
),
this_season AS (
SELECT
  player_name,
  COUNT(1) as number_of_events,
  MAX(season) as season
FROM bootcamp.nba_player_seasons
WHERE season = (SELECT year FROM current_year)
GROUP BY player_name
),
combined AS (
SELECT 
  COALESCE(ly.player_name, ty.player_name) AS player_name,
  COALESCE(ly.first_active_season, ty.season) AS first_active_season,
  ty.season,
  ly.last_active_season as active_season_ly,
  COALESCE(ty.season, ly.last_active_season) AS last_active_season,
  CASE
    WHEN
      ly.seasons_active IS NULL THEN ARRAY[ty.season]
    WHEN
      ty.season IS NULL THEN ly.seasons_active
    ELSE
      ly.seasons_active || ARRAY[ty.season]
  END as dates_active,
  (SELECT year FROM current_year) AS partition_date
FROM last_season ly
FULL OUTER JOIN this_season ty
ON ly.player_name = ty.player_name
)

SELECT player_name,
  first_active_season,
  last_active_season,
  dates_active AS seasons_active,
  CASE
    WHEN season - first_active_season = 0 THEN 'New'
		WHEN last_active_season - active_season_ly = 1 THEN 'Continued Playing'
		WHEN season IS NULL AND active_season_ly IS NOT NULL THEN 'Retired'
		WHEN season IS NULL AND active_season_ly IS NULL THEN 'Stayed Retired'
		WHEN active_season_ly IS NULL
				AND season IS NOT NULL
				AND season - first_active_season > 0
			THEN 'Returned from Retirement'
	END as yearly_active_state,
  partition_date AS season
FROM combined
