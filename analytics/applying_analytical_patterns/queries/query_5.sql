WITH games AS (
    SELECT DISTINCT
        season,
        game_id,
        home_team_id,
        visitor_team_id,
        home_team_wins,
        CASE
            WHEN home_team_wins = 1 THEN home_team_id
            ELSE visitor_team_id
        END AS winning_team
    FROM bootcamp.nba_games
),

teams AS (
    SELECT DISTINCT
        team_id,
        team_abbreviation,
        team_city
    FROM bootcamp.nba_game_details
)

SELECT
    teams.team_abbreviation,
    COUNT(*) AS total_wins
FROM games
LEFT JOIN teams
    ON games.winning_team = teams.team_id
GROUP BY team_abbreviation
ORDER BY COUNT(*) DESC

-- LAC	1838
