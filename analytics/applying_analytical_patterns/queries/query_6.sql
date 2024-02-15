WITH games AS (
    SELECT DISTINCT
        season,
        game_id,
        game_date_est,
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
),

wide_to_long AS (
    SELECT
        game_id,
        game_date_est,
        home_team_id AS team,
        CASE
            WHEN home_team_id = winning_team THEN 1
            ELSE 0
        END AS win
    FROM games
    UNION
    SELECT
        game_id,
        game_date_est,
        visitor_team_id AS team,
        CASE
            WHEN home_team_id = winning_team THEN 0
            ELSE 1
        END AS win
    FROM games
),

window AS (
    SELECT
        *,
        SUM(win)
            OVER (
                PARTITION BY team
                ORDER BY
                    game_date_est DESC
                ROWS BETWEEN 89 PRECEDING AND CURRENT ROW

            )
            AS wins_last_90_games
    FROM wide_to_long
)

SELECT
    team_abbreviation,
    MAX(wins_last_90_games) AS max_90_game_wins
FROM window
INNER JOIN teams
    ON window.team = teams.team_id
GROUP BY team_abbreviation
ORDER BY MAX(wins_last_90_games) DESC
