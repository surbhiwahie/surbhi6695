WITH over10pts AS (
    SELECT
        player_id,
        game_id,
        pts,
        CASE
            WHEN pts > 10 THEN 1
            ELSE 0
        END AS over10
    FROM bootcamp.nba_game_details
    WHERE
        player_name = 'LeBron James'
        AND pts IS NOT NULL
),

streaks AS (
    SELECT
        *,
        SUM(CASE WHEN over10 = 0 THEN 1 ELSE 0 END)
            OVER (ORDER BY game_id ASC)
            AS reset_counter
    FROM over10pts
),

streak_counts AS (
    SELECT
        player_id,
        game_id,
        pts,
        over10,
        reset_counter,
        (CASE
            WHEN
                over10 = 1
                THEN
                    COUNT(*)
                        OVER (PARTITION BY reset_counter ORDER BY game_id ASC)
            ELSE 0
        END) - 1 AS streak
    FROM streaks
)

SELECT MAX(streak)
FROM streak_counts
