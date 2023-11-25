WITH
  last_year_scd AS (
    SELECT * FROM surbhiwahie.actors_history_scd
    WHERE current_year = 2020
  ),
  current_year_scd AS (
    SELECT * FROM surbhiwahie.actors
    WHERE current_year = 2021
  ),
  combined AS (
    SELECT 
      COALESCE(ly.actor, cy.actor) AS actor,
      COALESCE(ly.start_date, cy.current_year) AS start_year,
      COALESCE(ly.end_date, cy.current_year) AS end_year,
      CASE
        WHEN ly.is_active <> cy.is_active THEN 1
        WHEN ly.is_active = cy.is_active THEN 0
      END AS did_change,
      ly.is_active AS is_active_last_year,
      cy.is_active AS is_active_this_year,
      2021 AS current_year
    FROM last_year_scd ly
    FULL OUTER JOIN current_year_scd cy
    ON ly.actor = cy.actor
    AND ly.end_date + 1 = cy.current_year
  )
  ,changes as(
  
SELECT 
  actor,
  CASE 
  
    WHEN did_change = 0 THEN 
    ARRAY[
    CAST(ROW(is_active_last_year, start_year, end_year + 1) 
    AS ROW (
              is_active BOOLEAN,
              start_year INTEGER,
              end_year INTEGER
            ))
    ] 
    
    WHEN did_change = 1 THEN 
    ARRAY[
    CAST(
    ROW(is_active_last_year, start_year, end_year) AS ROW (
              is_active BOOLEAN,
              start_year INTEGER,
              end_year INTEGER
            )
    
    ),
    CAST(
            ROW (
              is_active_this_year,
              current_year,
              current_year
            ) AS ROW (
              is_active BOOLEAN,
              start_year INTEGER,
              end_year INTEGER
            )
          )
        ]
 
    WHEN did_change IS NULL THEN 
    ARRAY[
    CAST(ROW(
    COALESCE(is_active_last_year, is_active_this_year), start_year, end_year)
    AS ROW(is_active BOOLEAN,
              start_year INTEGER,
              end_year INTEGER)
    )
    ]
  END as change_array
FROM combined)

select 
actor,
  arr.is_active,
  arr.start_year,
  arr.end_year
  2021 as current_year
   from changes
cross join unnest(change_array) as arr

