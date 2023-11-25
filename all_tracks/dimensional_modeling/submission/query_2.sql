insert into surbhiwahie.actors

WITH
  last_year AS (
    SELECT
      *
    FROM
      surbhiwahie.actors
    WHERE
      current_year = 2020
  ),
  this_year AS (
    SELECT
      actor,
      actor_id,
      ARRAY_AGG(CAST(ROW(film, votes, rating, film_id) AS ROW(film VARCHAR, votes INTEGER, rating DOUBLE, film_id VARCHAR))) as films,
      year
    FROM
      bootcamp.actor_films
    WHERE
      YEAR = 2021
    GROUP BY actor, actor_id, year
) , combined as (
SELECT
  COALESCE(ly.actor, ty.actor) AS actor,
  COALESCE(ly.actorid, ty.actor_id) AS actorid,
  CASE
    WHEN ty.year IS NULL THEN ly.films
    WHEN ty.year IS NOT NULL AND ly.current_year IS NULL THEN ty.films
    WHEN ty.year IS NOT NULL AND ly.current_year IS NOT NULL THEN 
      ty.films||ly.films
  END AS films,
  NULL AS quality_class,
  ty.year IS NOT NULL AS is_active,
  COALESCE(ty.year, ly.current_year + 1) AS current_year
FROM
  last_year ly
  FULL OUTER JOIN this_year ty ON ly.actorid = ty.actor_id )

select actor, actorid, films, 
CASE WHEN AVG(t.rating) > 8 THEN 'star'
    WHEN AVG(t.rating) > 7
    AND AVG(t.rating) <= 8 THEN 'good'
    WHEN AVG(t.rating) > 6
    AND AVG(t.rating) <= 7 THEN 'average'
    WHEN AVG(t.rating) <= 6 THEN 'bad'
  END AS quality_class
, is_active, current_year
from combined,
 UNNEST(films) AS t(film, votes, rating, film_id)
 group by actor,
actorid,
films,
is_active,
current_year
