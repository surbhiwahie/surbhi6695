from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_1(output_table_name: str) -> str:
    return f"""
    WITH year_base AS (
        SELECT {year} as year
    ),
    last_year AS (
        SELECT * 
            FROM actors
        WHERE current_year = (SELECT year FROM year_base)
    ),
    this_year AS (
        SELECT * 
            FROM actor_films
        WHERE year = (SELECT year+1 FROM year_base)
    ),
    actor_films_aggregated AS (
        SELECT 
            actor,
            actor_id,
            year,  
            array(struct(film AS film, votes AS votes, rating AS rating, film_id AS film_id)) as films_aggregated
        FROM this_year
        GROUP BY actor, actor_id, year, array(struct(film, votes, rating, film_id))
    ),
    actor_recent_year AS (
        SELECT actor_id, MAX(year) AS max_year
        FROM actor_films
        WHERE year <= (SELECT year+1 FROM year_base)
        GROUP BY actor_id
    ),
    actor_rating AS (
        SELECT af.actor_id, max_year, 
        AVG(rating) as avg_rating,
        CASE
            WHEN max_year != (SELECT year+1 FROM year_base) THEN false
            ELSE true
        END as is_active
        FROM actor_films af
        INNER JOIN actor_recent_year ary
            ON af.actor_id = ary.actor_id
            AND af.year = ary.max_year
        GROUP BY af.actor_id, ary.max_year 
    )
    SELECT 
    COALESCE(ly.actor, ty.actor) as actor,
    COALESCE(ly.actorid, ty.actor_id) as actorid,
    CASE
        WHEN ty.year IS NULL then ly.films
        WHEN ty.year IS NOT NULL and ly.films IS NULL THEN ty.films_aggregated
        WHEN ty.year IS NOT NULL and ly.films IS NOT NULL THEN CONCAT(ty.films_aggregated, ly.films)
    END as films,
    CASE
    WHEN (ty.year IS NULL) THEN ly.quality_class
    ELSE
        CASE
        WHEN (ar.avg_rating > 8) THEN 'star'
        WHEN (ar.avg_rating BETWEEN 7 AND 8) THEN 'good'
        WHEN (ar.avg_rating BETWEEN 6 AND 7) THEN 'average'
        ELSE 'bad'
        END
    END as quality_class,
    COALESCE(ar.is_active, ly.is_active) as is_active,
    COALESCE(ty.year, ly.current_year + 1) as current_year
    FROM last_year ly
    FULL OUTER JOIN actor_films_aggregated ty
        ON ly.actorid = ty.actor_id
    LEFT JOIN actor_rating ar
        ON ar.actor_id = COALESCE(ly.actorid, ty.actor_id)
    """

def job_1(spark_session: SparkSession, input_dataframe, output_dataframe, year:int) -> Optional[DataFrame]:
  query = query_1(year)
  input_dataframe.createOrReplaceTempView("actor_films")
  output_dataframe.createOrReplaceTempView("actors")
  return spark_session.sql(query)

def main():
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_1")
        .getOrCreate()
    )
    output_df = job_1(spark_session, spark_session.table("actor_films"), spark_session.table('actors'), 2022)
    output_df.write.mode("overwrite").insertInto("actors")
