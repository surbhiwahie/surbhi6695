import pytest
from chispa.dataframe_comparer import assert_df_equality
from ..src.job_2 import job_2
from collections import namedtuple

Actors = namedtuple("Actors", "actor actorid films quality_class is_active current_year")
Films = namedtuple("Films", "film votes rating film_id")
ActorsActiveLY = namedtuple("ActorsActiveLY", "actorid is_actor_active act_active_ly quality_class current_year")

def test_job_2(spark):
    year = 2023
    input_data = [
        Actors(
            actor="actor1",
            actorid=1,
            films= [
                Films(
                    film="film2",
                    votes=50,
                    rating=9,
                    film_id=2
                )
            ],
            quality_class="star",
            is_active=True,
            current_year=year-1
        ),
        Actors(
            actor="actor1",
            actorid=1,
            films= [
                Films(
                    film="film1",
                    votes=40,
                    rating=6,
                    film_id=1
                )
            ],
            quality_class="average",
            is_active=True,
            current_year=year
        )
    ]

    source_df = spark.createDataFrame(input_data)
    actual_df = job_2(spark, source_df, year)

    expected_data = [
        ActorsActiveLY(
            actorid=1,
            is_actor_active=1,
            act_active_ly=0,
            quality_class="star",
            current_year=year-1
        ),
        ActorsActiveLY(
            actorid=1,
            is_actor_active=1,
            act_active_ly=1,
            quality_class="average",
            current_year=year
        )
    ]

    expected_df = spark.createDataFrame(expected_data)
    expected_df = expected_df.withColumn('is_actor_active', expected_df['is_actor_active'].cast('int')).fillna({"is_actor_active": 0})
    expected_df = expected_df.withColumn('act_active_ly', expected_df['act_active_ly'].cast('int')).fillna({"act_active_ly": 0})

    assert_df_equality(actual_df, expected_df)
