import pytest
from chispa.dataframe_comparer import assert_df_equality
from ..src.job_1 import job_1
from collections import namedtuple

Actors = namedtuple("Actors",  "actor actor_id film year votes rating film_id")
ActorsAgg = namedtuple("ActorsAgg", "actor actorid films quality_class is_active current_year")
Films = namedtuple("Films", "film votes rating film_id")

def test_job_1(spark):
    year = 2022
    input_data = [
        # Make sure basic case is handled gracefully
        Actors(
            actor="actor1",
            actor_id=1,
            film="film1",
            year=year+1,
            votes=40,
            rating=6,
            film_id=1
        ),
        Actors(
            actor="actor1",
            actor_id=1,
            film="film2",
            year=year+1,
            votes=50,
            rating=9,
            film_id=2
        )
    ]
    source_df = spark.createDataFrame(input_data)

    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType
    initial_output_df_schema = StructType([
    StructField("actor", StringType(), True),
    StructField("actorid", IntegerType(), True),
    StructField("films", ArrayType(
        StructType([
            StructField("film", StringType(), True),
            StructField("votes", IntegerType(), True),
            StructField("rating", IntegerType(), True),
            StructField("film_id", IntegerType(), True)
        ])
    ), True),
    StructField("quality_class", StringType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("current_year", IntegerType(), True)
    ])
    initial_output_df = spark.createDataFrame([], schema=initial_output_df_schema)
    actual_df = job_1(spark, source_df, initial_output_df, year)

    expected_data = [
        ActorsAgg(
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
            quality_class="good",
            is_active=True,
            current_year=year+1
        ),
        ActorsAgg(
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
            quality_class="good",
            is_active=True,
            current_year=year+1
        )
    ]
    expected_df = spark.createDataFrame(expected_data)

    assert_df_equality(actual_df, expected_df)
