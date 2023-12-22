from pyspark.sql.functions import col, avg
import os
from create_spark_session import init_spark


def main():
    spark = init_spark()


    # Define the SQL query
    sql_query = """SELECT mapid, COUNT(*) AS play_count FROM surbhiwahie.joined_bucketed
                    GROUP BY mapid"""

    # Fetch data into a DataFrame
    map_play_counts = spark.sql(sql_query)

    # Show the DataFrame
    map_play_counts.show()

  # Calculate average kills per game per player
    most_played_map = (
    map_play_counts
    .orderBy(col("play_count").desc())
    .select("mapid", "play_count")
    .first()

)

    # Show the result
    most_played_map.show(1)

if __name__ == "__main__":
    main()
