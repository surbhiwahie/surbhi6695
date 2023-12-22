from pyspark.sql.functions import col, avg
import os
from create_spark_session import init_spark


def main():
    spark = init_spark()


    # Define the SQL query
    sql_query = """SELECT playlist_id, COUNT(*) AS play_count FROM surbhiwahie.joined_bucketed 
                        GROUP BY playlist_id"""

    # Fetch data into a DataFrame
    playlist_play_counts = spark.sql(sql_query)

    # Show the DataFrame
    playlist_play_counts.show()

  # Calculate average kills per game per player
    most_played_playlist = (
    playlist_play_counts
    .orderBy(col("play_count").desc())
    .select("playlist_id", "play_count")
    .first()
)

    # Show the result
    most_played_playlist.show(1)

if __name__ == "__main__":
    main()
