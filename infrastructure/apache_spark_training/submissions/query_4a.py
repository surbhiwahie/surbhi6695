from pyspark.sql.functions import col, avg
import os
from create_spark_session import init_spark


def main():
    spark = init_spark()


    # Define the SQL query
    sql_query = "SELECT * FROM surbhiwahie.joined_bucketed"

    # Fetch data into a DataFrame
    main_df = spark.sql(sql_query)

    # Show the DataFrame
    main_df.show()

  # Calculate average kills per game per player
    Avg_kills_per_game = (
        main_df.groupBy("player_gamertag", "match_id") \
        .agg(avg("player_total_kills").alias("avg_kills")) \
        .orderBy(desc("avg_kills")) \
        .limit(1)
    
    # Show the result
    Avg_kills_per_game.show(1)

if __name__ == "__main__":
    main()


## Output of the query
##  player_gamertag	== gimpinator14 | acf0e47e-20ac-4b12-b292-591d4b3a3df9
## avg_player_kills ==  109.0

