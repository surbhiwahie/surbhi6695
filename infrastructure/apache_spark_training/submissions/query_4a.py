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
        main_df.groupBy("player_gamertag")
        .agg(avg("count").alias("average_kills_per_game"))
        .orderBy(col("average_kills_per_game").desc())
    )

    # Show the result
    Avg_kills_per_game.show(1)

if __name__ == "__main__":
    main()


## Output of the query
##  player_gamertag	== nNina Dobrev
## avg_player_kills ==  5.473684210526316

