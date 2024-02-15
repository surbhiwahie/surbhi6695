from pyspark.sql.functions import broadcast
import os
from create_spark_session import init_spark


def main():
    spark = init_spark()
    
    maps = "/Users/scarstruck/Documents/GitHub/bootcamp3/infrastructure/4-apache-spark-training/Homework_data/maps.csv"
    matches = "/Users/scarstruck/Documents/GitHub/bootcamp3/infrastructure/4-apache-spark-training/Homework_data/matches.csv"
    MEDALS_MATCHES_PLAYERS = "/Users/scarstruck/Documents/GitHub/bootcamp3/infrastructure/4-apache-spark-training/Homework_data/medals_matches_players.csv"
    medals = "/Users/scarstruck/Documents/GitHub/bootcamp3/infrastructure/4-apache-spark-training/Homework_data/maps.csv"

    maps_df = spark.read.format("csv").option("header", "true").load(maps)
    matches_df = spark.read.format("csv").option("header", "true").load(matches)
    MEDALS_MATCHES_PLAYERS_df=spark.read.format("csv").option("header", "true").load(MEDALS_MATCHES_PLAYERS)
    medals_df = spark.read.format("csv").option("header", "true").load(medals)

    ##Join medals and maps tables applying the broadcast join
    
    maps_medals_matches_df = maps_df.join(broadcast(medals_matches_players_df), maps_df.mapid == medals_matches_players_df.map_id, 'left_outer') \
    .join(broadcast(matches_df), matches_df.match_id == medals_matches_players_df.match_id, 'left_outer')

    print(f"Row Count of Join: {maps_medals_matches_df.count()}")
    spark.stop()
    return maps_medals_matches_df

if __name__ == "__main__":
    main()
    
