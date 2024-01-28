from pyspark.sql.functions import col, avg
import os
from create_spark_session import init_spark


def main():
    spark = init_spark()

    medals_file = "/Users/scarstruck/Documents/GitHub/bootcamp3/infrastructure/4-apache-spark-training/Homework_data/medals.csv"
    medals = spark.read.format("csv").option("header", "true").load(medals_file)
    match_details_file = "/Users/scarstruck/Documents/GitHub/bootcamp3/infrastructure/4-apache-spark-training/Homework_data/match_details.csv"
    match_details = spark.read.format("csv").option("header", "true").load(match_details_file)
    medals_matches_players_file= "/Users/scarstruck/Documents/GitHub/bootcamp3/infrastructure/4-apache-spark-training/Homework_data/medals_matches_players.csv"
    medals_matches_players = spark.read.format("csv").option("header", "true").load(medals_matches_players_file)
    matches_file="/Users/scarstruck/Documents/GitHub/bootcamp3/infrastructure/4-apache-spark-training/Homework_data/matches.csv"
    matches=spark.read.format("csv").option("header", "true").load(matches_file)
    maps_file="/Users/scarstruck/Documents/GitHub/bootcamp3/infrastructure/4-apache-spark-training/Homework_data/maps.csv"
    maps=spark.read.format("csv").option("header", "true").load(maps_file)
    
    
    df = match_details \
    .join(medals_matches_players, ["match_id", "player_gamertag"]) \
    .join(matches, "match_id")

    most_killing_spree_map = df \
        .groupBy("mapid", "medal_id") \
        .agg(sum("count").alias("num_medals")) \
        .join(medals, "medal_id") \
        .where(col("classification") == "KillingSpree") \
        .join(maps.alias("m"), "mapid") \
        .select(col("m.name"), col("classification"), col("num_medals")) \
        .orderBy(desc("num_medals")) \
        .limit(1)

    # Show the result
    most_killing_spree_map.show(1)

if __name__ == "__main__":
    main()


## name =  Breakout Arena
## Classification ==  KillingSpree
## num_dedals =  6738 
