from pyspark.sql.functions import col
import os
from create_spark_session import init_spark


def bucket_matches(spark, data_path):
    spark.sql("""DROP TABLE IF EXISTS surbhiwahie.matches_bucketed""")
    bucketedDDL = """
    CREATE TABLE IF NOT EXISTS surbhiwahie.matches_bucketed (
        match_id STRING,
        is_team_game BOOLEAN,
        mapid STRING,
        playlist_id STRING,
        completion_date TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (bucket(16, match_id));
    """
    spark.sql(bucketedDDL)

    matches = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{data_path}/data/matches.csv")
    )

    (
        matches.select(col("match_id"), col("is_team_game"), col("mapid"), col("playlist_id"), col("completion_date"))
        .write.mode("overwrite")
        .bucketBy(16, "match_id")
        .saveAsTable("surbhiwahie.matches_bucketed")
    )


def bucket_match_detail(spark, data_path):
    spark.sql("""DROP TABLE IF EXISTS surbhiwahie.match_details_bucket""")
    bucketedDDL = """
    CREATE TABLE IF NOT EXISTS surbhiwahie.match_details_bucket (
        match_id STRING,
        player_gamertag STRING,
        previous_spartan_rank INTEGER,
        spartan_rank INTEGER
    )
    USING iceberg
    PARTITIONED BY (bucket(16, match_id));
    """
    spark.sql(bucketedDDL)
    matches = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{data_path}/data/match_details.csv")
    )

    (
        matches.select(col("match_id"), col("player_gamertag"), col("previous_spartan_rank"), col("spartan_rank"))
        .write.mode("overwrite")
        .bucketBy(16, "match_id")
        .saveAsTable("surbhiwahie.match_details_bucket")
    )

    return matches


def bucket_medals_matches_players(spark, data_path):
    spark.sql("""DROP TABLE IF EXISTS surbhiwahie.medals_matches_players_bucket""")
    bucketedDDL = """
    CREATE TABLE IF NOT EXISTS surbhiwahie.medals_matches_players_bucket (
        match_id STRING,
        player_gamertag STRING,
        medal_id INTEGER,
        count INTEGER
    )
    USING iceberg
    PARTITIONED BY (bucket(16, match_id));
    """
    spark.sql(bucketedDDL)

    matches = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{data_path}/data/medals_matches_players.csv")
    )

    (
        matches.select(col("match_id"), col("player_gamertag"), col("medal_id"), col("count"))
        .write.mode("overwrite")
        .bucketBy(16, "match_id")
        .saveAsTable("surbhiwahie.medals_matches_players_bucket")
    )

    return matches


def main():
    spark = init_spark()
    schema = "surbhiwahie"
    project_root = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    os.chdir(project_root)
    print(project_root)

    bucket_matches(spark, data_path=project_root)
    bucket_match_detail(spark, data_path=project_root)
    bucket_medals_matches_players(spark, data_path=project_root)

    matches = spark.read.format("iceberg").load(f"{schema}.matches_bucketed")
    match_details = spark.read.format("iceberg").load(f"{schema}.match_details_bucket")
    medals_matches_players = spark.read.format("iceberg").load(f"{schema}.medals_matches_players_bucket")

    # Alias each dataframe
    matches_df = matches.alias("matches")
    match_details_df = match_details.alias("match_details")
    medals_matches_players_df = medals_matches_players.alias("medals_matches_players")

    # Perform the join
    all_joined = matches_df.join(
        match_details_df, matches_df["match_id"] == match_details_df["match_id"], "full_outer"
    ).join(
        medals_matches_players_df, matches_df["match_id"] == medals_matches_players_df["match_id"], "leftouter"
    )

    # Select the required columns with appropriate aliases
    selected_columns = all_joined.select(
        matches_df["match_id"],
        match_details_df["player_gamertag"],
        matches_df["mapid"],
        matches_df["playlist_id"],
        medals_matches_players_df["medal_id"],
        medals_matches_players_df["count"],
        matches_df["completion_date"]
    )
    
#     - which player has the highest average kills per game? (`query_4a`)
#   - which playlist has received the most plays? (`query_4b`)
#   - which map was played the most? (`query_4c`)
#   - on which map do players receive the highest number of Killing Spree medals? (`query_4d`)
  
  
    spark.sql("""DROP TABLE IF EXISTS surbhiwahie.joined_bucketed""")
    bucketedDDL = """
    CREATE TABLE IF NOT EXISTS surbhiwahie.joined_bucketed (
        match_id STRING,
        player_gamertag STRING,
        mapid STRING,
        playlist_id STRING,
        medal_id STRING,
        count INTEGER,
        completion_date DATE
        )
    USING iceberg
    PARTITIONED BY (completion_date, bucket(16, match_id));
    """
    spark.sql(bucketedDDL)

    (selected_columns
        .write.mode("overwrite")
        .partitionBy("completion_date")
        .bucketBy(16, "match_id")
        .saveAsTable("surbhiwahie.joined_bucketed")
    )


if __name__ == "__main__":
    main()
