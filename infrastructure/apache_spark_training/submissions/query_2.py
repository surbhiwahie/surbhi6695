from pyspark.sql.functions import broadcast
import os
from create_spark_session import init_spark


def main():
    spark = init_spark()
    
    maps = "/Users/scarstruck/Documents/GitHub/bootcamp3/infrastructure/4-apache-spark-training/Homework_data/maps.csv"
    medals = "/Users/scarstruck/Documents/GitHub/bootcamp3/infrastructure/4-apache-spark-training/Homework_data/maps.csv"

    maps_df = spark.read.format("csv").option("header", "true").load(maps)

    medals_df = spark.read.format("csv").option("header", "true").load(medals)

    ## Join medals and maps tables
    medals_maps_df = medals_df.join(broadcast(maps_df), "description", "full_outer")
    print(f"Row Count of Join: {medals_maps_df.count()}")
    spark.stop()
    return medals_maps_df


if __name__ == "__main__":
    main()
    
