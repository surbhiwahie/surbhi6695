from pyspark.sql.functions import col
from create_spark_session import init_spark

def main():
    spark = init_spark()

    df = spark.read.table("surbhiwahie.joined_bucketed")
    
    # Version 5b: Partitioned by completion_date and mapid
    partitioned_table_5b = "surbhiwahie.partitioned_table_5b"
    
    # Write DataFrame to the new partitioned table
    df.write.mode("overwrite") \
    .partitionBy("completion_date", "mapid") \
    .sortWithinPartitions("playlist_id") \
    .saveAsTable(partitioned_table_5b)

if __name__ == "__main__":
    main()
