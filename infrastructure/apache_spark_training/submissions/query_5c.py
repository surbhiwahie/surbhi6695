from pyspark.sql.functions import col
from create_spark_session import init_spark

def main():
    spark = init_spark()

    df = spark.read.table("surbhiwahie.joined_bucketed")
    
    # Version 5c: Partitioned by playlist_id and medal_id
    partitioned_table_5c = "surbhiwahie.partitioned_table_5c"
    
    # Write DataFrame to the new partitioned table
    df.write.mode("overwrite") \
    .partitionBy("playlist_id", "medal_id") \
    .sortWithinPartitions("completion_date") \
    .saveAsTable(partitioned_table_5c)

if __name__ == "__main__":
    main()
