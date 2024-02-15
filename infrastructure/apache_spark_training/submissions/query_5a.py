from pyspark.sql.functions import col
import os
from create_spark_session import init_spark

def main():
    spark = init_spark()

    df = spark.read.table("surbhiwahie.joined_bucketed")
    
    # Version 5a: Partitioned by completion_date and playlist_id
    partitioned_table_5a = "surbhiwahie.partitioned_table_5a"
    
    # Write DataFrame to the new partitioned table
    df.write.mode("overwrite") \
    .partitionBy("completion_date", "playlist_id") \
    .sortWithinPartitions("mapid") \
    .saveAsTable(partitioned_table_5a)

## This is how we find the memory consumed for each partition 
## du -hs spark-warehouse/partitioned_table_5a/

if __name__ == "__main__":
    main()
