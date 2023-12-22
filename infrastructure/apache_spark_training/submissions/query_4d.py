from pyspark.sql.functions import col, avg
import os
from create_spark_session import init_spark


def main():
    spark = init_spark()

    medals = "/Users/scarstruck/Documents/GitHub/bootcamp3/infrastructure/4-apache-spark-training/Homework_data/maps.csv"
    medals_df = spark.read.format("csv").option("header", "true").load(medals)
    
    #now we will find the medal_ids whose classification is "Killing Spree"
    medal_id_killing_spree = medals_df.filter(col('medal_name') == 'Killing Spree').select('medal_id').first()['medal_id']


    # Define the SQL query with the actual medal_id
    sql_query = f"""
        SELECT mapid, COUNT(*) AS killing_spree_count
        FROM surbhiwahie.joined_bucketed
        WHERE medal_id = '{medal_id_killing_spree}'
        GROUP BY mapid
    """

    # Fetch data into a DataFrame
    killing_spree_counts = spark.sql(sql_query)

    # Show the DataFrame
    killing_spree_counts.show()

   # Find the map with the highest number of Killing Spree medals
    most_killing_spree_map = (
        killing_spree_counts
        .orderBy(col("killing_spree_count").desc())
        .select("mapid", "killing_spree_count")
        .first()
)

    # Show the result
    most_killing_spree_map.show(1)

if __name__ == "__main__":
    main()
