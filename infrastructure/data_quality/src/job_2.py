from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_2(output_table_name: str) -> str:
    return f"""
    SELECT actorid,
        CASE 
            WHEN is_active THEN 1 
            ELSE 0 
        END as is_actor_active,
        CASE WHEN LAG(is_active, 1) OVER (PARTITION BY actorid ORDER BY current_year) THEN 1 
        ELSE 0 
        END as act_active_ly,
        quality_class,
        current_year
    FROM actors
    WHERE current_year <= {year}
    """

def job_2(spark_session: SparkSession, input_dataframe, year:int) -> Optional[DataFrame]:
  query = query_2(year)
  input_dataframe.createOrReplaceTempView("actors")
  return spark_session.sql(query)

def main():
    output_table_name: str = "actor_active_ly"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_2")
        .getOrCreate()
    )
    output_df = job_2(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
