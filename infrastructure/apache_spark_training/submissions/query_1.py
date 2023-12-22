from pyspark.sql import SparkSession


def create_spark_session():
    if not os.environ["DATA_ENGINEER_IO_WAREHOUSE_CREDENTIAL"] or not os.environ["DATA_ENGINEER_IO_WAREHOUSE"]:
        raise ValueError(
            """You need to set environment variables:
                    DATA_ENGINEER_IO_WAREHOUSE_CREDENTIAL, 
                    DATA_ENGINEER_IO_WAREHOUSE to run this PySpark job!
        """
        )

    # Initialize SparkConf and SparkContext
    spark = (
        SparkSession.builder.appName("PySparkSQLReadFromTable")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.defaultCatalog", os.environ["DATA_ENGINEER_IO_WAREHOUSE"])
        .config("spark.sql.catalog.eczachly-academy-warehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.eczachly-academy-warehouse.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
        .config("spark.sql.catalog.eczachly-academy-warehouse.uri", "https://api.tabular.io/ws/")
        .config(
            "spark.sql.catalog.eczachly-academy-warehouse.credential",
            os.environ["DATA_ENGINEER_IO_WAREHOUSE_CREDENTIAL"],
        )
        .config("spark.sql.catalog.eczachly-academy-warehouse.warehouse", os.environ["DATA_ENGINEER_IO_WAREHOUSE"])
        .getOrCreate()
    )

    ## Disable Broadcast Hash Join
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    return spark


def main():
    spark = create_spark_session()
    # [rest of code]


if __name__ == "__main__":
    main()
