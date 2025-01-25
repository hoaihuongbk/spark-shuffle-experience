from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def prepare_dataset():
    spark = (
        SparkSession.builder.appName("PrepareDataset")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "/opt/spark/spark-events")
        .config("spark.history.fs.logDirectory", "/opt/spark/spark-events")
        .getOrCreate()
    )

    ## Create the transactions table
    (
        spark.range(0, 150000000, 1, 32)
        .select(
            "id",
            round(rand() * 10000, 2).alias("amount"),
            (col("id") % 10).alias("country_id"),
            (col("id") % 100).alias("store_id"),
        )
        .write.mode("overwrite")
        .parquet("/app/transactions")
    )

    ## Create the stores table
    (
        spark.range(0, 99)
        .select(
            "id",
            round(rand() * 100, 0).alias("employees"),
            (col("id") % 10).alias("country_id"),
            expr("uuid()").alias("name"),
        )
        .write.mode("overwrite")
        .parquet("/app/stores")
    )

    ## Create the countries table
    countries = [
        (0, "Italy"),
        (1, "Canada"),
        (2, "Mexico"),
        (3, "China"),
        (4, "Germany"),
        (5, "UK"),
        (6, "Japan"),
        (7, "Korea"),
        (8, "Australia"),
        (9, "France"),
        (10, "Spain"),
        (11, "USA"),
    ]

    columns = ["id", "name"]

    ## Create the Spark DataFrame and countries table
    (
        spark.createDataFrame(data=countries, schema=columns)
        .write.mode("overwrite")
        .parquet("/app/countries")
    )


if __name__ == "__main__":
    print("Preparing dataset...")
    prepare_dataset()
