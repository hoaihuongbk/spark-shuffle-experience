from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    TimestampType,
)


def fix_schema(spark, file_name):
    # Define unified schema that works for all files
    unified_schema = StructType(
        [
            StructField("VendorID", LongType(), True),
            StructField("tpep_pickup_datetime", TimestampType(), True),
            StructField("tpep_dropoff_datetime", TimestampType(), True),
            StructField("passenger_count", LongType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("RatecodeID", LongType(), True),
            StructField("store_and_fwd_flag", StringType(), True),
            StructField("PULocationID", LongType(), True),
            StructField("DOLocationID", LongType(), True),
            StructField("payment_type", LongType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("extra", DoubleType(), True),
            StructField("mta_tax", DoubleType(), True),
            StructField("tip_amount", DoubleType(), True),
            StructField("tolls_amount", DoubleType(), True),
            StructField("improvement_surcharge", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("congestion_surcharge", DoubleType(), True),
            StructField("airport_fee", DoubleType(), True),
        ]
    )

    issue_df = spark.read.parquet("/app/data/" + file_name)

    # Cast issue_df to the unified schema
    fixed_df = issue_df.select(
        *[
            col(c).cast(unified_schema.fields[i].dataType)
            for i, c in enumerate(issue_df.columns)
        ]
    )

    # Write to destination
    fixed_df.coalesce(1).write.mode("append").parquet("/app/fixed_data")


def fix_dataset():
    spark = (
        SparkSession.builder.appName("FixDataset")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "/opt/spark/spark-events")
        .config("spark.history.fs.logDirectory", "/opt/spark/spark-events")
        .getOrCreate()
    )

    # Loop through all files in the app/data directory and then fix the schema
    import os

    for file in os.listdir("/app/data"):
        if file.endswith(".parquet"):
            fix_schema(spark, file)
            print(f"Processed file: {file}")


if __name__ == "__main__":
    fix_dataset()