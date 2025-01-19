from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, sum
from pyspark.sql import Window


def create_dataset(spark):
    return spark.read.parquet("/app/fixed_data/*.parquet")


def write_output(df, file_name):
    df.write.mode("overwrite").parquet("/app/output/" + file_name)


def test_group_by(df):
    return df.groupBy("PULocationID").agg(
        avg("fare_amount").alias("avg_fare"),
        count("*").alias("trip_count"),
        sum("total_amount").alias("total_revenue"),
    )


def test_join(df):
    morning_trip_df = df.filter(
        col("tpep_pickup_datetime").between(
            "2023-03-01 00:00:00", "2023-03-01 12:00:00"
        )
    )
    evening_trip_df = df.filter(
        col("tpep_pickup_datetime").between(
            "2023-03-01 12:00:00", "2023-03-01 23:59:59"
        )
    )
    return morning_trip_df.join(evening_trip_df, on="PULocationID", how="inner")


def test_window_function(df):
    window_spec = Window.partitionBy("PULocationID").orderBy(
        col("tpep_pickup_datetime")
    )
    return df.withColumn("running_fare", sum("fare_amount").over(window_spec))


def run_shuffle_test(test_type, plugin):
    spark = (
        SparkSession.builder.appName("ShuffleTest")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "/opt/spark/spark-events")
        .config("spark.history.fs.logDirectory", "/opt/spark/spark-events")
        .getOrCreate()
    )

    df = create_dataset(spark)
    df.printSchema()

    # Force shuffle with groupBy
    if test_type == "group_by":
        grouped_df = test_group_by(df)
        grouped_df.explain()
        grouped_output_file = plugin + "_grouped_df"
        write_output(grouped_df, grouped_output_file)
    # Force shuffle with join
    elif test_type == "join":
        joined_df = test_join(df)
        joined_df.explain()
        joined_df.show()
    elif test_type == "window":
        windowed_df = test_window_function(df)
        windowed_df.explain()
        windowed_output_file = plugin + "_windowed_df"
        write_output(windowed_df, windowed_output_file)
    else:
        raise Exception("Invalid test type")


if __name__ == "__main__":
    # Get arguments from command line
    import sys

    args = sys.argv[1:]
    if len(args) != 2:
        print("Usage: Require <test_type> <plugin>")
        sys.exit(1)

    test_type = args[0]
    plugin = args[1]

    print(f"Running test: {test_type}")
    print(f"Plugin: {plugin}")

    run_shuffle_test(test_type, plugin)
