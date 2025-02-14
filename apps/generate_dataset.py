from pyspark.sql.functions import (
    monotonically_increasing_id,
    rand,
    array,
)
from utils import write_table, init_spark


def generate_test_data(table_format):
    spark = init_spark("Generate Test Data", table_format)

    # Generate main fact table
    fact_df = spark.range(0, 100_000_000).select(
        (rand() * 1000).cast("int").alias("city_id"),
        (rand() * 100000).cast("int").alias("eater_id"),
        (rand() * 50).cast("int").alias("feature_id"),
        rand().alias("value"),
        rand().cast("double").alias("value_1"),
        array(
            (rand() * 1000).cast("long"),
            (rand() * 1000).cast("long"),
            (rand() * 1000).cast("long"),
        ).alias("value_2"),
        (rand() * 100).cast("int").alias("value_3"),
    )

    # Create specific test cases for fact table
    test_fact = spark.createDataFrame(
        [(1, 100, 1000, 0.5, 0.7, [123, 456, 789], 42)],
        [
            "city_id",
            "eater_id",
            "feature_id",
            "value",
            "value_1",
            "value_2",
            "value_3",
        ],
    )

    # Generate dimension table
    dimension_df = spark.range(0, 2_000_000).select(
        (rand() * 1000).cast("int").alias("city_id"),
        monotonically_increasing_id().cast("string").alias("request_id"),
        (rand() * 100000).cast("int").alias("eater_id"),
        (rand() * 50).cast("int").alias("feature_id"),
        (rand() * 50).cast("int").alias("country_id"),
    )

    # Create specific test cases for dimension table
    test_dimension = spark.createDataFrame(
        [(1, "test_req_1", 100, 1000, 1), (1, "test_req_2", 100, 1000, 1)],
        ["city_id", "request_id", "eater_id", "feature_id", "country_id"],
    )

    # Union with main tables
    fact_df = fact_df.union(test_fact)
    dimension_df = dimension_df.union(test_dimension)

    # Print some statistics
    print("\nFact table schema:")
    fact_df.printSchema()
    print("\nDimension table schema:")
    dimension_df.printSchema()

    # Write tables
    write_table(fact_df, table_format, "fact_table")
    write_table(dimension_df, table_format, "dimension_table")

    # Verify test cases
    print("Verifying test cases in dimension table:")
    dimension_df.filter("city_id = 1 AND eater_id = 100 AND feature_id = 1000").show(
        vertical=True
    )

    print("\nVerifying test cases in fact table:")
    fact_df.filter("city_id = 1 AND eater_id = 100 AND feature_id = 1000").show(
        vertical=True
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--table-format",
        type=str,
        default="parquet",
        choices=["parquet", "delta", "iceberg"],
        help="Table format to use",
    )
    args = parser.parse_args()

    generate_test_data(table_format=args.table_format)
