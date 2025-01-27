from pyspark.sql.functions import *
from utils import write_table, init_spark


def prepare_dataset(table_format="parquet"):
    """
    Prepare the dataset with specified table format.
    Args:
        table_format (str): Format to save tables. Can be 'parquet', 'delta', or 'iceberg'. Default is 'parquet'.
    """
    spark = init_spark("Prepare Dataset", table_format)

    ## Create the transactions table
    transaction_size = 150000000
    transactions_df = spark.range(0, transaction_size).select(
        "id",
        round(rand() * 10000, 2).alias("amount"),
        (col("id") % 10).alias("country_id"),
        (col("id") % 100).alias("store_id"),
    )
    write_table(transactions_df, table_format, "transactions")

    ## Create the stores table
    stores_df = spark.range(0, 99).select(
        "id",
        round(rand() * 100, 0).alias("employees"),
        (col("id") % 10).alias("country_id"),
        expr("uuid()").alias("name"),
    )
    write_table(stores_df, table_format, "stores")

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
    countries_df = spark.createDataFrame(data=countries, schema=columns)
    write_table(countries_df, table_format, "countries")


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

    print("Preparing dataset...")
    prepare_dataset(table_format=args.table_format)
