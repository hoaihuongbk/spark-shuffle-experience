from utils import init_spark, get_table_path

def optimize_table(spark, table_name, table_format):
    table_path = get_table_path(table_name, table_format)
    if table_format == "delta":
        spark.sql(f"OPTIMIZE delta.`{table_path}`").show(truncate=False)

def optimize_dataset(table_format):
    """
    Optimize the dataset with specified table format.
    Args:
        table_format (str): Format to save tables. Can be 'parquet', 'delta', or 'iceberg'. Default is 'parquet'.
    """
    spark = init_spark("Optimize Dataset", table_format)

    # Optimize the transactions table
    optimize_table(spark, "transactions", table_format)

    # Optimize the stores table
    optimize_table(spark, "stores", table_format)

    # Optimize the countries table
    optimize_table(spark, "countries", table_format)

    # spark.sql("DESC HISTORY delta.`/app/transactions`").show(truncate=False)

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

    print("Optimize dataset...")
    optimize_dataset(table_format=args.table_format)
