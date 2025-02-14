from utils import build_table_query, init_spark, write_table

def test_left_join(table_format, test_join):
    spark = init_spark("Left Join Test", table_format)

    # Read tables
    dimension_df = build_table_query(spark, table_format, "dimension_table").hint("shuffle_hash")
    fact_df = build_table_query(spark, table_format, "fact_table").hint("shuffle_hash")

    # Perform left join
    result = dimension_df.join(
        fact_df,
        on=["city_id", "eater_id", "feature_id"],
        how=test_join
    )

    print("Joining plan")
    result.explain(True)

    # Check before writing
    print("Before writing - Test case results:")
    before_result = result.filter(
        "city_id = 1 AND eater_id = 100 AND feature_id = 1000"
    )
    before_result.show(truncate=False)
    before_count = before_result.count()


    # Remove the path /app/data/parquet/test_left_join first
    import shutil
    import os

    # Remove the existing test_left_join directory if it exists
    result_table_name = f"test_{test_join}_join"
    test_path = "/app/data/parquet/test_left_join"
    if os.path.exists(test_path):
        shutil.rmtree(test_path)

    # Write to new location with random suffix
    write_table(result, table_format, result_table_name, "country_id")

    # Read back and check
    read_back_df = build_table_query(spark, table_format, result_table_name)
    print("\nAfter writing - Test case results:")
    after_result = read_back_df.filter(
        "city_id = 1 AND eater_id = 100 AND feature_id = 1000"
    )
    after_result.show(truncate=False)
    after_count = after_result.count()

    # Compare results
    print("\nComparison:")
    print(f"Before count: {before_count}")
    print(f"After count: {after_count}")
    print(f"Counts match: {before_count == after_count}")

    # Compare data content
    print("\nDetailed comparison:")
    before_collected = before_result.collect()
    after_collected = after_result.collect()
    print("Row-by-row comparison:")
    for i, (before_row, after_row) in enumerate(zip(before_collected, after_collected)):
        print(f"\nRow {i+1}:")
        print(f"Before: {before_row}")
        print(f"After:  {after_row}")

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
    parser.add_argument(
        "--test-join",
        type=str,
        default="left",
        choices=["left", "inner", "right"],
        help="The join algo to use e.g. left, inner, right",
    )
    args = parser.parse_args()

    test_left_join(table_format=args.table_format, test_join=args.test_join)
