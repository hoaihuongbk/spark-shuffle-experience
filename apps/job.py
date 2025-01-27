from utils import write_table, load_table, init_spark, validate_output


def run_shuffle_test(test_type, plugin, table_format):
    # Initialize Spark session
    spark = init_spark("Shuffle Test", table_format)

    ## Load the source data and create a temporary view
    load_table(spark, table_format, "transactions")
    load_table(spark, table_format, "stores")
    load_table(spark, table_format, "countries")

    if test_type == "join":
        ## Disabling the automatic broadcast join entirely. That is, Spark will never broadcast any dataset for joins, regardless of its size.
        ## spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

        ## Test join w/o broadcast
        joined_df_no_broadcast = spark.sql("""
            SELECT 
                transactions.id,
                amount,
                countries.name as country_name,
                employees,
                stores.name as store_name
            FROM
                transactions
            LEFT JOIN
                stores
                ON
                    transactions.store_id = stores.id
            LEFT JOIN
                countries
                ON
                    transactions.country_id = countries.id
        """)
        ## Create a table with the joined data
        output_table_name = plugin + "_transact_countries"
        write_table(joined_df_no_broadcast, table_format, output_table_name)

        ## Validate the output
        # total_rows = validate_output(spark, table_format, output_table_name)
        # print(total_rows)

    elif test_type == "aggregate":
        ## Test groupBy
        grouped_df = spark.sql("""
            SELECT 
              country_id, 
              COUNT(*) AS count,
              AVG(amount) AS avg_amount
            FROM transactions
            GROUP BY country_id
        """)

        ## Create a table with the grouped_df data
        output_table_name = plugin + "_country_agg"
        write_table(grouped_df, table_format, output_table_name)

        ## Validate the output
        # total_rows = validate_output(spark, table_format, output_table_name)
        # print(total_rows)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--test-type",
        type=str,
        default="join",
        choices=["join", "aggregate"],
        help="Kind of testing",
    )
    parser.add_argument(
        "--plugin",
        type=str,
        default="none",
        choices=["none", "comet"],
        help="Kind of plugin",
    )
    parser.add_argument(
        "--table-format",
        type=str,
        default="parquet",
        choices=["parquet", "delta", "iceberg"],
        help="Table format to use",
    )
    args = parser.parse_args()

    run_shuffle_test(
        test_type=args.test_type, plugin=args.plugin, table_format=args.table_format
    )
