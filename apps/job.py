from pyspark.sql import SparkSession


def load_transactions(spark):
    spark.read.parquet("/app/transactions/*.parquet").createOrReplaceTempView(
        "transactions"
    )

def load_stores(spark):
    spark.read.parquet("/app/stores/*.parquet").createOrReplaceTempView("stores")


def load_countries(spark):
    spark.read.parquet("/app/countries/*.parquet").createOrReplaceTempView("countries")


def run_shuffle_test(test_type, plugin):
    spark = (
        SparkSession.builder.appName("ShuffleTest")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "/opt/spark/spark-events")
        .config("spark.history.fs.logDirectory", "/opt/spark/spark-events")
        .config("spark.sql.explain.codegen", "true")
        .config("spark.sql.explain.mode", "extended")
        .getOrCreate()
    )

    ## Load the source data and create a temporary view
    load_transactions(spark)
    load_stores(spark)
    load_countries(spark)

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
        (
            joined_df_no_broadcast.write.mode("overwrite").parquet(
                "/app/" + plugin + "_transact_countries"
            )
        )

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
        (grouped_df.write.mode("overwrite").parquet("/app/" + plugin + "_country_agg"))


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
