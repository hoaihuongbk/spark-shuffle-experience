from pyspark.sql import SparkSession


def get_table_path(table_name):
    return f"/app/{table_name}"


def get_iceberg_full_table_name(table_name):
    return f"iceberg.db.{table_name}"


def build_table_query(spark, table_format, table_name):
    table_path = get_table_path(table_name)
    if table_format == "delta":
        df = spark.read.format("delta").load(table_path)
    elif table_format == "iceberg":
        # Iceberg written has a bit different API
        full_table_name = get_iceberg_full_table_name(table_name)
        df = spark.table(full_table_name)
    else:
        df = spark.read.parquet(table_path)

    return df


def load_table(spark, table_format, table_name):
    df = build_table_query(spark, table_format, table_name)
    df.createOrReplaceTempView(table_name)


# Helper function to write dataframe based on format
def write_table(df, table_format, table_name):
    writer = df.write.mode("overwrite")
    table_path = get_table_path(table_name)
    if table_format == "delta":
        return writer.format("delta").save(table_path)
    elif table_format == "iceberg":
        # Iceberg written has a bit different API
        full_table_name = get_iceberg_full_table_name(table_name)
        return df.writeTo(full_table_name).createOrReplace()
    else:
        return writer.parquet(table_path)


def validate_output(spark, table_format, table_name):
    df = build_table_query(spark, table_format, table_name)
    return df.count()


def init_spark(app_name, table_format):
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "/opt/spark/spark-events")
        .config("spark.history.fs.logDirectory", "/opt/spark/spark-events")
        .config("spark.sql.explain.codegen", "true")
        .config("spark.sql.explain.mode", "extended")
    )

    if table_format == "iceberg":
        spark = (
            builder.config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.iceberg.spark.SparkSessionCatalog",
            )
            .config("spark.sql.catalog.spark_catalog.type", "hive")
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", "/app/warehouse")
            .config("spark.sql.defaultCatalog", "local")
            .getOrCreate()
        )
    elif table_format == "delta":
        spark = (
            builder.config(
                "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .getOrCreate()
        )
    else:
        spark = builder.getOrCreate()

    return spark