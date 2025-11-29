import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

def test_spark_connectivity():
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), 'test.env'))

    pg_host = os.environ.get("POSTGRES_HOST", "localhost")
    pg_port = int(os.environ.get("POSTGRES_PORT", "5432"))
    pg_user = os.environ.get("POSTGRES_USER", "postgres")
    pg_password = os.environ.get("POSTGRES_PASSWORD", "postgres")
    pg_database = os.environ.get("POSTGRES_DB", "postgres")

    ch_host = os.environ.get("CLICKHOUSE_HOST", "localhost")
    ch_port = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
    ch_user = os.environ.get("CLICKHOUSE_USER", "default")
    ch_password = os.environ.get("CLICKHOUSE_PASSWORD", "")
    ch_database = os.environ.get("CLICKHOUSE_DB", "default")

    print("Creating SparkSession...")
    spark = SparkSession.builder \
        .appName("SparkConnectivityTest") \
        .master("spark://spark-master:7077") \
        .config("spark.driver.host", "localhost") \
        .config("spark.jars", "/opt/spark/jars/postgresql.jar,/opt/spark/jars/clickhouse-jdbc.jar") \
        .getOrCreate()
    print("SparkSession created.")

    print("Submitting a simple Spark job...")
    rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5], 2)
    result = rdd.map(lambda x: x * 2).collect()
    print("Simple job result:", result)

    try:
        print("Testing connection to Postgres...")
        pg_df = spark.read \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_database}") \
            .option("dbtable", "information_schema.tables") \
            .option("user", pg_user) \
            .option("password", pg_password) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        print("Postgres connection successful. Tables sample:")
        pg_df.show(5)
    except Exception as e:
        print("Failed to connect to Postgres:", e)

    try:
        print("Testing connection to ClickHouse...")
        ch_df = spark.read \
            .format("jdbc") \
            .option("url", f"jdbc:clickhouse://{ch_host}:{ch_port}/{ch_database}") \
            .option("dbtable", "system.numbers") \
            .option("user", ch_user) \
            .option("password", ch_password) \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .option("fetchsize", "5") \
            .load()
        print("ClickHouse connection successful. Numbers sample:")
        ch_df.show(5)
    except Exception as e:
        print("Failed to connect to ClickHouse:", e)

    spark.stop()
    print("SparkSession stopped.")

if __name__ == "__main__":
    test_spark_connectivity()
