from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

#! creds

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "test.env"))

pg_host = os.environ.get("POSTGRES_HOST")
pg_port = int(os.environ.get("POSTGRES_PORT"))
pg_user = os.environ.get("POSTGRES_USER")
pg_password = os.environ.get("POSTGRES_PASSWORD")
pg_database = os.environ.get("POSTGRES_DB")

ch_host = os.environ.get("CLICKHOUSE_HOST")
ch_port = int(os.environ.get("CLICKHOUSE_PORT"))
ch_user = os.environ.get("CLICKHOUSE_USER")
ch_password = os.environ.get("CLICKHOUSE_PASSWORD")
ch_database = os.environ.get("CLICKHOUSE_DB")

#! setup

spark = SparkSession.builder \
    .appName("pg-ch-test") \
    .config(
        "spark.jars",
        "~/spark/jars/clickhouse-jdbc-0.9.4-all-dependencies.jar,"
        # "~/spark/jars/clickhouse-jdbc.jar,"
        "~/spark/jars/postgresql.jar"
    ) \
    .getOrCreate()

#! load

pg_df = spark.read \
    .format("jdbc") \
    .option("url", f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_database}") \
    .option("dbtable", "public.app_user_visits_fact") \
    .option("user", pg_user) \
    .option("password", pg_password) \
    .option("driver", "org.postgresql.Driver") \
    .load()
pg_df.show(10)

#! save

pg_df.write \
    .format("jdbc") \
    .option("url", f"jdbc:clickhouse://{ch_host}:{ch_port}/{ch_database}") \
    .option("dbtable", "app_user_visits_fact") \
    .option("user", ch_user) \
    .option("password", ch_password) \
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
    .mode("append") \
    .save()

#! verify

ch_df = spark.read \
    .format("jdbc") \
    .option("url", f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_database}") \
    .option("dbtable", "public.app_user_visits_fact") \
    .option("user", pg_user) \
    .option("password", pg_password) \
    .option("driver", "org.postgresql.Driver") \
    .load()
ch_df.show(10)
