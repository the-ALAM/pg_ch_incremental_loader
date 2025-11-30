from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os


"""
TODO - 
struct logging
no spark logs
---
scheduler (cron)
    load env var
    create spark session
    connect to dbs
    incremental sync
        get last sync timestamp from clickhouse
        read newer records from postgres
        write them back to clickhouse
    stop the session
"""

#! creds
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

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

def main():
    print("Hello from pg-ch-streamer!")
    #! spark configs
    spark = SparkSession.builder \
        .appName("pg-ch-incremental-loader") \
        .config(
            "spark.jars",
            "~/spark/jars/clickhouse-jdbc-0.9.4-all-dependencies.jar,"
            "~/spark/jars/postgresql.jar"
        ) \
        .getOrCreate()

    #! pg read
    pg_df = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_database}") \
        .option("dbtable", "public.app_user_visits_fact") \
        .option("user", pg_user) \
        .option("password", pg_password) \
        .option("driver", "org.postgresql.Driver") \
        .load()
    pg_df.show(10)

if __name__ == "__main__":
    main()
