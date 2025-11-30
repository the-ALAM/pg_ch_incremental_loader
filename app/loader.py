from pyspark.sql.functions import col, greatest, lit, coalesce
from logger import setup_logger #, suppress_spark_logs
from pyspark.sql import SparkSession
from config import Config
import time


log = setup_logger()
#TODO - create the DDL for the logs table and stream it

def create_spark_session():
    """Configures and returns the SparkSession with necessary JDBC drivers."""
    jars = f"{Config.JAR_POSTGRES},{Config.JAR_CLICKHOUSE}"

    return SparkSession.builder \
        .appName("pg-ch-incremental-loader") \
        .config("spark.jars", jars) \
        .getOrCreate()

def get_last_sync_timestamp(spark):
    """
    Queries ClickHouse to find the maximum timestamp.
    """
    #TODO - add 'created_at' fallback to handle first run condition and return a tuple with the column name and value -> (str, str)
    #TODO - add backfill from min ts if ch table is empty

    query = "SELECT max(updated_at) as last_ts FROM app_user_visits_fact"
    try:
        df = spark.read \
            .format("jdbc") \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .option("url", Config.get_ch_url()) \
            .option("user", Config.CH_USER) \
            .option("password", Config.CH_PASSWORD) \
            .option("query", query) \
            .load()
        
        row = df.collect()[0]
        return row['last_ts']
    except Exception as e:
        log.warning(f"Could not fetch last timestamp (Table might be empty or new): {e}")
        return None

def main():
    spark = create_spark_session()
    #deprecated - modified it from the log4j prop conf
    # suppress_spark_logs(spark)
    log.info("Starting Incremental Sync Job...")

    #! cutoff ts
    last_sync_ts = get_last_sync_timestamp(spark)
    log.info(f"last_sync_ts: {last_sync_ts}")
    
    if last_sync_ts is not None and last_sync_ts > 0:
        log.info(f"Last sync detected at timestamp: {last_sync_ts}")
        cutoff_ts = last_sync_ts
    else:
        #! lookback in increments of a day to handle first run condition with milliseconds int64 epochs
        current_time = int(time.time() * 1000) 
        time_block = 12 * 30 * 24 * 60 * 60 * 1000
        cutoff_ts = current_time - time_block
        log.info(f"No previous data found. trying to backfill the data in a one year lookback period: {cutoff_ts}")

    #! pg read
    try:
        log.info(f"Reading PostgreSQL records modified after {cutoff_ts}...")
        
        pg_df = spark.read \
            .format("jdbc") \
            .option("driver", "org.postgresql.Driver") \
            .option("url", Config.get_pg_url()) \
            .option("dbtable", "app_user_visits_fact") \
            .option("user", Config.PG_USER) \
            .option("password", Config.PG_PASSWORD) \
            .load()

        incremental_df = pg_df.filter(
            (col("updated_at") > cutoff_ts) | 
            (col("updated_at").isNull() & (col("created_at") > cutoff_ts))
        )

        record_count = incremental_df.count()
        if record_count == 0:
            log.info("No new records found. Sync complete.")
            return

        log.info(f"Found {record_count} new/updated records to sync.")

        #! ch write
        #FIXME - change ch table engine to handle duplicate consolidation
        log.info("Writing records to ClickHouse...")
        
        incremental_df.write \
            .format("jdbc") \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .option("url", Config.get_ch_url()) \
            .option("user", Config.CH_USER) \
            .option("password", Config.CH_PASSWORD) \
            .option("dbtable", "app_user_visits_fact") \
            .option("batchsize", "5000") \
            .option("isolationLevel", "NONE") \
            .mode("append") \
            .save()
            
        log.info("Write successful. Job finished.")

    except Exception as e:
        log.error(f"Sync Job Failed: {e}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
