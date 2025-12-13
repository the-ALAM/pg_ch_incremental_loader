from pyspark.sql.functions import col, greatest, lit, coalesce
from logger import setup_logger #, suppress_spark_logs
from pyspark.sql import SparkSession
from config import Config


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
    Queries ClickHouse to find the maximum timestamps for both updated_at and created_at.
    Returns a tuple (last_updated_at, last_created_at, is_empty).
    If table is empty, returns (None, None, True).
    """
    query = """
    SELECT 
        max(updated_at) as last_updated_ts,
        max(created_at) as last_created_ts,
        CAST(count(*) AS Int32) as record_count
    FROM app_user_visits_fact
    """
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
        record_count = row['record_count'] if row['record_count'] else 0
        log.debug(f"row: {record_count}")
        
        if record_count == 0:
            log.info("ClickHouse table is empty - will perform full backfill")
            return (None, None, True)
        
        last_updated_ts = row['last_updated_ts']
        last_created_ts = row['last_created_ts']
        
        log.info(f"Found {record_count} records. Last updated_at: {last_updated_ts}, Last created_at: {last_created_ts}")
        return (last_updated_ts, last_created_ts, False)
        
    except Exception as e:
        log.warning(f"Could not fetch last timestamp (Table might be empty or new): {e}")
        return (None, None, True)

def main():
    spark = create_spark_session()
    #deprecated - modified it from the log4j prop conf
    # suppress_spark_logs(spark)
    print("-" * 100)
    log.info("Starting Incremental Sync Job...")

    #! last sync timestamps
    last_updated_ts, last_created_ts, is_empty = get_last_sync_timestamp(spark)
    log.debug(f"updated: {last_updated_ts}\ncreated: {last_created_ts}\nempty: {is_empty}")

    #! pg read - load all data from PostgreSQL
    try:
        print("-" * 100)
        log.info("Reading PostgreSQL records...")

        pg_df = spark.read \
            .format("jdbc") \
            .option("driver", "org.postgresql.Driver") \
            .option("url", Config.get_pg_url()) \
            .option("dbtable", "app_user_visits_fact") \
            .option("user", Config.PG_USER) \
            .option("password", Config.PG_PASSWORD) \
            .load()

        #! backfill if ch table empty
        if is_empty:
            log.info("ClickHouse table is empty. Loading all records from PostgreSQL...")
            total_count = pg_df.count()
            if total_count == 0:
                log.info("No records found in PostgreSQL. Sync complete.")
                return
            log.info(f"Found {total_count} records to sync (full backfill).")
            
            pg_df.write \
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
            
            log.info("Full backfill successful. Job finished.")
            return

        #! dual stream incremental sync
        log.info(f"Performing incremental sync. Last updated_at: {last_updated_ts}, Last created_at: {last_created_ts}")
        
        #! updated records
        if last_updated_ts is not None:
            updated_df = pg_df.filter(
                col("updated_at").isNotNull() & 
                (col("updated_at") > last_updated_ts)
            )
        else:
            updated_df = pg_df
        
        #! created records
        if last_created_ts is not None:
            created_df = pg_df.filter(
                col("updated_at").isNull() & 
                col("created_at").isNotNull() & 
                (col("created_at") > last_created_ts)
            )
        else:
            created_df = pg_df
        
        updated_count = updated_df.count()
        created_count = created_df.count()
        
        log.info(f"Found {updated_count} updated records and {created_count} created records to sync.")
        
        if updated_count == 0 and created_count == 0:
            log.info("No new records found. Sync complete.")
            return

        #! ch load newly updated
        if updated_count > 0:
            log.info(f"Writing {updated_count} updated records to ClickHouse...")
            updated_df.write \
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
            log.info("Updated records written successfully.")

        #! ch load newly created
        if created_count > 0:
            log.info(f"Writing {created_count} created records to ClickHouse...")
            created_df.write \
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
            log.info("Created records written successfully.")

        log.info("Incremental sync completed successfully. Job finished.")

    except Exception as e:
        log.error(f"Sync Job Failed: {e}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
