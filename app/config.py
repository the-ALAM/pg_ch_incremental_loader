from dotenv import load_dotenv
import os

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

class Config:
    """Central configuration class to validate and serve env vars."""
    
    #! pg
    PG_HOST = os.environ.get("POSTGRES_HOST")
    PG_PORT = os.environ.get("POSTGRES_PORT")
    PG_USER = os.environ.get("POSTGRES_USER")
    PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
    PG_DB = os.environ.get("POSTGRES_DB")
    
    #! ch
    CH_HOST = os.environ.get("CLICKHOUSE_HOST")
    CH_PORT = os.environ.get("CLICKHOUSE_PORT")
    CH_USER = os.environ.get("CLICKHOUSE_USER")
    CH_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD")
    CH_DB = os.environ.get("CLICKHOUSE_DB")

    #! jars
    JAR_POSTGRES = "/mnt/d/code/pg_ch_incremental_loader/spark/jar/postgresql.jar"
    JAR_CLICKHOUSE = "/mnt/d/code/pg_ch_incremental_loader/spark/jar/clickhouse-jdbc-0.9.4-all-dependencies.jar"

    @classmethod
    def get_pg_url(cls):
        return f"jdbc:postgresql://{cls.PG_HOST}:{cls.PG_PORT}/{cls.PG_DB}"

    @classmethod
    def get_ch_url(cls):
        return f"jdbc:clickhouse://{cls.CH_HOST}:{cls.CH_PORT}/{cls.CH_DB}"
