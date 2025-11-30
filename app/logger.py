import logging
import sys

def setup_logger(name="pg-ch-sync"):
    """
    Configures a clean logger.
    Suppresses internal Spark JVM logs to keep console output readable.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)s | %(module)s | %(message)s', 
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger

def suppress_spark_logs(spark_session):
    """Reduces Spark verbosity."""
    spark_session.sparkContext.setLogLevel("ERROR")
