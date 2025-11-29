import os
import psycopg2
from dotenv import load_dotenv

def test_postgres_connection():
    try:
        load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), 'test.env'))

        pg_host = os.environ.get("POSTGRES_HOST", "localhost")
        pg_port = int(os.environ.get("POSTGRES_PORT", "5432"))
        pg_user = os.environ.get("POSTGRES_USER", "postgres")
        pg_password = os.environ.get("POSTGRES_PASSWORD", "postgres")
        pg_database = os.environ.get("POSTGRES_DB", "postgres")

        conn = psycopg2.connect(
            host=pg_host,
            port=pg_port,
            user=pg_user,
            password=pg_password,
            dbname=pg_database
        )
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        result = cur.fetchone()
        print("Connection successful. Query result:", result)
        cur.close()
        conn.close()
    except Exception as e:
        print("Failed to connect or execute query:", e)

if __name__ == "__main__":
    test_postgres_connection()
