import os
from clickhouse_driver import Client
from dotenv import load_dotenv

def test_clickhouse_connection():
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), 'test.env'))

    ch_host = os.environ.get("CLICKHOUSE_HOST", "localhost")
    ch_port = int(os.environ.get("CLICKHOUSE_PORT", "9000"))
    ch_user = os.environ.get("CLICKHOUSE_USER", "default")
    ch_password = os.environ.get("CLICKHOUSE_PASSWORD", "")
    ch_database = os.environ.get("CLICKHOUSE_DB", "default")

    try:
        client = Client(
            host=ch_host,
            port=ch_port,
            user=ch_user,
            password=ch_password,
            database=ch_database
        )
        result = client.execute('SELECT 1')
        print("Connection successful. Query result:", result)
    except Exception as e:
        print("Failed to connect or execute query:", e)

if __name__ == "__main__":
    test_clickhouse_connection()
