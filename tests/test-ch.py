import clickhouse_connect
from dotenv import load_dotenv
import os

def test_clickhouse_connection():
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), 'test.env'))

    ch_host = os.environ.get("CLICKHOUSE_HOST")
    ch_port = int(os.environ.get("CLICKHOUSE_PORT"))
    ch_database = os.environ.get("CLICKHOUSE_DB")
    ch_user = os.environ.get("CLICKHOUSE_USER")
    ch_password = os.environ.get("CLICKHOUSE_PASSWORD")

    try:
        client = clickhouse_connect.get_client(
            host=ch_host,
            port=ch_port,
            username=ch_user,
            password=ch_password,
            database=ch_database
        )
        result = client.command('SELECT True;')
        print("Connection successful. Query result:", result)
    except Exception as e:
        print("Failed to connect or execute query:", e)

if __name__ == "__main__":
    print("Testing ClickHouse connection...")
    test_clickhouse_connection()
    print("ClickHouse connection test completed.")
