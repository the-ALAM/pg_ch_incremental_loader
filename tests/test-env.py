import clickhouse_connect
from dotenv import load_dotenv
import os

def test_env_var():
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), 'test.env'))

    print('CLICKHOUSE_HOST: ', os.environ.get("CLICKHOUSE_HOST"))
    print('CLICKHOUSE_PORT: ', int(os.environ.get("CLICKHOUSE_PORT")))
    print('CLICKHOUSE_DB: ', os.environ.get("CLICKHOUSE_DB"))
    print('CLICKHOUSE_USER: ', os.environ.get("CLICKHOUSE_USER"))
    print('CLICKHOUSE_PASSWORD: ', os.environ.get("CLICKHOUSE_PASSWORD"))

    print('POSTGRES_HOST: ', os.environ.get("POSTGRES_HOST"))
    print('POSTGRES_PORT: ', int(os.environ.get("POSTGRES_PORT")))
    print('POSTGRES_DB: ', os.environ.get("POSTGRES_DB"))
    print('POSTGRES_USER: ', os.environ.get("POSTGRES_USER"))
    print('POSTGRES_PASSWORD: ', os.environ.get("POSTGRES_PASSWORD"))

if __name__ == "__main__":
    print("Testing env var...")
    test_env_var()
    print("env var test completed.")
