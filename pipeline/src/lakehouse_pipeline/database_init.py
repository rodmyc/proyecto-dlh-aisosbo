import os
import time

import psycopg


def initialize_database() -> None:
    connection_options = {
        "host": os.environ["DAGSTER_PG_HOST"],
        "port": int(os.getenv("DAGSTER_PG_PORT", "5432")),
        "dbname": os.environ["DAGSTER_PG_DB"],
        "user": os.environ["DAGSTER_PG_USER"],
        "password": os.environ["DAGSTER_PG_PASSWORD"],
        "connect_timeout": 10,
        "autocommit": True,
    }

    for attempt in range(1, 13):
        try:
            with psycopg.connect(**connection_options) as connection:
                connection.execute("CREATE SCHEMA IF NOT EXISTS raw")
                connection.execute("CREATE SCHEMA IF NOT EXISTS analytics")
            print("PostgreSQL connection verified; raw and analytics schemas are ready.")
            return
        except psycopg.OperationalError:
            if attempt == 12:
                raise
            print(f"PostgreSQL is not ready (attempt {attempt}/12); retrying in 5s.")
            time.sleep(5)


if __name__ == "__main__":
    initialize_database()
