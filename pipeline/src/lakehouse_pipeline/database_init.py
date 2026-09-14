import os
import time
from importlib.resources import files

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
                migration_root = files("lakehouse_pipeline.sql")
                migrations = sorted(
                    migration
                    for migration in migration_root.iterdir()
                    if migration.name.endswith(".sql")
                )
                for migration in migrations:
                    script = migration.read_text(encoding="utf-8")
                    for statement in script.split(";"):
                        if statement.strip():
                            connection.execute(statement)
            print("PostgreSQL connection verified; database migrations are ready.")
            return
        except psycopg.OperationalError:
            if attempt == 12:
                raise
            print(f"PostgreSQL is not ready (attempt {attempt}/12); retrying in 5s.")
            time.sleep(5)


if __name__ == "__main__":
    initialize_database()
