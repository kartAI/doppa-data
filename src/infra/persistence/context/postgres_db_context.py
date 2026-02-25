import psycopg2
from psycopg2.extensions import connection, ISOLATION_LEVEL_AUTOCOMMIT, cursor

from src import Config


def create_postgres_db_context() -> cursor:
    postgres_db_connection: connection = psycopg2.connect(
        host=Config.POSTGRES_HOST,
        port=Config.POSTGRES_PORT,
        user=Config.POSTGRES_USERNAME,
        password=Config.POSTGRES_PASSWORD,
        dbname=Config.POSTGRES_DB
    )

    postgres_db_connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    postgres_db_context = postgres_db_connection.cursor()
    postgres_db_context.execute("CREATE EXTENSION IF NOT EXISTS postgis;")

    return postgres_db_context
