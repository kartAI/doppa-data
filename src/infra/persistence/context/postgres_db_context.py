from sqlalchemy import Engine, create_engine

from src import Config


def create_postgres_db_context() -> Engine:
    """
    Creates a SQLAlchemy engine for the configured PostgreSQL Flexible Server using SSL. Ensures
    the `postgis` extension exists in the target database before returning the engine. The engine
    enables `pool_pre_ping` so stale connections are detected before use.
    :return: SQLAlchemy engine connected to the PostgreSQL/PostGIS database.
    :rtype: Engine
    """
    conn_str = (
        f"postgresql+psycopg2://{Config.POSTGRES_USERNAME}:{Config.POSTGRES_PASSWORD}"
        f"@{Config.POSTGRES_HOST}:{Config.POSTGRES_PORT}/{Config.POSTGRES_DB}"
        f"?sslmode=require"
    )

    engine = create_engine(
        conn_str,
        future=True,
        pool_pre_ping=True,
    )

    with engine.connect() as conn:
        conn.exec_driver_sql("CREATE EXTENSION IF NOT EXISTS postgis;")
        conn.commit()

    return engine
