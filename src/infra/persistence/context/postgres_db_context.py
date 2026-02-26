from sqlalchemy import Engine, create_engine

from src import Config


def create_postgres_db_context() -> Engine:
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
