import duckdb

from src import Config


def create_duckdb_context() -> duckdb.DuckDBPyConnection:
    db_context: duckdb.DuckDBPyConnection = duckdb.connect()
    db_context.install_extension("spatial")
    db_context.load_extension("spatial")
    db_context.install_extension("azure")
    db_context.load_extension("azure")

    db_context.execute("""
    CREATE OR REPLACE SECRET azure_secret(
        TYPE azure,
        CONNECTION_STRING ?
    );
    """, [Config.BLOB_STORAGE_CONNECTION_STRING])

    db_context.execute("SET azure_storage_connection_string = ?;", [Config.BLOB_STORAGE_CONNECTION_STRING])

    return db_context
