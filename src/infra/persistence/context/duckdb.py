import platform

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
        PROVIDER config,
        ACCOUNT_NAME ?
    );
    """, [Config.AZURE_BLOB_STORAGE_ACCOUNT_NAME])

    if platform.system() == "Linux":
        db_context.execute("SET azure_transport_option_type = curl")

    return db_context
