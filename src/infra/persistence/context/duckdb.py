import duckdb

def create_duckdb_context() -> duckdb.DuckDBPyConnection:
    db_context: duckdb.DuckDBPyConnection = duckdb.connect()
    db_context.install_extension("spatial")
    db_context.load_extension("spatial")

    return db_context
