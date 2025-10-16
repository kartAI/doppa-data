import duckdb

duckdb_context: duckdb.DuckDBPyConnection = duckdb.connect()
duckdb_context.install_extension("spatial")
duckdb_context.load_extension("spatial")
