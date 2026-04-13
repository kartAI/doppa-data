# Databricks notebook source

# COMMAND ----------

from sedona.spark import SedonaContext

# COMMAND ----------

account_key = dbutils.widgets.get("account_key")
account_name = dbutils.widgets.get("account_name")
release = dbutils.widgets.get("release")
municipalities_file = dbutils.widgets.get("municipalities_file")

# COMMAND ----------

spark.conf.set(
    f"fs.azure.account.key.{account_name}.dfs.core.windows.net",
    account_key,
)

sedona = SedonaContext.create(spark)

# COMMAND ----------

buildings_path = (
    f"abfss://data@{account_name}.dfs.core.windows.net"
    f"/release/{release}/theme=buildings/region=*/*.parquet"
)
municipalities_path = (
    f"abfss://metadata@{account_name}.dfs.core.windows.net"
    f"/{municipalities_file}"
)

buildings_df = sedona.read.format("geoparquet").load(buildings_path)

municipalities_raw = spark.read.parquet(municipalities_path)
municipalities_df = municipalities_raw.selectExpr(
    "ST_GeomFromWKB(wkb) AS geometry",
    "region AS municipality_name",
)

buildings_df.createOrReplaceTempView("buildings")
municipalities_df.createOrReplaceTempView("municipalities")

# COMMAND ----------

result = sedona.sql("""
    SELECT
        m.municipality_name,
        COUNT(b.geometry) AS building_count
    FROM municipalities m
    JOIN buildings b
      ON ST_Intersects(m.geometry, b.geometry)
    GROUP BY m.municipality_name
    ORDER BY building_count DESC
""")

count = result.count()
print(f"Spatial join complete. Regions with matched buildings: {count}")
