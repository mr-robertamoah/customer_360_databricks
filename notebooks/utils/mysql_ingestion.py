import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

def ingest_mysql_table(spark: SparkSession, table_name: str, data: dict) -> DataFrame:
    return spark.read.format("mysql") \
        .option("dbtable", table_name) \
        .option("host", data["host"]) \
        .option("port", data["port"] or "3306") \
        .option("database", data["database"]) \
        .option("user", data["user"] or "root") \
        .option("password", data["password"] or "") \
        .load()

def write_ingested_mysql_dataframe(df: DataFrame, table_name: str, mode: str = 'overwrite') -> None:
    df \
        .withColumn("ingest_ts", F.current_timestamp()) \
        .write \
        .format('delta') \
        .mode(mode) \
        .saveAsTable(table_name)