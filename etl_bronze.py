from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, max
from delta.tables import DeltaTable
import sys

# --- 1. SparkSession 설정 ---
jdbc_driver_path = "/path/to/postgresql-42.2.18.jar"

spark = SparkSession.builder \
    .appName("Bronze_ETL") \
    .config("spark.jars", jdbc_driver_path) \
    .config("spark.jars", "/opt/spark/jars/gcs-connector-hadoop3-2.2.11-shaded.jar") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/opt/spark/secrets/gcp-key.json") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# --- 2. 경로 및 RDB 정보 ---
bronze_path = "gs://your-bucket/delta/bronze/"
watermark_path = "gs://your-bucket/delta/watermarks/"

jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
jdbc_props = {"user": "postgres", "password": "1234", "driver": "org.postgresql.Driver"}

tables = ["branch", "member", "orders", "order_detail", "pizza", "pizza_types", "pizza_type_topping", "topping"]

for table in tables:
    # 워터마크 로딩
    try:
        df_wm = spark.read.format("delta").load(f"{watermark_path}{table}")
        wm_row = df_wm.collect()
        last_watermark = wm_row[0]["watermark_value"] if wm_row else "1900-01-01 00:00:00"
    except:
        last_watermark = "1900-01-01 00:00:00"

    # 증분 데이터 로드
    query = f"(SELECT * FROM {table} WHERE last_updated_timestamp > '{last_watermark}') AS tmp"
    df_new = spark.read.jdbc(url=jdbc_url, table=query, properties=jdbc_props)

    if df_new.isEmpty():
        print(f"No new data for {table}, skipping...")
        continue

    df_new = df_new.withColumn("etl_load_timestamp", current_timestamp())

    # Bronze Delta 테이블 MERGE
    bronze_table_path = f"{bronze_path}{table}"
    if not DeltaTable.isDeltaTable(spark, bronze_table_path):
        df_new.write.format("delta").mode("overwrite").save(bronze_table_path)
        delta_table = DeltaTable.forPath(spark, bronze_table_path)
    else:
        delta_table = DeltaTable.forPath(spark, bronze_table_path)
        delta_table.alias("target") \
            .merge(df_new.alias("source"), f"target.{table}_id = source.{table}_id") \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()

    # 워터마크 업데이트
    new_wm = df_new.select(max("last_updated_timestamp")).collect()[0][0]
    spark.createDataFrame([(table, new_wm)], ["table_name", "watermark_value"]) \
        .write.format("delta").mode("overwrite").save(f"{watermark_path}{table}")

spark.stop()
