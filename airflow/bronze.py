from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, max, coalesce, lit
from delta.tables import DeltaTable
import sys
from bronze_validator import validate_dataframe
import logging
import os

TABLE_KEYS = {
    "branch": ["bran_id"],
    "member": ["member_id"],
    "orders": ["order_id"],
    "order_detail": ["order_detail_id"],
    "pizza": ["pizza_id"],
    "pizza_types": ["pizza_type_id"],
    "pizza_type_topping": ["pizza_type_id", "pizza_topping_id"], 
    "topping": ["pizza_topping_id"]
}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = SparkSession.builder \
    .appName("Bronze_ETL") \
    .getOrCreate()

# 경로 및 RDB 정보
bronze_path = "gs://pz-buck-888/bronze/"
watermark_path = "gs://pz-buck-888/watermarks/"

db_host = os.getenv("DB_HOST", "")
db_port = os.getenv("DB_PORT", "")
db_name = os.getenv("DB_NAME", "")
db_pw = os.getenv("DB_PW", "")
db_user = os.getenv("DB_USER", "")

jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
jdbc_props = {"user": db_user, "password": db_pw, "driver": "org.postgresql.Driver"}

tables = ["branch", "member", "orders", "order_detail", "pizza", "pizza_types", "pizza_type_topping", "topping"]

for table in tables:
    default_timestamp_for_null = '1900-01-01 00:00:01' 

    try:
        df_wm = spark.read.format("delta").load(f"{watermark_path}{table}")
        wm_row = df_wm.collect()
        last_watermark = wm_row[0]["watermark_value"] if wm_row else "1900-01-01 00:00:00"

        if last_watermark is None:
            last_watermark = "1900-01-01 00:00:00"
    except Exception as e:
        logger.info(f"Watermark not found for {table} or other error: {e}. Defaulting to initial watermark.")
        last_watermark = "1900-01-01 00:00:00"

    query = f"(SELECT * FROM {table} WHERE COALESCE(last_updated_timestamp, '{default_timestamp_for_null}') > '{last_watermark}') AS tmp"
    df_new = spark.read.jdbc(url=jdbc_url, table=query, properties=jdbc_props)

    if df_new.isEmpty():
        logger.info(f"No new data for {table}, skipping...")
        continue

    df_new = df_new.withColumn("etl_load_timestamp", current_timestamp())

    # 검증 단계
    errors = validate_dataframe(df_new, table)
    if errors:
        logger.error(f"[Validation Failed] {table}: {errors}")
        continue

    # Bronze Delta 테이블 MERGE
    bronze_table_path = f"{bronze_path}{table}"
    if not DeltaTable.isDeltaTable(spark, bronze_table_path):
        df_new.write.format("delta").mode("overwrite").save(bronze_table_path)
    else:
        delta_table = DeltaTable.forPath(spark, bronze_table_path)

        delta_table.alias("target") \
            .merge(df_new.alias("source"), f"target.{TABLE_KEYS[table][0]} = source.{TABLE_KEYS[table][0]}") \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()

    # 워터마크 업데이트
    new_wm_df = df_new.select(
        max(
            coalesce(col("last_updated_timestamp"), lit(default_timestamp_for_null))
        ).alias("new_watermark")
    )
    new_wm = new_wm_df.collect()[0]["new_watermark"]

    if new_wm is not None and new_wm > last_watermark:
        new_watermark_to_write = new_wm
    else:
        new_watermark_to_write = last_watermark

    spark.createDataFrame([(table, new_watermark_to_write)], ["table_name", "watermark_value"]) \
        .write.format("delta").mode("overwrite").save(f"{watermark_path}{table}")

    logger.info(f"Successfully processed {table}. New watermark: {new_watermark_to_write}")
spark.stop()
