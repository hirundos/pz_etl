from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, year, month, dayofmonth, dayofweek, expr

# --- 1. SparkSession 설정 ---
spark = SparkSession.builder \
    .appName("Gold_ETL") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# --- 2. 경로 정의 (GCS) ---
silver_path = "gs://pz-buck-888/silver/"

# --- 3. Silver 읽기 ---
df_silver = spark.read.format("delta").load(f"{silver_path}ods_orders")

# --- 4. 차원 테이블 생성 및 BigQuery 적재 ---
# dim_pizza
df_dim_pizza = df_silver.select("pizza_id", "pizza_type_id", "size", "price").distinct()
df_dim_pizza.write.format("bigquery") \
    .option("table", "your_project.your_dataset.dim_pizza") \
    .mode("overwrite") \
    .save()

# dim_member
df_dim_member = df_silver.select("member_id", "member_nm").distinct()
df_dim_member.write.format("bigquery") \
    .option("table", "your_project.your_dataset.dim_member") \
    .mode("overwrite") \
    .save()

# dim_pizza_type
df_dim_ptype = df_silver.select("pizza_type_id", "pizza_nm", "pizza_categ").distinct()
df_dim_ptype.write.format("bigquery") \
    .option("table", "your_project.your_dataset.dim_pizza_type") \
    .mode("overwrite") \
    .save()

# dim_date
df_dim_date = df_silver.select(to_date("date").alias("date")) \
    .distinct() \
    .withColumn("year", year("date")) \
    .withColumn("month", month("date")) \
    .withColumn("day", dayofmonth("date")) \
    .withColumn("weekday", dayofweek("date")) \
    .withColumn("is_weekend", expr("CASE WHEN dayofweek(date) IN (1,7) THEN 1 ELSE 0 END"))

df_dim_date.write.format("bigquery") \
    .option("table", "your_project.your_dataset.dim_date") \
    .mode("overwrite") \
    .save()

# fact_order
df_fact_order = df_silver.select(
    "order_detail_id","order_id","date","member_id","pizza_id","pizza_type_id","pizza_topping_id",
    "time","size","quantity","price","bran_id"
).withColumnRenamed("price","unit_price") \
 .withColumn("total_price", expr("unit_price * quantity"))

df_fact_order.write.format("bigquery") \
    .option("table", "your_project.your_dataset.fact_order") \
    .mode("overwrite") \
    .save()

spark.stop()
