from pyspark.sql import SparkSession
from delta.tables import DeltaTable

spark = SparkSession.builder \
    .appName("Silver_ETL") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# GCS 경로로 수정
bronze_path = "gs://your-bucket/bronze/"
silver_path = "gs://your-bucket/silver/"

# 예시: 주문 중심 Silver 테이블
df_orders = spark.read.format("delta").load(f"{bronze_path}orders")
df_order_detail = spark.read.format("delta").load(f"{bronze_path}order_detail")
df_pizza = spark.read.format("delta").load(f"{bronze_path}pizza")
df_pizza_type = spark.read.format("delta").load(f"{bronze_path}pizza_types")
df_member = spark.read.format("delta").load(f"{bronze_path}member")
df_branch = spark.read.format("delta").load(f"{bronze_path}branch")
df_pizza_topping = spark.read.format("delta").load(f"{bronze_path}topping")
df_bridge = spark.read.format("delta").load(f"{bronze_path}pizza_type_topping")

# join 및 정제
df_silver = df_order_detail \
    .join(df_orders, "order_id", "left") \
    .join(df_pizza, "pizza_id", "left") \
    .join(df_pizza_type, "pizza_type_id", "left") \
    .join(df_member, "member_id", "left") \
    .join(df_branch, "bran_id", "left") \
    .join(df_bridge, "pizza_type_id", "left") \
    .join(df_pizza_topping, "pizza_topping_id", "left")

# Silver 적재 → GCS에 Delta 포맷 저장
df_silver.write.format("delta").mode("overwrite").save(f"{silver_path}ods_orders")

spark.stop()
