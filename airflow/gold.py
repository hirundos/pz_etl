from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    to_date, year, month, dayofmonth, dayofweek, expr,
    date_format
)
from gold_validator import validate_dataframes
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def write_to_bq(df, table_name, project, dataset):
    full_table_id = f"{project}.{dataset}.{table_name}"
    logger.info(f"Writing to {full_table_id}...")
    df.write.format("bigquery") \
        .option("table", full_table_id) \
        .mode("overwrite") \
        .save()
    logger.info(f"{full_table_id} 적재 완료.")

def main():
    # SparkSession 설정
    spark = SparkSession.builder \
        .appName("Gold_ETL_With_Validation") \
        .config("temporaryGcsBucket", "pz-buck-888")
        .getOrCreate()
    
    logger.info("SparkSession 생성 완료.")

    # 경로 정의 (GCS)
    silver_path = "gs://pz-buck-888/silver/"
    bq_project = "pz-project-473804"
    bq_dataset = "pz_storg"

    df_silver = None
    try:
        # Silver 읽기 (Extract)
        df_silver = spark.read.format("delta").load(f"{silver_path}ods_orders")
        df_silver.cache()
        logger.info(f"Silver 테이블 (ods_orders) 로드 완료. (Total: {df_silver.count()} rows)")
    except Exception as e:
        logger.error(f"Silver 테이블 로드 실패: {e}")
        spark.stop()
        return

    try:
        # ETL (Transformation) 단계
        logger.info("Gold 테이블 변환 시작...")

        df_dim_pizza = df_silver.select("pizza_id", "pizza_type_id", "size", "price").distinct()
        
        guest_member_df = spark.createDataFrame(
            [("0", "비회원(Guest)")], 
            ["member_id", "member_nm"]
        )
        df_dim_member = df_silver.select("member_id", "member_nm") \
            .distinct() \
            .union(guest_member_df) \
            .distinct()
        
        df_dim_ptype = df_silver.select("pizza_type_id", "pizza_nm", "pizza_categ").distinct()
        df_dim_date = df_silver.select(to_date("date").alias("date")) \
            .distinct() \
            .withColumn("year", year("date")) \
            .withColumn("month", month("date")) \
            .withColumn("day", dayofmonth("date")) \
            .withColumn("weekday_num", dayofweek("date")) \
            .withColumn("weekday", date_format("date", "E")) \
            .withColumn("is_weekend", expr("CASE WHEN dayofweek(date) IN (1,7) THEN 1 ELSE 0 END"))

        unknown_branch_df = spark.createDataFrame(
            [("0", "알 수 없는 지점")], 
            ["bran_id", "bran_nm"]
        )

        df_dim_branch = df_silver.select("bran_id", "bran_nm") \
            .distinct() \
            .union(unknown_branch_df) \
            .distinct()

        df_dim_topping = df_silver.select("pizza_topping_id", "pizza_topping_nm").distinct()
        df_bridge_topping = df_silver.select("pizza_type_id", "pizza_topping_id").distinct()
        
        df_fact_order = df_silver.select(
            "order_detail_id","order_id","date","member_id","pizza_id","pizza_type_id","pizza_topping_id",
            "time","size","quantity","price","bran_id"
        ).withColumnRenamed("price","unit_price") \
        .withColumn("total_price", expr("unit_price * quantity")) \
        .fillna("0", subset=["member_id", "bran_id"]) 

        logger.info("Gold 테이블 변환 완료. 검증 시작...")

        # Data Validation (Validate) 단계
        validation_dfs = {
            "silver": df_silver,
            "fact": df_fact_order,
            "dim_pizza": df_dim_pizza,
            "dim_member": df_dim_member,
            "dim_ptype": df_dim_ptype,
            "dim_date": df_dim_date,
            "dim_branch": df_dim_branch,
            "dim_topping": df_dim_topping
        }
        
        validate_dataframes(validation_dfs)
        logger.info("검증 통과. BigQuery 적재 시작...")
        
        # BigQuery 적재 (Load) 단계
        write_to_bq(df_dim_pizza, "dim_pizza", bq_project, bq_dataset)
        write_to_bq(df_dim_member, "dim_member", bq_project, bq_dataset)
        write_to_bq(df_dim_ptype, "dim_pizza_type", bq_project, bq_dataset)
        write_to_bq(df_dim_date, "dim_date", bq_project, bq_dataset)
        write_to_bq(df_dim_branch, "dim_branch", bq_project, bq_dataset)
        write_to_bq(df_dim_topping, "dim_pizza_topping", bq_project, bq_dataset)
        write_to_bq(df_bridge_topping, "bridge_pizza_type_topping", bq_project, bq_dataset)
        write_to_bq(df_fact_order, "fact_order", bq_project, bq_dataset)
        
        logger.info("모든 BigQuery 테이블 적재 완료.")

    except ValueError as ve:
        logger.error(f"\n 데이터 검증 실패. BigQuery 적재 중단")
        logger.error(str(ve))
    except Exception as e:
        logger.error(f"\n ETL 파이프라인 중 알 수 없는 오류 발생 ")
        logger.error(str(e))

    finally:
        if df_silver:
            df_silver.unpersist()
        spark.stop()
        logger.info("SparkSession 종료.")

if __name__ == "__main__":
    main()
