from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, when
from delta.tables import DeltaTable
import json
from silver_validator import check_duplicate_pks, check_null_fks, check_cardinality, check_referential_integrity
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = SparkSession.builder \
    .appName("Silver_ETL") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

bronze_path = "gs://pz-buck-888/bronze/"
silver_path = "gs://pz-buck-888/silver/"

try:
    od = spark.read.format("delta").load(f"{bronze_path}orderdetail").alias("orderdetail")
    o = spark.read.format("delta").load(f"{bronze_path}orders").alias("orders")
    p = spark.read.format("delta").load(f"{bronze_path}pizza").alias("pizza")
    pt = spark.read.format("delta").load(f"{bronze_path}pizzatypes").alias("pizzatypes")
    m = spark.read.format("delta").load(f"{bronze_path}member").alias("member")
    b = spark.read.format("delta").load(f"{bronze_path}branch").alias("branch")
    t = spark.read.format("delta").load(f"{bronze_path}topping").alias("topping")
    br = spark.read.format("delta").load(f"{bronze_path}pizzatypetopping").alias("pizzatypetopping")
    
    logger.info("모든 Bronze 테이블 로드 및 별칭 설정 성공")
except Exception as e:
    logger.error(f"ERROR: Bronze 테이블 로드 실패: {e}", exc_info=True)
    spark.stop()
    exit(1)

logger.info("--- 조인 전 검증 시작 ---")
validation_results = {}

# Primary Key (PK) 중복 검사
dim_tables_to_check = {
    "orders": (o, "order_id"),
    "orderdetail": (od, "order_detail_id"),
    "pizza": (p, "pizza_id"),
    "pizzatypes": (pt, "pizza_type_id"),
    "member": (m, "member_id"),
    "branch": (b, "bran_id"),
    "topping": (t, "pizza_topping_id"),
    "pizzatypetopping": (br, ["pizza_type_id", "pizza_topping_id"])
}

validation_results["duplicate_primary_keys"] = check_duplicate_pks(dim_tables_to_check)

# Foreign Key (FK) Null 값 검사
validation_results["null_foreign_keys"] = check_null_fks(od, o, p)

logger.info("--- 조인 전 검증 완료 ---")

logger.info("--- Silver ETL (Join) 로직 시작 ---")
try:
    df_silver = od \
        .join(o, od.order_id == o.order_id, "inner") \
        .join(p, od.pizza_id == p.pizza_id, "inner") \
        .join(pt, p.pizza_type_id == pt.pizza_type_id, "inner") \
        .join(m, o.member_id == m.member_id, "left") \
        .join(b, o.bran_id == b.bran_id, "left") \
        .join(br, pt.pizza_type_id == br.pizza_type_id, "left") \
        .join(t, br.pizza_topping_id == t.pizza_topping_id, "left")
    
    logger.info("--- Silver ETL (Join) 로직 완료 ---")
except Exception as e:
    logger.error(f"ERROR: Silver ETL 조인 실패: {e}", exc_info=True)
    spark.stop()
    exit(1)

df_silver.cache()

logger.info("--- 조인 후 검증 시작 ---")

validation_results["row_count_check"] = check_cardinality(od, df_silver)
validation_results["missing_join_references"] = check_referential_integrity(df_silver)

logger.info("--- 조인 후 검증 완료 ---")

logger.info("\n=== 최종 검증 요약 ===")
logger.info(json.dumps(validation_results, indent=2))

critical_errors = []
warnings = []

duplicate_pks = validation_results.get("duplicate_primary_keys", {})
for table, count in duplicate_pks.items():
    if count > 0:
        error_msg = f"[CRITICAL] {table} 테이블에 {count}건의 중복 PK가 발견되었습니다. 데이터 증폭 위험."
        logger.error(error_msg)
        critical_errors.append(error_msg)

row_check = validation_results.get("row_count_check", {})
bronze_count = row_check.get("bronze_order_detail_count", 0)
silver_count = row_check.get("silver_distinct_order_detail_id_count", 0)
dropped_rows = bronze_count - silver_count

if dropped_rows < 0:
    error_msg = f"[CRITICAL] Cardinality 오류: Silver의 고유 order_detail 건수({silver_count})가 Bronze({bronze_count})보다 많습니다. 조인 로직 검토 필요."
    logger.error(error_msg)
    critical_errors.append(error_msg)
elif dropped_rows > 0:
    info_msg = f"[WARNING] Cardinality: {dropped_rows}건의 order_detail이 inner join으로 제외됨. (Bronze: {bronze_count}, Silver(Distinct): {silver_count})"
    logger.warning(info_msg)
    warnings.append(info_msg)
else:
    info_msg = f"[INFO] Cardinality: 모든 order_detail 행이 'inner' 조인에 성공했습니다. ({bronze_count}건)"
    logger.info(info_msg)


missing_joins = validation_results.get("missing_join_references", {})
for check, count in missing_joins.items():
    if count > 0:
        warning_msg = f"[WARNING] Left Join 참조 누락: '{check}' 항목에서 {count}건의 매칭 실패가 발견되었습니다."
        logger.warning(warning_msg)
        warnings.append(warning_msg)

if critical_errors:
    logger.error(f"\n[!!!] 총 {len(critical_errors)}건의 심각한 오류가 감지되어 Silver 적재를 중단합니다.")
    raise Exception("Silver ETL Validation FAILED: \n" + "\n".join(critical_errors))
else:
    logger.info("\n[SUCCESS] 모든 심각한 검증 항목을 통과했습니다. Silver 적재를 계속합니다.")
    if warnings:
        logger.info(f"  (총 {len(warnings)}건의 경고/정보 로그가 있습니다. 위 내용을 확인하세요.)")

# Silver 적재 
logger.info(f"\nSilver 테이블 적재 시작: {silver_path}ods_orders")
try:
    df_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{silver_path}ods_orders")
    logger.info("Silver 테이블 적재 성공")
except Exception as e:
    logger.error(f"ERROR: Silver 테이블 적재 실패: {e}", exc_info=True)

df_silver.unpersist()
spark.stop()
