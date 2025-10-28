from pyspark.sql.functions import col, count, countDistinct
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_duplicate_pks(dim_tables_to_check):
    logger.info("  [검증 2-1]  테이블 PK 중복 검사 시작")
    duplicate_keys = {}
    for name, (df, key) in dim_tables_to_check.items():
        if isinstance(key, list):
            dup_count = df.groupBy(*key).count().filter(col("count") > 1).count()
        else:
            dup_count = df.groupBy(key).count().filter(col("count") > 1).count()
            
        if dup_count > 0:
            logger.warning(f"[경고] {name} 테이블에 중복 {key}가 {dup_count}건 존재합니다.")
        duplicate_keys[name] = dup_count
    logger.info("  [검증 2-1] 완료")
    return duplicate_keys

def check_null_fks(df_order_detail, df_orders, df_pizza):
    logger.info("  [검증 2-2] FK Null 값 검사 시작")
    null_fks = {}
    null_fks["order_detail_order_id_null"] = df_order_detail.filter(col("order_id").isNull()).count()
    null_fks["order_detail_pizza_id_null"] = df_order_detail.filter(col("pizza_id").isNull()).count()
    null_fks["orders_member_id_null"] = df_orders.filter(col("member_id").isNull()).count()
    null_fks["orders_bran_id_null"] = df_orders.filter(col("bran_id").isNull()).count()
    null_fks["pizza_pizza_type_id_null"] = df_pizza.filter(col("pizza_type_id").isNull()).count()

    logger.info(f"    [정보] Null FK 검사 결과: {null_fks}")
    logger.info("  [검증 2-2] 완료")
    return null_fks

def check_cardinality(df_bronze_order_detail, df_silver):
    logger.info("  [검증 4-1] Cardinality (행 개수) 검증 시작")
    count_bronze_order_detail = df_bronze_order_detail.count()
    
    count_silver_distinct_order_detail = df_silver.select(countDistinct(col("order_detail_id"))).collect()[0][0]

    results = {
        "bronze_order_detail_count": count_bronze_order_detail,
        "silver_distinct_order_detail_id_count": count_silver_distinct_order_detail
    }

    dropped_rows = count_bronze_order_detail - count_silver_distinct_order_detail
    if dropped_rows > 0:
        logger.info(f"    [정보] Cardinality: 'inner' 조인으로 인해 {dropped_rows}건의 order_detail이 Silver에서 제외되었습니다.")
    elif dropped_rows < 0:
         logger.warning(f"    [경고] Cardinality: Silver의 고유 order_detail 건수({count_silver_distinct_order_detail})가 Bronze({count_bronze_order_detail})보다 많습니다. 로직 검토 필요.")
    else:
         logger.info(f"    [정보] Cardinality: 모든 order_detail 행이 'inner' 조인에 성공했습니다. ({count_bronze_order_detail}건)")
    logger.info("  [검증 4-1] 완료")
    return results

def check_referential_integrity(df_silver):
    logger.info("  [검증 4-2] 참조 무결성 (Left Join Null) 검사 시작")
    missing_joins = {}

    try:
        # member_id는 있으나 (from orders), member_nm이 없는 경우 (from member)
        missing_joins["missing_member"] = df_silver.filter(
            col("member_id").isNotNull() & col("member_nm").isNull()
        ).count()

        # bran_id는 있으나 (from orders), bran_nm이 없는 경우 (from branch)
        missing_joins["missing_branch"] = df_silver.filter(
            col("bran_id").isNotNull() & col("bran_nm").isNull()
        ).count()

        # pizza_topping_id는 있으나 (from bridge), pizza_topping_nm이 없는 경우
        missing_joins["missing_topping_master"] = df_silver.filter(
            col("pizza_topping_id").isNotNull() & col("pizza_topping_nm").isNull()
        ).count()

        # 토핑 정보가 아예 없는 피자
        missing_joins["pizzas_with_no_toppings (distinct_type)"] = df_silver.filter(
            col("pizza_topping_id").isNull()
        ).select(col("pizza_type_id")).distinct().count()

        logger.info(f"    [정보] 'Left Join' 실패 (참조 데이터 누락) 건수: {missing_joins}")
        logger.info("  [검증 4-2] 완료")
        
    except Exception as e:
        logger.error(f"ERROR: [검증 4-2] 참조 무결성 검사 중 에러 발생: {e}", exc_info=True)
        missing_joins["VALIDATION_ERROR"] = 1 
        
    return missing_joins
