from pyspark.sql.functions import col, count, countDistinct, F
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_validation_check(check_name, condition, error_message):
    if not condition:
        logger.error(f"[VALIDATION FAILED] {check_name}: {error_message}")
        raise ValueError(f"[Validation Error] {check_name}: {error_message}")
    else:
        logger.info(f"[VALIDATION SUCCESS] {check_name}")

def _check_uniqueness(df, key_col, table_name):
    count = df.count()
    distinct_count = df.select(key_col).distinct().count()
    run_validation_check(
        f"{table_name} Key Uniqueness",
        count == distinct_count,
        f"Duplicate keys found. Total: {count}, Unique: {distinct_count} in '{key_col}'."
    )

def _check_nulls(df, col_name):
    null_count = df.where(col(col_name).isNull()).count()
    run_validation_check(
        f"Fact Table Null Check ({col_name})",
        null_count == 0,
        f"Found {null_count} nulls in foreign key '{col_name}'."
    )

def _check_referential_integrity(fact_df, fact_key, dim_df, dim_key, dim_name):
    orphan_count = fact_df.alias("f") \
        .join(dim_df.alias("d"), F.col(f"f.{fact_key}") == F.col(f"d.{dim_key}"), "left_anti") \
        .count()
    run_validation_check(
        f"Referential Integrity (fact -> {dim_name})",
        orphan_count == 0,
        f"Found {orphan_count} orphan keys in fact_order.{fact_key} that do not exist in {dim_name}."
    )

def validate_dataframes(dfs):
    logger.info("Data Validation (External Module)")

    try:
        df_silver = dfs['silver']
        df_fact_order = dfs['fact']
        df_dim_pizza = dfs['dim_pizza']
        df_dim_member = dfs['dim_member']
        df_dim_ptype = dfs['dim_ptype']
        df_dim_date = dfs['dim_date']
        df_dim_branch = dfs['dim_branch']
        df_dim_topping = dfs['dim_topping']
    except KeyError as e:
        raise ValueError(f"검증에 필요한 DataFrame이 누락되었습니다: {e}")

    # 건수 검증 
    source_count = df_silver.count()
    fact_count = df_fact_order.count()
    run_validation_check(
        "Source vs Fact Row Count",
        source_count == fact_count,
        f"Fact count ({fact_count}) does not match source count ({source_count})."
    )
    
    # Dimension 테이블 PK 고유성 검증
    _check_uniqueness(df_dim_pizza, "pizza_id", "dim_pizza")
    _check_uniqueness(df_dim_member, "member_id", "dim_member")
    _check_uniqueness(df_dim_ptype, "pizza_type_id", "dim_pizza_type")
    _check_uniqueness(df_dim_date, "date", "dim_date")
    _check_uniqueness(df_dim_branch, "bran_id", "dim_branch")
    _check_uniqueness(df_dim_topping, "pizza_topping_id", "dim_pizza_topping")
    _check_uniqueness(df_fact_order, "order_detail_id", "fact_order (PK)")

    # Fact 테이블 FK Null 검증
    _check_nulls(df_fact_order, "date")
    _check_nulls(df_fact_order, "member_id")
    _check_nulls(df_fact_order, "pizza_id")
    _check_nulls(df_fact_order, "bran_id")
    _check_nulls(df_fact_order, "pizza_type_id")
    _check_nulls(df_fact_order, "pizza_topping_id")

    # 참조 무결성 (FK - PK) 검증
    _check_referential_integrity(df_fact_order, "pizza_id", df_dim_pizza, "pizza_id", "dim_pizza")
    _check_referential_integrity(df_fact_order, "member_id", df_dim_member, "member_id", "dim_member")
    _check_referential_integrity(df_fact_order, "pizza_type_id", df_dim_ptype, "pizza_type_id", "dim_pizza_type")
    _check_referential_integrity(df_fact_order, "date", df_dim_date, "date", "dim_date")
    _check_referential_integrity(df_fact_order, "bran_id", df_dim_branch, "bran_id", "dim_branch")
    _check_referential_integrity(df_fact_order, "pizza_topping_id", df_dim_topping, "pizza_topping_id", "dim_pizza_topping")

    # 비즈니스 로직 검증
    # total_price 계산 검증
    bad_price_calc_count = df_fact_order.where(col("total_price") != (col("unit_price") * col("quantity"))).count()
    run_validation_check(
        "Business Logic (total_price)",
        bad_price_calc_count == 0,
        f"Found {bad_price_calc_count} rows where total_price != unit_price * quantity."
    )
    
    # 수량 및 가격 > 0 검증
    negative_quantity_count = df_fact_order.where(col("quantity") <= 0).count()
    run_validation_check(
        "Business Logic (quantity > 0)",
        negative_quantity_count == 0,
        f"Warning (non-blocking): Found {negative_quantity_count} rows with quantity <= 0."
    )

    logger.info("\n--- 모든 데이터 검증 통과 (External Module) ---")
