from pyspark.sql.functions import col, isnan, lit, current_timestamp

TABLE_KEYS = {
    "branch": ["bran_id"],
    "member": ["member_id"],
    "orders": ["order_id"],
    "orderdetail": ["order_detail_id"],
    "pizza": ["pizza_id"],
    "pizzatypes": ["pizza_type_id"],
    "pizzatypetopping": ["pizza_type_id", "pizza_topping_id"], 
    "topping": ["pizza_topping_id"]
}

# Row 존재 검증
def check_row_count(df):
    count_rows = df.count()
    if count_rows == 0:
        return "No new data."
    return None

# 기본키 NULL 검증
def check_null_keys(df, key_cols):
    if not key_cols:
        return "No key columns defined for this table."

    errors = []
    for key_col in key_cols:
        if key_col not in df.columns:
            errors.append(f"Column {key_col} not found.")
            continue
        null_count = df.filter(col(key_col).isNull() | isnan(col(key_col))).count()
        if null_count > 0:
            errors.append(f"{key_col} has {null_count} null or NaN values.")
    return " | ".join(errors) if errors else None


# 중복 키 검증
def check_duplicates(df, key_cols):
    if not key_cols:
        return None

    from pyspark.sql import functions as F
    dup_count = df.groupBy(*key_cols).count().filter(F.col("count") > 1).count()
    if dup_count > 0:
        return f"Duplicate records detected on {key_cols}: {dup_count} rows."
    return None

def validate_dataframe(df, table_name):
    errors = []

    key_cols = TABLE_KEYS.get(table_name, [])
    for check_fn in [
        check_row_count,
        lambda df: check_null_keys(df, key_cols),
        lambda df: check_duplicates(df, key_cols),
    ]:
        err = check_fn(df)
        if err:
            errors.append(err)

    return errors
