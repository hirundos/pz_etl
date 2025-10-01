-- dm 스키마 생성 및 권한 설정
CREATE SCHEMA IF NOT EXISTS dm; -- ​gold 데이터 마트 스키마 생성

/*2차 가공본*/
-- dim_pizza
CREATE TABLE dm.dim_pizza
AS
SELECT DISTINCT pizza_id, pizza_type_id, size, price
FROM ods.ods_orders

--dim_회원
CREATE TABLE dm.dim_member 
AS
SELECT DISTINCT member_id, member_nm 
FROM ods.ods_orders

--dim pizza_type
CREATE TABLE dm.dim_pizza_type
AS
SELECT DISTINCT pizza_type_id, pizza_nm, pizza_categ
FROM ods.ods_orders

--dim date
CREATE TABLE dm.dim_date
AS
SELECT
  TO_DATE(date, 'YYYY-MM-DD') AS date,
  EXTRACT(YEAR FROM TO_DATE(date, 'YYYY-MM-DD')) AS year,
  EXTRACT(MONTH FROM TO_DATE(date, 'YYYY-MM-DD')) AS month,
  EXTRACT(DAY FROM TO_DATE(date, 'YYYY-MM-DD')) AS day,
  TO_CHAR(TO_DATE(date, 'YYYY-MM-DD'), 'Day') AS weekday,
  CASE 
        WHEN EXTRACT(DOW FROM TO_DATE(date, 'YYYY-MM-DD')) IN (0, 6) 
		THEN 1
        ELSE 0
  END as is_weekend
  FROM 
  (
  	SELECT DISTINCT date
	FROM ods.ods_orders
)

--dim branch
CREATE TABLE dm.dim_branch
AS
SELECT DISTINCT bran_id, bran_nm
FROM ods.ods_orders

--dim pizza_topping
CREATE TABLE dm.dim_pizza_topping
AS
SELECT DISTINCT pizza_topping_id, pizza_topping_nm
FROM ods.ods_orders

--Bridge Table
CREATE TABLE dm.bridge_pizza_type_topping
AS
SELECT DISTINCT pizza_type_id, pizza_topping_id
FROM ods.ods_orders

-- fact_주문
CREATE TABLE dm.fact_order
AS
SELECT DISTINCT order_detail_id, order_id, date, member_id, pizza_id, pizza_type_id , pizza_topping_id, time, size, quantity, 
	price as unit_price, bran_id, (price * quantity ) total_price
FROM ods.ods_orders

