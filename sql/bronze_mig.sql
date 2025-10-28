-- stg 스키마 생성 및 권한 설정
CREATE SCHEMA IF NOT EXISTS stg; -- ​스테이징/원시 적재용 스키마 생성

-- branch 테이블 구조 복제 및 데이터 복사
CREATE TABLE stg.branch (LIKE public.branch INCLUDING ALL); -- ​원본 구조(제약·인덱스 포함) 복제
INSERT INTO stg.branch SELECT * FROM public.branch; -- ​데이터 복사(원본 보존)

-- member 테이블 구조 복제 및 데이터 복사
CREATE TABLE stg."member" (LIKE public."member" INCLUDING ALL); 
INSERT INTO stg."member" SELECT * FROM public."member"; 

-- order_detail 테이블 구조 복제 및 데이터 복사
CREATE TABLE stg.orderdetail (LIKE public.orderdetail INCLUDING ALL); 
INSERT INTO stg.orderdetail SELECT * FROM public.orderdetail; 

-- orders 테이블 구조 복제 및 데이터 복사
CREATE TABLE stg.orders (LIKE public.orders INCLUDING ALL); 
INSERT INTO stg.orders SELECT * FROM public.orders; 

-- pizza 테이블 구조 복제 및 데이터 복사
CREATE TABLE stg.pizza (LIKE public.pizza INCLUDING ALL); 
INSERT INTO stg.pizza SELECT * FROM public.pizza; 

-- pizza_types 테이블 구조 복제 및 데이터 복사
CREATE TABLE stg.pizzatypes (LIKE public.pizzatypes INCLUDING ALL); 
INSERT INTO stg.pizzatypes SELECT * FROM public.pizzatypes;

-- pizza_type_topping 테이블 구조 복제 및 데이터 복사
CREATE TABLE stg.pizzatypetopping (LIKE public.pizzatypetopping INCLUDING ALL);
INSERT INTO stg.pizzatypetopping SELECT * FROM public.pizzatypetopping; 

-- topping 테이블 구조 복제 및 데이터 복사
CREATE TABLE stg.topping (LIKE public.topping INCLUDING ALL);
INSERT INTO stg.topping SELECT * FROM public.topping;