-- ODS 스키마 생성 및 권한 설정
CREATE SCHEMA IF NOT EXISTS ods; ​-- ODS(Operational Data Store) 적재용 스키마 생성

/*1차 가공 데이터*/
CREATE TABLE ods.ods_orders AS
    SELECT o.order_id, od.order_detail_id, p.pizza_id, pt.pizza_type_id, o.member_id, m.member_nm, o.date, o.time, od.quantity, p.size, 
		p.price, pt.pizza_nm, pt.pizza_categ, b.bran_id, b.bran_nm, ptto.pizza_topping_id, tp.pizza_topping_nm
	FROM stg.orderdetail od
	JOIN stg.orders o 
	ON od.order_id = o.order_id
	JOIN stg.pizza p 
	ON od.pizza_id = p.pizza_id
	JOIN stg.pizzatypes pt 
	ON p.pizza_type_id = pt.pizza_type_id
	LEFT JOIN stg.branch b
	ON o.bran_id = b.bran_id
	LEFT JOIN stg.member m 
	ON o.member_id = m.member_id
	LEFT JOIN stg.pizzatypetopping ptto
	ON pt.pizza_type_id = ptto.pizza_type_id
	LEFT JOIN stg.topping tp
	ON ptto.pizza_topping_id = tp.pizza_topping_id
