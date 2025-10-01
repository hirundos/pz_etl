--월별 매출 추이
select CONCAT(dd."year",dd.month) as 월, ROUND(SUM(total_price)::NUMERIC,3) as 월매출
from pizza.fact_order_detail fd
join pizza.dim_date dd 
on TO_DATE(fd.date, 'YYYY-MM-DD') = dd.date
group by dd.year, dd."month"
order by year, month

--카테고리별 매출
select round(sum(fd.total_price)::numeric, 3) as , dpt.category
from pizza.fact_order_detail fd
join pizza.dim_pizza_type dpt 
on fd.pizza_type_id = dpt.pizza_type_id
group by category 

--시간대별 주문
select EXTRACT(HOUR FROM TO_TIMESTAMP(fd.time, 'HH24:MI:SS')) AS hour,
	count(fd.order_id)
from pizza.fact_order_detail fd 
join pizza.dim_date dd 
on TO_DATE(fd.date, 'YYYY-MM-DD') = dd.date
group by EXTRACT(HOUR FROM TO_TIMESTAMP(fd.time, 'HH24:MI:SS'))

--시간대별 매출 추이
select EXTRACT(HOUR FROM TO_TIMESTAMP(fd.time, 'HH24:MI:SS')) AS hour,
	fd.date,
	dd.weekday,
	round(sum(fd.total_price)::numeric, 3)
from pizza.fact_order_detail fd 
join pizza.dim_date dd 
on TO_DATE(fd.date, 'YYYY-MM-DD') = dd.date
group by 1, 2, 3

--피자 종류별 매출
select round(sum(fd.total_price)::numeric, 3) as "피자별매출", fd.pizza_type_id
from pizza.fact_order_detail fd
group by fd.pizza_type_id 