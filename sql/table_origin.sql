CREATE TABLE public.branch (
	bran_id varchar(100) NOT NULL,
	bran_nm varchar(100) NULL,
	CONSTRAINT branch_pk PRIMARY KEY (bran_id)
);

CREATE TABLE public."member" (
	member_id varchar(100) NOT NULL,
	member_pwd varchar(100) NULL,
	member_nm varchar(100) NULL,
	CONSTRAINT mem_pkey PRIMARY KEY (member_id)
);

CREATE TABLE public.order_detail (
	order_detail_id int4 NOT NULL,
	order_id int4 NULL,
	pizza_id varchar(50) NULL,
	quantity int4 NULL,
	CONSTRAINT order_details_pk PRIMARY KEY (order_detail_id)
);

-- public.order_detail foreign keys

ALTER TABLE public.order_detail  ADD CONSTRAINT fk_order_id FOREIGN KEY (order_id) REFERENCES public."orders"(order_id) ON DELETE CASCADE;
ALTER TABLE public.order_detail  ADD CONSTRAINT fk_pizza_id FOREIGN KEY (pizza_id) REFERENCES public.pizza(pizza_id) ON DELETE CASCADE;

CREATE TABLE public.orders (
	order_id int4 NOT NULL,
	"date" varchar(50) NULL,
	"time" varchar(50) NULL,
	member_id varchar(100) NULL,
	bran_id varchar(100) NULL,
	CONSTRAINT orders_pk PRIMARY KEY (order_id)
);

-- public.orders foreign keys
ALTER TABLE public.orders ADD CONSTRAINT fk_mem_id FOREIGN KEY (member_id) REFERENCES public."member"(member_id) ON DELETE CASCADE;
ALTER TABLE public.orders ADD CONSTRAINT fk_bran_id FOREIGN KEY (bran_id) REFERENCES public."branch"(bran_id) ON DELETE CASCADE;



CREATE TABLE public.pizza (
	pizza_id varchar(50) NOT NULL,
	pizza_type_id varchar(50) NOT NULL,
	"size" varchar(50) NULL,
	price float4 NULL,
	CONSTRAINT pizzas_pk PRIMARY KEY (pizza_id)
);

-- public.pizza foreign keys
ALTER TABLE public.pizza  ADD CONSTRAINT fk_pizza_type_id FOREIGN KEY (pizza_type_id) REFERENCES public.pizza_types(pizza_type_id) ON DELETE CASCADE;


CREATE TABLE public.pizza_type_topping (
	pizza_type_id varchar(100) NOT NULL,
	pizza_topping_id varchar(100) NOT NULL,
	CONSTRAINT pizza_type_topping_pk PRIMARY KEY (pizza_type_id, pizza_topping_id)
);

-- public.pizza_type_topping foreign keys
ALTER TABLE public.pizza_type_topping ADD CONSTRAINT fk_pizza_type_id FOREIGN KEY (pizza_type_id) REFERENCES public.pizza_types(pizza_type_id) ON DELETE CASCADE;
ALTER TABLE public.pizza_type_topping ADD CONSTRAINT fk_pizza_topping_id FOREIGN KEY (pizza_topping_id) REFERENCES public.topping(pizza_topping_id) ON DELETE CASCADE;

CREATE TABLE public.pizza_types (
	pizza_type_id varchar(50) NOT NULL,
	pizza_nm varchar(50) NULL,
	pizza_categ varchar(50) NULL,
	pizza_img_url varchar(1000) NULL,
	CONSTRAINT pizza_types_pk PRIMARY KEY (pizza_type_id)
);


CREATE TABLE public.topping (
	pizza_topping_id varchar(100) NOT NULL,
	pizza_topping_nm varchar(100) NULL,
	CONSTRAINT topping_pk PRIMARY KEY (pizza_topping_id)
);