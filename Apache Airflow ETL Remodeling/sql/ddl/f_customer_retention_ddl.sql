--drop
drop table if exists mart.f_customer_retention;

--delete
delete from mart.f_customer_retention;


-- f_customer_retention creation
create table if not exists mart.f_customer_retention (
	new_customers_count bigint,
	returning_customers_count bigint,
	refunded_customer_count bigint,
	period_name varchar(10),
	period_id int4,
	item_id int4,
	new_customers_revenue numeric(14,2),
	returning_customers_revenue numeric(14,2),
	customers_refunded numeric(14,2),
	
	constraint f_customer_retention_item_id_fkey foreign key (item_id) references mart.d_item (item_id) on delete cascade
);

