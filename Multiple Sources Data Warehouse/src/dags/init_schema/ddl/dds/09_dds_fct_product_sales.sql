-- drop table dds.fct_product_sales;

create table if not exists dds.fct_product_sales (
    id serial4 not null constraint fct_product_sales_pk primary key,
    product_id int4 not null,
    order_id int4 not null,
    "count" int4 default 0 not null constraint fct_product_sales_count_check check ("count" >= 0),
    price numeric(14,2) default 0 not null constraint fct_product_sales_price_check check (price >= 0),
    total_sum numeric(14,2) default 0 not null constraint fct_product_sales_total_sum_check check (total_sum >= 0),
    bonus_payment numeric(14,2) default 0 not null constraint fct_product_sales_bonus_payment_check check (bonus_payment >= 0),
    bonus_grant numeric(14,2) default 0 not null constraint fct_product_sales_bonus_grant_check check (bonus_grant >= 0),
    constraint fct_product_sales_product_id_fkey foreign key (product_id) references dds.dm_products (id),
    constraint fct_product_sales_order_id_fkey foreign key (order_id) references dds.dm_orders (id)
);
