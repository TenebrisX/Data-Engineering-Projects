-- drop table if exists dds.dm_products;

create table if not exists dds.dm_products (
    id serial4 not null constraint dm_products_pk primary key,
    restaurant_id int4 not null,
    product_id varchar not null,
    product_name varchar not null,
    product_price numeric(14,2) default 0 not null constraint dm_products_product_price_check check (product_price >= 0),
    active_from timestamp not null,
    active_to timestamp not null,
    constraint dm_products_restaurant_id_fkey foreign key (restaurant_id) references dds.dm_restaurants (id)
);
