-- drop table if exists dds.dm_orders;

create table if not exists dds.dm_orders (
    id serial4 not null constraint dm_orders_pk primary key,
    user_id int4 not null,
    restaurant_id int4 not null,
    timestamp_id int4 not null,
    delivery_id int4 not null,
    courier_id int4 not null,
    order_key varchar not null,
    order_status varchar not null,
    constraint dm_orders_user_id_fkey foreign key (user_id) references dds.dm_users (id),
    constraint dm_orders_restaurant_id_fkey foreign key (restaurant_id) references dds.dm_restaurants (id),
    constraint dm_orders_timestamp_id_fkey foreign key (timestamp_id) references dds.dm_timestamps (id),
    constraint dm_orders_delivery_id_fkey foreign key (delivery_id) references dds.c_deliveries (id),
    constraint dm_orders_courier_id_fkey foreign key (courier_id) references dds.c_couriers (id)
);