--drop table dds.dm_restaurants;

create table if not exists dds.dm_restaurants (
	id serial4 not null constraint dm_restaurants_pk primary key,
	restaurant_id varchar not null,
	restaurant_name varchar not null,
	active_from timestamp not null,
	active_to timestamp not null
);