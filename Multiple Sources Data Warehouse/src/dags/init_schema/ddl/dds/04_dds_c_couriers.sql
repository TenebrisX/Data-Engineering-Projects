
-- c_couriers creation
create table if not exists dds.c_couriers (
	id serial4 not null constraint c_couriers_pk primary key,
	courier_id varchar not null,
	courier_name varchar not null
);
