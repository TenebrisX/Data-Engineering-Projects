-- c_deliveries creation
create table if not exists dds.c_deliveries (
	id serial4 not null constraint c_deliveries_pk primary key,
	delivery_id varchar not null,
	order_key varchar not null,
	order_ts timestamp not null,
	courier_id int4 not null,
	delivery_ts timestamp not null,
	tip_sum numeric(10,2) default 0 not null constraint c_deliveries_tip_sum_check check (tip_sum >= 0),
	rate int default 0 not null constraint c_deliveries_rate_check check (rate >= 0 and rate <= 5),
	"sum" numeric(10,2) default 0 not null constraint c_deliveries_sum_check check ("sum" >= 0),
	constraint c_deliveries_courier_id_fkey foreign key (courier_id) references dds.c_couriers (id)
);