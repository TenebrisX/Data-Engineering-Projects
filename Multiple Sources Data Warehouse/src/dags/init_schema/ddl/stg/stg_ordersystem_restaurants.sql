CREATE TABLE if not exists stg.ordersystem_restaurants (
	id serial4 NOT NULL,
	object_id varchar(200) NOT NULL,
	object_value text NOT NULL,
	update_ts timestamp NOT NULL,
	CONSTRAINT ordersystem_restaurants_object_id_uindex UNIQUE (object_id),
	CONSTRAINT ordersystem_restaurants_pkey PRIMARY KEY (id)
);