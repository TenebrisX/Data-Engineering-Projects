CREATE TABLE IF NOT EXISTS stg.deliverysystem_restaurants (
    id serial4 NOT NULL,
    object_id varchar(200) NOT NULL,
    object_value text NOT NULL,
    update_ts timestamp not null,
    CONSTRAINT deliverysystem_restaurants_object_id_uindex UNIQUE (object_id),
    CONSTRAINT deliverysystem_restaurants_pkey PRIMARY KEY (id)
);
