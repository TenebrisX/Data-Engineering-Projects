CREATE table if not exists stg.bonussystem_users (
	id int4 NOT null,
	order_user_id text NOT NULL,
	CONSTRAINT users_pkey PRIMARY KEY (id)
);