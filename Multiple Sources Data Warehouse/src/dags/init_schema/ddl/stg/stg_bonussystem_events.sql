CREATE TABLE if not exists stg.bonussystem_events (
	id int4 NOT null,
	event_ts timestamp NOT NULL,
	event_type varchar NOT NULL,
	event_value text NOT NULL,
	CONSTRAINT outbox_pkey PRIMARY KEY (id)
);
CREATE INDEX if not exists idx_bonussystem_events__event_ts ON stg.bonussystem_events USING btree (event_ts);
