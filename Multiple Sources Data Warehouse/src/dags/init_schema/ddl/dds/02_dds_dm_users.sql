--drop table dds.dm_users;

create table if not exists dds.dm_users (
	id serial4 not null constraint dm_users_pk primary key,
	user_id varchar not null,
	user_name varchar not null,
	user_login varchar not null,
	CONSTRAINT unique_id UNIQUE (id) 
);
