--drops
drop table if exists STV2024021962__STAGING.dialogs;
drop table if exists STV2024021962__STAGING.dialogs_rejected;

drop table if exists STV2024021962__STAGING.group_log;
drop table if exists STV2024021962__STAGING.group_log_rejected;

drop table if exists STV2024021962__STAGING.groups;
drop table if exists STV2024021962__STAGING.groups_rejected;

drop table if exists STV2024021962__STAGING.users;
drop table if exists STV2024021962__STAGING.users_rejected;


--ddl
CREATE TABLE IF NOT EXISTS STV2024021962__STAGING.users (
    id INT PRIMARY KEY,
    chat_name VARCHAR(200),
    registration_dt TIMESTAMP,
    country VARCHAR(200),
    age INT
)
order by id
segmented by hash(id) all nodes;


create table if not exists STV2024021962__STAGING.groups(
	id int primary key,
	admin_id int references STV2024021962__STAGING.users(id),
	group_name varchar(100),
	registration_dt timestamp(6),
	is_private boolean
)
order by id, admin_id
segmented by hash(id) all nodes
PARTITION BY registration_dt::date
GROUP BY calendar_hierarchy_day(registration_dt::date, 3, 2);


create table if not exists STV2024021962__STAGING.dialogs(
	message_id int primary key,
	message_ts timestamp(6),
	message_from int not null references STV2024021962__STAGING.users(id),
	message_to int not null references STV2024021962__STAGING.users(id),
	message varchar(1000),
	message_group int references STV2024021962__STAGING.groups(id)
)
order by message_id
segmented by hash(message_id) all nodes
PARTITION BY message_ts::date
GROUP BY calendar_hierarchy_day(message_ts::date, 3, 2);


create table if not exists STV2024021962__STAGING.group_log(
	group_id int primary key,
	user_id int references STV2024021962__STAGING.users(id),
	user_id_from int references STV2024021962__STAGING.users(id),
	event varchar(10),
	dt timestamp
)
order by group_id, user_id
segmented by hash(group_id) all nodes
PARTITION BY dt::date
GROUP BY calendar_hierarchy_day(dt::date, 3, 2);


