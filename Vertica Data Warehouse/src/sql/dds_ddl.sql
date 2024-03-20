--drops
--hubs
drop table if exists STV2024021962__DWH.h_users;
drop table if exists STV2024021962__DWH.h_groups;
drop table if exists STV2024021962__DWH.h_dialogs;

--links
drop table if exists STV2024021962__DWH.l_user_message;
drop table if exists STV2024021962__DWH.l_groups_dialogs;
drop table if exists STV2024021962__DWH.l_admins;
drop table if exists STV2024021962__DWH.l_user_group_activity;

--sathelites
drop table if exists STV2024021962__DWH.s_admins;
drop table if exists STV2024021962__DWH.s_group_name;
drop table if exists STV2024021962__DWH.s_group_private_status;
drop table if exists STV2024021962__DWH.s_dialog_info;
drop table if exists STV2024021962__DWH.s_user_socdem;
drop table if exists STV2024021962__DWH.s_user_chatinfo;
drop table if exists STV2024021962__DWH.s_auth_history;

--ddl
--hubs
create table STV2024021962__DWH.h_users
(
    hk_user_id bigint primary key,
    user_id      int,
    registration_dt datetime,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


create table STV2024021962__DWH.h_groups
(
    hk_group_id bigint primary key,
    group_id      int,
    registration_dt datetime,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_group_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


create table STV2024021962__DWH.h_dialogs
(
    hk_message_id bigint primary key,
    message_id      int,
    message_ts datetime,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_message_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


--links
create table STV2024021962__DWH.l_user_message
(
	hk_l_user_message bigint primary key,
	hk_user_id bigint not null CONSTRAINT fk_l_user_message_user REFERENCES STV2024021962__DWH.h_users(hk_user_id),
	hk_message_id bigint not null CONSTRAINT fk_l_user_message_message REFERENCES STV2024021962__DWH.h_dialogs(hk_message_id),
	load_dt datetime,
	load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


create table STV2024021962__DWH.l_groups_dialogs
(
	hk_l_groups_dialogs bigint primary key,
	hk_message_id bigint not null CONSTRAINT l_groups_dialogs_message REFERENCES STV2024021962__DWH.h_dialogs (hk_message_id),
	hk_group_id bigint not null CONSTRAINT l_groups_dialogs_group REFERENCES STV2024021962__DWH.h_groups (hk_group_id),
	load_dt datetime,
	load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_groups_dialogs all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


create table STV2024021962__DWH.l_admins
(
	hk_l_admin_id bigint primary key,
	hk_group_id bigint not null CONSTRAINT fk_l_admins_group REFERENCES STV2024021962__DWH.h_groups (hk_group_id),
	hk_user_id bigint not null CONSTRAINT fk_l_admins_user REFERENCES STV2024021962__DWH.h_users (hk_user_id),
	load_dt datetime,
	load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_admin_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


create table STV2024021962__DWH.l_user_group_activity
(
	hk_l_user_group_activity bigint primary key,
	hk_user_id bigint not null CONSTRAINT fk_l_user_group_activity_user REFERENCES STV2024021962__DWH.h_users (hk_user_id),
	hk_group_id bigint not null CONSTRAINT fk_l_user_group_activity_gorup REFERENCES STV2024021962__DWH.h_groups (hk_group_id),
	load_dt datetime,
	load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

-- sathelite
create table STV2024021962__DWH.s_admins
(
	hk_admin_id bigint not null CONSTRAINT fk_s_admins_l_admins REFERENCES STV2024021962__DWH.l_admins (hk_l_admin_id),
	is_admin boolean,
	admin_from datetime,
	load_dt datetime,
	load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_admin_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


create table STV2024021962__DWH.s_group_name
(
	hk_group_id bigint not null CONSTRAINT fk_s_group_name_h_groups REFERENCES STV2024021962__DWH.h_groups (hk_group_id),
	group_name varchar(100),
	load_dt datetime,
	load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_group_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


create table STV2024021962__DWH.s_group_private_status
(
	hk_group_id bigint not null CONSTRAINT fk_s_group_name_h_groups REFERENCES STV2024021962__DWH.h_groups (hk_group_id),
	is_private boolean,
	load_dt datetime,
	load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_group_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


create table STV2024021962__DWH.s_dialog_info
(
	hk_message_id bigint not null CONSTRAINT fk_s_dialog_info_h_dialogs REFERENCES STV2024021962__DWH.h_dialogs (hk_message_id),
	message varchar(1000),
	message_from int,
	message_to int,
	load_dt datetime,
	load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_message_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


create table STV2024021962__DWH.s_user_socdem
(
	hk_user_id bigint not null CONSTRAINT fk_s_user_socdem_h_users REFERENCES STV2024021962__DWH.h_users (hk_user_id),
	country varchar(200),
	age int,
	load_dt datetime,
	load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


create table STV2024021962__DWH.s_user_chatinfo
(
	hk_user_id bigint not null CONSTRAINT fk_s_user_chatinfo_h_users REFERENCES STV2024021962__DWH.h_users (hk_user_id),
	chat_name varchar(1000),
	load_dt datetime,
	load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

create table STV2024021962__DWH.s_auth_history
(
	hk_l_user_group_activity bigint not null CONSTRAINT fk_s_auth_history_l_user_group_activity REFERENCES STV2024021962__DWH.l_user_group_activity (hk_l_user_group_activity),
	user_id_from bigint,
	event varchar(10),
	event_dt timestamp,
	load_dt timestamp,
	load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);



