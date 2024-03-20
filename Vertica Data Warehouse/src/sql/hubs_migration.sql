--truncate
truncate table STV2024021962__DWH.h_dialogs;
truncate table STV2024021962__DWH.h_groups;
truncate table STV2024021962__DWH.h_users;


--migration
INSERT INTO STV2024021962__DWH.h_users(hk_user_id, user_id,registration_dt,load_dt,load_src)
select
       hash(id) as hk_user_id,
       id as user_id,
       registration_dt,
       now() as load_dt,
       's3' as load_src
       from STV2024021962__STAGING.users
where hash(id) not in (select hk_user_id from STV2024021962__DWH.h_users);


INSERT INTO STV2024021962__DWH.h_groups(hk_group_id, group_id ,registration_dt,load_dt,load_src)
select
       hash(id) as hk_group_id,
       id as group_id,
       registration_dt,
       now() as load_dt,
       's3' as load_src
       from STV2024021962__STAGING.groups
where hash(id) not in (select hk_group_id from STV2024021962__DWH.h_groups);


INSERT INTO STV2024021962__DWH.h_dialogs(hk_message_id, message_id, message_ts ,load_dt,load_src)
select
       hash(message_id) as hk_message_id,
       message_id,
       message_ts,
       now() as load_dt,
       's3' as load_src
       from STV2024021962__STAGING.dialogs d
where hash(message_id) not in (select hk_message_id from STV2024021962__DWH.h_dialogs);