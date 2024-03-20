--truncate
truncate table STV2024021962__DWH.l_admins;
truncate table STV2024021962__DWH.l_groups_dialogs;
truncate table STV2024021962__DWH.l_user_message;
truncate table STV2024021962__DWH.l_user_group_activity;

INSERT INTO STV2024021962__DWH.l_admins(hk_l_admin_id, hk_group_id,hk_user_id,load_dt,load_src)
select
	hash(hg.hk_group_id,hu.hk_user_id),
	hg.hk_group_id,
	hu.hk_user_id,
	now() as load_dt,
	's3' as load_src
from STV2024021962__STAGING.groups as g
left join STV2024021962__DWH.h_users as hu on g.admin_id = hu.user_id
left join STV2024021962__DWH.h_groups as hg on g.id = hg.group_id
where hash(hg.hk_group_id,hu.hk_user_id) not in (select hk_l_admin_id from STV2024021962__DWH.l_admins);

INSERT INTO STV2024021962__DWH.l_groups_dialogs (hk_l_groups_dialogs, hk_message_id,hk_group_id,load_dt,load_src)
select
	hash(hd.hk_message_id, hg.hk_group_id),
	hd.hk_message_id,
	hg.hk_group_id,
	now() as load_dt,
	's3' as load_src
from STV2024021962__STAGING.dialogs d
left join STV2024021962__DWH.h_dialogs hd on hd.message_id = d.message_id
left join STV2024021962__DWH.h_groups hg on hg.group_id = d.message_group
where hash(hd.hk_message_id, hg.hk_group_id) not in (SELECT hk_l_groups_dialogs from STV2024021962__DWH.l_groups_dialogs)
and d.message_group is not null;


INSERT INTO STV2024021962__DWH.l_user_message(hk_l_user_message, hk_user_id, hk_message_id,load_dt,load_src)
select
	hash(hu.hk_user_id, hd.hk_message_id),
	hu.hk_user_id,
	hd.hk_message_id,
	now() as load_dt,
	's3' as load_src
from STV2024021962__STAGING.dialogs d
left join STV2024021962__DWH.h_users hu on d.message_from = hu.user_id 
left join STV2024021962__DWH.h_dialogs hd on hd.message_id = d.message_id 
where hash(hu.hk_user_id, hd.hk_message_id) not in (select hk_l_user_message from STV2024021962__DWH.l_user_message);


INSERT INTO STV2024021962__DWH.l_user_group_activity
(hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src)
select distinct
	hash(hu.hk_user_id, hg.hk_group_id)
	hu.hk_user_id,
	hg.hk_group_id,
	now() load_dt,
	's3' load_src 
from STV2024021962__STAGING.group_log gl
left join STV2024021962__DWH.h_users hu on hu.user_id = gl.user_id
left join STV2024021962__DWH.h_groups hg on hg.group_id = gl.group_id
where hash(hu.hk_user_id, hg.hk_group_id) not in (select hk_l_user_group_activity from STV2024021962__DWH.l_user_group_activity);
