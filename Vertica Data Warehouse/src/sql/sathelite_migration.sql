--truncate
truncate table STV2024021962__DWH.s_admins;
truncate table STV2024021962__DWH.s_auth_history;
truncate table STV2024021962__DWH.s_dialog_info;
truncate table STV2024021962__DWH.s_group_name;
truncate table STV2024021962__DWH.s_user_chatinfo;
truncate table STV2024021962__DWH.s_user_socdem;
truncate table STV2024021962__DWH.s_group_private_status;

--migration
INSERT INTO STV2024021962__DWH.s_admins(hk_admin_id, is_admin,admin_from,load_dt,load_src)
select la.hk_l_admin_id,
	True as is_admin,
	hg.registration_dt,
	now() as load_dt,
	's3' as load_src
from STV2024021962__DWH.l_admins as la
left join STV2024021962__DWH.h_groups as hg on la.hk_group_id = hg.hk_group_id;


INSERT INTO STV2024021962__DWH.s_dialog_info
(hk_message_id, message, message_from, message_to, load_dt, load_src)
select
	hd.hk_message_id,
	d.message,
	d.message_from,
	d.message_to,
	now() as load_dt,
	's3' load_src
from STV2024021962__DWH.h_dialogs hd 
left join STV2024021962__STAGING.dialogs d on d.message_id = hd.message_id;


INSERT INTO STV2024021962__DWH.s_group_name
(hk_group_id, group_name, load_dt, load_src)
select 
	hg.hk_group_id,
	g.group_name,
	now() load_dt,
	's3' load_src
from STV2024021962__DWH.h_groups hg
left join STV2024021962__STAGING.groups g on g.id = hg.group_id;


INSERT INTO STV2024021962__DWH.s_group_private_status
(hk_group_id, is_private, load_dt, load_src)
select
	hg.hk_group_id,
	g.is_private,
	now() load_dt,
	's3' load_src
from STV2024021962__DWH.h_groups hg
left join STV2024021962__STAGING.groups g on g.id = hg.group_id;


INSERT INTO STV2024021962__DWH.s_user_chatinfo
(hk_user_id, chat_name, load_dt, load_src)
select 
	hu.hk_user_id,
	u.chat_name,
	now() as load_dt,
	's3' load_src
from STV2024021962__DWH.h_users hu
left join STV2024021962__STAGING.users u on u.id = hu.user_id;

INSERT INTO STV2024021962__DWH.s_user_socdem
(hk_user_id, country, age, load_dt, load_src)
select
	hu.hk_user_id,
	u.country,
	u.age,
	now() load_dt,
	's3' load_src 
from STV2024021962__DWH.h_users hu
left join STV2024021962__STAGING.users u on u.id = hu.user_id;


INSERT INTO STV2024021962__DWH.s_auth_history
(hk_l_user_group_activity, user_id_from ,event,event_dt,load_dt,load_src)
select
	luga.hk_l_user_group_activity,
	gl.user_id_from,
	gl.event,
	gl.dt event_dt,
	now() load_dt,
	's3' load_src
from STV2024021962__STAGING.group_log as gl
left join STV2024021962__DWH.h_groups as hg on gl.group_id = hg.group_id
left join STV2024021962__DWH.h_users as hu on gl.user_id = hu.user_id
left join STV2024021962__DWH.l_user_group_activity as luga on hg.hk_group_id = luga.hk_group_id and hu.hk_user_id = luga.hk_user_id;