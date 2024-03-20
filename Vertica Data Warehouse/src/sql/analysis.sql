
-- Find Earliest Groups
select hk_group_id
from stv2024021962__dwh.h_groups
order by registration_dt
limit 10;

-- Messages in Early Groups
select hk_message_id
from stv2024021962__dwh.l_groups_dialogs
join (
    select hk_group_id
    from stv2024021962__dwh.h_groups
    order by registration_dt
    limit 10
) as early_groups using(hk_group_id);

-- Users Sending Messages
select distinct hk_user_id
from stv2024021962__dwh.l_user_message
join stv2024021962__dwh.l_groups_dialogs using(hk_message_id)
where stv2024021962__dwh.l_groups_dialogs.hk_group_id in (
    select hk_group_id
    from stv2024021962__dwh.h_groups
    order by registration_dt
    limit 10
);
-- Age Distribution
with early_groups as (
    select hk_group_id
    from stv2024021962__dwh.h_groups
    order by registration_dt
    limit 10
),
early_group_messages as (
    select hk_message_id
    from stv2024021962__dwh.l_groups_dialogs
    where hk_group_id in (select hk_group_id from early_groups)
),
early_group_users as (
    select distinct hk_user_id
    from stv2024021962__dwh.l_user_message
    where hk_message_id in (select hk_message_id from early_group_messages)
)
select age, count(1)
from stv2024021962__dwh.s_user_socdem sus
where sus.hk_user_id in (select hk_user_id from early_group_users)
group by age
order by age;

-- User Group Message Activity Analysis
with early_groups as (
        select hk_group_id
        from stv2024021962__dwh.h_groups
        order by registration_dt 
        limit 10
     ),
    user_group_messages as (
        select 
            lgd.hk_group_id, 
            count(distinct lum.hk_user_id) as cnt_users_in_group_with_messages 
        from 
            stv2024021962__dwh.l_groups_dialogs lgd
        join stv2024021962__dwh.l_user_message lum using (hk_message_id)
        join stv2024021962__dwh.l_user_group_activity luga using (hk_user_id) 
        where lgd.hk_group_id in (select hk_group_id from early_groups) 
        group by lgd.hk_group_id
    ) 
select * 
from user_group_messages 
order by cnt_users_in_group_with_messages 
limit 10;

-- User Addition Analysis for Early Groups
with early_groups as (
    select hk_group_id
    from stv2024021962__dwh.h_groups
    order by registration_dt
    limit 10
),
early_group_users as (
    select distinct hk_user_id 
    from stv2024021962__dwh.l_user_message lum
    join stv2024021962__dwh.l_groups_dialogs lgd using (hk_message_id)
    join early_groups eg using (hk_group_id) 
),
user_group_log as (
  select 
    hk_group_id, 
    count(hk_user_id) as cnt_added_users 
  from stv2024021962__dwh.l_user_group_activity luga
  join stv2024021962__dwh.s_auth_history sah using (hk_l_user_group_activity)
  where sah.event = 'add'
    and luga.hk_user_id in (select hk_user_id from early_group_users)
  group by hk_group_id
) 
select 
  hk_group_id, 
  cnt_added_users 
from user_group_log 
order by cnt_added_users 
limit 10;

-- Recent Group Conversion Analysis 
with early_groups as (
    select hk_group_id
    from stv2024021962__dwh.h_groups
    order by registration_dt
),
early_group_users as ( 
    select distinct hk_user_id 
    from stv2024021962__dwh.l_user_message lum
    join stv2024021962__dwh.l_groups_dialogs lgd using(hk_message_id)
    join early_groups eg using(hk_group_id)
),
user_group_messages as (
  select 
    lgd.hk_group_id, 
    count(distinct lum.hk_user_id) as cnt_users_in_group_with_messages 
  from 
    stv2024021962__dwh.l_groups_dialogs lgd
  join stv2024021962__dwh.l_user_message lum using(hk_message_id)
  join stv2024021962__dwh.l_user_group_activity luga using(hk_user_id)
  where lgd.hk_group_id in (select hk_group_id from early_groups) 
  group by lgd.hk_group_id
), 
user_group_log as (
  select 
    hk_group_id, 
    count(hk_user_id) as cnt_added_users 
  from stv2024021962__dwh.l_user_group_activity luga
  join stv2024021962__dwh.s_auth_history sah using(hk_l_user_group_activity)
  where sah.event = 'add'
    and luga.hk_user_id in (select hk_user_id from early_group_users)
  group by hk_group_id
)
select 
    ugm.hk_group_id,
    ugl.cnt_added_users,
    ugm.cnt_users_in_group_with_messages,
    (ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users)::numeric(15,2) as group_conversion
from user_group_messages ugm
join user_group_log ugl using(hk_group_id)
order by group_conversion desc;

-- Group Growth Over Time
select 
    to_char(date_trunc('month', registration_dt), 'Month') as month,
    count(*) as new_groups
from stv2024021962__dwh.h_groups
group by month
order by new_groups;

-- Distribution of Messages by User
select 
    count(*) as message_count,
    percentile_cont(0.25) within group (order by count(*) desc) as q1,
    percentile_cont(0.5) within group (order by count(*) desc) as median,
    percentile_cont(0.75) within group (order by count(*) desc) as q3
from stv2024021962__dwh.l_user_message 
group by hk_user_id; 

-- Correlation Between Group Size and Activity (Monthly)
select 
    member_count,
    avg(monthly_messages)::numeric(12,2) as avg_messages_per_month
from (
    select 
        lgd.hk_group_id, 
        date_trunc('month', message_ts) as month,
        count(distinct hk_user_id) as member_count,
        count(*) as monthly_messages
    from stv2024021962__dwh.l_groups_dialogs lgd
    join stv2024021962__dwh.l_user_group_activity luga using (hk_group_id)
    join STV2024021962__DWH.h_dialogs hd using(hk_message_id)
    group by lgd.hk_group_id, month 
)t 
group by member_count
order by member_count;

