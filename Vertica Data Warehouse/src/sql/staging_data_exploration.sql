-- Count of Records in Users, Groups, and Dialogs
select
  count(id) as total,
  count(distinct id) as uniq,
  'users' as dataset
from STV2024021962__STAGING.users

union all

select
  count(id) as total,
  count(distinct id) as uniq,
  'groups' as dataset
from STV2024021962__STAGING.groups

union all

select
  count(message_id) as total,
  count(distinct message_id) as uniq,
  'dialogs' as dataset
from STV2024021962__STAGING.dialogs;

-- Count of Hashed Group Names in Groups
select count(hash(g.group_name)), count(distinct hash(g.group_name)) 
from STV2024021962__STAGING.groups g;

-- Count and Sum of Ages in Users
select count(1), sum(sign(u.age)) from STV2024021962__STAGING.users u;

-- Earliest and Latest Timestamps in Users, Groups, and Dialogs
(select 
  min(u.registration_dt) as datestamp,
  'earliest user registration' as info
from STV2024021962__STAGING.users u)

union all

(select
  max(u.registration_dt),
  'latest user registration'
from STV2024021962__STAGING.users u)

union all

(select min(g.registration_dt) as datestamp,
  'earliest group creation' as info
from STV2024021962__STAGING.groups g)

union all

(select max(g.registration_dt),
  'latest group creation'
from STV2024021962__STAGING.groups g)

union all

(select min(d.message_ts),
  'earliest group creation'
from STV2024021962__STAGING.dialogs d)

union all

(select max(d.message_ts),
  'latest group creation'
from STV2024021962__STAGING.dialogs d)

-- Checking Future and False-Start Dates
(select 
  max(u.registration_dt) < now() as 'no future dates',
  (min(u.registration_dt) > '2020-09-03'::timestamp) as 'no false-start dates',
  'users' as dataset
from STV2024021962__STAGING.users u)
union all
(select
  max(g.registration_dt) < now() as 'no future dates',
  (min(g.registration_dt) > '2020-09-03'::timestamp) as 'no false-start dates',
  'groups'
from STV2024021962__STAGING.groups g)
union all
(select
  max(d.message_ts) < now() as 'no future dates',
  (min(d.message_ts) > '2020-09-03'::timestamp) as 'no false-start dates',
  'dialogs'
from STV2024021962__STAGING.dialogs d);

-- Count of Group Admin IDs with No Corresponding User
select count(g.admin_id)
from STV2024021962__STAGING.groups as g 
left join STV2024021962__STAGING.users as u 
on u.id = g.admin_id
where u.id is null

-- Count of Missing Information in Groups and Dialogs
(select count(1), 'missing group admin info' as info
from STV2024021962__STAGING.groups g
where g.admin_id is null)

union all

(select count(1), 'missing sender info'
from STV2024021962__STAGING.dialogs d
where d.message_from is null)

union all

(select count(1), 'missing receiver info'
from STV2024021962__STAGING.dialogs d
where d.message_to is null)

union all 

(select count(1), 'norm receiver info'
from STV2024021962__STAGING.dialogs d
where d.message_to is not null);

--Distribution of Users by top 15 Countries
select country, count(*) as user_count
from STV2024021962__STAGING.users
group by country
order by user_count desc; 

-- Average Group Size
select avg(member_count)::int as avg_group_size
from (
    select group_id, count(*) as member_count
    from STV2024021962__STAGING.group_log 
    group by group_id
) t; 

-- Dialog Activity Over Time (Yearly)
select 
    extract(year from message_ts) as year, 
    count(*) as message_count
from STV2024021962__STAGING.dialogs
group by year
order by year;
