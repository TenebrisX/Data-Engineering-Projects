-- tmp_rfm_recency table fill

-- truncate table analysis.tmp_rfm_recency;
insert into analysis.tmp_rfm_recency
with r as (select user_id,
ntile(5) over (order by day_recency desc) recency
--case 
--	when percent_rank() over (order by day_recency) <= 0.2 then 5 
--	when percent_rank() over (order by day_recency) <= 0.4 then 4
--	when percent_rank() over (order by day_recency) <= 0.6 then 3
--	when percent_rank() over (order by day_recency) <= 0.8 then 2
--	else 1
--end recency
from (select user_id,
	  		 date_part('day', current_timestamp - max(order_ts)) day_recency
      from analysis.orders
	  where status = 4
	  group by user_id) t
),

u as (
select distinct on (id) id
from analysis.users
)

select u.id,
	   coalesce(r.recency, 1)
from u
left join r on u.id = r.user_id;

