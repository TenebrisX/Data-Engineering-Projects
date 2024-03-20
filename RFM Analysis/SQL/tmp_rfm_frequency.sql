-- tmp_rfm_frequency table fill

--truncate table analysis.tmp_rfm_frequency;
insert into analysis.tmp_rfm_frequency
with f as (select user_id,
ntile(5) over (order by total_orders) frequency
--case 
--	when percent_rank() over (order by total_orders) <= 0.2 then 1
--	when percent_rank() over (order by total_orders) <= 0.4 then 2
--	when percent_rank() over (order by total_orders) <= 0.6 then 3
--	when percent_rank() over (order by total_orders) <= 0.8 then 4
--	else 5
--end frequency
from (select user_id,
	  		 count(order_id) total_orders
	  from analysis.orders
	  where status = 4
      group by user_id) t
),

u as (select distinct on (id) id
from analysis.users
)

select u.id,
	   coalesce(f.frequency, 1)
from u 
left join f on u.id = f.user_id;
