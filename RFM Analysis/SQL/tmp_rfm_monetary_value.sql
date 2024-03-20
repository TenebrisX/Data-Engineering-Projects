-- tmp_rfm_monetary_value table fill

--truncate table analysis.tmp_rfm_monetary_value;
insert into analysis.tmp_rfm_monetary_value 
with m as (
select user_id,
ntile(5) over (order by total_payment_amount) monetary_value
--case 
--	when percent_rank() over (order by total_payment_amount) <= 0.2 then 1
--	when percent_rank() over (order by total_payment_amount) <= 0.4 then 2
--	when percent_rank() over (order by total_payment_amount) <= 0.6 then 3
--	when percent_rank() over (order by total_payment_amount) <= 0.8 then 4
--	else 5
--end monetary_value
from (select user_id,
	   sum(payment) total_payment_amount
from analysis.orders
where status = 4
group by user_id) t
),

u as (
select distinct on (id) id 
from analysis.users
)

select u.id,
	   coalesce(monetary_value, 1)
from u 
left join m on u.id = m.user_id;


