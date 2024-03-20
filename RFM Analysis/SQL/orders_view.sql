-- updated analysis.orders View.
drop view if exists analysis.orders;
create view analysis.orders as
select o.order_id,
       o.order_ts,
       o.user_id,
       o.bonus_payment,
       o.payment,
       o."cost",
       o.bonus_grant,
	   s_log.status_id as status
from production.orders o
join production.orderstatuslog s_log
	on o.order_id = s_log.order_id 
	and o.order_ts = s_log.dttm;