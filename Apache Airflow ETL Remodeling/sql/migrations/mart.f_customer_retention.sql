
--delete
delete from mart.f_customer_retention;

--migration
insert into mart.f_customer_retention 
(new_customers_count, returning_customers_count, refunded_customer_count, period_name, 
period_id, item_id, new_customers_revenue, returning_customers_revenue, customers_refunded)
	select
		count(distinct case when order_count = 1 then customer_id end) new_customers_count,
		count(distinct case when order_count > 1 then customer_id end) returning_customers_count,
		count(distinct case when refunded_amount > 0 then customer_id end) refunded_customer_count,
		'weekly' period_name,
		extract(week from max(order_date)) period_id,
		item_id,
		sum(distinct case when order_count = 1 then total_payment end) new_customers_revenue,
		sum(distinct case when order_count > 1 then total_payment end) returning_customers_revenue,
		sum(distinct case when refunded_amount > 0 then total_payment end) customers_refunded
	from (select 
			uol.customer_id,
			uol.item_id,
			count(uol.uniq_id) order_count,
			count(distinct case when uol.status = 'refunded' then uol.uniq_id end) refunded_amount,
			sum(fs.payment_amount) total_payment,
			max(uol.date_time) as order_date
			from staging.user_order_log uol
		join mart.f_sales fs 
			using(customer_id, item_id)
		group by uol.customer_id, uol.item_id) t
	group by item_id
	having max(order_date)::Date = '{{ds}}';
	
	
	