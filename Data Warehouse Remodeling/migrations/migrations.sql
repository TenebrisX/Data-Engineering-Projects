
--shipping_country_rates migration
insert into shipping_country_rates 
(shipping_country, shipping_country_base_rate)
	select distinct shipping_country, 
		shipping_country_base_rate 
 	from shipping;


--shipping_agreement migration
insert into shipping_agreement 
(agreement_id, agreement_number, agreement_rate, agreement_commission)
	select description[1]::bigint as agreement_id,
		description[2] as agreement_number,
		description[3]::numeric(14, 3) as agreement_rate,
		description[4]::numeric(14, 3) as agreement_commission
	from
		(select distinct on (vendor_agreement_description) 
			regexp_split_to_array(vendor_agreement_description, E'\\:+') description
		from shipping) t
	order by 1;


--shipping_transfer migration
insert into shipping_transfer
(transfer_type, transfer_model, shipping_transfer_rate)
	select trans_desc[1]::text as transfer_type,
	   trans_desc[2]::text as transfer_model,
	   shipping_transfer_rate::numeric(14, 3)
	from 
		(select distinct on (shipping_transfer_description)
	 		regexp_split_to_array(shipping_transfer_description, E'\\:+') trans_desc,
			shipping_transfer_rate
		from shipping) t;
		
	
-- shipping_info megration
insert into shipping_info
(shipping_id, vendor_id, payment_amount, shipping_plan_datetime, shipping_transfer_id, 
 shipping_agreement_id, shipping_country_rate_id)
	select shippingid as shipping_id,
			vendorid as vendor_id,
			payment_amount,
			shipping_plan_datetime,
			st.id as shipping_transfer_id,
			sa.agreement_id as shipping_agreement_id,
			scr.id as shipping_country_rate_id
	from 
		(select distinct on (shippingid) 
			shippingid, 
			shipping_plan_datetime, 
			payment_amount, 
			vendorid,
			shipping_country,
			shipping_country_base_rate,
			regexp_split_to_array(shipping_transfer_description, E'\\:+') as transfer,
			regexp_split_to_array(vendor_agreement_description, E'\\:+') as description
		from shipping) t
	left join shipping_transfer st 
		on st.transfer_type = t.transfer[1]::text and st.transfer_model = t.transfer[2]::text
	left join shipping_agreement sa 
		on sa.agreement_id = t.description[1]::bigint
	left join shipping_country_rates scr 
		on scr.shipping_country = t.shipping_country 
			and scr.shipping_country_base_rate = t.shipping_country_base_rate;

		
		
-- shipping_status migration
insert into shipping_status
(shipping_id, status, state, shipping_start_fact_datetime, shipping_end_fact_datetime)
with cte as (
select shippingid,
	max(case when state = 'booked' then state_datetime else null end) as start_datetime,
	max(case when state = 'recieved' or state = 'returned' then state_datetime else null end) as end_datetime,
	max(state_datetime) as max_date_time
from shipping
group by shippingid)

select distinct on (s.shippingid)
	s.shippingid as shipping_id,
	s.status as status,
	s.state as state,
	cte.start_datetime as shipping_start_fact_datetime,
	cte.end_datetime as shipping_end_fact_datetime
from shipping s
join cte on cte.shippingid = s.shippingid
where s.state_datetime = cte.max_date_time;

		