/*
 * Creating a shipping_datamart view based on the ready-made tables for 
 * analytics and include the following in it:

* - shipping_id
* - vendor_id
* - transfer_type — the type of delivery from the shipping_transfer table
* full_day_at_shipping — the number of full days during which the delivery lasted.
* Calculated as follows: shipping_end_fact_datetime − shipping_start_fact_datetime
* - is_delay — the status showing whether the delivery is delayed. Calculated as follows: 
* shipping_end_fact_datetime > shipping_plan_datetime → 1; 0
* - is_shipping_finish — the status showing that the delivery is complete. 
* If the final status = finished → 1; 0
* - delay_day_at_shipping — the number of days by which the delivery was delayed. 
* Calculated as: shipping_end_fact_datetime > shipping_plan_datetime → shipping_end_fact_datetime − shipping_plan_datetime; 0)
* - payment_amount — the user's payment amount
* - vat — the final delivery tax Calculated as follows: 
* payment_amount ∗ (shipping_country_base_rate + agreement_rate + shipping_transfer_rate)
* - profit — the final company income from the delivery. Calculated as: 
* payment_amount ∗ agreement_commission
 */

-- creating view "shipping_datamart"

create or replace view shipping_datamart as 
select si.shipping_id, 
	si.vendor_id, 
	st.transfer_type,
	ss.shipping_end_fact_datetime::date - ss.shipping_start_fact_datetime::date as full_day_at_shipping,
	(case when ss.shipping_end_fact_datetime > si.shipping_plan_datetime then 1 else 0 end) as is_delay,
	(case when ss.status = 'finished' then 1 else 0 end) as is_shipping_finish,
	(case when ss.shipping_end_fact_datetime > si.shipping_plan_datetime 
		then shipping_end_fact_datetime::date - shipping_plan_datetime::date else 0 end) as delay_day_at_shipping,
	si.payment_amount,
	si.payment_amount * (scr.shipping_country_base_rate + sa.agreement_rate + st.shipping_transfer_rate) as vat,
	si.payment_amount * sa.agreement_commission as profit
from shipping_info si
left join shipping_transfer st on
	shipping_transfer_id = st.id
left join shipping_agreement sa on
	sa.agreement_id = si.shipping_agreement_id
left join shipping_country_rates scr on
	scr.id = si.shipping_country_rate_id 
left join shipping_status ss on
	si.shipping_id = ss.shipping_id;