
-- delete
delete from mart.f_sales;

-- f_sales migration
insert into mart.f_sales 
(date_id, item_id, customer_id, city_id, quantity, payment_amount, status)
    select 
        dc.date_id, 
        item_id, customer_id, 
        city_id, quantity, 
        case when status = 'refunded' then payment_amount * -1 else payment_amount end payment_amount, 
        case when status = 'refunded' then 'shipped' else status end status
    from staging.user_order_log uol
    left join mart.d_calendar as dc on 
        uol.date_time::Date = dc.date_actual
    where uol.date_time::Date = '{{ds}}';