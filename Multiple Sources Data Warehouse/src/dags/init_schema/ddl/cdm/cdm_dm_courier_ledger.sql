create table if not exists cdm.dm_courier_ledger (
    id serial primary key,
    courier_id integer not null,
    courier_name varchar(255) not null,
    settlement_year integer not null,
    settlement_month integer not null check (settlement_month >= 1 and settlement_month <= 12),
    orders_count integer not null,
    orders_total_sum decimal(10, 2) not null,
    rate_avg decimal(3, 2) not null check (rate_avg >= 0 and rate_avg <= 5),
    order_processing_fee decimal(10, 2) not null,
    courier_order_sum decimal(10, 2) not null,
    courier_tips_sum decimal(10, 2) not null,
    courier_reward_sum decimal(10, 2) not null
);
