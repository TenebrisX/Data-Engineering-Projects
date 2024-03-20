-- drop
-- drop table if exists cdm.dm_settlement_report;

create table if not exists cdm.dm_settlement_report (
    id serial not null,
    restaurant_id varchar not null,
    restaurant_name varchar not null,
    settlement_date date not null
        check (settlement_date >= '2022-01-01' and settlement_date < '2500-01-01'),
    orders_count integer not null
        default 0
        check (orders_count >= 0),
    orders_total_sum numeric(14,2) not null
        default 0
        check (orders_total_sum >= 0),
    orders_bonus_payment_sum numeric(14,2) not null
        default 0
        check (orders_bonus_payment_sum >= 0),
    orders_bonus_granted_sum numeric(14,2) not null
        default 0
        check (orders_bonus_granted_sum >= 0),
    order_processing_fee numeric(14,2) not null
        default 0
        check (order_processing_fee >= 0),
    restaurant_reward_sum numeric(14,2) not null
        default 0
        check (restaurant_reward_sum >= 0),
    primary key (id),
    unique (restaurant_id, settlement_date)
);

