## dm_courier_ledger Datamart Structure:

| Column             | Data Type       | Constraints                                           |
|:-------------------|:----------------|:-------------------------------------------------------|
| id                 | serial          | Primary Key                                           |
| courier_id         | integer         | Not Null                                              |
| courier_name       | varchar(255)    | Not Null                                              |
| settlement_year    | integer         | Not Null                                              |
| settlement_month   | integer         | Not Null, Check (1 ≤ settlement_month ≤ 12)            |
| orders_count       | integer         | Not Null                                              |
| orders_total_sum   | decimal(10, 2)  | Not Null                                              |
| rate_avg           | decimal(3, 2)   | Not Null, Check (0 ≤ rate_avg ≤ 5)                     |
| order_processing_fee | decimal(10, 2) | Not Null                                              |
| courier_order_sum  | decimal(10, 2)  | Not Null                                              |
| courier_tips_sum   | decimal(10, 2)  | Not Null                                              |
| courier_reward_sum | decimal(10, 2)  | Not Null                                              |


## DDS Tables Needed:
#### Existing:
- dds.dm_timestamps
- dds.dm_orders
- dds.fct_product_sales

#### Not Existing:
- dds.c_couriers
- dds.c_deliveries

## Required API Entities and Tables
#### /Couriers:
- _id
- name

#### /Deliveries:
- order_id
- order_ts
- delivery_id
- courier_id
- delivery_ts
- rate
- tip_sum
