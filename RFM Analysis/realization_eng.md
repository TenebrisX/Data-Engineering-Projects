# RFM Data warehouse
---

## 1.1. Data warehouse requirements

### Create a data mart for RFM customer segmentation:

    Data mart name: dm_rfm_segments
    Data mart location: de.analysis
    Data location: de.production

    Data mart structure:
        user_id
        recency (number from 1 to 5) - Number of days since the last order
        frequency (number from 1 to 5) - Number of orders
        monetary_value (number from 1 to 5) - Total customer spend 
    Data mart depth: 2022/01/01 - 2022/12/31 
    No updates needed A successful order is an order with status "Closed" Segment users into 5 equal RMF segments, 
    if the number of orders is equal, then order them randomly.




## 1.2. Data source structure

### Requirements for the RFM customer segmentation data mart

#### Input data:

The data mart will be based on data from the ```de.production``` database.

The data mart will use data from the following tables:
- ```orders```: orders
- `products`: products
- `orderitems`: ordered items
- `orderstatuslog`: order statuses log
- `orderstatuses`: order statuses
- `users`: users

Fields:

- **`orders`**:
	- `order_id` - int4 NOT NULL
	- `order_ts` - timestamp NOT NULL
	- `user_id` - int4 NOT NULL
	- `bonus_payment` - numeric(19, 5) NOT NULL DEFAULT 0
	- `payment` - numeric(19, 5) NOT NULL DEFAULT 0
	- `"cost"` - numeric(19, 5) NOT NULL DEFAULT 0
	- `bonus_grant` - numeric(19, 5) NOT NULL DEFAULT 0
	- `status` - int4 NOT NULL

- **`products`**:
	- `id` - int4 NOT NULL
	- `"name"` - varchar(2048) NOT NULL
	- `price` - numeric(19, 5) NOT NULL DEFAULT 0

- **`orderitems`**:
	- `id` - int4 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE)
	- `product_id` - int4 NOT NULL
	- `order_id` - int4 NOT NULL
	- `"name"` - varchar(2048) NOT NULL
	- `price` - numeric(19, 5) NOT NULL DEFAULT 0
	- `discount` - numeric(19, 5) NOT NULL DEFAULT 0
	- `quantity` - int4 NOT NULL

- **`orderstatuslog`**:
	- `id` - int4 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE)
	- `order_id` - int4 NOT NULL
	- `status_id` - int4 NOT NULL
	- `dttm` - timestamp NOT NULL

- **`orderstatuses`**:
	- `id` - int4 NOT NULL
	- `"key"` - varchar(255) NOT NULL

- **`users`**:
	- `id` - int4 NOT NULL
	- `"name"` - varchar(2048) NULL
	- `login` - varchar(2048) NOT NULL


#### Output data:

The data mart will calculate the RFM segments for each customer.
The RFM segments are calculated as follows:
- `user_id`: user id
- `recency`: the number of days since the customer's most recent order
- `frequency`: the number of orders placed by the customer
- `monetary_value`: the total amount spent by the customer

The following fields will be used from the orders table:
- `order_id`: order ID
- `order_ts`: order timestamp
- `user_id`: user ID
- `payment`: payment amount
- `status`: order status

Data mart depth: 2022-01-01 - 2022-12-31.

The data mart will not be updated.

A successful order is an order with the status "Closed"

Divide users into 5 equal RMF segments if the number of orders is equal, regardless of who is higher or lower.


## 1.3 Data quality analysis

## Data Quality
---
- Data integrity check: checking for duplicates, out-of-range values, logical errors, etc.
- Data missing analysis: determining the number and causes of data missing.
- Anomalous value analysis: identifying values that are outside of expected values.

## Summary:

Data Integrity Verification:

Missing Values: The orders table exhibits no missing values in the ```order_ts```, ```order_id```, ```payment```, ```user_id```, or ```status``` columns.
Duplicate Values: No duplicate ```order_id``` values were detected in the ```orders``` table.


### Data Range and Value Distribution Analysis:


- Order IDs: The ```order_id``` values fall within a valid range, with no negative values.
Order Timestamps: The ```order_ts``` values span approximately one month, 
from February 12, 2022, to March 14, 2022.

- User IDs: The ```user_id``` values include a user with ID 0. Further investigation revealed
that this user is valid, with an average number of orders comparable to other users 
and a total of 13 orders with user ID = 0.

- Payment Amounts: The ```payment``` amounts range from 60 to 6360, aligning with the minimum
and maximum product prices.

- Order Statuses: The ```status``` values adhere to the valid range of 1 to 5, 
with no out-of-range errors.


### Additional Observations and Recommendations:


Order Status Distribution: Approximately half of the orders have the status ```"Closed"```.
Users with "Closed" Orders: 988 users have placed orders with the status ```"Closed"```.

User Table Inconsistencies: The ```users``` table appears to have inconsistencies in the
```login``` and ```name``` columns. Since this table is not directly relevant to the current task, 
it will not be addressed at this time. Implementing a data validation process could prevent 
the introduction of inconsistent data in the future.

Overall, the ```orders``` table exhibits good data integrity and falls within expected value ranges.
The observed patterns and distributions align with reasonable business scenarios.

### Database constraints description:


| **Table Name**            | **Object**                                      | **Tool**           | **Purpose**                                                      |
|---------------------------|-------------------------------------------------|--------------------|------------------------------------------------------------------|
| production.orderitems     | id int4 NOT NULL                                | _PRIMARY KEY_      | Ensures uniqueness of order item records                         |
| production.orderitems     | order_id int4 NOT NULL                          | _INDEX_            | Speeds up search for order item records by order ID              |
| production.orderitems     | product_id int4 NOT NULL                        | _INDEX_            | Speeds up search for order item records by product ID            |
| production.orderitems     | discount numeric(19, 5) NOT NULL DEFAULT 0      | _CHECK CONSTRAINT_ | Limits the discount value to be between 0 and the product price  |
| production.orderitems     | price numeric(19, 5) NOT NULL DEFAULT 0         | _CHECK CONSTRAINT_ | Limits the product price to be non-negative                      |
| production.orderitems     | quantity int4 NOT NULL                          | _CHECK CONSTRAINT_ | Limits the order quantity to be greater than 0                   |
| production.orders         | order_id int4 NOT NULL                          | _PRIMARY KEY_      | Ensures uniqueness of order records                              |
| production.orders         | order_ts timestamp NOT NULL                     | _CHECK CONSTRAINT_ | Limits the order timestamp to be no later than the current date  |
| production.orders         | user_id int4 NOT NULL                           | _INDEX_            | Speeds up search for order records by user ID                    |
| production.orders         | bonus_payment numeric(19, 5) NOT NULL DEFAULT 0 | _CHECK CONSTRAINT_ | Limits the bonus payment to be non-negative                      |
| production.orders         | payment numeric(19, 5) NOT NULL DEFAULT 0       | _CHECK CONSTRAINT_ | Limits the payment to be non-negative                            |
| production.orders         | "cost" numeric(19, 5) NOT NULL DEFAULT 0        | _CHECK CONSTRAINT_ | Limits the order cost to be equal to the payment + bonus payment |
| production.orders         | bonus_grant numeric(19, 5) NOT NULL DEFAULT 0   | _CHECK CONSTRAINT_ | Limits the bonus grant to be non-negative                        |
| production.orders         | status int4 NOT NULL                            | _INDEX_            | Speeds up search for order records by status                     |
| production.orderstatuses  | id int4 NOT NULL                                | _PRIMARY KEY_      | Ensures uniqueness of order status records                       |
| production.orderstatuslog | id int4 NOT NULL                                | _PRIMARY KEY_      | Ensures uniqueness of order status log records                   |
| production.orderstatuslog | order_id int4 NOT NULL                          | _INDEX_            | Speeds up search for order status log records by order ID        |
| production.orderstatuslog | status_id int4 NOT NULL                         | _INDEX_            | Speeds up search for order status log records by status ID       |
| production.products       | id int4 NOT NULL                                | _PRIMARY KEY_      | Ensures uniqueness of product records                            |
| production.products       | "name" varchar(2048) NOT NULL                   | _INDEX_            | Speeds up search for product records by name                     |
| production.products       | price numeric(19, 5) NOT NULL DEFAULT 0         | _CHECK CONSTRAINT_ | Limits the product price to be non-negative                      |
| production.users          | id int4 NOT NULL                                | _PRIMARY KEY_      | Ensures uniqueness of user records                               |
| production.users          | "name" varchar(2048) NULL                       | _INDEX_            | Speeds up search for user records by name (if not null)          |
| production.users          | login varchar(2048) NOT NULL                    | _INDEX_            | Speeds up search for user records by login                       |




## 1.4. Preparing the data warehouse 

### 1.4.1. Creating VIEWs for tables from the production database

```SQL
-- Users View
--drop view if exists analysis.users;
create view analysis.users as
select *
from production.users;

-- OrderItems View
--drop view if exists analysis.orderitems;
create view analysis.orderitems as
select *
from production.orderitems;

-- OrderStatuses View
--drop view if exists analysis.orderstatuses;
create view analysis.orderstatuses as
select *
from production.orderstatuses;

-- Products View
--drop view if exists analysis.products;
create view analysis.products as
select *
from production.products;

-- Orders View
--drop view if exists analysis.orders;
create view analysis.orders as
select *
from production.orders;


```

### 1.4.2. DDL query to create a data warehouse

``` SQL

--Data mart ddl

create table if not exists dm_rfm_segments (
user_id int4 not null,
recency int4 not null,
frequency int4 not null,
monetary_value int4 not null,

constraint dm_rfm_segments_user_id_pk primary key(user_id),
constraint dm_rfm_segments_recency_check check(recency >= 1 and recency <= 5),
constraint dm_rfm_segments_frequency_check check(frequency >= 1 and frequency <= 5),
constraint dm_rfm_segments_monetary_value_check check(monetary_value >= 1 and monetary_value <= 5)
);


-- checking unique index
select indexdef 
from pg_catalog.pg_indexes
where tablename = 'dm_rfm_segments';

```

### 1.4.3. SQL query to load data into a data warehouse
```SQL
-- tmp_rfm_recency table fill

-- truncate table analysis.tmp_rfm_recency;
insert into analysis.tmp_rfm_recency
with r as (select user_id,
ntile(5) over (order by day_recency desc) recency
--case 
--	when percent_rank() over (order by day_recency) <= 0.2 then 5 
--	when percent_rank() over (order by day_recency) <= 0.4 then 4
--	when percent_rank() over (order by day_recency) <= 0.6 then 3
--	when percent_rank() over (order by day_recency) <= 0.8 then 2
--	else 1
--end recency
from (select user_id,
	  		 date_part('day', current_timestamp - max(order_ts)) day_recency
      from analysis.orders
	  where status = 4
	  group by user_id) t
),

u as (
select distinct on (id) id
from analysis.users
)

select u.id,
	   coalesce(r.recency, 1)
from u
left join r on u.id = r.user_id;


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





-- dm_rfm_segments table fill

--truncate table analysis.dm_rfm_segments;
insert into analysis.dm_rfm_segments
select trr.user_id, 
	   trr.recency,
	   trf.frequency,
	   trmv.monetary_value
from analysis.tmp_rfm_recency trr 
left join analysis.tmp_rfm_frequency trf 
	on trr.user_id = trf.user_id
left join analysis.tmp_rfm_monetary_value trmv
	on trmv.user_id = trr.user_id;


-- 10 first rows
select *
from analysis.dm_rfm_segments drs
order by user_id
limit 10;

ser_id|recency|frequency|monetary_value|
------+-------+---------+--------------+
     0|      1|        3|             4|
     1|      4|        3|             3|
     2|      2|        3|             5|
     3|      2|        3|             3|
     4|      4|        3|             3|
     5|      5|        5|             5|
     6|      1|        3|             5|
     7|      4|        2|             2|
     8|      1|        1|             3|
     9|      2|        2|             2|

```



