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


