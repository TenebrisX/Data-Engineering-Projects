## Data warehouse requirements

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
    

## Data source structure

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
