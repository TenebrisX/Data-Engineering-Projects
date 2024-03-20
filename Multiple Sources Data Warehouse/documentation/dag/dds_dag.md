
### DDS DAG Information

- **DAG ID:** `dds_dag`
- **Schedule Interval:** Every 15 minutes
- **Start Date:** May 5, 2022, 00:00:00 UTC
- **Catch-up:** Disabled (Do not run DAG for previous periods on startup)
- **Tags:** `stg`, `dds`
- **Is Paused Upon Creation:** True (The DAG is paused upon creation)

### Tasks

#### Task: `dm_users_load`

- **Description:** Load DM Users data.
- **Dependencies:** None
- **Python Function:** `load_dm_users`

#### Task: `dm_restaurants_load`

- **Description:** Load DM Restaurants data.
- **Dependencies:** None
- **Python Function:** `load_dm_restaurants`

#### Task: `c_couriers_load`

- **Description:** Load C Couriers data.
- **Dependencies:** None
- **Python Function:** `load_c_couriers`

#### Task: `dm_timestamps_load`

- **Description:** Load DM Timestamps data.
- **Dependencies:** None
- **Python Function:** `load_dm_timestamps`

#### Task: `c_deliveries_load`

- **Description:** Load C Deliveries data.
- **Dependencies:** None
- **Python Function:** `load_c_deliveries`

#### Task: `dm_products_load`

- **Description:** Load DM Products data.
- **Dependencies:** None
- **Python Function:** `load_dm_products`

#### Task: `dm_orders_load`

- **Description:** Load DM Orders data.
- **Dependencies:** None
- **Python Function:** `load_dm_orders`

#### Task: `fct_product_sales_load`

- **Description:** Load FCT Product Sales data.
- **Dependencies:** None
- **Python Function:** `load_fct`

### DAG Execution Flow

The execution flow of tasks is defined as follows:

[users_dict, restaurants_dict, couriers_dict, timestamps_dict] >> deliveries_dict >> products_dict >> orders_dict >> fct_dict