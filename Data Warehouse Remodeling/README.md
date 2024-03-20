# Data Warehouse Remodeling Project
## Shipping Data Processing

This project involves processing and transforming shipping-related data from the `shipping` table into several derived tables and a view for analytical purposes.

## Table of Contents
- [Overview](#overview)
- [Input Data Structure](#input-data-structure)
- [Output Data](#output-data)
- [Tables](#tables)
- [View](#view)
- [Analytics View Fields](#analytics-view-fields)

## Overview

The input data consists of a PostgreSQL table named `shipping` with various attributes related to shipping transactions. The goal is to create several output tables (`shipping_country_rates`, `shipping_agreement`, `shipping_transfer`, `shipping_info`, `shipping_status`) and a view (`shipping_datamart`) that provides valuable insights for analytics.

## Input Data Structure

![](https://github.com/TenebrisX/Data-Engineering-Projects/blob/main/Data%20Warehouse%20Remodeling/old_schema.png)

The `shipping` table includes the following columns:
- `ID`
- `shipping_id`
- `sale_id`
- `order_id`
- `client_id`
- `payment_amount`
- `state_datetime`
- `product_id`
- `description`
- `vendor_id`
- `name_category`
- `base_country`
- `status`
- `state`
- `shipping_plan_datetime`
- `hours_to_plan_shipping`
- `shipping_transfer_description`
- `shipping_transfer_rate`
- `shipping_country`
- `shipping_country_base_rate`
- `vendor_agreement_description`

## Output Data

![](https://github.com/TenebrisX/Data-Engineering-Projects/blob/main/Data%20Warehouse%20Remodeling/new_schema.png)

The output data consists of several tables and a view:

### Tables
1. `shipping_country_rates`: Contains shipping country rates.
2. `shipping_agreement`: Includes shipping vendor agreements.
3. `shipping_transfer`: Contains information about shipping transfers.
4. `shipping_info`: A consolidated table with shipping information.
5. `shipping_status`: Provides status information for each shipping transaction.

### View
- `shipping_datamart`: A view for analytics, combining data from the above tables and additional calculated fields.

## Processing Steps

The project involves the following processing steps:
1. Creation of `shipping_country_rates` based on unique `shipping_country` and `shipping_country_base_rate`.
2. Creation of `shipping_agreement` from `vendor_agreement_description`.
3. Creation of `shipping_transfer` from `shipping_transfer_description`.
4. Creation of `shipping_info` by combining data from various tables, including `shipping_country_rates`, `shipping_agreement`, and `shipping_transfer`.
5. Creation of `shipping_status` based on the `status` and `state` columns.
6. Migration of data into the respective tables.
7. Creation of the `shipping_datamart` view for analytical purposes.

## Analytics View Fields

The `shipping_datamart` view includes the following fields:
- `shipping_id`
- `vendor_id`
- `transfer_type`
- `full_day_at_shipping`
- `is_delay`
- `is_shipping_finish`
- `delay_day_at_shipping`
- `payment_amount`
- `vat`
- `profit`

These fields provide insights into the duration, delays, completion status, and financial aspects of shipping transactions.
