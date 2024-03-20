# Airflow - MongoDB - AWS S3 - PostgreSQL ETL Data Warehouse Project

## Project Overview

This project involves enhancing existing data warehouse by adding a new data source and data mart. The goal is to build a data mart for calculating payments to couriers based on their performance. The data from the new source needs to be integrated with the existing data in the warehouse.

### Project Tasks

1. **Data Integration:**
   - Study possible data sources, their internal data models, and the technologies used to extract data.
   - Design a multi-layered Data Warehouse (DWH) model, detailing each data layer.
   - Implement ETL processes to transform and move data from sources to final data layers.

2. **Courier Payments Data Mart:**
   - Build a data mart for calculating payments to couriers.
   - Include the following fields in the data mart:
     - `id`: Record identifier.
     - `courier_id`: ID of the courier receiving payment.
     - `courier_name`: Full name of the courier.
     - `settlement_year`: Reporting year.
     - `settlement_month`: Reporting month (1 for January, 12 for December).
     - `orders_count`: Number of orders for the period (month).
     - `orders_total_sum`: Total cost of orders.
     - `rate_avg`: Average courier rating based on user reviews.
     - `order_processing_fee`: Amount retained by the company for order processing (calculated as `orders_total_sum * 0.25`).
     - `courier_order_sum`: Amount to be paid to the courier for delivered orders (dependent on rating).
     - `courier_tips_sum`: Sum of tips left by users for the courier.
     - `courier_reward_sum`: Total amount to be paid to the courier (calculated as `courier_order_sum + courier_tips_sum * 0.95`).

3. **Rating-based Payment Rules:**
   - Define rules for calculating the percentage of payment to the courier based on their average rating (`r`):
     - `r < 4`: 5% of the order amount, but not less than 100$.
     - `4 <= r < 4.5`: 7% of the order amount, but not less than 150$.
     - `4.5 <= r < 4.9`: 8% of the order amount, but not less than 175$.
     - `4.9 <= r`: 10% of the order amount, but not less than 200$.

4. **Integration with Courier Service API:**
   - Fetch courier service data from the API.
   - Merge this data with the existing order subsystem data in the warehouse.

5. **Reporting:**
   - Generate reports based on order dates, considering cases where orders made at night might be delivered the next day.

## Documentation

>> [Documentation](https://github.com/TenebrisX/de-project-sprint-5/tree/main/documentation)
Include all necessary documentation.
