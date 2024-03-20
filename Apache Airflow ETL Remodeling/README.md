# Airflow ETL Pipelines for Sales Mart and Customer Retention

## Table of Contents
- [Overview](#overview)
- [File Structure](#file-structure)
- [DAG Descriptions](#dag-descriptions)
  - [Original Tables DAG](#original-tables-dag)
  - [Source Tables DAG](#source-tables-dag)
  - [Increment DAG](#increment-dag)

## Overview
This repository contains Apache Airflow DAGs for ETL pipelines designed to manage the update process for tables in a sales data mart and compute customer retention metrics. The pipelines engage with an external API for report generation and retrieval, handle file uploads from S3, and execute data transformations to update both staging and mart tables.

## File Structure

- `/src/dags`
  - `original_tables_dag.py`
  - `source_tables_dag.py`
  - `increment_dag.py`
- `/sql`
	- `migrations`
	  - `mart.d_city.sql`
	  - `mart.d_customer.sql`
	  - `mart.d_item.sql`
	  - `mart.f_customer_retention.sql`
	  - `mart.f_sales.sql`
	- `ddl`
		- `dq_checks_result.sql`
		- `f_customer_retention_ddl.sql`
	- `data_quality_check`
		- `user_activity_log_isNull_check.sql`
		- `user_order_log_isNull_check.sql`

## DAG Descriptions

### Original Tables DAG

The `original_tables_dag.py` DAG file orchestrates tasks to handle the initial load of data into the data mart. It performs the following tasks:

1. **generate_report:** Initiates the generation of a report via a POST request and pushes the resulting task_id to XCom.

2. **get_report:** Retrieves a report_id from an API endpoint and pushes it to XCom.

3. **upload_from_s3:** Uploads files from an S3 bucket to a local directory.

4. **load_customer_research:** Uploads data from the 'customer_research.csv' file to the 'stage.customer_research' table in PostgreSQL.

5. **load_user_order_log:** Uploads data from the 'user_order_log.csv' file to the 'stage.user_order_log' table in PostgreSQL.

6. **load_user_activity_log:** Uploads data from the 'user_activity_log.csv' file to the 'stage.user_activity_log' table in PostgreSQL.

7. **load_price_log:** Uploads data from the 'price_log.csv' file to the 'stage.price_log' table in PostgreSQL.

### Original Tables DAG Schema:

![](https://github.com/TenebrisX/Data-Engineering-Projects/blob/main/Apache%20Airflow%20ETL%20Remodeling/images/original_tables_schema.png)


### Source Tables DAG
#### Same as the Original Tables DAG but with extra steps for data quality check

The `sourse_tables_dag.py` DAG file orchestrates tasks to handle the initial load of data into the data mart. It performs the following tasks:

1. **generate_report:** Initiates the generation of a report via a POST request and pushes the resulting task_id to XCom.

2. **get_report:** Retrieves a report_id from an API endpoint and pushes it to XCom.

3. **upload_from_s3:** Uploads files from an S3 bucket to a local directory.

4. **load_customer_research:** Uploads data from the 'customer_research.csv' file to the 'stage.customer_research' table in PostgreSQL.

5. **load_user_order_log:** Uploads data from the 'user_order_log.csv' file to the 'stage.user_order_log' table in PostgreSQL.

6. **load_user_activity_log:** Uploads data from the 'user_activity_log.csv' file to the 'stage.user_activity_log' table in PostgreSQL.

7. **load_price_log:** Uploads data from the 'price_log.csv' file to the 'stage.price_log' table in PostgreSQL.

8. **user_order_log_isNull:** Performs a SQL check to ensure there are no null values in the 'user_order_log' table.

9. **user_activity_log_isNull:** Performs a SQL check to ensure there are no null values in the 'user_activity_log' table.

10. **check_row_count_user_order_log:** Performs a SQL check on the row count in the 'user_order_log' table.

11. **check_row_count_user_activity_log:** Performs a SQL check on the row count in the 'user_activity_log' table.


### Source Tables DAG Schema:

![](https://github.com/TenebrisX/Data-Engineering-Projects/blob/main/Apache%20Airflow%20ETL%20Remodeling/images/sourse_tables_schema.png)


### Increment DAG

The `increment_dag.py` DAG file manages incremental updates to the data mart. It performs the following tasks:

1. **generate_report:** Initiates the generation of a report via a POST request and pushes the resulting task_id to XCom.

2. **get_report:** Retrieves a report_id from an API endpoint and pushes it to XCom.

3. **get_increment:** Retrieves an increment_id from an API endpoint and pushes it to XCom.

4. **upload_user_order_log_inc:** Uploads data from an S3 file to a PostgreSQL table in a staging environment for the 'user_order_log' table.

5. **upload_customer_research_inc:** Uploads data from an S3 file to a PostgreSQL table in a staging environment for the 'customer_research' table.

6. **upload_user_activity_log_inc:** Uploads data from an S3 file to a PostgreSQL table in a staging environment for the 'user_activity_log' table.

7. **update_d_city_table:** Executes a SQL script for updating the 'd_city' table.

8. **update_d_item_table:** Executes a SQL script for updating the 'd_item' table.

9. **update_d_customer_table:** Executes a SQL script for updating the 'd_customer' table.

10. **update_f_sales:** Executes a SQL script for updating the 'f_sales' table.

11. **update_f_customer_retention:** Executes a SQL script for updating the 'f_customer_retention' table.

