# DataVault ETL Project

## Table of Contents

1. [Overview](#overview)
2. [Technologies Used](#technologies-used)
3. [Project Structure](#project-structure)
6. [Analysis and Queries](#analysis-and-queries)

## Overview

This project involves the extraction, transformation, and loading (ETL) of data from AWS S3 to a Vertica database. The ETL pipeline is orchestrated using Apache Airflow and containerized using Docker. SQL and Python are the primary languages for data manipulation and analysis.

## Technologies Used

* **AWS S3:**  Object storage for raw data.
* **Vertica:** Columnar database for high-performance analytics.
* **Apache Airflow:**  Workflow orchestration tool to manage the ETL pipeline.
* **Docker:** Containerization for streamlined development and deployment.
* **SQL:**  For querying and manipulating data within Vertica.
* **Python:**  For data transformation and pipeline logic.

## Project Structure

project_root/

├── src/dags/            # Directory for Airflow DAGs

├── src/sql/             # Vertica SQL scripts (DDL, queries, etc.)

├── img/                 # Layer Diagrams

## Analysis and Queries

- Staging Data Analysis: [Link to Staging Data Analysis](https://github.com/TenebrisX/Data-Engineering-Projects/blob/main/Vertica%20Data%20Warehouse/staging_data_analysis.md)
- DDS Data Analysis: [Link to DDS Data Analysis](https://github.com/TenebrisX/Data-Engineering-Projects/blob/main/Vertica%20Data%20Warehouse/dds_data_analysis.md)

