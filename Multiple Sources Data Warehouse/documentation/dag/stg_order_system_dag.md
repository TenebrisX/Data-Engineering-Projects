# Order System DAG Documentation

## Overview

The Order System DAG is designed to extract data from MongoDB collections related to orders, restaurants, and users, and load this data into a PostgreSQL data warehouse. The DAG is scheduled to run every 15 minutes, starting from May 5, 2022, in Coordinated Universal Time (UTC).

## Components

### 1. Connection Details

- **PostgreSQL Connection**: The DAG establishes a connection to the PostgreSQL data warehouse using the credentials specified in the `PG_WAREHOUSE_CONNECTION` Airflow variable.

- **MongoDB Connection**: MongoDB connection details are fetched from the following Airflow variables:
  - `MONGO_DB_CERTIFICATE_PATH`: Path to the MongoDB certificate file.
  - `MONGO_DB_USER`: MongoDB username.
  - `MONGO_DB_PASSWORD`: MongoDB password.
  - `MONGO_DB_REPLICA_SET`: MongoDB replica set name.
  - `MONGO_DB_DATABASE_NAME`: MongoDB database name.
  - `MONGO_DB_HOST`: MongoDB host.

### 2. Tasks

#### a. Load Restaurants (`load_restaurants`)

- **Description**: This task reads restaurant data from the MongoDB "restaurants" collection and loads it into the PostgreSQL data warehouse.

- **Components**:
  - **MongoReader**: `RestaurantReader` reads data from the MongoDB "restaurants" collection.
  - **MongoLoader**: `RestaurantLoader` processes and saves the data into the PostgreSQL data warehouse.

#### b. Load Users (`load_users`)

- **Description**: This task reads user data from the MongoDB "users" collection and loads it into the PostgreSQL data warehouse.

- **Components**:
  - **MongoReader**: `UsersReader` reads data from the MongoDB "users" collection.
  - **MongoLoader**: `UsersLoader` processes and saves the data into the PostgreSQL data warehouse.

#### c. Load Orders (`load_orders`)

- **Description**: This task reads order data from the MongoDB "orders" collection and loads it into the PostgreSQL data warehouse.

- **Components**:
  - **MongoReader**: `OrdersReader` reads data from the MongoDB "orders" collection.
  - **MongoLoader**: `OrdersLoader` processes and saves the data into the PostgreSQL data warehouse.

#### d. Dependencies

- `load_restaurants` and `load_users` are independent tasks, meaning they can run in parallel.
- `load_orders` depends on the successful completion of both `load_restaurants` and `load_users`.

### 3. Logging

- The DAG utilizes the Python `logging` module for logging messages and tracking the progress of each task.

### 4. Airflow Variables

- **PG_WAREHOUSE_CONNECTION**: Connection string for the PostgreSQL data warehouse.
- **MONGO_DB_CERTIFICATE_PATH**: Path to the MongoDB certificate file.
- **MONGO_DB_USER**: MongoDB username.
- **MONGO_DB_PASSWORD**: MongoDB password.
- **MONGO_DB_REPLICA_SET**: MongoDB replica set name.
- **MONGO_DB_DATABASE_NAME**: MongoDB database name.
- **MONGO_DB_HOST**: MongoDB host.

## Schedule

The DAG is scheduled to run every 15 minutes, starting from May 5, 2022. It is set to not catch up on previous periods and is initially paused upon creation.

## Conclusion

The Order System DAG efficiently extracts data from MongoDB collections and loads it into a PostgreSQL data warehouse, providing a robust and automated ETL process for order-related information.
