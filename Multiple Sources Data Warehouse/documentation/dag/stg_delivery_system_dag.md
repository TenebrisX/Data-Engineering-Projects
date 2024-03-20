# Delivery System DAG Documentation

## Overview

The Delivery System DAG is an Airflow Directed Acyclic Graph designed to extract data from external APIs related to courier, restaurant, and delivery information. The extracted data is then loaded into a staging database for further processing.

## Configuration

The DAG is configured with the following parameters:

- **Schedule Interval:** Set to run every 15 minutes.
- **Start Date:** Commenced from May 5, 2022, in UTC timezone.
- **Catchup:** Disabled to avoid backfilling for previous periods.
- **Tags:** Categorized with 'api', 'stg', and 'origin'.
- **Pause Upon Creation:** Enabled to prevent automatic execution upon creation.

## Tasks

### Task 1: Load Couriers

**Functionality:**
Loads courier data from the API to the staging database.

**Configuration:**
- Uses `CourierApiReader` to fetch data from the courier API.
- Inserts data into the staging database using `CourierDest`.
- Logs messages using the specified logger.

### Task 2: Load Restaurants

**Functionality:**
Loads restaurant data from the API to the staging database.

**Configuration:**
- Uses `RestaurantApiReader` to fetch data from the restaurant API.
- Inserts data into the staging database using `RestaurantDest`.
- Logs messages using the specified logger.

### Task 3: Load Deliveries

**Functionality:**
Loads delivery data from the API to the staging database.

**Configuration:**
- Uses `DeliveryApiReader` to fetch data from the delivery API.
- Inserts data into the staging database using `DeliveryDest`.
- Logs messages using the specified logger.

### DAG Dependencies

- Task 1 (`load_couriers`) is followed by Task 2 (`load_restaurants`) and Task 3 (`load_deliveries`).

## Logging

All tasks are configured to log messages using the specified logger (`logger`). Ensure that the logging level is appropriately set.

## Notes

- The DAG is designed to run every 15 minutes to keep the data up-to-date.
- Data is extracted from APIs using corresponding API readers.
- Extracted data is loaded into the staging database using appropriate loaders.
- The DAG is paused upon creation, and catchup is disabled to maintain real-time data extraction.

## Revision History

- **Version 1.0:** Initial creation of the DAG.
