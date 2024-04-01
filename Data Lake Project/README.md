# Data Lake Project

This project analyzes messaging data to provide insights into user behavior, geographic trends, and potential friend recommendations.

## Data Processing

The project utilizes the following data processing steps:

1. **Data Loading:** Loads events and geo data into Spark DataFrames.
2. **Data Cleaning:** Handles missing values, inconsistencies, and potential errors.
3. **Geospatial Calculations:** Determines the nearest city for each event and calculates distances between users.
4. **User Analysis:** Calculates metrics such as:
   * User activity within zones
   * Home city determination based on consecutive days spent in a location
   * Travel patterns identification  

5. **Friend Recommendations:** Generates friend recommendations based on:
   * Shared subscriptions to channels
   * Geographical proximity (users within a specified distance)

## Datamarts

The project creates the following datamarts for easy consumption of analytical results:

* **User Datamart:**
   * User ID
   * Last known city
   * Potential home city
   * Travel history
   * Local time information 

* **Zone Datamart:**
   * Zone (city) ID
   * Week 
   * Month
   * Number of messages sent
   * Number of reactions 
   * Number of subscriptions
   * Number of new users (registrations)

* **Friend Recommendations Datamart:**
   * User ID (potential friend)
   * Recommended User ID
   * Distance between users
   * Processing datetime

## Technologies Used

* **Spark:** Distributed data processing framework for efficient handling of large datasets.
* **Python:** Programming language used for data manipulation, analysis, and recommendation logic.
* **PySpark:** Python API for Spark.
* **Apache Hadoop:**  An open source framework based on Java that manages the storage and processing of large amounts of data for applications.
* **Apache Airflow** An open-source workflow management platform for data engineering pipelines.
* **Docker** A software platform that allows you to build, test, and deploy applications quickly.
* **YARN** Allocating system resources to the various applications running in a Hadoop cluster and scheduling tasks to be executed on different cluster nodes.
* **Jupiter** A web-based application used to create and share interactive notebook documents.