# Social Media Data Lake Project

This project builds a robust, scalable data lake, enabling advanced analytics on social media data.

**Key Components**

* **Data Sources:**
     * Social media feeds 
     * CRM data
     * Website logs
     * External data sets
* **Data Ingestion:**
    * Batch process
    * Real-time ingestion
* **Data Lake Storage:**
    * HDFS for cost-effective storage
    * AWS S3 Cloud object storage for scalability and accessibility
* **Data Processing:**
   * Spark for distributed data processing 
   * Stream processing for real-time insights
* **Data Modeling** 
    * Raw Data Layer: Data stored in its original format
    * Staging Layer: Data preparation and quality checks 
    * Processed Data Layer (Data Marts): Data aggregated and optimized for analysis (user analytics, zone analytics, friend recommendations)

**ETL Pipeline**

1. **Load Australian Geo Data:** Load geodata CSV, containing Australian city data, into HDFS ([useful_info.md](./useful_info.md))

2. **Data Ingestion (Batch):**
   * Import events and geo CSV files into HDFS using **hdfs dfs -copyFromLocal** ([useful_info.md](./useful_info.md)) 

3. **Data Transformation with Spark:**
    * [main.py](./src/scripts/main.py):
        * Loads raw data
        * Performs data cleaning and transformations
        * Derives geographic insights (nearest zones, user zones, travel patterns)
        * Calculates friend recommendations 
    * [data_processor.py](./src/scripts/utils/data_processor.py): Defines classes for data loading and writing 

4. **Store Processed Data in Datamarts:** 
    * Parquet format for efficient querying
    * Stored in the 'analytics' folder of the data lake

5. **Data Access and Exploration:**
    * Spark SQL to query the datamarts

**Airflow DAG**

* [dag.py](./src/dags/dag.py): Defines the Airflow DAG with task dependencies 
    * **dm_users** - Creates user datamart  
    * **dm_zones** - Creates  zone-level datamart
    * **dm_friends_recomendation** - Creates friend recommendations datamart

**Project Structure** 
```
├── README.md
├── useful_info.md
├── src
│   ├── dags
│   │   └── dag.py
│   │
│   └── scripts
│       ├── main.py
│       ├── analytics_engine.py
│       ├── utils
│       │   ├── data_processor.py
│       │   └── distance_calculator.py
│       ├── geo_processor.py
│       ├── friend_recommender.py
│       └── user_geo_analyzer.py
├── requirements.txt
```