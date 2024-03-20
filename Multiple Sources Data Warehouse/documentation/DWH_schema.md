# Data Warehouse Documentation

## STG Layer (AS IS)
![Data Warehouse STG LAYER Architecture](https://github.com/TenebrisX/de-project-sprint-5/blob/main/img/de%20-%20stg.png)

### Postgres Database source:

#### Airflow DAG
- [stg_bonus_system_dag](https://github.com/TenebrisX/de-project-sprint-5/blob/main/src/dags/stg/bonus_system_dag/bonus_system_dag.py)

#### SQL scripts
- [stg.bonussystem_events Table](https://github.com/TenebrisX/de-project-sprint-5/blob/main/src/dags/init_schema/ddl/stg/stg_bonussystem_events.sql)
- [stg.bonussystem_ranks Table](https://github.com/TenebrisX/de-project-sprint-5/blob/main/src/dags/init_schema/ddl/stg/stg_bonussystem_ranks.sql)
- [stg.bonussystem_users Table](https://github.com/TenebrisX/de-project-sprint-5/blob/main/src/dags/init_schema/ddl/stg/stg_bonussystem_users.sql)

### API AWS S3 source:

#### Airflow DAG
- [stg_delivery_system_dag DAG]([link-to-repo/stg/dags/stg_delivery_system_dag.sql](https://github.com/TenebrisX/de-project-sprint-5/blob/main/src/dags/stg/delivery_system_dag/delivery_system_dag.py))

#### SQL scripts
- [stg.deliverysystem_couriers Table](https://github.com/TenebrisX/de-project-sprint-5/blob/main/src/dags/init_schema/ddl/stg/stg_deliverysystem_couriers.sql)
- [stg.deliverysystem_deliveries Table](https://github.com/TenebrisX/de-project-sprint-5/blob/main/src/dags/init_schema/ddl/stg/stg_deliverysystem_deliveries.sql)
- [stg.deliverysystem_restaurants Table](https://github.com/TenebrisX/de-project-sprint-5/blob/main/src/dags/init_schema/ddl/stg/stg_deliverysystem_resturants.sql)

### MongoDB source:

#### Airflow DAG
- [stg_order_system_dag DAG](https://github.com/TenebrisX/de-project-sprint-5/blob/main/src/dags/stg/order_system_dag/order_system_dag.py)

#### SQL scripts
- [stg.ordersystem_orders Table](https://github.com/TenebrisX/de-project-sprint-5/blob/main/src/dags/init_schema/ddl/stg/stg_ordersystem_orders.sql)
- [stg.ordersystem_restaurants Table](https://github.com/TenebrisX/de-project-sprint-5/blob/main/src/dags/init_schema/ddl/stg/stg_ordersystem_restaurants.sql)
- [stg.ordersystem_users Table](https://github.com/TenebrisX/de-project-sprint-5/blob/main/src/dags/init_schema/ddl/stg/stg_ordersystem_users.sql)

### Workflow settings
- [stg.srv_wf_settings Table](https://github.com/TenebrisX/de-project-sprint-5/blob/main/src/dags/init_schema/ddl/stg/stg_srv_wf_settings.sql)

## DDS Layer (Snowflake)
![Data Warehouse STG LAYER Architecture](https://github.com/TenebrisX/de-project-sprint-5/blob/main/img/de-dds-new.png)

#### Airflow DAG
- [dds DAG](https://github.com/TenebrisX/de-project-sprint-5/blob/main/src/dags/dds/dds_dag.py)

#### SQL scripts
### Dimentions:
- [dds.dm_users Table](https://github.com/TenebrisX/de-project-sprint-5/blob/main/src/dags/init_schema/ddl/dds/02_dds_dm_users.sql)
- [dds.dm_restaurants Table](https://github.com/TenebrisX/de-project-sprint-5/blob/main/src/dags/init_schema/ddl/dds/03_dds_dm_restaurants.sql)
- [dds.c_couriers Table](https://github.com/TenebrisX/de-project-sprint-5/blob/main/src/dags/init_schema/ddl/dds/04_dds_c_couriers.sql)
- [dds.dm_products Table](https://github.com/TenebrisX/de-project-sprint-5/blob/main/src/dags/init_schema/ddl/dds/05_dds_dm_products.sql)
- [dds.dm_timestamps Table](https://github.com/TenebrisX/de-project-sprint-5/blob/main/src/dags/init_schema/ddl/dds/06_dds_dm_timestamps.sql)
- [dds.c_deliveries Table](https://github.com/TenebrisX/de-project-sprint-5/blob/main/src/dags/init_schema/ddl/dds/07_dds_c_deliveries.sql)
- [dds.dm_orders Table](https://github.com/TenebrisX/de-project-sprint-5/blob/main/src/dags/init_schema/ddl/dds/08_dds_dm_orders.sql)

### Facts table
- [dds.fct_product_sales Table](https://github.com/TenebrisX/de-project-sprint-5/blob/main/src/dags/init_schema/ddl/dds/09_dds_fct_product_sales.sql)

### Workflow settings
- [dds.srv_wf_settings Table](https://github.com/TenebrisX/de-project-sprint-5/blob/main/src/dags/init_schema/ddl/dds/dds_srv_wf_settings.sql)

## CDM Layer
![Data Warehouse STG LAYER Architecture](https://github.com/TenebrisX/de-project-sprint-5/blob/main/img/de%20-%20cdm.png)

#### Airflow DAG
- [cdm DAG](https://github.com/TenebrisX/de-project-sprint-5/blob/main/src/dags/cdm/cdm_dag.py)

### Data Marts:
- [cdm.dm_courier_ledger Table](https://github.com/TenebrisX/de-project-sprint-5/blob/main/src/dags/init_schema/ddl/cdm/cdm_dm_courier_ledger.sql)
- [cdm.dm_settlement_report Table](https://github.com/TenebrisX/de-project-sprint-5/blob/main/src/dags/init_schema/ddl/cdm/cdm_dm_settlement_report.sql)

### Workflow settings
- [cdm.srv_wf_settings Table](https://github.com/TenebrisX/de-project-sprint-5/blob/main/src/dags/init_schema/ddl/cdm/cdm_srv_wf_settings.sql)
