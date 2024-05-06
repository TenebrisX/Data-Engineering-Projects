# Restaurant Subscriber Streaming Service

This project implements a real-time streaming service that enables restaurants to send targeted promotional campaigns to subscribed users. The service leverages Apache Spark Structured Streaming, Kafka, and PostgreSQL for efficient data processing and storage.

**Key Features**

* **Real-time processing:** Processes incoming restaurant advertisement data from Kafka with low latency, ensuring timely notifications.
* **Targeted notifications:** Joins streaming ad data with subscriber information from PostgreSQL to personalize notifications.
* **Feedback analytics:** Stores processed data in PostgreSQL for analyzing campaign effectiveness.
* **Push notification integration:** Sends prepared notification data to Kafka for delivery by an external push notification service.

**Technologies**

* **Spark Structured Streaming (PySpark):** Core engine for real-time data processing.
* **Kafka:** Message broker for receiving restaurant ad data and sending prepared notifications.
* **PostgreSQL:** Database for subscriber data and feedback analytics.

