## Infrastructure Customizations

This section describs what our infrastructure customizations are and how they used in our project.

### Apache Airflow Image Customization:
---
Building a `custom Docker image` for **Apache Airflow** is necessary to tailor the environment specifically to the project’s requirements. 

A standard Airflow installation may not include all the necessary extensions, libraries, and drivers needed for complex workflows, such as interacting with **PostgreSQL**, **Cassandra**, **Kafka**, or other services. By creating a custom image, we can pre-install essential packages like **apache-airflow-providers-postgres**, **cassandra-driver**, **confluent-kafka**, and others, ensuring that the Airflow instance is **fully equipped to handle our data ingestion, transformation, and orchestration tasks**. 

Additionally, this approach allows for consistent and reproducible deployment across environments, as the customized image encapsulates all dependencies and configurations, reducing the risk of compatibility issues and simplifying the deployment process.

Custom docker build process is automatically triggered upon project setup via our `docker compose build` command mentioned in our [project's setup instructions](../README.md).

**IMPORTANT NOTE:** Dependency management in our Apache Airflow's customized docker image is done via Poetry package dependency manager.

### Cassandra Database Initialization Script:
---
The Cassandra initialization scripts in your infrastructure folder are responsible for setting up the schema required for your data processing pipelines. These scripts define the necessary tables, such as **PAX** and **Loading** and **Sales** table, along with their primary keys and data types. 

The schema is **optimized for handling time-series data**, ensuring **high performance** and **scalability** for storing and querying **large volumes of flight-related metrics**.

These scripts also ensure the correct creation of indexes to speed up query operations based on primary keys.