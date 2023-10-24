# Data Engineering Project for Income Classification

## Take home assignment
This take-home assignment is designed to evaluate your proficiency in data engineering tasks. It will test skills in using Python, Apache Airflow, PostgreSQL, and ClickHouse to handle data ingestion, transformation, storage, transfer, and querying. 


## Ojectives
This project serves as a comprehensive guide to building an end-to-end data engineering pipeline.
When doing the assignment, I find it necessary to include other technologies as descibed: 
It covers each stage from data ingestion to processing and finally to storage, utilizing a robust tech stack that includes Apache Airflow, Python, Apache Kafka, Apache Zookeeper, Apache Spark, Cassandra and ClickHouse. Everything is containerized using Docker for ease of deployment and scalability.

## System Architecture

The project is designed with the following components:

- **Data Source**: We use UCI Machine Learning dataset Income Classification (https://archive.ics.uci.edu/dataset/20/census+income) which has bank user data for our pipeline. The data downloaded and save to income.csv
- **Apache Airflow**: Responsible for orchestrating the pipeline and storing fetched data in a PostgreSQL database.
- **Apache Kafka and Zookeeper**: Used for streaming data from PostgreSQL to the processing engine.
- **Control Center and Schema Registry**: Helps in monitoring and schema management of our Kafka streams.
- **Apache Spark**: For data processing with its master and worker nodes.
- **ClickHouse**: Where the processed data will be stored. Then write ClickHouse SQL queries to answer:
1. How many unique values are in User Id?
2. What is the average of Target Income grouped by Age, Sex, Job, and Nationality?


## Technologies

- Apache Airflow
- Python
- Apache Kafka
- Apache Zookeeper
- Apache Spark
- PostgreSQL
- Docker
- ClickHouse

## Getting Started


1. Navigate to the project directory:
    ```bash
    cd data-engineering
    ```

2. Run Docker Compose to spin up the services:
    ```bash
    docker-compose up
    ```

