Data Engineering Challenge - ETL Pipeline for Company Asset Data
This project sets up a data processing pipeline using Docker, Spark, and PostgreSQL. The pipeline reads data from CSV and Excel files, processes the data using PySpark, and writes the processed data to a PostgreSQL database.

Project Structure
├── docker-compose.yml
├── postgres
│			├── Dockerfile
│			└── init.sql
└── spark
    ├── Dockerfile
    ├── app.py
    ├── data
    │		├── Global-Nuclear-Power-Tracker-October-2023.xlsx
    │		└── electricity-generation_emissions_sources_ownership.csv
    └── postgresql-42.7.2.jar

Files and Directories
docker-compose.yml: Defines the services for Docker Compose to set up the Spark and PostgreSQL containers.
postgres/: Directory for PostgreSQL related files.
Dockerfile: Dockerfile to build the PostgreSQL image.
init.sql: SQL script to initialize the PostgreSQL database with the required schema.
spark/: Directory for Spark related files.
Dockerfile: Dockerfile to build the Spark image.
app.py: Main application script for data processing.
data/: Directory containing the data files to be processed.
Global-Nuclear-Power-Tracker-October-2023.xlsx: Excel file containing nuclear power data.
electricity-generation_emissions_sources_ownership.csv: CSV file containing electricity generation data.
postgresql-42.7.2.jar: PostgreSQL JDBC driver.
Getting Started
Prerequisites
Docker
Docker Compose

Setup
Clone the repository
git clone <repository-url>
cd <repository-directory>

Build and start the Docker containers

PostgreSQL Initialization
The PostgreSQL container will be initialized using the init.sql script, which creates the necessary tables for the data.

Spark Application
The Spark application (app.py) reads data from the CSV and Excel files located in the data directory, processes the data using PySpark, and writes the processed data to the PostgreSQL database.

Running the Spark Application
Access the Spark container

docker exec -it <spark-container-id> /bin/bash


Run the Spark application

spark-submit /app/app.py


Verifying Data in PostgreSQL
Access the PostgreSQL database to verify the data has been written correctly.

Access the PostgreSQL container

docker exec -it <postgres-container-id> /bin/bash


Connect to the database

psql -U techuser -d techdb

Query the tables

SELECT * FROM companies;
SELECT * FROM assets;
SELECT * FROM indicators;
SELECT * FROM metadata;


Notes
Ensure that the paths in app.py match the actual paths in your Docker containers.
Adjust the Dockerfile configurations as necessary for your environment.
