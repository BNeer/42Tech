# Base image with Python and Java
FROM python:3.8-slim-buster

# Install Java for Spark
RUN apt-get update && apt-get install -y openjdk-11-jre-headless && rm -rf /var/lib/apt/lists/*

# Install Spark
ENV APACHE_SPARK_VERSION 3.2.1
ENV HADOOP_VERSION 3.2

RUN apt-get update && apt-get install -y curl wget && \
    wget --no-verbose https://archive.apache.org/dist/spark/spark-$APACHE_SPARK_VERSION/spark-$APACHE_SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz && \
    tar -xzf spark-$APACHE_SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz -C /opt && \
    rm spark-$APACHE_SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz

ENV SPARK_HOME=/opt/spark-$APACHE_SPARK_VERSION-bin-hadoop$HADOOP_VERSION
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PYSPARK_PYTHON=python3

# Download PostgreSQL JDBC Driver
RUN wget -P /opt/spark/jars/ https://jdbc.postgresql.org/download/postgresql-42.7.2.jar

# Install Python libraries
RUN pip install pyspark==3.2.1 pandas pytest

# Set up working directory and copy application code
WORKDIR /app
COPY . /app

# Ensure pytest runs correctly
ENV PYTHONPATH=/app

# Run tests
RUN pytest /app/test/test_etl.py || true

# Command to run the Spark application
CMD ["spark-submit", "--packages", "com.crealytics:spark-excel_2.12:0.13.5", "--jars", "/opt/spark/jars/postgresql-42.7.2.jar", "/app/app.py"]
