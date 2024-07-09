from pyspark.sql.functions import col, lower, regexp_replace, udf, monotonically_increasing_id
from pyspark.sql.types import StringType
import pandas as pd
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("CSV Reader") \
    .getOrCreate()

csv_path = "/app/data/electricity-generation_emissions_sources_ownership.csv"
csv_df = spark.read.csv(csv_path, header=True, inferSchema=True)
csv_df.printSchema()

# Create a SparkSession
spark = SparkSession.builder \
    .appName("ReadExcelWithHeader") \
    .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.5") \
    .getOrCreate()

# Define the path to your Excel file
excel_file_path = "/app/data/Global-Nuclear-Power-Tracker-October-2023.xlsx"
xlsx_spark_df = spark.read \
    .format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("dataAddress", "'Data'!A1") \
    .load(excel_file_path)

xlsx_spark_df.printSchema()

# Function to standardize company names
def standardize_name(name):
    import re
    if name is None:
        return ""
    name = name.lower()
    name = re.sub(r'\s+', ' ', name).strip()
    suffixes = ['ltd', 'limited', 'inc', 'corporation', 'corp', 'company', 'co', 'gmbh', 'llc']
    for suffix in suffixes:
        name = re.sub(r'\b{}\b'.format(suffix), '', name).strip()
    name = re.sub(r'[^a-z0-9\s]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name

standardize_name_udf = udf(standardize_name, StringType())

# Apply the function to the DataFrames
csv_df = csv_df.withColumn("standardized_company_name", standardize_name_udf(col("company_name")))
xlsx_spark_df = xlsx_spark_df.withColumn("standardized_project_name", standardize_name_udf(col("Project Name")))

# Generate unique IDs for the companies in the XLSX data
unique_names_df = xlsx_spark_df.select("standardized_project_name").distinct().withColumn("company_id_xlsx", monotonically_increasing_id())
xlsx_spark_df = xlsx_spark_df.join(unique_names_df, on="standardized_project_name", how="left")

# Select relevant columns
csv_columns_to_keep = [
    'source_id', 'source_name', 'iso3_country', 'lat', 'lon', 'relationship',
    'ultimate_parent_name', 'ultimate_parent_id', 'standardized_company_name', 'company_id'
]
xlsx_columns_to_keep = [
    'Date Last Researched', 'Country', 'Project Name', 'Unit Name', 
    'Capacity (MW)', 'Status', 'Reactor Type', 'Model', 'Location Accuracy', 
    'City', 'Local Area (taluk, county)', 'Major Area (prefecture, district)', 
    'State/Province', 'Subregion', 'Region', 'GEM location ID', 
    'GEM unit ID', 'Wiki URL', 'standardized_project_name', 'company_id_xlsx'
]

csv_df_filtered = csv_df.select([col for col in csv_columns_to_keep])
xlsx_spark_df_filtered = xlsx_spark_df.select([col for col in xlsx_columns_to_keep])

# Merge the DataFrames
merged_df = csv_df_filtered.join(
    xlsx_spark_df_filtered, 
    (csv_df_filtered.standardized_company_name == xlsx_spark_df_filtered.standardized_project_name) &
    (csv_df_filtered.company_id == xlsx_spark_df_filtered.company_id_xlsx), 
    how="outer"
)

# Filter out rows with null company_id
merged_df = merged_df.filter(col("company_id").isNotNull())

# Create DataFrames for each table
companies_df = merged_df.select(
    "company_id", "standardized_company_name", "Country"
).distinct().withColumnRenamed("standardized_company_name", "company_name")

assets_df = merged_df.select(
    monotonically_increasing_id().alias("asset_id"), "company_id",
    "source_name", "source_id", "lat", "lon", "relationship"
).distinct().withColumnRenamed("source_name", "asset_name").withColumnRenamed("relationship", "asset_type")

indicators_df = merged_df.select(
    monotonically_increasing_id().alias("indicator_id"), "source_id", "Capacity (MW)", "Status",
    "Reactor Type", "Model", "Date Last Researched"
).distinct().withColumnRenamed("source_id", "asset_id").withColumnRenamed("Capacity (MW)", "indicator_value").withColumnRenamed("Date Last Researched", "timestamp")

metadata_df = merged_df.select(
    monotonically_increasing_id().alias("metadata_id"), "source_id",
    "source_name", "Date Last Researched"
).distinct().withColumnRenamed("source_name", "data_source").withColumnRenamed("source_id", "data_point_id").withColumnRenamed("Date Last Researched", "additional_info")

# Define PostgreSQL connection properties
postgres_url = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"
postgres_properties = {
    "user": "consultants",
    "password": "WelcomeItc@2022",
    "driver": "org.postgresql.Driver"
}

# Deduplicate the companies_df to avoid duplicate primary key issues
companies_df = companies_df.dropDuplicates(["company_id"])

# Add try-except blocks to handle and log errors during ETL processes
try:
    companies_df.write.jdbc(url=postgres_url, table="companies", mode="append", properties=postgres_properties)
except Exception as e:
    print(f"Error writing companies_df to PostgreSQL: {e}")

try:
    assets_df.write.jdbc(url=postgres_url, table="assets", mode="append", properties=postgres_properties)
except Exception as e:
    print(f"Error writing assets_df to PostgreSQL: {e}")

try:
    indicators_df.write.jdbc(url=postgres_url, table="indicators", mode="append", properties=postgres_properties)
except Exception as e:
    print(f"Error writing indicators_df to PostgreSQL: {e}")

try:
    metadata_df.write.jdbc(url=postgres_url, table="metadata", mode="append", properties=postgres_properties)
except Exception as e:
    print(f"Error writing metadata_df to PostgreSQL: {e}")
