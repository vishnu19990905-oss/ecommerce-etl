# 📦 Ecommerce ETL Pipeline Project

## 📖 Overview

This project implements an end-to-end batch ETL pipeline for an e-commerce dataset using PySpark.

The pipeline demonstrates core Data Engineering concepts such as data ingestion, transformation,Data Modelling using STAR schema, validation, and loading into an analytics-ready warehouse.

------------------------------------------------------------------------

## 🏗️ Architecture

Raw Data (CSV/JSON)
⬇
Extract Layer
⬇
Ingestion Layer
⬇
Transformation Layer
⬇
Data Modelling (Fact & Dimension Table)
⬇
Validation Layer
⬇
Load Layer (Warehouse - Parquet)

------------------------------------------------------------------------

## 🛠️ Tech Stack

-   Python
-   PySpark
-   Spark SQL
-   Linux
-   Git & GitHub

------------------------------------------------------------------------

## 📂 Project Structure

ecommerce-etl/ │ ├── data/ │ ├── raw/\
│ ├── processed/\
│ ├── warehouse/\
│ ├── src/ │ ├── extract/\
│ ├── ingestion/\
│ ├── transformation/ │ ├── validation/\
│ ├── modelling/\
│ ├── data/ │ │ └── processed/\
│ ├── main.py\
│ ├── README.md

------------------------------------------------------------------------

## 🔍 Key Implementations

### ✅ Data Extraction

-   Extracted data from CSV & JSON sources
-   Applied schema using StructType
-   Handled corrupt records

### ✅ Data Ingestion

-   Loaded data into PySpark DataFrames
-   Standardized schema across datasets

### ✅ Data Transformation

-   Removed duplicates
-   Handled NULL values
-   Performed joins across multiple datasets
-   Applied business transformations


### ✅ Data Modelling

-   Created Fact table: `fact_sales`
-   Created Dimension tables:
    -   `dim_customers`
    -   `dim_products`
    -   `dim_payments`


### ✅ Data Validation

-   Null checks
-   Duplicate checks
-   Basic data quality validations


### ✅ Data Loading

-   Stored final data in Parquet format
-   Organized into a warehouse layer
-   Optimized for analytics


------------------------------------------------------------------------

## ▶️ How to Run the Project

cd ecommerce-etl/src/ spark-submit main.py

------------------------------------------------------------------------

## 📊 Output

data/warehouse/

-   Fact and dimension tables
-   Ready for analytics

------------------------------------------------------------------------

## 🎯 Learning Outcomes

-   Built a complete ETL pipeline using PySpark
-   Implemented real-world data cleaning & validation
-   Designed fact and dimension data models
-   Worked with distributed data processing

------------------------------------------------------------------------

## 🚀 Project Status

Completed

------------------------------------------------------------------------

## 💡 Future Enhancements

-   Load data into SQL warehouse (PostgreSQL / Snowflake)
-   Add Airflow for orchestration
