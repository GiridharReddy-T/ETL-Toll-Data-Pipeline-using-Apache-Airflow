# ETL-Toll Data-Pipeline-Apache Airflow + PostgreSQL
A complete end-to-end ETL pipeline that ingests raw toll transaction data from multiple file formats (CSV, TSV & Fixed Width), processes and transforms it using Airflow PythonOperators, and loads the final curated dataset into a PostgreSQL fact table.

This project is fully containerized using AWS MWAA Local Runner (Airflow 2.10.3) and includes integration with a local Postgres database for easy development and testing.
