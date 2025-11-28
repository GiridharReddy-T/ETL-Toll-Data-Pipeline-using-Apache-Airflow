# 🚦 ETL Toll Data Pipeline — Apache Airflow  + PostgreSQL
A complete end-to-end ETL pipeline that ingests raw toll transaction data from multiple file formats (CSV, TSV & Fixed Width), processes and transforms it using Airflow PythonOperators, and loads the final curated dataset into a PostgreSQL fact table.

This project is fully containerized using AWS MWAA Local Runner (Airflow 2.10.3) and includes integration with a local Postgres database for easy development and testing.

## 📁 Project Structure
<img width="570" height="605" alt="Image" src="https://github.com/user-attachments/assets/b44730dd-adfc-4e6a-ae7f-0604fd408f30" />

## 🛠️ Technology Stack
Component                                      Description
Apache Airflow (MWAA Local Runner)             Manages and orchestrates the ETL
PythonOperators                                Implements ETL logic
PostgreSQL                                     Final destination for toll fact table
Docker Compose                                 Full reproducible pipeline
Requests / tarfile / CSV                       Core data extraction libraries

## 📦 Data Sources
The ETL downloads a TGZ bundle containing:

1️⃣ Vehical-data.csv

Component | Description
-- | --
Apache   Airflow (MWAA Local Runner) | Manages and orchestrates the ETL
PythonOperators | Implements ETL logic
PostgreSQL | Final destination for toll fact table
Docker   Compose | Full reproducible pipeline
Requests   / tarfile / CSV | Core data extraction libraries

2️⃣ Tollplaza-data.tsv

Field | Description
-- | --
Rowid | Unique   identifier
Timestamp | Vehicle   timestamp
Anonymized   Vehicle Number | Car   ID
Vehicle   Type | Type
Number   of Axles | Axles

3️⃣ Payment-data.txt (Fixed Width)

Field | Description
-- | --
Rowid | UID
Timestamp | Timestamp
Vehicle   Number | ID
Tollplaza   ID | ID
Tollplaza   Code | Code
Payment   Type | CASH /   PREPAID
Vehicle   Code | Category

## 🚀 ETL Architecture (Mermaid Diagram)
<img width="1648" height="678" alt="Image" src="https://github.com/user-attachments/assets/16c2d5b1-695b-49f9-856c-117f1e22fc80" />

## ✨ DATA FLOW DIAGRAM (SOURCE → FACT TABLE)
            +---------------------+          +------------------+
            |  vehicle-data.csv   |          | tollplaza-data   |
            +---------------------+          +------------------+
            | Rowid               | -------> | Rowid            |
            | Timestamp           | -------> | Timestamp        |
            | Vehicle Number      | -------> | Vehicle Number   |
            | Vehicle Type        | -------> | Vehicle Type     |
            | Number of Axles     | -------> | Number of Axles  |
            | Vehicle Code        |          | Tollplaza ID     |
            +---------------------+          | Tollplaza Code   |
                                             +------------------+
                                                        |
                                                        |
                                                        v
                                    +-------------------------------------+
                                    |         TOLLDATA_FACT TABLE         |
                                    |-------------------------------------|
                                    | rowid                               |
                                    | event_ts                            |
                                    | vehicle_number                      |
                                    | vehicle_type                        |
                                    | number_of_axles                     |
                                    | tollplaza_id                        |
                                    | tollplaza_code                      |
                                    | payment_type                        |
                                    | vehicle_code                        |
                                    +-------------------------------------+
                                                        ^
                                                        |
            +------------------------+                  |
            |   payment-data.txt     |------------------+
            +------------------------+
            | Rowid                 |
            | Timestamp             |
            | Vehicle Number        |
            | Tollplaza ID          |
            | Tollplaza Code        |
            | Payment Type          |
            | Vehicle Code          |
            +------------------------+


