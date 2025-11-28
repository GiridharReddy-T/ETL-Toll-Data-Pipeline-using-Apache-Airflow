# ETL-Toll Data-Pipeline-Apache Airflow + PostgreSQL
A complete end-to-end ETL pipeline that ingests raw toll transaction data from multiple file formats (CSV, TSV & Fixed Width), processes and transforms it using Airflow PythonOperators, and loads the final curated dataset into a PostgreSQL fact table.

This project is fully containerized using AWS MWAA Local Runner (Airflow 2.10.3) and includes integration with a local Postgres database for easy development and testing.

## Project Structure

ETL-Toll Data-Pipeline/
│
│── assets/
│   └── DAG-Structure.png
│   
├── dags/
│   └── ETL_tolldata_python.py          # Main ETL DAG
│
├── data/
│   └── python_etl/
│       └── staging/                    # Extracted data (ignored by Git)
│
├── db-data/
│
├── docker/
│   ├── config/
│   │   ├── .env.localrunner                 ← env file for MWAA Local Runner
│   │   └── airflow.cfg (optional override)
│   │ 
│   ├── script/
│   ├── docker-compose-local.yml.            ← your LOCAL Airflow + Postgres
│   ├── docker-compose-resetdb.yml           ← DB reset script
│   ├── docker-compose-sequential.yml        ← Sequential executor mode
│   └── Dockerfile
│
├── plugins/  
│    └── README.md                        # Airflow plugins (optional)
│
├── requirements/
│    └── requirements.txt                # Python deps for MWAA local runner
│
├── startup_script
│    └── startup.sh
├── .gitignore
├── LICENSE
├──mwaa-local-env
└── README.md

## Technology Stack
Component                                      Description
Apache Airflow (MWAA Local Runner)             Manages and orchestrates the ETL
PythonOperators                                Implements ETL logic
PostgreSQL                                     Final destination for toll fact table
Docker Compose                                 Full reproducible pipeline
Requests / tarfile / CSV                       Core data extraction libraries

## Data Sources
The ETL downloads a TGZ bundle containing:

1. Vehical-data.csv
Field                       Description
Rowid                       Row identifier (consistent across all files)
Timestamp                   Vehicle passage time
Anonymized Vehicle Number   Car identifier
Vehicle Type                CAR, VAN, TRUCK
Number of Axles             Axles count
Vehicle Code                Category code


2. Tollplaza-data.tsv
Field                        Description
Rowid                        Unique identifier
Timestamp                    Vehicle timestamp
Anonymized Vehicle Number    Car ID
Vehicle Type                 Type
Number of Axles              Axles
Tollplaza ID                 ID
Tollplaza Code               Code


3. Payment-data.txt (Fixed Width)
Field                Description
Rowid                UID
Timestamp            Timestamp
Vehicle Number       ID
Tollplaza ID         ID
Tollplaza Code       Code
Payment Type         CASH / PREPAID
Vehicle Code         Category
