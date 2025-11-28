"""
MWAA-compatible Python ETL DAG for Toll Data
- Extracts: vehicle-data.csv (CSV), tollplaza-data.tsv (TSV), payment-data.txt (fixed width)
- Consolidates and transforms into transformed_data.csv
- Loads transformed data into tolldata_fact (Postgres) using PostgresHook (conn_id=postgres_tolldata)
"""

from datetime import datetime, timedelta
from pathlib import Path
import csv
import os
import tarfile
import requests
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# -----------------------
# Config
# -----------------------
BASE_DIR = "/usr/local/airflow/data/python_etl/staging"
TGZ_FILE = f"{BASE_DIR}/tolldata.tgz"
URL = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz"

# Ensure directory exists (container path)
Path(BASE_DIR).mkdir(parents=True, exist_ok=True)

# Connection id that you must create in Airflow UI
PG_CONN_ID = "postgres_tolldata"

# -----------------------
# Helper / extraction functions
# -----------------------

def download_dataset(**context):
    # Download TGZ to BASE_DIR/tolldata.tgz
    resp = requests.get(URL, stream=True)
    if resp.status_code != 200:
        raise RuntimeError(f"Failed download: status {resp.status_code}")
    with open(TGZ_FILE, "wb") as fh:
        fh.write(resp.raw.read())
    print("Downloaded:", TGZ_FILE)

def untar_dataset(**context):
    if not os.path.exists(TGZ_FILE):
        raise FileNotFoundError("TGZ file not found: " + TGZ_FILE)
    with tarfile.open(TGZ_FILE, "r:gz") as tar:
        tar.extractall(BASE_DIR)
    print("Extracted files to:", BASE_DIR)
    # Confirm expected files exist (optional)
    for f in ["vehicle-data.csv", "tollplaza-data.tsv", "payment-data.txt"]:
        p = Path(BASE_DIR) / f
        if not p.exists():
            print("Warning: expected file missing:", p)

def extract_data_from_csv(**context):
    in_file = Path(BASE_DIR) / "vehicle-data.csv"
    out_file = Path(BASE_DIR) / "csv_data.csv"
    if not in_file.exists():
        raise FileNotFoundError(str(in_file))
    with in_file.open("r", encoding="utf-8") as inp, out_file.open("w", newline="", encoding="utf-8") as out:
        w = csv.writer(out)
        # From reference: vehicle-data.csv has 6 fields but we only need the first 6 as described
        # We'll output consistent header for consolidation
        w.writerow(["Rowid","Timestamp","Vehicle_number","Vehicle_type","Number_of_axles","Vehicle_code"])
        for line in inp:
            parts = [p.strip() for p in line.strip().split(",")]
            # defensive: ensure at least 6 items
            while len(parts) < 6:
                parts.append("")
            w.writerow([parts[0], parts[1], parts[2], parts[3], parts[4], parts[5]])
    print("CSV extracted ->", out_file)

def extract_data_from_tsv(**context):
    in_file = Path(BASE_DIR) / "tollplaza-data.tsv"
    out_file = Path(BASE_DIR) / "tsv_data.csv"
    if not in_file.exists():
        raise FileNotFoundError(str(in_file))
    with in_file.open("r", encoding="utf-8") as inp, out_file.open("w", newline="", encoding="utf-8") as out:
        w = csv.writer(out)
        # Reference: tollplaza-data.tsv has 7 fields (Rowid, Timestamp, Vehicle number, Vehicle type, Number of axles, Tollplaza id, Tollplaza code)
        w.writerow(["Rowid","Timestamp","Vehicle_number","Vehicle_type","Number_of_axles","Tollplaza_id","Tollplaza_code"])
        for line in inp:
            parts = [p.strip() for p in line.rstrip("\n").split("\t")]
            while len(parts) < 7:
                parts.append("")
            w.writerow([parts[0], parts[1], parts[2], parts[3], parts[4], parts[5], parts[6]])
    print("TSV extracted ->", out_file)

def extract_data_from_fixed_width(**context):
    in_file = Path(BASE_DIR) / "payment-data.txt"
    out_file = Path(BASE_DIR) / "fixed_width_data.csv"
    if not in_file.exists():
        raise FileNotFoundError(str(in_file))
    # We must know the fixed-field widths. From your reference, 7 fields exist:
    # We'll parse by positions typically used in this dataset — if different, adjust slice indices.
    # Common approach: use Rowid (0:1..), timestamp next, etc. Here we'll use conservative slicing:
    with in_file.open("r", encoding="utf-8") as inp, out_file.open("w", newline="", encoding="utf-8") as out:
        w = csv.writer(out)
        w.writerow(["Rowid","Timestamp","Vehicle_number","Tollplaza_id","Tollplaza_code","Payment_Type","Vehicle_Code"])
        for line in inp:
            # remove trailing newline but preserve spacing for fixed width
            raw = line.rstrip("\n")
            # These slice indices are sensible defaults for this dataset shape — they can be tuned if needed.
            rowid = raw[0:1].strip() or raw.split()[0]  # fallback
            timestamp = raw[1:33].strip() if len(raw) >= 33 else ""
            vehicle_num = raw[33:48].strip() if len(raw) >= 48 else ""
            tollplaza_id = raw[48:55].strip() if len(raw) >= 55 else ""
            tollplaza_code = raw[55:62].strip() if len(raw) >= 62 else ""
            payment_type = raw[62:67].strip() if len(raw) >= 67 else ""
            vehicle_code = raw[67:77].strip() if len(raw) >= 77 else ""
            w.writerow([rowid, timestamp, vehicle_num, tollplaza_id, tollplaza_code, payment_type, vehicle_code])
    print("Fixed-width extracted ->", out_file)

def consolidate_data(**context):
    csv_file = Path(BASE_DIR) / "csv_data.csv"
    tsv_file = Path(BASE_DIR) / "tsv_data.csv"
    fw_file = Path(BASE_DIR) / "fixed_width_data.csv"
    out_file = Path(BASE_DIR) / "extracted_data.csv"

    for p in (csv_file, tsv_file, fw_file):
        if not p.exists():
            raise FileNotFoundError(f"Missing intermediate file: {p}")

    with csv_file.open("r", encoding="utf-8") as c, tsv_file.open("r", encoding="utf-8") as t, fw_file.open("r", encoding="utf-8") as f, out_file.open("w", newline="", encoding="utf-8") as out:
        cr = csv.reader(c)
        tr = csv.reader(t)
        fr = csv.reader(f)
        writer = csv.writer(out)

        # skip headers in sources
        next(cr)
        next(tr)
        next(fr)

        # write consolidated header matching the reference + order
        writer.writerow([
            "Rowid","Timestamp","Vehicle_number","Vehicle_type",
            "Number_of_axles","Tollplaza_id","Tollplaza_code",
            "Payment_Type","Vehicle_Code"
        ])

        # zip will stop at shortest; this dataset should align by Rowid
        for rrow, trow, frow in zip(cr, tr, fr):
            # Ensure we map fields correctly: sources sometimes repeat same info; we follow reference mapping
            # csv: [Rowid, Timestamp, Vehicle_number, Vehicle_type, Number_of_axles, Vehicle_code]
            # tsv: [Rowid, Timestamp, Vehicle_number, Vehicle_type, Number_of_axles, Tollplaza_id, Tollplaza_code]
            # fw:  [Rowid, Timestamp, Vehicle_number, Tollplaza_id, Tollplaza_code, Payment_Type, Vehicle_Code]
            # Build canonical output by preferring sensible non-empty values in order
            rowid = rrow[0].strip() if len(rrow) > 0 else ""
            timestamp = rrow[1].strip() if len(rrow) > 1 and rrow[1].strip() else (trow[1].strip() if len(trow) > 1 else "")
            vehicle_number = rrow[2].strip() if len(rrow) > 2 and rrow[2].strip() else (trow[2].strip() if len(trow) > 2 else (frow[2].strip() if len(frow)>2 else ""))
            vehicle_type = rrow[3].strip() if len(rrow) > 3 and rrow[3].strip() else (trow[3].strip() if len(trow) > 3 else "")
            num_axles = rrow[4].strip() if len(rrow) > 4 and rrow[4].strip() else (trow[4].strip() if len(trow) > 4 else "")
            tollplaza_id = trow[5].strip() if len(trow) > 5 and trow[5].strip() else (frow[3].strip() if len(frow) > 3 else "")
            tollplaza_code = trow[6].strip() if len(trow) > 6 and trow[6].strip() else (frow[4].strip() if len(frow) > 4 else "")
            payment_type = frow[5].strip() if len(frow) > 5 else ""
            vehicle_code = rrow[5].strip() if len(rrow) > 5 and rrow[5].strip() else (frow[6].strip() if len(frow) > 6 else "")

            writer.writerow([
                rowid, timestamp, vehicle_number, vehicle_type,
                num_axles, tollplaza_id, tollplaza_code,
                payment_type, vehicle_code
            ])
    print("Consolidated ->", out_file)

def transform_data(**context):
    in_file = Path(BASE_DIR) / "extracted_data.csv"
    out_file = Path(BASE_DIR) / "transformed_data.csv"

    if not in_file.exists():
        raise FileNotFoundError(str(in_file))

    with in_file.open("r", encoding="utf-8") as inp, out_file.open("w", newline="", encoding="utf-8") as out:
        reader = csv.DictReader(inp)
        writer = csv.DictWriter(out, fieldnames=reader.fieldnames)
        writer.writeheader()
        for row in reader:
            # Normalize vehicle type
            row["Vehicle_type"] = row.get("Vehicle_type", "").upper()
            # Normalize timestamps (optional) - keep as-is for now
            writer.writerow(row)
    print("Transformed ->", out_file)

# -----------------------
# Loader: load transformed CSV to Postgres (tolldata_fact)
# -----------------------
def load_fact(**context):
    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    conn = pg_hook.get_conn()
    cur = conn.cursor()
    
    # Delete table before loading the records

    # create fact table if it doesn't exist (use snake_case columns)
    cur.execute("""
    DROP TABLE IF EXISTS tolldata_fact;
    
    CREATE TABLE IF NOT EXISTS tolldata_fact (
        rowid INT PRIMARY KEY,
        event_ts TEXT,
        vehicle_number TEXT,
        vehicle_type TEXT,
        number_of_axles INT,
        tollplaza_id INT,
        tollplaza_code TEXT,
        payment_type TEXT,
        vehicle_code TEXT
    );
    """)
    conn.commit()

    csv_path = Path(BASE_DIR) / "transformed_data.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"transformed csv not found: {csv_path}")

    # Option A: use COPY via cursor.copy_expert for speed (works with psycopg2)
    with csv_path.open("r", encoding="utf-8") as f:
        # We expect header names in file to map to table columns - we'll use COPY with header and column order
        # Ensure the CSV header order matches the table columns:
        # Rowid,Timestamp,Vehicle_number,Vehicle_type,Number_of_axles,Tollplaza_id,Tollplaza_code,Payment_Type,Vehicle_Code
        sql = """
            COPY tolldata_fact (
                rowid, event_ts, vehicle_number, vehicle_type,
                number_of_axles, tollplaza_id, tollplaza_code, payment_type, vehicle_code
            ) FROM STDIN WITH CSV HEADER DELIMITER ','
        """
        cur.copy_expert(sql, f)
    conn.commit()
    cur.close()
    conn.close()
    print("Loaded transformed_data.csv -> tolldata_fact")

# -----------------------
# DAG definition
# -----------------------
default_args = {
    "owner": "Analyst",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ETL_tolldata_python",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["tolldata","etl"]
) as dag:

    start = EmptyOperator(task_id="start")

    download = PythonOperator(task_id="download_dataset", python_callable=download_dataset)
    untar = PythonOperator(task_id="untar_dataset", python_callable=untar_dataset)

    extract_csv = PythonOperator(task_id="extract_csv", python_callable=extract_data_from_csv)
    extract_tsv = PythonOperator(task_id="extract_tsv", python_callable=extract_data_from_tsv)
    extract_fw = PythonOperator(task_id="extract_fixed_width", python_callable=extract_data_from_fixed_width)

    consolidate = PythonOperator(task_id="consolidate", python_callable=consolidate_data)
    transform = PythonOperator(task_id="transform", python_callable=transform_data)

    load = PythonOperator(task_id="load_fact", python_callable=load_fact)

    end = EmptyOperator(task_id="end")

    # pipeline
    start >> download >> untar >> [extract_csv, extract_tsv, extract_fw] >> consolidate >> transform >> load >> end