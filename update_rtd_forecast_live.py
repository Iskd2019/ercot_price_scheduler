import requests
import zipfile
import io
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# --- CONFIGURATION ---
METADATA_URL = "https://www.ercot.com/misapp/servlets/IceDocListJsonWS?reportTypeId=13073"
TARGET_ZONES = ["LZ_HOUSTON", "LZ_NORTH", "LZ_SOUTH", "LZ_WEST"]
HEADERS = {"User-Agent": "Mozilla/5.0"}

DB_CONFIG = {
    'dbname': 'power',
    'user': 'odoo',
    'password': 'odoo123',
    'host': '10.10.112.106',
    'port': '5432',
    'sslmode': 'prefer'
}

TABLE_NAME = 'rtd_price_forecast_live'

# --- STEP 1: Fetch Metadata and Get Latest DocID ---
try:
    response = requests.get(METADATA_URL, headers=HEADERS)
    response.raise_for_status()
    metadata = response.json()
    latest_doc = metadata["ListDocsByRptTypeRes"]["DocumentList"][0]["Document"]
    doc_id = latest_doc["DocID"]
    print(f"📦 Latest DocID: {doc_id}")
except Exception as e:
    print(f"❌ Failed to fetch metadata: {e}")
    exit(1)

# --- STEP 2: Download ZIP and Read CSV ---
try:
    zip_url = f"https://www.ercot.com/misdownload/servlets/mirDownload?doclookupId={doc_id}"
    zip_response = requests.get(zip_url, headers=HEADERS)
    zip_file = zipfile.ZipFile(io.BytesIO(zip_response.content))
    for file_name in zip_file.namelist():
        if file_name.endswith(".csv"):
            with zip_file.open(file_name) as f:
                df = pd.read_csv(f)
                break
    print(f"✅ Data loaded from: {file_name}")
except Exception as e:
    print(f"❌ Failed to download or read ZIP: {e}")
    exit(1)

# --- STEP 3: Filter Data ---
try:
    filtered_df = df[df["SettlementPoint"].isin(TARGET_ZONES)][
        ["IntervalEnding", "SettlementPoint", "LMP"]
    ].copy()
    filtered_df["IntervalEnding"] = pd.to_datetime(filtered_df["IntervalEnding"])
    print(f"✅ Filtered {len(filtered_df)} records for {TARGET_ZONES}")
except Exception as e:
    print(f"❌ Failed to filter or format data: {e}")
    exit(1)

# --- STEP 4: Insert into PostgreSQL ---
try:
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Truncate the table
    cursor.execute(f"TRUNCATE TABLE {TABLE_NAME};")

    # Prepare records for insertion
    records = filtered_df[["IntervalEnding", "SettlementPoint", "LMP"]].values.tolist()
    insert_query = f"""
        INSERT INTO {TABLE_NAME} (interval_ending, settlement_point, lmp)
        VALUES %s;
    """
    execute_values(cursor, insert_query, records)

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ {len(records)} records inserted into {TABLE_NAME}")
except Exception as e:
    print(f"❌ Failed to insert into DB: {e}")
    exit(1)