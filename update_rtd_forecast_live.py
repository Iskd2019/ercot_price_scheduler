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

# --- STEP 5: Calculate Future 60-min Average and Send to MQTT ---
import paho.mqtt.client as mqtt
import json
from datetime import timedelta

MQTT_HOST = "10.10.112.130"
MQTT_PORT = 1883
MQTT_USER = "mqttusr3"
MQTT_PASS = "uu56890CCE#218"

# 保留最晚的 IntervalEnding 时间
latest_ts = filtered_df["IntervalEnding"].max()
end_time = latest_ts
start_time = end_time - timedelta(minutes=60)

# 过滤出最后一小时的数据
next_60_df = filtered_df[
    (filtered_df["IntervalEnding"] > start_time) &
    (filtered_df["IntervalEnding"] <= end_time)
]

# 按结算点计算平均值
avg_lmp = next_60_df.groupby("SettlementPoint")["LMP"].mean().round(2).to_dict()

# 构造 JSON payload
payload = {"timestamp": latest_ts.strftime('%Y-%m-%d %H:%M:%S')}
for zone in TARGET_ZONES:
    payload[f"{zone}_NEXT60"] = avg_lmp.get(zone, None)

# 打印检查
print("📡 即将发布的未来60分钟电价:")
print(payload)

# 发送 MQTT
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="publisher_lmp_future", protocol=mqtt.MQTTv5)
client.username_pw_set(MQTT_USER, MQTT_PASS)
client.connect(MQTT_HOST, MQTT_PORT, 60)
client.publish("PWR/LMP60FUTURE", json.dumps(payload), qos=1, retain=True)
client.disconnect()
print("✅ MQTT 发布成功：PWR/LMP60FUTURE")