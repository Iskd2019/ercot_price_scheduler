import requests
from bs4 import BeautifulSoup
from datetime import datetime
import psycopg2

# --- Database Config ---
DB_CONFIG = {
    'dbname': 'power',
    'user': 'odoo',
    'password': 'odoo123',
    'host': '10.10.112.106',
    'port': '5432',
    'sslmode': 'prefer'
}

# --- Step 1: Get Today's URL ---
today = datetime.now()
date_str = today.strftime('%Y%m%d')
url = f'https://www.ercot.com/content/cdr/html/{date_str}_real_time_spp.html'

print(f"📥 Fetching ERCOT Real-Time SPP from: {url}")
try:
    response = requests.get(url, timeout=15)
    response.raise_for_status()
except Exception as e:
    print(f"❌ Failed to fetch page: {e}")
    exit(1)

# --- Step 2: Parse HTML ---
soup = BeautifulSoup(response.content, 'html.parser')
table = soup.find('table')
if not table:
    print("❌ No table found in the page.")
    exit(1)

headers = []
data_rows = []
for i, row in enumerate(table.find_all('tr')):
    cols = [col.get_text(strip=True) for col in row.find_all(['th', 'td'])]
    if i == 0:
        headers = cols
    else:
        data_rows.append(cols)

# --- Step 3: Identify Column Indices ---
try:
    op_day_idx = headers.index('Oper Day')
    interval_idx = headers.index('Interval Ending')
    houston_idx = headers.index('LZ_HOUSTON')
    north_idx = headers.index('LZ_NORTH')
    south_idx = headers.index('LZ_SOUTH')
    west_idx = headers.index('LZ_WEST')
except ValueError as ve:
    print(f"❌ Required column missing: {ve}")
    exit(1)

# --- Step 4: Insert to DB ---
conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()
inserted = 0

for row in data_rows:
    try:
        oper_day = datetime.strptime(row[op_day_idx], '%m/%d/%Y').date()
        interval_time = datetime.strptime(row[interval_idx], '%H%M').time()
        lz_houston = float(row[houston_idx])
        lz_north = float(row[north_idx])
        lz_south = float(row[south_idx])
        lz_west = float(row[west_idx])

        cursor.execute("""
            INSERT INTO daily_energy_price (
                oper_day, interval_ending, lz_houston, lz_north, lz_south, lz_west
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (oper_day, interval_ending) DO NOTHING;
        """, (oper_day, interval_time, lz_houston, lz_north, lz_south, lz_west))
        inserted += 1

    except Exception as e:
        print(f"⚠️ Skipping bad row: {row}, Error: {e}")

conn.commit()
cursor.close()
conn.close()
print(f"✅ Inserted {inserted} records into daily_energy_price.")

"""
# --- Step 5: Publish the latest data point to MQTT ---
import paho.mqtt.client as mqtt
import json

if data_rows:
    last_row = data_rows[-1]
    try:
        oper_day = datetime.strptime(last_row[op_day_idx], '%m/%d/%Y').date()
        interval_time = datetime.strptime(last_row[interval_idx], '%H%M').time()
        timestamp = datetime.combine(oper_day, interval_time).strftime('%Y-%m-%d %H:%M:%S')

        payload = {
            "timestamp": timestamp,
            "LZ_HOUSTON15": float(last_row[houston_idx]),
            "LZ_NORTH15": float(last_row[north_idx]),
            "LZ_SOUTH15": float(last_row[south_idx]),
            "LZ_WEST15": float(last_row[west_idx])
        }

        print(f"📤 Pushing latest data to MQTT: {payload}")

        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="publisher_15min", protocol=mqtt.MQTTv5)
        client.username_pw_set("mqttusr3", "uu56890CCE#218")
        client.connect("10.10.112.130", 1883, 60)
        client.publish("PWR/ERCOTLMP15", payload=json.dumps(payload), qos=1, retain=True)
        client.disconnect()

        print("✅ Latest 15-min price pushed to MQTT topic PWR/ERCOTLMP15")
    except Exception as e:
        print(f"❌ Failed to publish MQTT message: {e}")
else:
    print("⚠️ No data row available to push to MQTT.")
"""