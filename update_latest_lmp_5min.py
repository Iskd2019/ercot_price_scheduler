import requests, zipfile, io, pandas as pd
import psycopg2
from datetime import datetime
from datetime import timedelta

# --- CONFIGURATION ---
METADATA_URL = "https://www.ercot.com/misapp/servlets/IceDocListJsonWS?reportTypeId=12300"
DOWNLOAD_TEMPLATE = "https://www.ercot.com/misdownload/servlets/mirDownload?doclookupId={}"
SETTLEMENT_POINTS = ["LZ_HOUSTON", "LZ_NORTH", "LZ_SOUTH", "LZ_WEST"]

DB_CONFIG = {
    'dbname': 'power',
    'user': 'odoo',
    'password': 'odoo123',
    'host': '10.10.112.106',
    'port': '5432',
    'sslmode': 'prefer'
}
TABLE_NAME = "daily_energy_price_5min"

# --- STEP 1: 获取最近几个文档 ID ---
print("📡 获取 ERCOT 5 分钟 LMP 文件元数据...")
md = requests.get(METADATA_URL).json()
recent_docs = md["ListDocsByRptTypeRes"]["DocumentList"][:5]
doc_ids = [doc["Document"]["DocID"] for doc in recent_docs]

# --- STEP 2: 下载并解析 CSV ---
all_dfs = []
for doc_id in doc_ids:
    try:
        print(f"⬇️ 下载 DocID: {doc_id}")
        zip_data = requests.get(DOWNLOAD_TEMPLATE.format(doc_id)).content
        zf = zipfile.ZipFile(io.BytesIO(zip_data))
        
        for fname in zf.namelist():
            if fname.endswith(".csv"):
                df = pd.read_csv(zf.open(fname))
                all_dfs.append(df)
    except Exception as e:
        print(f"⚠️ 下载或读取失败 DocID {doc_id}: {e}")

if not all_dfs:
    print("❌ 未能成功获取任何数据")
    exit()

# --- STEP 3: 合并并清洗数据 ---
df = pd.concat(all_dfs, ignore_index=True)
df = df[df["SettlementPoint"].isin(SETTLEMENT_POINTS)].copy()
df["SCEDTs"] = pd.to_datetime(df["SCEDTimestamp"], format="%m/%d/%Y %H:%M:%S")
df["LMP"] = df["LMP"].astype(float)

# --- STEP 4: 写入数据库 (先清空，再插入) ---
try:
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute(f"TRUNCATE {TABLE_NAME};")

    insert_query = f"""
        INSERT INTO {TABLE_NAME} (sced_timestamp, settlement_point, lmp)
        VALUES (%s, %s, %s);
    """

    for _, row in df.iterrows():
        cur.execute(insert_query, (
            row["SCEDTs"], row["SettlementPoint"], row["LMP"]
        ))

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ 已成功插入 {len(df)} 条记录至 {TABLE_NAME}")
except Exception as e:
    print(f"❌ 数据库写入失败: {e}")

#写入MQTT
import psycopg2
import json
import paho.mqtt.client as mqtt

# --- MQTT 设置 ---
MQTT_HOST = "10.10.112.130"
MQTT_PORT = 1883
MQTT_USER = "mqttusr3"
MQTT_PASS = "uu56890CCE#218"

# --- 数据库配置 ---
DB_CONFIG = {
    'dbname': 'power',
    'user': 'odoo',
    'password': 'odoo123',
    'host': '10.10.112.106',
    'port': '5432'
}

# 建立数据库连接
conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

# 获取最新一笔（四个结算点）数据
cur.execute("""
    SELECT sced_timestamp + interval '5 hour' AS time, settlement_point, lmp
    FROM daily_energy_price_5min
    WHERE sced_timestamp = (SELECT MAX(sced_timestamp) FROM daily_energy_price_5min)
""")
rows = cur.fetchall()


# 构造 JSON payload
latest_5min = {"timestamp": None}
for ts, point, lmp in rows:
    adjusted_ts = ts - timedelta(hours=5)
    latest_5min["timestamp"] = adjusted_ts.strftime('%Y-%m-%d %H:%M:%S')
    latest_5min[f"{point}"] = float(round(lmp, 2))

# 发送到 PWR/ERCOTLMP
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="publisher_lmp_5min", protocol=mqtt.MQTTv5)
client.username_pw_set(MQTT_USER, MQTT_PASS)
client.connect(MQTT_HOST, MQTT_PORT, 60)
client.publish("PWR/ERCOTLMP", json.dumps(latest_5min), qos=1, retain=True)
client.disconnect()

print(f"✅ 最新 5 分钟 LMP 已发送: {latest_5min}")

# 使用你写的 SQL 查询最新15分钟 LMP 均值
cur.execute("""
WITH latest_ts AS (
  SELECT MAX(sced_timestamp) AS latest_time FROM daily_energy_price_5min
),
lookback AS (
  SELECT
    latest_time,
    CASE
      WHEN EXTRACT(MINUTE FROM latest_time) % 15 BETWEEN 1 AND 4 THEN 3
      WHEN EXTRACT(MINUTE FROM latest_time) % 15 BETWEEN 5 AND 9 THEN 1
      WHEN EXTRACT(MINUTE FROM latest_time) % 15 BETWEEN 10 AND 14 THEN 2
      ELSE 3
    END AS num_points
  FROM latest_ts
),
recent_times AS (
  SELECT sced_timestamp
  FROM lookback,
       LATERAL (
         SELECT DISTINCT sced_timestamp
         FROM daily_energy_price_5min
         ORDER BY sced_timestamp DESC
         LIMIT lookback.num_points
       ) AS recent
),
base_data AS (
  SELECT p.*
  FROM daily_energy_price_5min p
  JOIN recent_times r ON p.sced_timestamp = r.sced_timestamp
),
avg_lmp AS (
  SELECT
    settlement_point,
    ROUND(AVG(lmp), 2) AS avg_lmp
  FROM base_data
  GROUP BY settlement_point
)
SELECT
  MAX(b.sced_timestamp) + interval '5 hour' AS time,
  b.settlement_point,
  a.avg_lmp
FROM base_data b
JOIN avg_lmp a
  ON b.settlement_point = a.settlement_point
GROUP BY b.settlement_point, a.avg_lmp
""")

rows = cur.fetchall()

# 构造 JSON payload
lmp_15min = {"timestamp": None}
for ts, point, avg in rows:
    adjusted_ts = ts - timedelta(hours=5)
    lmp_15min["timestamp"] = adjusted_ts.strftime('%Y-%m-%d %H:%M:%S')
    lmp_15min[f"{point}15"] = float(avg)

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="publisher_lmp_15min", protocol=mqtt.MQTTv5)
client.username_pw_set(MQTT_USER, MQTT_PASS)
client.connect(MQTT_HOST, MQTT_PORT, 60)
client.publish("PWR/ERCOTLMP15", json.dumps(lmp_15min), qos=1,retain=True)
client.disconnect()

print(f"✅ 最新 15 分钟 LMP 均值已发送: {lmp_15min}")
cur.close()
conn.close()