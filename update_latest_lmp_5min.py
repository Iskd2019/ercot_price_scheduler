import requests, zipfile, io, pandas as pd
import psycopg2
from datetime import datetime

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