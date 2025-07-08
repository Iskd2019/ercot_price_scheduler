import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import psycopg2

# --- DB Config ---
DB_CONFIG = {
    'dbname': 'power',
    'user': 'odoo',
    'password': 'odoo123',
    'host': '10.10.112.106',
    'port': '5432',
    'sslmode': 'prefer'
}

# --- Step 1: Build URL for Today's DAM Data ---
today = datetime.now()
date_str = today.strftime('%Y%m%d')
url = f"https://www.ercot.com/content/cdr/html/{date_str}_dam_spp.html"

print(f"📥 Fetching DAM data from: {url}")
try:
    response = requests.get(url, timeout=15)
    response.raise_for_status()
except Exception as e:
    print(f"❌ Failed to fetch page: {e}")
    exit(1)

# --- Step 2: Parse HTML Table ---
soup = BeautifulSoup(response.content, 'html.parser')
table = soup.find('table')
if not table:
    print("❌ No table found.")
    exit(1)

rows = table.find_all('tr')
headers = [cell.get_text(strip=True) for cell in rows[0].find_all(['th', 'td'])]

# Find the indices of the columns we need
try:
    idx_day = headers.index("Oper Day")
    idx_hour = headers.index("Hour Ending")
    idx_houston = headers.index("LZ_HOUSTON")
    idx_north = headers.index("LZ_NORTH")
    idx_south = headers.index("LZ_SOUTH")
    idx_west = headers.index("LZ_WEST")
except ValueError as ve:
    print(f"❌ Missing required column: {ve}")
    exit(1)

# --- Step 3: Extract and Prepare Data ---
data_rows = []
for row in rows[1:]:
    cells = [cell.get_text(strip=True) for cell in row.find_all(['td'])]
    if len(cells) < max(idx_day, idx_hour, idx_houston, idx_north, idx_south, idx_west) + 1:
        continue
    try:
        oper_day = datetime.strptime(cells[idx_day], "%m/%d/%Y").date()
        hour_ending = int(cells[idx_hour])
        # Convert to HH:00:00 time format
        #hour_ending_time = (datetime(2000, 1, 1) + timedelta(hours=hour_ending)).time()
        #edge case when hour is 24
        if hour_ending == 24:
            # Move to next day at 00:00
            hour_ending_time = datetime.strptime("00:00", "%H:%M").time()
            oper_day = oper_day + timedelta(days=1)
        else:
            hour_ending_time = (datetime(2000, 1, 1) + timedelta(hours=hour_ending)).time()
        lz_houston = float(cells[idx_houston])
        lz_north = float(cells[idx_north])
        lz_south = float(cells[idx_south])
        lz_west = float(cells[idx_west])
        data_rows.append((oper_day, hour_ending_time, lz_houston, lz_north, lz_south, lz_west))
    except Exception as e:
        print(f"⚠️ Skipping row due to error: {e}")

# --- Step 4: Insert into DB ---
conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()
inserted = 0

for record in data_rows:
    try:
        cursor.execute("""
            INSERT INTO dam_hourly_price (
                oper_day, hour_ending, lz_houston, lz_north, lz_south, lz_west
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (oper_day, hour_ending) DO NOTHING;
        """, record)
        inserted += 1
    except Exception as e:
        print(f"⚠️ Insert error: {e}")

conn.commit()
cursor.close()
conn.close()

print(f"✅ Inserted {inserted} records into dam_hourly_price.")