import requests
import zipfile
import io
import pandas as pd
import json
from datetime import datetime

# Configuration
METADATA_URL = "https://www.ercot.com/misapp/servlets/IceDocListJsonWS?reportTypeId=13073"
TARGET_ZONES = ["LZ_HOUSTON", "LZ_NORTH", "LZ_SOUTH", "LZ_WEST"]
HEADERS = {"User-Agent": "Mozilla/5.0"}

# Step 1: Fetch metadata and get latest DocID
response = requests.get(METADATA_URL, headers=HEADERS)
metadata = response.json()
latest_doc = metadata["ListDocsByRptTypeRes"]["DocumentList"][0]["Document"]
doc_id = latest_doc["DocID"]
rtd_timestamp = latest_doc["PublishDate"]
# Step 2: Build download URL and fetch ZIP
zip_url = f"https://www.ercot.com/misdownload/servlets/mirDownload?doclookupId={doc_id}"
zip_response = requests.get(zip_url, headers=HEADERS)
zip_file = zipfile.ZipFile(io.BytesIO(zip_response.content))

# Step 3: Extract CSV and filter data
for file_name in zip_file.namelist():
    if file_name.endswith(".csv"):
        with zip_file.open(file_name) as f:
            df = pd.read_csv(f)

# Step 4: Filter and select desired rows/columns
filtered_df = df[df["SettlementPoint"].isin(TARGET_ZONES)][
    ["IntervalEnding", "SettlementPoint", "LMP"]
].copy()

#filtered_df["RTDTimestamp"] = rtd_timestamp
#"RTDTimestamp",

# Step 5: Reorder columns and convert to JSON
filtered_df = filtered_df[[ "IntervalEnding", "SettlementPoint", "LMP"]]
result = filtered_df.to_dict(orient="records")

# Step 6: Save as JSON
with open("rtd_indicative_lmp_forecast.json", "w") as f:
    json.dump(result, f, indent=2)

print("✅ Saved forecast to rtd_indicative_lmp_forecast.json")