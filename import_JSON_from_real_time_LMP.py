import requests
import zipfile
import io
import pandas as pd
import json

# Step 1: Fetch metadata
metadata_url = "https://www.ercot.com/misapp/servlets/IceDocListJsonWS?reportTypeId=12300"
headers = {"User-Agent": "Mozilla/5.0"}
response = requests.get(metadata_url, headers=headers)
data = response.json()

# Step 2: Get latest document ID and build ZIP URL
latest_doc = data["ListDocsByRptTypeRes"]["DocumentList"][0]["Document"]
doc_id = latest_doc["DocID"]
zip_url = f"https://www.ercot.com/misdownload/servlets/mirDownload?doclookupId={doc_id}"

# Step 3: Download and unzip
zip_response = requests.get(zip_url, headers=headers)
zip_file = zipfile.ZipFile(io.BytesIO(zip_response.content))

# Step 4: Read CSV and filter
for file_name in zip_file.namelist():
    if file_name.endswith(".csv"):
        with zip_file.open(file_name) as f:
            df = pd.read_csv(f)

            if {'SettlementPoint', 'LMP', 'SCEDTimestamp'}.issubset(df.columns):
                target_zones = ['LZ_HOUSTON', 'LZ_NORTH', 'LZ_SOUTH', 'LZ_WEST']
                df_filtered = df[df['SettlementPoint'].isin(target_zones)]

                # Get the latest timestamp
                latest_time = df_filtered['SCEDTimestamp'].max()
                df_latest = df_filtered[df_filtered['SCEDTimestamp'] == latest_time]

                # Select required columns
                result_df = df_latest[['SettlementPoint', 'LMP', 'SCEDTimestamp']]

                # Convert to JSON and save
                output = result_df.to_dict(orient='records')
                with open("latest_real_time_LMP.json", "w") as outfile:
                    json.dump(output, outfile, indent=2)

                print("✅ Saved to latest_real_time_LMP.json")