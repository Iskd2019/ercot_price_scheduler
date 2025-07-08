import requests
from bs4 import BeautifulSoup
import datetime
import json
import os

def fetch_ercot_real_time_lmp():
    # Step 1: Get today's date in the required format
    today = datetime.datetime.now()
    #CHANGE DATE STR FOR ACCESS DIFFERENT DATE
    #date_str = today.strftime('%Y%m%d')
    #url = f'https://www.ercot.com/content/cdr/html/{date_str}_real_time_spp.html'
    date_str = '20250626'
    url = f'https://www.ercot.com/content/cdr/html/{date_str}_real_time_spp.html'

    print(f"Fetching: {url}")
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
    except Exception as e:
        print(f"Failed to download page: {e}")
        return

    # Step 2: Parse HTML
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table')
    if not table:
        print("Table not found in page.")
        return

    headers = []
    data_rows = []
    for i, row in enumerate(table.find_all('tr')):
        cols = [col.get_text(strip=True) for col in row.find_all(['th', 'td'])]

        if i == 0:
            headers = cols
        else:
            data_rows.append(cols)

    # Step 3: Extract desired columns
    try:
        op_day_idx = headers.index('Oper Day')
        interval_idx = headers.index('Interval Ending')
        houston_idx = headers.index('LZ_HOUSTON')
        north_idx = headers.index('LZ_NORTH')
        south_idx = headers.index('LZ_SOUTH')
        west_idx = headers.index('LZ_WEST')
    except ValueError as ve:
        print(f"Column missing: {ve}")
        return

    result = []
    for row in data_rows:
        if len(row) < len(headers):
            continue  # skip incomplete rows
        record = {
            'Oper Day': row[op_day_idx],
            'Interval Ending': row[interval_idx],
            'LZ_HOUSTON': row[houston_idx],
            'LZ_NORTH': row[north_idx],
            'LZ_SOUTH': row[south_idx],
            'LZ_WEST': row[west_idx],
        }
        result.append(record)

    # Step 4: Save to JSON
    filename = f'ercot_realtime_{date_str}.json'
    with open(filename, 'w') as f:
        json.dump(result, f, indent=2)

    print(f"Saved {len(result)} records to {filename}")

if __name__ == '__main__':
    fetch_ercot_real_time_lmp()
