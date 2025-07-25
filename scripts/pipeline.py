import os
import json
import requests
from pathlib import Path

def download_and_log_file(taxi_type, year, month,
                          log_path="etl_logs/processed_files.json",
                          output_dir="data/raw"):
    """
    Download NYC TLC trip data file and log its name for incremental tracking.
    """

    # Format the file name
    month_str = f"{month:02d}"
    file_name = f"{taxi_type}_tripdata_{year}-{month_str}.parquet"
    file_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"
    local_path = os.path.join(output_dir, taxi_type, file_name)

    # Make sure output directory exists
    Path(os.path.join(output_dir, taxi_type)).mkdir(parents=True, exist_ok=True)
    Path(os.path.dirname(log_path)).mkdir(parents=True, exist_ok=True)

    # Load or initialize the log
    if os.path.exists(log_path):
        with open(log_path, "r") as f:
            processed_files = set(json.load(f))
    else:
        processed_files = set()

    if file_name in processed_files:
        print(f"[SKIP] Already downloaded: {file_name}")
        return

    # Download the file
    try:
        print(f"[DOWNLOAD] {file_name} from {file_url}")
        r = requests.get(file_url, stream=True)
        if r.status_code == 200:
            with open(local_path, "wb") as f:
                for chunk in r.iter_content(1024):
                    f.write(chunk)
            print(f"[SUCCESS] Saved to: {local_path}")
            processed_files.add(file_name)
        else:
            print(f"[NOT FOUND] {file_url} — HTTP {r.status_code}")
    except Exception as e:
        print(f"[ERROR] Download failed: {file_name} — {e}")

    # Save updated log
    with open(log_path, "w") as f:
        json.dump(list(processed_files), f, indent=2)
