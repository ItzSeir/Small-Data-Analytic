"""
Appendix A: Automated TLC download script
Creates raw_data/ and downloads 24 Parquet files (two years, monthly) and the taxi zone lookup CSV.
Usage: python src/download_tlc_data.py

Notes:
- Requires `requests` (pip install requests)
- Saves files into raw_data/
- Skips files that already exist and are non-zero in size.
- Retries downloads up to `MAX_RETRIES` on failure.
"""

import os
import sys
import time
from pathlib import Path

import requests

# Configuration
RAW_DIR = Path("raw_data")
YEARS = [2022, 2023]  # two years -> 24 files
MONTHS = list(range(1, 13))
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet"
LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
LOOKUP_OUT = RAW_DIR / "taxi_zone_lookup.csv"
MAX_RETRIES = 3
CHUNK_SIZE = 1024 * 1024  # 1MB


def ensure_raw_dir():
    RAW_DIR.mkdir(parents=True, exist_ok=True)


def download_file(url, out_path, max_retries=MAX_RETRIES):
    out_path = Path(out_path)
    temp_path = out_path.with_suffix(out_path.suffix + ".part")

    # If a completed file exists and is non-empty, skip
    if out_path.exists() and out_path.stat().st_size > 0:
        print(f"Skipping {out_path.name} (exists and non-zero)")
        return True

    for attempt in range(1, max_retries + 1):
        try:
            print(f"Downloading {url} -> {out_path} (attempt {attempt})")
            with requests.get(url, stream=True, timeout=60) as r:
                r.raise_for_status()
                with open(temp_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                        if chunk:
                            f.write(chunk)
            # Verify file size
            if temp_path.exists() and temp_path.stat().st_size > 0:
                temp_path.replace(out_path)
                print(f"Saved {out_path} ({out_path.stat().st_size} bytes)")
                return True
            else:
                print(f"Downloaded file is empty for {out_path}")
        except Exception as e:
            print(f"Error downloading {url}: {e}")
        # Backoff before retrying
        sleep = 2 ** attempt
        print(f"Retrying in {sleep} seconds...")
        time.sleep(sleep)

    # Clean up temp file if present and size zero
    if temp_path.exists():
        try:
            temp_path.unlink()
        except Exception:
            pass
    print(f"Failed to download {url} after {max_retries} attempts")
    return False


def main():
    ensure_raw_dir()

    # Build list of trip file URLs and target paths
    tasks = []
    for year in YEARS:
        for month in MONTHS:
            fname = f"yellow_tripdata_{year}-{month:02d}.parquet"
            url = BASE_URL.format(year=year, month=month)
            out = RAW_DIR / fname
            tasks.append((url, out))

    # Add lookup file
    tasks.append((LOOKUP_URL, LOOKUP_OUT))

    failures = []
    for url, out in tasks:
        ok = download_file(url, out)
        if not ok:
            failures.append((url, out))

    print("\nSummary:")
    print(f"Total tasks: {len(tasks)}")
    print(f"Failures: {len(failures)}")
    if failures:
        for url, out in failures:
            print(f" - {out}: {url}")
        sys.exit(2)
    else:
        print("All files downloaded successfully (or already present).")


if __name__ == "__main__":
    main()
