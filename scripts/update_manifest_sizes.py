#!/usr/bin/env python3
"""
Update dataset_manifest.csv with local file sizes from raw_data/.
Usage: python scripts/update_manifest_sizes.py
This script writes the updated manifest back to data/dataset_manifest.csv (overwrites).
It only updates the `local_file_size_bytes` column when it is empty.
"""
import csv
from pathlib import Path

RAW = Path("raw_data")
MANIFEST = Path("data/dataset_manifest.csv")
TMP = Path("data/dataset_manifest.tmp.csv")

if not MANIFEST.exists():
    print(f"Manifest not found: {MANIFEST}")
    raise SystemExit(1)

with MANIFEST.open(newline="") as inf, TMP.open("w", newline="") as outf:
    reader = csv.DictReader(inf)
    fieldnames = reader.fieldnames
    writer = csv.DictWriter(outf, fieldnames=fieldnames)
    writer.writeheader()
    for row in reader:
        fname = row.get("file_name")
        if fname:
            p = RAW / fname
            if p.exists() and p.is_file():
                size = str(p.stat().st_size)
            else:
                size = ""
            # Only overwrite if empty
            if not row.get("local_file_size_bytes"):
                row["local_file_size_bytes"] = size
        writer.writerow(row)

# Replace original manifest
TMP.replace(MANIFEST)
print(f"Updated manifest written to {MANIFEST}")
