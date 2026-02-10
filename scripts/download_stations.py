# path makes file/folder paths safer and crossplatform
from pathlib import Path

# csv module (built-in).
# Y: simple, correct CSV writing (handles commas/quotes safely).

import csv

# Import requests (3rd party library)
# requests make HTTP downloads simple (GET requests, 
# streaming, timeouts, error handling)

import requests

BASE = "https://www.ncei.noaa.gov/pub/data/ghcn/daily"

OUT_DIR = Path("data/landing/ghcn_meta")
OUT_DIR.mkdir(parents=True, exist_ok=True)

def download(url: str, out_path: Path, chunk_size: int = 1024 * 1024) -> None:
    if out_path.exists() and out_path.stat().st_size > 0:
        print(f"[SKIP] {out_path.name} already exists")
        return

    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        tmp = out_path.with_suffix(out_path.suffix + ".part")
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
        tmp.replace(out_path)
        print(f"[DONE] {out_path.name} saved")

if __name__ == "__main__":

    # File locations we will create.
    # txt_path is the raw NOAA fixed-width metadata file
    
    txt_path = OUT_DIR / "ghcnd-stations.txt"
    
    # csv_path is the cleaned version we create for easier loading into 
    # Postgres
    
    csv_path = OUT_DIR / "ghcnd-stations.csv"

    # 1) Download station metadata text file from NOAA.

    download(f"{BASE}/ghcnd-stations.txt", txt_path)
    # 2) Convert NOAA fixed-width format into CSV.
    # WHY convert: Postgres \copy works easiest with CSV.
    # NOAA station line contains columns at fixed character positions.

    # Open input TXT as text.
    # encoding="utf-8" to correctly read station names.
    # Open output CSV with newline="" so CSV writer does not insert 
    # blank lines

    # Convert fixed-width station metadata to CSV.
    
    with open(txt_path, "r", encoding="utf-8") as fin, open(
        csv_path, "w", newline="", encoding="utf-8"
    ) as fout:

        # Create a CSV writer object.
        
        writer = csv.writer(fout)

        # Write the CSV header row (column names).
        # WHY: makes it self-describing and easy to load with CSV HEADER

        writer.writerow(
            ["ID", "LATITUDE", "LONGITUDE", "ELEVATION", "STATE", "NAME", "GSN", "HCN", "WMO"]
        )

        # Loop through each fixed-width line in the NOAA txt file
        
        for line in fin

            # Build one CSV row by slicing exact character positions.
            # .strip() removes extra spaces

            row = [
                line[0:11].strip(),
                line[12:20].strip(),
                line[21:30].strip(),
                line[31:37].strip(),
                line[38:40].strip(),
                line[41:71].strip(),
                line[72:75].strip(),
                line[76:79].strip(),
                line[80:85].strip(),
            ]
            
            # Only write row if ID exists (extra safety).
            
            if row[0]:
                writer.writerow(row)

    # Print where the CSV ended up (absolute path).
    
    print(f"[DONE] CSV saved to: {csv_path.resolve()}")
