# path makes file/folder paths safer and crossplatform

from pathlib import Path

# Import requests (3rd party library)
# requests make HTTP downloads simple (GET requests, 
# streaming, timeouts, error handling)

import requests

BASE = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/by_year"

# locally : OUT_DIR is the landing zone folder (raw downloaded data).

OUT_DIR = Path("data/landing/ghcn_by_year_2016_2023")

# Create the output folder if it does not exist:
# parents = True -> create any missing parent folders.
# exist_ok = True -> no error if folder already exists.

OUT_DIR.mkdir(parents=True, exist_ok=True)

# Define a function that downloads a file from the 
# internet onto disk.
# url: the HTTP link to download
# out_path: where to save the file
# chunk_size: size of each streamed piece --> default size 1MB


def download(url: str, out_path: Path, chunk_size: int = 1024 * 1024) -> None:
    """
    Download a file from the url to out_path using streaming.
    WHY streaming: prevents loading huge files into RAM.
    WHY .part temp file: avoids leaving corrupt files 
    if download breaks halfway. If the download fails halfway, 
    the broken file is not mistaken for a valid one.
    """
    # WHY: Saves time and bandwidth. Prevents re-downloading a file you
    # already have.

    if out_path.exists() and out_path.stat().st_size > 0:
        print(f"[SKIP] {out_path.name} already exists")
        return
        
    # requests.get(...) downloads the URL.
    # stream=True -> don't download everything into memory at 
    # once; stream in chunks.
    # timeout=60 -> if server doesn't respond in time, error 
    # out (prevents hanging forever).
    # "as r" -> r is the response object (status code, headers,
    # body stream, etc.)

    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        tmp = out_path.with_suffix(out_path.suffix + ".part")
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
        
        # After complete download, rename temp file to final file.
        # tmp.replace(out_path) is atomic on most systems: final file
        # appears only when complete.

        tmp.replace(out_path)        
        print(f"[DONE] {out_path.name} saved")

if __name__ == "__main__":

    # Loop through years 2016 to 2023 inclusive
    
    for year in range(2016, 2024):

        # Build the download URL for that year.
        # Example: https://.../by_year/2016.csv.gz
        
        url = f"{BASE}/{year}.csv.gz"
        
        # Build the output file path.
        # Example: data/landing/.../2016.csv.gz

        out = OUT_DIR / f"{year}.csv.gz"
        download(url, out)

    # Print the absolute directory path so you know exactly where the 
    # files are.

    print(f"\nAll files are in: {OUT_DIR.resolve()}")
