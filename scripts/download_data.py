"""Downloads NYC Yellow Taxi trip-data Parquet files into data/raw/.

Source: NYC TLC Trip Record Data (public, free, no auth):
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
"""
import sys
from pathlib import Path

import requests

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"

# Start small: 2 months. Add more (year, month) tuples once the pipeline
# is proven, per the project's "start with 3-6 months" guidance.
MONTHS = [
    (2024, 1),
    (2024, 2),
]


def download(year: int, month: int) -> None:
    url = BASE_URL.format(year=year, month=month)
    dest = RAW_DIR / f"yellow_tripdata_{year}-{month:02d}.parquet"
    if dest.exists():
        print(f"skip (exists): {dest.name}")
        return

    print(f"downloading: {url}")
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        written = 0
        tmp = dest.with_suffix(".tmp")
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
                written += len(chunk)
                if total:
                    pct = written / total * 100
                    print(f"\r  {dest.name}: {written / 1e6:.1f}MB / {total / 1e6:.1f}MB ({pct:.0f}%)", end="")
        print()
        tmp.rename(dest)


def main() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    for year, month in MONTHS:
        download(year, month)
    print("done.")


if __name__ == "__main__":
    sys.exit(main())
