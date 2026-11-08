"""Download one month of NYC TLC yellow taxi trips into data/raw (never committed)."""

import sys
import urllib.request
from pathlib import Path

URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"


def main(month: str) -> None:
    dest = Path("data/raw") / f"yellow_tripdata_{month}.parquet"
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        print(f"{dest} already present, skipping")
        return
    urllib.request.urlretrieve(URL.format(month=month), dest)
    print(f"downloaded {dest} ({dest.stat().st_size / 1e6:.1f} MB)")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "2024-01")
