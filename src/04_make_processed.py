"""
04_make_processed.py

Purpose:
    Read the validated interim scrobble dataset, derive explicit UTC-based
    time features, and write a processed CSV for downstream analysis.

Input:
    - data/interim/lastfm_scrobbles_interim.csv

Output:
    - data/processed/lastfm_scrobbles_processed.csv

Derived fields:
    - played_at_epoch (integer Unix timestamp, UTC)
    - played_at_ts_utc (ISO 8601 UTC timestamp)
    - date_utc (calendar date)
    - year_utc, month_utc, dow_utc, hour_utc (UTC components)

Notes:
    - Timestamps are parsed explicitly using timezone-aware UTC handling.
    - No filtering, deduplication, or analytical aggregation is performed.
    - All interim fields are preserved; only derived time fields are added.
"""


from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path
import sys


INTERIM_FIELDS = [
    "played_at_utc",
    "track_name",
    "artist_name",
    "album_name",
    "track_mbid",
    "artist_mbid",
    "album_mbid",
    "source_page",
    "source_file",
]

DERIVED_FIELDS = [
    "played_at_epoch",
    "played_at_ts_utc",
    "date_utc",
    "year_utc",
    "month_utc",
    "dow_utc",
    "hour_utc",
]


def main() -> int:
    project_root = Path(__file__).resolve().parents[1]
    interim_path = project_root / "data" / "interim" / "lastfm_scrobbles_interim.csv"
    processed_dir = project_root / "data" / "processed"
    processed_path = processed_dir / "lastfm_scrobbles_processed.csv"

    if not interim_path.exists():
        print(f"ERROR: missing interim CSV: {interim_path}")
        return 2

    processed_dir.mkdir(parents=True, exist_ok=True)

    out_fields = INTERIM_FIELDS + DERIVED_FIELDS

    rows_in = 0
    with interim_path.open("r", encoding="utf-8", newline="") as f_in, processed_path.open(
        "w", encoding="utf-8", newline=""
    ) as f_out:
        reader = csv.DictReader(f_in)
        writer = csv.DictWriter(f_out, fieldnames=out_fields)
        writer.writeheader()

        for row in reader:
            rows_in += 1

            epoch = int(row["played_at_utc"])
            dt = datetime.fromtimestamp(epoch, tz=timezone.utc)

            out_row = {k: row.get(k, "") for k in INTERIM_FIELDS}
            out_row["played_at_epoch"] = str(epoch)
            out_row["played_at_ts_utc"] = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            out_row["date_utc"] = dt.date().isoformat()
            out_row["year_utc"] = str(dt.year)
            out_row["month_utc"] = str(dt.month)
            out_row["dow_utc"] = str(dt.weekday())  # Mon=0 ... Sun=6
            out_row["hour_utc"] = str(dt.hour)

            writer.writerow(out_row)

    print(f"Wrote processed CSV: {processed_path}")
    print(f"Rows: {rows_in}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
