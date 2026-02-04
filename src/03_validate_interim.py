# src/03_validate_interim.py
from __future__ import annotations

import csv
from pathlib import Path
from collections import Counter

INTERIM_CSV = Path("data/interim/lastfm_scrobbles_interim.csv")

EXPECTED_FIELDS = [
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

def is_blank(value: str | None) -> bool:
    return value is None or value.strip() == ""

def main() -> int:
    if not INTERIM_CSV.exists():
        print(f"ERROR: missing file: {INTERIM_CSV}")
        return 2

    row_count = 0
    blank_counts = Counter()
    bad_played_at = 0
    dup_key_counts = Counter()

    with INTERIM_CSV.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)

        if reader.fieldnames != EXPECTED_FIELDS:
            print("ERROR: header mismatch.")
            print("Expected:", EXPECTED_FIELDS)
            print("Found:   ", reader.fieldnames)
            return 3

        for row in reader:
            row_count += 1

            # Required-ish fields
            for key in ("played_at_utc", "track_name", "artist_name"):
                if is_blank(row.get(key)):
                    blank_counts[key] += 1

            # played_at_utc should be an integer epoch seconds string
            val = (row.get("played_at_utc") or "").strip()
            try:
                int(val)
            except ValueError:
                bad_played_at += 1

            # Very simple duplication signal (not deduping yet)
            dup_key = (
                (row.get("played_at_utc") or "").strip(),
                (row.get("track_name") or "").strip(),
                (row.get("artist_name") or "").strip(),
            )
            dup_key_counts[dup_key] += 1

    dup_groups = sum(1 for c in dup_key_counts.values() if c > 1)
    dup_rows_over_1 = sum(c - 1 for c in dup_key_counts.values() if c > 1)

    print("Interim CSV validation report")
    print("-----------------------------")
    print(f"Path: {INTERIM_CSV}")
    print(f"Rows: {row_count:,}")
    print(f"Blank played_at_utc: {blank_counts['played_at_utc']:,}")
    print(f"Blank track_name:    {blank_counts['track_name']:,}")
    print(f"Blank artist_name:   {blank_counts['artist_name']:,}")
    print(f"Non-integer played_at_utc: {bad_played_at:,}")
    print(f"Duplicate groups (played_at_utc+track+artist): {dup_groups:,}")
    print(f"Duplicate extra rows beyond first:             {dup_rows_over_1:,}")

    # Fail only on structural problems
    if bad_played_at > 0:
        print("ERROR: found non-integer played_at_utc values.")
        return 4

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
