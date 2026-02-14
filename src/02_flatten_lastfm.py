"""
02_flatten_lastfm.py

Purpose:
    Read raw paginated Last.fm JSON responses and extract one row per scrobble,
    writing a flattened interim CSV dataset.

Input:
    - Raw JSON files in data/raw/lastfm_getrecenttracks_pages/

Output:
    - data/interim/lastfm_scrobbles_interim.csv

Notes:
    - One row is written per track object returned by the API.
    - No filtering, deduplication, or timestamp transformation is performed.
    - Unix timestamps (UTC) are preserved as provided by the API.
    - Lineage fields (source_page, source_file) are included for traceability.
"""

from pathlib import Path
import json
import csv


def track_to_row(track: dict, source_page: int, source_file: str) -> dict:
    date = track.get("date", {})
    artist = track.get("artist", {})
    album = track.get("album", {})

    return {
        "played_at_utc": date.get("uts"),
        "track_name": track.get("name"),
        "artist_name": artist.get("#text"),
        "album_name": album.get("#text"),
        "track_mbid": track.get("mbid"),
        "artist_mbid": artist.get("mbid"),
        "album_mbid": album.get("mbid"),
        "source_page": source_page,
        "source_file": source_file,
    }


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    raw_dir = project_root / "data" / "raw" / "lastfm_getrecenttracks_pages"
    interim_dir = project_root / "data" / "interim"
    interim_dir.mkdir(parents=True, exist_ok=True)

    rows = []

    for path in sorted(raw_dir.glob("page_*.json")):
        page_num = int(path.stem.split("_")[1])

        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        tracks = data["recenttracks"]["track"]

        for track in tracks:
            row = track_to_row(
                track,
                source_page=page_num,
                source_file=path.name,
            )
            rows.append(row)

    out_path = interim_dir / "lastfm_scrobbles_interim.csv"

    fieldnames = rows[0].keys()

    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {out_path}")


if __name__ == "__main__":
    main()
