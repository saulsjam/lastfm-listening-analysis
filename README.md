# Last.fm Listening Analysis

## Project Goal
Analyze long-run listening patterns using my personal Last.fm listening history (~146k scrobbles). This project is designed as a resume-ready analytics portfolio piece emphasizing a conventional, reproducible ETL pipeline and time-based descriptive analysis.

The focus is on clarity, separation of concerns, and boring-on-purpose correctness rather than novelty or clever optimization.

---

## Data Source
Listening history is sourced from the Last.fm API via the `user.getRecentTracks` endpoint. Each scrobble represents a completed listening event. Data are retrieved via paginated API requests and stored locally as raw JSON responses.

Raw API data are not committed to this repository; all downstream analysis is performed on derived tabular datasets.

---

## Known API Characteristics
- The Last.fm API can be flaky and intermittently returns server errors (500/502/503/504).
- The endpoint represents “recent” listening activity, so total scrobble counts will change between runs as listening continues.
- No cleaning or transformation is performed during data ingestion to preserve source fidelity.

Retry logic and staged processing are used to mitigate these issues.

---

## Analysis Pipeline
This repository is organized as a linear, reproducible pipeline:

1. **01_fetch_lastfm.py**  
   Fetch paginated listening history from the Last.fm API and write one raw JSON file per page to disk. No transformation or filtering is performed at this stage.

2. **02_flatten_lastfm.py**  
   Read all raw JSON page files, extract one row per scrobble, and write a single flattened interim dataset as CSV. Lineage fields (`source_page`, `source_file`) are included for traceability.
   
3. **03_validate_interim.py**  
   Validate structural assumptions about the interim dataset (schema, required fields, timestamp parseability, duplicate detection). This script enforces pipeline contracts before any transformation is applied.

4. **04_make_processed.py**  
   Read the validated interim dataset, derive explicit UTC timestamp and time-part fields (date, year, month, day-of-week, hour), and write a processed CSV for downstream analysis. No filtering, deduplication, or analytical aggregation is performed.


Running these scripts in order reproduces the interim dataset used for analysis.

---

## Interim Output
- Flattened scrobble dataset  
  `data/interim/lastfm_scrobbles_interim.csv`

Schema:
- `played_at_utc`
- `track_name`
- `artist_name`
- `album_name`
- `track_mbid`
- `artist_mbid`
- `album_mbid`
- `source_page`
- `source_file`

## Processed Output
- Time-enriched scrobble dataset  
  `data/processed/lastfm_scrobbles_processed.csv`

Derived fields include:
- UTC timestamp (`played_at_ts_utc`)
- Calendar date (`date_utc`)
- Year, month, day-of-week, and hour (UTC)

The processed dataset preserves all interim fields and adds only derived time features. No records are filtered or modified.


---

## Environment & Reproducibility
- Python 3.11 (conda / Anaconda)
- Key packages:
  - `requests`
  - `python-dotenv`
- Secrets are managed via a local `.env` file (not committed)
- `.env.example` documents required environment variables

To reproduce:
1. Clone the repository
2. Create a `.env` file with:
	LASTFM_API_KEY=...
	LASTFM_USERNAME=...
3. Run:
	python src/01_fetch_lastfm.py
	python src/02_flatten_lastfm.py
	python src/03_validate_interim.py
	python src/04_make_processed.py

---

## Project Status
The raw, interim, and processed data layers are complete and reproducible.

Next steps include:
- Exploratory analysis of long-run listening patterns
- Optional modeling of assumed local listening times
- Tableau dashboard development