# src/01_fetch_lastfm.py
from pathlib import Path
import json
import os
import time

import requests
from dotenv import load_dotenv


API_ROOT = "https://ws.audioscrobbler.com/2.0/"
METHOD = "user.getrecenttracks"



def main() -> None:
    load_dotenv()

    api_key = os.getenv("LASTFM_API_KEY")
    username = os.getenv("LASTFM_USERNAME")
    if not api_key:
        raise RuntimeError("Missing LASTFM_API_KEY in .env")
    if not username:
        raise RuntimeError("Missing LASTFM_USERNAME in .env")
    
    project_root = Path(__file__).resolve().parents[1]
    out_dir = project_root / "data" / "raw" / "lastfm_getrecenttracks_pages"
    out_dir.mkdir(parents=True, exist_ok=True)

    # ---- fetch page 1 first ----
    params = {
        "method": METHOD,
        "user": username,
        "api_key": api_key,
        "format": "json",
        "limit": 200,
        "page": 1,
        }

    resp = requests.get(API_ROOT, params=params, timeout=30)
    if resp.status_code != 200:
        print("Status:", resp.status_code)
        print(resp.text[:1000])
        return
    data = resp.json()

    attr = data["recenttracks"]["@attr"]
    total_pages = int(attr["totalPages"])

    print("perPage:", attr["perPage"])
    print("page:", attr["page"])
    print("totalPages:", attr["totalPages"])
    print("total:", attr["total"])

    # Save page 1
    out_path = out_dir / "page_00001.json"
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Saved page 1/{total_pages}")

    # ---- loop pages 2..total_pages ----
    # ---- loop pages 2..total_pages ----
    for page_num in range(2, total_pages + 1):
        params["page"] = page_num

        max_attempts = 6
        attempt = 0

        while True:
            attempt += 1
            resp = requests.get(API_ROOT, params=params, timeout=30)

            if resp.status_code == 200:
                break  # success, exit retry loop

            # transient server trouble (502/503/504 are common)
            if resp.status_code in (500, 502, 503, 504) and attempt < max_attempts:
                wait_s = 30 * attempt  # 30s, 60s, 90s, ...
                print(f"Status: {resp.status_code} on page {page_num}. Waiting {wait_s}s then retrying ({attempt}/{max_attempts})...")
                time.sleep(wait_s)
                continue

            # non-transient or too many attempts
            print("Status:", resp.status_code)
            print("Failed on page:", page_num)
            print(resp.text[:1000])
            return

        data = resp.json()

        out_path = out_dir / f"page_{page_num:05d}.json"
        with out_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        if page_num % 25 == 0 or page_num == total_pages:
            print(f"Saved page {page_num}/{total_pages}")


if __name__ == "__main__":
    main()
