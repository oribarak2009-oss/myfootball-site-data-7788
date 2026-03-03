import json, os, time
from datetime import datetime, timezone
from io import StringIO

import pandas as pd
import requests

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"
}

TIMEOUT = 25
SLEEP_BETWEEN_PAGES_SEC = 2

LEAGUES = [
    {"key": "kids_c_sharon_25_26", "url": "https://www.juniorleague.co.il/%D7%99%D7%9C%D7%93%D7%99%D7%9D-%D7%92-%D7%A9%D7%A8%D7%95%D7%9F-25-26/"},
    {"key": "kids_b_sharon_25_26", "url": "https://www.juniorleague.co.il/%D7%9C%D7%99%D7%92%D7%AA-%D7%99%D7%9C%D7%93%D7%99%D7%9D-%D7%91-%D7%A9%D7%A8%D7%95%D7%9F-25-26/"},
    {"key": "kids_a_sharon_25_26", "url": "https://www.juniorleague.co.il/%D7%9C%D7%99%D7%92%D7%AA-%D7%99%D7%9C%D7%93%D7%99%D7%9D-%D7%90-%D7%A9%D7%A8%D7%95%D7%9F-25-26/"},
]

OUT_DIR = "docs/data"


def ensure_dirs():
    os.makedirs(os.path.join(OUT_DIR, "standings"), exist_ok=True)
    os.makedirs(os.path.join(OUT_DIR, "results"), exist_ok=True)


def fetch_html(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r.text


def df_to_records(df: pd.DataFrame) -> dict:
    df = df.fillna("")
    df.columns = [str(c).strip() for c in df.columns]
    df = df.astype(str)
    return {"columns": list(df.columns), "rows": df.to_dict(orient="records")}


def split_standings_and_results(tables: list[pd.DataFrame]):
    standings = None
    results = None

    for df in tables:
        cols = " ".join([str(c) for c in df.columns])

        if standings is None and any(k in cols for k in ["נק", "הפרש", "נצ", "תיק", "הפס", "שער"]):
            standings = df_to_records(df)
            continue

        if results is None and any(k in cols for k in ["מח", "תאריך", "משחק", "תוצאה"]):
            results = df_to_records(df)
            continue

    if standings is None and len(tables) >= 1:
        standings = df_to_records(tables[0])
    if results is None and len(tables) >= 2:
        results = df_to_records(tables[1])

    return standings, results


def save_json(path: str, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def main():
    ensure_dirs()

    manifest = {
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
        "updated": []
    }

    for league in LEAGUES:
        key, url = league["key"], league["url"]

        try:
            html = fetch_html(url)

            # 🔥 התיקון החשוב פה
            tables = pd.read_html(StringIO(html))

            standings, results = split_standings_and_results(tables)

            if standings:
                payload = {"key": key, "source": url, "type": "standings", **standings}
                save_json(os.path.join(OUT_DIR, "standings", f"{key}.json"), payload)
                manifest["updated"].append({"key": key, "type": "standings"})

            if results:
                payload = {"key": key, "source": url, "type": "results", **results}
                save_json(os.path.join(OUT_DIR, "results", f"{key}.json"), payload)
                manifest["updated"].append({"key": key, "type": "results"})

        except Exception as e:
            manifest["updated"].append({"key": key, "type": "error", "error": str(e)})

        time.sleep(SLEEP_BETWEEN_PAGES_SEC)

    save_json("docs/manifest.json", manifest)
    print("Done:", manifest["updated"])


if __name__ == "__main__":
    main()
