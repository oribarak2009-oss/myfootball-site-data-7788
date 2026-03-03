import json
import os
import time
import hashlib
import re
from datetime import datetime, timezone

import pandas as pd
import requests
from bs4 import BeautifulSoup


# ----------------------------
# General config
# ----------------------------
HEADERS = {"User-Agent": "TopLevelBot/1.0"}
TIMEOUT = 25

# JuniorLeague pacing
SLEEP_BETWEEN_PAGES_SEC = 2

# IFA pacing + safety limits (so GitHub Actions won't run forever)
IFA_SLEEP_BETWEEN_GAMES_SEC = 1.5
MAX_GAMES_PER_LEAGUE = 60  # change if you want more/less


# ----------------------------
# JuniorLeague config
# ----------------------------
JUNIORLEAGUE = [
    {"key": "kids_a_central_25_26", "url": "https://www.juniorleague.co.il/%D7%9C%D7%99%D7%92%D7%AA-%D7%99%D7%9C%D7%93%D7%99%D7%9D-%D7%90-%D7%9E%D7%A8%D7%9B%D7%96-25-26/"},
    {"key": "kids_b_central_25_26", "url": "https://www.juniorleague.co.il/%D7%9C%D7%99%D7%92%D7%AA-%D7%99%D7%9C%D7%93%D7%99%D7%9D-%D7%91-%D7%9E%D7%A8%D7%9B%D7%96-25-26/"},
    {"key": "kids_c_central_25_26", "url": "https://www.juniorleague.co.il/%D7%99%D7%9C%D7%93%D7%99%D7%9D-%D7%92-%D7%9E%D7%A8%D7%9B%D7%96-25-26/"},
    {"key": "youth_premier_25_26", "url": "https://www.juniorleague.co.il/%D7%9C%D7%99%D7%92%D7%AA-%D7%94%D7%A2%D7%9C-%D7%9C%D7%A0%D7%95%D7%A2%D7%A8-25-26/"},
    {"key": "na_arim_a_premier_25_26", "url": "https://www.juniorleague.co.il/%D7%9C%D7%99%D7%92%D7%AA-%D7%A0%D7%A2%D7%A8%D7%99%D7%9D-%D7%90-%D7%A2%D7%9C-25-26/"},
    {"key": "na_arim_b_premier_25_26", "url": "https://www.juniorleague.co.il/%D7%9C%D7%99%D7%92%D7%AA-%D7%A0%D7%A2%D7%A8%D7%99%D7%9D-%D7%91-%D7%A2%D7%9C-25-26/"},
    {"key": "na_arim_c_premier_25_26", "url": "https://www.juniorleague.co.il/%D7%9C%D7%99%D7%92%D7%AA-%D7%A0%D7%A2%D7%A8%D7%99%D7%9D-%D7%92-%D7%A2%D7%9C-25-26/"},
    # Sharon (you added)
    {"key": "kids_a_sharon_25_26", "url": "https://www.juniorleague.co.il/%D7%9C%D7%99%D7%92%D7%AA-%D7%99%D7%9C%D7%93%D7%99%D7%9D-%D7%90-%D7%A9%D7%A8%D7%95%D7%9F-25-26/"},
    {"key": "kids_b_sharon_25_26", "url": "https://www.juniorleague.co.il/%D7%9C%D7%99%D7%92%D7%AA-%D7%99%D7%9C%D7%93%D7%99%D7%9D-%D7%91-%D7%A9%D7%A8%D7%95%D7%9F-25-26/"},
    {"key": "kids_c_sharon_25_26", "url": "https://www.juniorleague.co.il/%D7%99%D7%9C%D7%93%D7%99%D7%9D-%D7%92-%D7%A9%D7%A8%D7%95%D7%9F-25-26/"},
    # Artzit + Noar (you added)
    {"key": "na_arim_g_artzit_darom_25_26", "url": "https://www.juniorleague.co.il/%D7%A0%D7%A2%D7%A8%D7%99%D7%9D-%D7%92-%D7%90%D7%A8%D7%A6%D7%99%D7%AA-%D7%93%D7%A8%D7%95%D7%9D-25-26/"},
    {"key": "na_arim_b_artzit_darom_25_26", "url": "https://www.juniorleague.co.il/%D7%A0%D7%A2%D7%A8%D7%99%D7%9D-%D7%91-%D7%90%D7%A8%D7%A6%D7%99%D7%AA-%D7%93%D7%A8%D7%95%D7%9D-25-26/"},
    {"key": "na_arim_b_artzit_tzafon_25_26", "url": "https://www.juniorleague.co.il/%D7%A0%D7%A2%D7%A8%D7%99%D7%9D-%D7%91-%D7%90%D7%A8%D7%A6%D7%99%D7%AA-%D7%A6%D7%A4%D7%95%D7%9F-25-26/"},
    {"key": "na_arim_a_artzit_darom_25_26", "url": "https://www.juniorleague.co.il/%D7%A0%D7%A2%D7%A8%D7%99%D7%9D-%D7%90-%D7%90%D7%A8%D7%A6%D7%99%D7%AA-%D7%93%D7%A8%D7%95%D7%9D-25-26/"},
    {"key": "na_arim_a_artzit_tzafon_25_26", "url": "https://www.juniorleague.co.il/%D7%A0%D7%A2%D7%A8%D7%99%D7%9D-%D7%90-%D7%90%D7%A8%D7%A6%D7%99%D7%AA-%D7%A6%D7%A4%D7%95%D7%9F-25-26/"},
    {"key": "noar_laumit_darom_25_26", "url": "https://www.juniorleague.co.il/%D7%A0%D7%95%D7%A2%D7%A8-%D7%9C%D7%90%D7%95%D7%9E%D7%99%D7%AA-%D7%93%D7%A8%D7%95%D7%9D-25-26/"},
    {"key": "noar_laumit_tzafon_25_26", "url": "https://www.juniorleague.co.il/%D7%A0%D7%95%D7%A2%D7%A8-%D7%9C%D7%90%D7%95%D7%9E%D7%99%D7%AA-%D7%A6%D7%A4%D7%95%D7%9F-25-26/"},
]


# ----------------------------
# IFA config (football.org.il)
# ----------------------------
IFA_OUT_DIR = "docs/ifa"
IFA_LEAGUES = [
    {"key": "ifa_165", "league_id": 165, "season_id": 27},
    {"key": "ifa_163", "league_id": 163, "season_id": 27},
    {"key": "ifa_173", "league_id": 173, "season_id": 27},
    {"key": "ifa_661", "league_id": 661, "season_id": 27},
    {"key": "ifa_154", "league_id": 154, "season_id": 27},
    {"key": "ifa_824", "league_id": 824, "season_id": 27},
    {"key": "ifa_773", "league_id": 773, "season_id": 27},
    {"key": "ifa_726", "league_id": 726, "season_id": 27},
    {"key": "ifa_156", "league_id": 156, "season_id": 27},
]


# ----------------------------
# Output dirs
# ----------------------------
OUT_DIR = "docs/data"
STATE_DIR = "docs/_state"


# ----------------------------
# Helpers
# ----------------------------
def stable_hash(obj) -> str:
    s = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def fetch_html(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r.text


def save_json(path: str, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def df_to_records(df: pd.DataFrame) -> dict:
    df = df.fillna("")
    df.columns = [str(c).strip() for c in df.columns]
    df = df.astype(str)
    return {"columns": list(df.columns), "rows": df.to_dict(orient="records")}


# ----------------------------
# JuniorLeague
# ----------------------------
def ensure_juniorleague_dirs():
    os.makedirs(os.path.join(OUT_DIR, "standings"), exist_ok=True)
    os.makedirs(os.path.join(OUT_DIR, "results"), exist_ok=True)
    os.makedirs(STATE_DIR, exist_ok=True)


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


def load_prev_hash(key: str, kind: str):
    p = os.path.join(STATE_DIR, f"{key}.{kind}.sha")
    if not os.path.exists(p):
        return None
    return open(p, "r", encoding="utf-8").read().strip()


def save_hash(key: str, kind: str, h: str):
    p = os.path.join(STATE_DIR, f"{key}.{kind}.sha")
    with open(p, "w", encoding="utf-8") as f:
        f.write(h)


def run_juniorleague():
    ensure_juniorleague_dirs()

    manifest = {
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
        "updated": []
    }

    for league in JUNIORLEAGUE:
        key, url = league["key"], league["url"]
        try:
            html = fetch_html(url)
            tables = pd.read_html(html)
            standings, results = split_standings_and_results(tables)

            if standings:
                payload = {"key": key, "source": url, "type": "standings", **standings}
                h = stable_hash(payload)
                prev = load_prev_hash(key, "standings")
                if h != prev:
                    save_json(os.path.join(OUT_DIR, "standings", f"{key}.json"), payload)
                    save_hash(key, "standings", h)
                    manifest["updated"].append({"key": key, "type": "standings"})

            if results:
                payload = {"key": key, "source": url, "type": "results", **results}
                h = stable_hash(payload)
                prev = load_prev_hash(key, "results")
                if h != prev:
                    save_json(os.path.join(OUT_DIR, "results", f"{key}.json"), payload)
                    save_hash(key, "results", h)
                    manifest["updated"].append({"key": key, "type": "results"})

        except Exception as e:
            manifest["updated"].append({"key": key, "type": "error", "error": str(e)})

        time.sleep(SLEEP_BETWEEN_PAGES_SEC)

    save_json("docs/manifest.json", manifest)
    return manifest


# ----------------------------
# IFA (football.org.il)
# ----------------------------
def ifa_league_games_url(league_id: int, season_id: int) -> str:
    return f"https://www.football.org.il/leagues/games/?league_id={league_id}&season_id={season_id}"


def ifa_game_url(game_id: int) -> str:
    return f"https://www.football.org.il/leagues/games/game/?game_id={game_id}"


def fetch_soup(url: str) -> BeautifulSoup:
    return BeautifulSoup(fetch_html(url), "html.parser")


def extract_game_ids_from_league(league_id: int, season_id: int) -> list[int]:
    soup = fetch_soup(ifa_league_games_url(league_id, season_id))
    ids = set()
    for a in soup.find_all("a", href=True):
        m = re.search(r"game_id=(\d+)", a["href"])
        if m:
            ids.add(int(m.group(1)))
    # Keep latest ones at the end (usually)
    ids_sorted = sorted(ids)
    if len(ids_sorted) > MAX_GAMES_PER_LEAGUE:
        ids_sorted = ids_sorted[-MAX_GAMES_PER_LEAGUE:]
    return ids_sorted


def text_clean(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def parse_ifa_game(game_id: int) -> dict:
    url = ifa_game_url(game_id)
    soup = fetch_soup(url)

    page_text = text_clean(soup.get_text(" "))

    # Event hints (fallback)
    events = []
    for m in re.finditer(r"(\d{1,3})\D{0,30}(חילוף|נכנס|יצא)", page_text):
        minute = int(m.group(1))
        snippet = page_text[max(0, m.start() - 70): m.start() + 140]
        events.append({"type": "maybe_sub_or_event", "minute": minute, "raw": snippet})

    for m in re.finditer(r"(\d{1,3})\D{0,30}(צהוב|אדום)", page_text):
        minute = int(m.group(1))
        snippet = page_text[max(0, m.start() - 70): m.start() + 140]
        events.append({"type": "maybe_card", "minute": minute, "raw": snippet})

    # Tables often contain lineups / events
    tables = []
    try:
        dfs = pd.read_html(str(soup))
        for df in dfs[:12]:
            tables.append(df_to_records(df))
    except Exception:
        pass

    return {
        "source": url,
        "game_id": game_id,
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
        "tables": tables,
        "events_hints": events
    }


def run_ifa_all():
    os.makedirs(os.path.join(IFA_OUT_DIR, "leagues"), exist_ok=True)
    os.makedirs(os.path.join(IFA_OUT_DIR, "games"), exist_ok=True)

    ifa_manifest = {
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
        "leagues": [],
        "games_saved": 0
    }

    for L in IFA_LEAGUES:
        key = L["key"]
        league_id = L["league_id"]
        season_id = L["season_id"]

        try:
            game_ids = extract_game_ids_from_league(league_id, season_id)

            league_payload = {
                "key": key,
                "league_id": league_id,
                "season_id": season_id,
                "games_url": ifa_league_games_url(league_id, season_id),
                "game_ids": game_ids
            }
            save_json(os.path.join(IFA_OUT_DIR, "leagues", f"{key}.json"), league_payload)
            ifa_manifest["leagues"].append({"key": key, "count": len(game_ids)})

            for gid in game_ids:
                game_payload = parse_ifa_game(gid)
                save_json(os.path.join(IFA_OUT_DIR, "games", f"{gid}.json"), game_payload)
                ifa_manifest["games_saved"] += 1
                time.sleep(IFA_SLEEP_BETWEEN_GAMES_SEC)

        except Exception as e:
            ifa_manifest["leagues"].append({"key": key, "error": str(e)})

    save_json(os.path.join(IFA_OUT_DIR, "manifest.json"), ifa_manifest)
    return ifa_manifest


# ----------------------------
# Main
# ----------------------------
def main():
    jl = run_juniorleague()
    ifa = run_ifa_all()
    print("JuniorLeague updated:", jl.get("updated", []))
    print("IFA leagues:", ifa.get("leagues", []), "games_saved:", ifa.get("games_saved", 0))


if __name__ == "__main__":
    main()
