import json, os, time, hashlib
from datetime import datetime, timezone
import requests

HEADERS = {"User-Agent": "TopLevelBot/1.0"}
TIMEOUT = 30
SLEEP_SEC = 1.0

LEAGUES = [
  {"key": "kids_b_sharon_25_26",  "matches_url": "https://junior-league-api-server-928016183646.us-central1.run.app/api/public/leagues/%D7%9C%D7%99%D7%92%D7%AA%20%D7%99%D7%9C%D7%93%D7%99%D7%9D%20%D7%91'%20%D7%A9%D7%A8%D7%95%D7%9F/matches"},
  {"key": "kids_b_central_25_26", "matches_url": "https://junior-league-api-server-928016183646.us-central1.run.app/api/public/leagues/%D7%9C%D7%99%D7%92%D7%AA%20%D7%99%D7%9C%D7%93%D7%99%D7%9D%20%D7%91'%20%D7%9E%D7%A8%D7%9B%D7%96/matches"},
  {"key": "kids_a_central_25_26", "matches_url": "https://junior-league-api-server-928016183646.us-central1.run.app/api/public/leagues/%D7%9C%D7%99%D7%92%D7%AA%20%D7%99%D7%9C%D7%93%D7%99%D7%9D%20%D7%90'%20%D7%9E%D7%A8%D7%9B%D7%96/matches"},
  {"key": "kids_a_sharon_25_26",  "matches_url": "https://junior-league-api-server-928016183646.us-central1.run.app/api/public/leagues/%D7%9C%D7%99%D7%92%D7%AA%20%D7%99%D7%9C%D7%93%D7%99%D7%9D%20%D7%90'%20%D7%A9%D7%A8%D7%95%D7%9F/matches"},
  {"key": "kids_c_central_25_26", "matches_url": "https://junior-league-api-server-928016183646.us-central1.run.app/api/public/leagues/%D7%9C%D7%99%D7%92%D7%AA%20%D7%99%D7%9C%D7%93%D7%99%D7%9D%20%D7%92'%20%D7%9E%D7%A8%D7%9B%D7%96/matches"},
  {"key": "kids_c_sharon_25_26",  "matches_url": "https://junior-league-api-server-928016183646.us-central1.run.app/api/public/leagues/%D7%9C%D7%99%D7%92%D7%AA%20%D7%99%D7%9C%D7%93%D7%99%D7%9D%20%D7%92'%20%D7%A9%D7%A8%D7%95%D7%9F/matches"},

  {"key": "na_arim_c_premier_25_26", "matches_url": "https://junior-league-api-server-928016183646.us-central1.run.app/api/public/leagues/%D7%A0%D7%A2%D7%A8%D7%99%D7%9D%20%D7%92'%20%D7%A2%D7%9C/matches"},
  {"key": "na_arim_b_premier_25_26", "matches_url": "https://junior-league-api-server-928016183646.us-central1.run.app/api/public/leagues/%D7%9C%D7%99%D7%92%D7%AA%20%D7%A0%D7%A2%D7%A8%D7%99%D7%9D%20%D7%91'%20%D7%A2%D7%9C/matches"},
  {"key": "na_arim_a_premier_25_26", "matches_url": "https://junior-league-api-server-928016183646.us-central1.run.app/api/public/leagues/%D7%9C%D7%99%D7%92%D7%AA%20%D7%A0%D7%A2%D7%A8%D7%99%D7%9D%20%D7%90'%20%D7%A2%D7%9C/matches"},

  {"key": "youth_premier_25_26", "matches_url": "https://junior-league-api-server-928016183646.us-central1.run.app/api/public/leagues/%D7%9C%D7%99%D7%92%D7%AA%20%D7%94%D7%A2%D7%9C%20%D7%9C%D7%A0%D7%95%D7%A2%D7%A8/matches"},
]

OUT_DIR = "docs/data"
STATE_DIR = "docs/_state"

def ensure_dirs():
    os.makedirs(os.path.join(OUT_DIR, "results"), exist_ok=True)
    os.makedirs(os.path.join(OUT_DIR, "standings"), exist_ok=True)
    os.makedirs(STATE_DIR, exist_ok=True)

def save_json(path: str, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def stable_hash(obj) -> str:
    s = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def load_prev_hash(key: str, kind: str):
    p = os.path.join(STATE_DIR, f"{key}.{kind}.sha")
    if not os.path.exists(p):
        return None
    return open(p, "r", encoding="utf-8").read().strip()

def save_hash(key: str, kind: str, h: str):
    p = os.path.join(STATE_DIR, f"{key}.{kind}.sha")
    with open(p, "w", encoding="utf-8") as f:
        f.write(h)

def fetch_json(url: str) -> dict:
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

def parse_score(m: dict):
    for hg_key, ag_key in [
        ("homeGoals", "awayGoals"),
        ("homeScore", "awayScore"),
        ("home_goals", "away_goals"),
        ("home_score", "away_score"),
    ]:
        if hg_key in m and ag_key in m:
            try:
                return int(m[hg_key]), int(m[ag_key])
            except Exception:
                pass

    for k in ["result", "score", "finalScore", "final_score"]:
        v = m.get(k)
        if isinstance(v, str) and "-" in v:
            parts = v.replace("–", "-").split("-")
            if len(parts) >= 2:
                try:
                    return int(parts[0].strip()), int(parts[1].strip())
                except Exception:
                    pass
    return None, None

def team_name(m: dict, side: str):
    for k in [f"{side}Team", f"{side}_team", f"{side}TeamName", f"{side}_team_name"]:
        v = m.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None

def build_standings(matches: list[dict]) -> dict:
    table = {}

    def ensure(team: str):
        if team not in table:
            table[team] = {
                "Pos": 0,
                "Team": team,
                "Pld": 0, "W": 0, "D": 0, "L": 0,
                "GF": 0, "GA": 0, "GD": 0,
                "Pts": 0
            }

    for m in matches:
        home = team_name(m, "home")
        away = team_name(m, "away")
        if not home or not away:
            continue

        hg, ag = parse_score(m)
        if hg is None or ag is None:
            continue

        ensure(home); ensure(away)

        table[home]["Pld"] += 1
        table[away]["Pld"] += 1

        table[home]["GF"] += hg
        table[home]["GA"] += ag
        table[away]["GF"] += ag
        table[away]["GA"] += hg

        if hg > ag:
            table[home]["W"] += 1
            table[away]["L"] += 1
            table[home]["Pts"] += 3
        elif hg < ag:
            table[away]["W"] += 1
            table[home]["L"] += 1
            table[away]["Pts"] += 3
        else:
            table[home]["D"] += 1
            table[away]["D"] += 1
            table[home]["Pts"] += 1
            table[away]["Pts"] += 1

    rows = list(table.values())
    for r in rows:
        r["GD"] = r["GF"] - r["GA"]

    rows.sort(key=lambda r: (r["Pts"], r["GD"], r["GF"]), reverse=True)

    # ✅ מיקום
    for i, r in enumerate(rows, start=1):
        r["Pos"] = i

    return {
        "columns": ["Pos", "Team", "Pld", "W", "D", "L", "GF", "GA", "GD", "Pts"],
        "rows": rows
    }

def main():
    ensure_dirs()
    manifest = {"fetched_at_utc": datetime.now(timezone.utc).isoformat(), "updated": []}

    for L in LEAGUES:
        key = L["key"]
        url = L["matches_url"]

        try:
            data = fetch_json(url)
            matches = data.get("matches") or []

            results_payload = {"key": key, "type": "results", "source": url, "data": data}
            rh = stable_hash(results_payload)
            if rh != load_prev_hash(key, "results"):
                save_json(os.path.join(OUT_DIR, "results", f"{key}.json"), results_payload)
                save_hash(key, "results", rh)
                manifest["updated"].append({"key": key, "type": "results"})

            standings_obj = build_standings(matches)
            standings_payload = {"key": key, "type": "standings", "source": url, **standings_obj}
            sh = stable_hash(standings_payload)
            if sh != load_prev_hash(key, "standings"):
                save_json(os.path.join(OUT_DIR, "standings", f"{key}.json"), standings_payload)
                save_hash(key, "standings", sh)
                manifest["updated"].append({"key": key, "type": "standings"})

        except Exception as e:
            manifest["updated"].append({"key": key, "type": "error", "error": str(e)})

        time.sleep(SLEEP_SEC)

    save_json("docs/manifest.json", manifest)
    print("Done:", manifest["updated"])

if __name__ == "__main__":
    main()
