#!/usr/bin/env python3
from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
import urllib.parse
import urllib.request

API_KEY = "123"
BASE = "https://www.thesportsdb.com/api/v1/json"
REQUESTS_PER_MINUTE = 30
# One HTTP call per calendar day; stay under free-tier rate limits.
SLEEP_AFTER_EACH_DAY_REQUEST = 60.0 / REQUESTS_PER_MINUTE

DB_PATH = Path(__file__).resolve().parent / "nba_schedule.sqlite3"

NBA_ID = "4387" # nba league id
teams = [
    "Atlanta Hawks",
    "Boston Celtics",
    "Brooklyn Nets",
    "Charlotte Hornets",
    "Chicago Bulls",
    "Cleveland Cavaliers",
    "Dallas Mavericks",
    "Denver Nuggets",
    "Detroit Pistons",
    "Golden State Warriors",
    "Houston Rockets",
    "Indiana Pacers",
    "Los Angeles Clippers",
    "Los Angeles Lakers",
    "Memphis Grizzlies",
    "Miami Heat",
    "Milwaukee Bucks",
    "Minnesota Timberwolves",
    "New Orleans Pelicans",
    "New York Knicks",
    "Oklahoma City Thunder",
    "Orlando Magic",
    "Philadelphia 76ers",
    "Phoenix Suns",
    "Portland Trail Blazers",
    "Sacramento Kings",
    "San Antonio Spurs",
    "Toronto Raptors",
    "Utah Jazz",
    "Washington Wizards",
]

empty_stats = {"wins": 0, "losses": 0, "ties": 0, "Points For": 0, "Points Against": 0}

# SQLite: `home_{slug}` / `away_{slug}`; stats.nba `LeagueGameFinder` column on each team row.
GAME_TEAM_BOX_STATS: tuple[tuple[str, str], ...] = (
    ("ast", "AST"),
    ("oreb", "OREB"),
    ("dreb", "DREB"),
    ("stl", "STL"),
    ("blk", "BLK"),
    ("tov", "TOV"),
    ("pf", "PF"),
    ("plus_minus", "PLUS_MINUS"),
)


def _games_column_names(conn: sqlite3.Connection) -> set[str]:
    return {row[1] for row in conn.execute("PRAGMA table_info(games)")}


def init_games_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS games (
            id_event TEXT PRIMARY KEY,
            date_event TEXT,
            str_event TEXT,
            home_team TEXT NOT NULL,
            away_team TEXT NOT NULL,
            home_score INTEGER NOT NULL,
            away_score INTEGER NOT NULL
        )
        """
    )
    cols = _games_column_names(conn)
    if "api_date_event" in cols:
        conn.executescript(
            """
            CREATE TABLE games__no_api_col (
                id_event TEXT PRIMARY KEY,
                date_event TEXT,
                str_event TEXT,
                home_team TEXT NOT NULL,
                away_team TEXT NOT NULL,
                home_score INTEGER NOT NULL,
                away_score INTEGER NOT NULL
            );
            INSERT INTO games__no_api_col (
                id_event, date_event, str_event, home_team, away_team, home_score, away_score
            )
            SELECT id_event, date_event, str_event, home_team, away_team, home_score, away_score
            FROM games;
            DROP TABLE games;
            ALTER TABLE games__no_api_col RENAME TO games;
            """
        )
    cols = _games_column_names(conn)
    for slug, _ in GAME_TEAM_BOX_STATS:
        for prefix in ("home_", "away_"):
            col_name = f"{prefix}{slug}"
            if col_name not in cols:
                conn.execute(f"ALTER TABLE games ADD COLUMN {col_name} INTEGER")
    conn.commit()


def stable_game_id(game: dict, schedule_date: str) -> str:
    eid = game.get("idEvent")
    if eid not in (None, ""):
        return str(eid)
    home = game.get("strHomeTeam") or ""
    away = game.get("strAwayTeam") or ""
    return f"{schedule_date}|{home}|{away}"


# Full regular-season window (edit here). Used by `python pull_one_game.py`.
months = {
    "2025-10": ["21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31"],
    "2025-11": [
        "01", "02", "03", "04", "05", "06", "07", "08", "09", "10",
        "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
        "21", "22", "23", "24", "25", "26", "27", "28", "29", "30",
    ],
    "2025-12": [
        "01", "02", "03", "04", "05", "06", "07", "08", "09", "10",
        "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
        "21", "22", "23", "24", "25", "26", "27", "28", "29", "30",
        "31",
    ],
    "2026-01": [
        "01", "02", "03", "04", "05", "06", "07", "08", "09", "10",
        "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
        "21", "22", "23", "24", "25", "26", "27", "28", "29", "30",
        "31",
    ],
    "2026-02": [
        "01", "02", "03", "04", "05", "06", "07", "08", "09", "10",
        "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
        "21", "22", "23", "24", "25", "26", "27", "28",
    ],
    "2026-03": [
        "01", "02", "03", "04", "05", "06", "07", "08", "09", "10",
        "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
        "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31"],
    "2026-04": [
        "01", "02", "03", "04", "05", "06", "07", "08", "09", "10",
        "11", "12",
    ],
}


def run_pull(
    months_to_fetch: dict[str, list[str]],
    db_path: Path,
    *,
    print_team_summary: bool = True,
) -> tuple[int, int]:
    """Fetch day-by-day NBA results into SQLite and aggregate per-team stats.

    Returns (skipped_missing_scores, skipped_team_mismatch).
    """
    records: dict[str, dict[str, dict[str, int]]] = {}
    for team in teams:
        records[team] = {month_key: dict(empty_stats) for month_key in months_to_fetch}

    skipped_missing_scores = 0
    skipped_team_mismatch = 0
    conn = sqlite3.connect(db_path)
    init_games_table(conn)
    try:
        for month_key in months_to_fetch:
            for day in months_to_fetch[month_key]:
                date = f"{month_key}-{day}"

                params = urllib.parse.urlencode({"d": date, "l": NBA_ID})
                url = f"{BASE}/{API_KEY}/eventsday.php?{params}"
                req = urllib.request.Request(url, headers={"User-Agent": "NBAMModel/1.0"})
                with urllib.request.urlopen(req, timeout=30) as resp:
                    payload = json.load(resp)

                for game in payload.get("events") or []:
                    home_team = game.get("strHomeTeam")
                    away_team = game.get("strAwayTeam")
                    raw_home = game.get("intHomeScore")
                    raw_away = game.get("intAwayScore")
                    if raw_home is None or raw_away is None:
                        skipped_missing_scores += 1
                        continue
                    if home_team not in records or away_team not in records:
                        skipped_team_mismatch += 1
                        continue
                    home_score = int(raw_home)
                    away_score = int(raw_away)

                    # US "game night" when present; else API date; else the d= we queried (see note below).
                    calendar_ymd = (
                        game.get("dateEventLocal")
                        or game.get("dateEvent")
                        or date
                    )
                    gid = stable_game_id(game, calendar_ymd)
                    # date_event: prefer dateEventLocal so late CT games match NBA/ESPN (e.g. OKC@DAL Oct 27
                    # appears under eventsday d=2025-10-28 with dateEvent 2025-10-28 but dateEventLocal 2025-10-27).
                    # Missing games on some calendar days are an API coverage issue: eventsday often returns
                    # only a subset of games vs the real slate (demo/public feed), not skipped by this script.
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO games (
                            id_event, date_event, str_event,
                            home_team, away_team, home_score, away_score
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            gid,
                            calendar_ymd,
                            game.get("strEvent"),
                            home_team,
                            away_team,
                            home_score,
                            away_score,
                        ),
                    )

                    bucket = records[home_team][month_key]
                    bucket_away = records[away_team][month_key]

                    bucket["Points For"] += home_score
                    bucket_away["Points For"] += away_score

                    bucket["Points Against"] += away_score
                    bucket_away["Points Against"] += home_score

                    if home_score > away_score:
                        bucket["wins"] += 1
                        bucket_away["losses"] += 1
                    elif away_score > home_score:
                        bucket_away["wins"] += 1
                        bucket["losses"] += 1
                    else:
                        bucket["ties"] += 1
                        bucket_away["ties"] += 1

                conn.commit()
                time.sleep(SLEEP_AFTER_EACH_DAY_REQUEST)
    finally:
        conn.close()

    print(f"Database: {db_path.resolve()}")
    print(
        f"Ingest summary: skipped_missing_scores={skipped_missing_scores}, "
        f"skipped_team_mismatch={skipped_team_mismatch} "
        "(mismatch = API team name not exactly in your `teams` list)"
    )
    print("=" * 100)

    if print_team_summary:
        for team in teams:
            wins = losses = ties = points_for = points_against = 0
            for mk in months_to_fetch:
                s = records[team][mk]
                wins += s["wins"]
                losses += s["losses"]
                ties += s["ties"]
                points_for += s["Points For"]
                points_against += s["Points Against"]
            denom = wins + losses
            win_loss_ratio = wins / denom if denom else 0.0
            print(
                f"{team}: Wins: {wins}, Losses: {losses}, Ties: {ties}, "
                f"Win Loss Ratio: {win_loss_ratio}, Points For: {points_for}, Points Against: {points_against}"
            )
            print("-" * 100)

    return skipped_missing_scores, skipped_team_mismatch


if __name__ == "__main__":
    run_pull(months, DB_PATH)
