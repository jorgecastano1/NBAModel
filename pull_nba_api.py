#!/usr/bin/env python3
"""Ingest NBA games from stats.nba.com using the unofficial `nba_api` package.

Compared to TheSportsDB `eventsday`:
- One `LeagueGameFinder` request loads the full regular season (then we filter by your `months` dates).
- Schedules/results come from the league’s own stats site, so slates are complete for that season.

`nba_api` is a community wrapper around public NBA endpoints; be polite (small delay, don’t hammer
parallel workers) so requests are not blocked.
"""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path

import pandas as pd
from nba_api.stats.endpoints import leaguegamefinder

from pull_one_game import (
    GAME_TEAM_BOX_STATS,
    init_games_table,
    months as SEASON_MONTHS,
    teams as NBA_TEAMS,
)

DB_PATH = Path(__file__).resolve().parent / "nba_schedule_nba_api.sqlite3"

# Season label as used by stats.nba.com (e.g. 2025-26).
SEASON = "2025-26"

# Single heavy request; still pause briefly to be nice to stats.nba.com.
REQUEST_PAUSE_SEC = 0.6

TEAM_SET = set(NBA_TEAMS)

# stats.nba.com uses this string; `pull_one_game.teams` uses "Los Angeles Clippers".
_STATS_TO_CANONICAL = {"LA Clippers": "Los Angeles Clippers"}


def _canonical_team_name(api_name: str) -> str:
    return _STATS_TO_CANONICAL.get(api_name, api_name)


def _int_from_series(row: pd.Series, col: str) -> int | None:
    v = row[col]
    if pd.isna(v):
        return None
    return int(float(v))


def allowed_game_dates(months_dict: dict[str, list[str]]) -> set[str]:
    return {f"{month_key}-{day}" for month_key, days in months_dict.items() for day in days}


def query_season_team_stats(conn: sqlite3.Connection) -> dict[str, dict[str, int]]:
    """Aggregate points for/against and wins/losses per team from `games` (one row per game)."""
    rows = conn.execute(
        """
        WITH per_team_game AS (
            SELECT
                home_team AS team,
                home_score AS pts_for,
                away_score AS pts_against,
                CASE WHEN home_score > away_score THEN 1 ELSE 0 END AS win,
                CASE WHEN home_score < away_score THEN 1 ELSE 0 END AS loss
            FROM games
            UNION ALL
            SELECT
                away_team,
                away_score,
                home_score,
                CASE WHEN away_score > home_score THEN 1 ELSE 0 END,
                CASE WHEN away_score < home_score THEN 1 ELSE 0 END
            FROM games
        )
        SELECT
            team,
            COUNT(*) AS games,
            SUM(win) AS wins,
            SUM(loss) AS losses,
            SUM(pts_for) AS pts_for,
            SUM(pts_against) AS pts_against
        FROM per_team_game
        GROUP BY team
        """
    ).fetchall()
    return {
        r[0]: {
            "games": int(r[1]),
            "wins": int(r[2]),
            "losses": int(r[3]),
            "pts_for": int(r[4]),
            "pts_against": int(r[5]),
        }
        for r in rows
    }


def print_season_team_summary(conn: sqlite3.Connection, *, team_order: list[str] | None = None) -> None:
    """Print season totals using stable team order (defaults to `NBA_TEAMS`)."""
    order = team_order if team_order is not None else NBA_TEAMS
    stats = query_season_team_stats(conn)
    print("=" * 100)
    print("Season totals (from games in this database)")
    print("=" * 100)
    for team in order:
        s = stats.get(team)
        if not s:
            print(f"{team}: (no rows)")
            print("-" * 100)
            continue
        w, l_, g = s["wins"], s["losses"], s["games"]
        # NBA-style: wins / games played (handles rare ties: win+loss may be < games)
        win_ratio = w / g if g else 0.0
        print(
            f"{team}: {w}-{l_} ({g} gp), win_ratio={win_ratio:.3f}, "
            f"PTS For={s['pts_for']}, PTS Against={s['pts_against']}"
        )
        print("-" * 100)


def _away_home_rows(grp: pd.DataFrame) -> tuple[pd.Series, pd.Series] | None:
    """Return (away_row, home_row) from the two team rows for one GAME_ID."""
    uniq = list(dict.fromkeys(grp["MATCHUP"].astype(str)))
    if len(uniq) == 2:
        away_m = next((u for u in uniq if " @ " in u), None)
        home_m = next((u for u in uniq if " vs." in u), None)
        if away_m and home_m:
            return grp.loc[grp["MATCHUP"] == away_m].iloc[0], grp.loc[grp["MATCHUP"] == home_m].iloc[0]
    # Neutral-site style: both rows share the same "AWAY @ HOME" string.
    m0 = uniq[0]
    if " @ " not in m0:
        return None
    away_a, home_a = m0.split(" @ ", 1)
    away_a, home_a = away_a.strip(), home_a.strip()
    try:
        return (
            grp.loc[grp["TEAM_ABBREVIATION"] == away_a].iloc[0],
            grp.loc[grp["TEAM_ABBREVIATION"] == home_a].iloc[0],
        )
    except (KeyError, IndexError):
        return None


def run_nba_api_pull(
    months_to_use: dict[str, list[str]],
    db_path: Path,
    *,
    season: str = SEASON,
    print_season_summary: bool = True,
) -> tuple[int, int]:
    """Fetch regular-season rows, keep games whose GAME_DATE is in months_to_use, write SQLite.

    After a successful ingest, optionally prints per-team PTS for/against, record, and win ratio
    (wins / games) from the `games` table.

    Returns (inserted_games, skipped_games).
    """
    allowed = allowed_game_dates(months_to_use)

    time.sleep(REQUEST_PAUSE_SEC)
    df = leaguegamefinder.LeagueGameFinder(
        season_nullable=season,
        league_id_nullable="00",
        season_type_nullable="Regular Season",
    ).get_data_frames()[0]

    df = df[df["GAME_DATE"].isin(allowed)].copy()

    conn = sqlite3.connect(db_path)
    init_games_table(conn)
    inserted = 0
    skipped = 0
    try:
        for game_id, grp in df.groupby("GAME_ID", sort=False):
            pair = _away_home_rows(grp)
            if pair is None:
                skipped += 1
                continue
            away_row, home_row = pair
            away = _canonical_team_name(str(away_row["TEAM_NAME"]))
            home = _canonical_team_name(str(home_row["TEAM_NAME"]))
            if away not in TEAM_SET or home not in TEAM_SET:
                skipped += 1
                continue
            away_score = int(away_row["PTS"])
            home_score = int(home_row["PTS"])
            game_date = str(away_row["GAME_DATE"])
            str_event = f"{away} @ {home}"

            box_cols: list[str] = []
            box_vals: list[int | None] = []
            for slug, nba_col in GAME_TEAM_BOX_STATS:
                box_cols.append(f"home_{slug}")
                box_cols.append(f"away_{slug}")
                box_vals.append(_int_from_series(home_row, nba_col))
                box_vals.append(_int_from_series(away_row, nba_col))

            base_cols = [
                "id_event",
                "date_event",
                "str_event",
                "home_team",
                "away_team",
                "home_score",
                "away_score",
            ]
            all_cols = base_cols + box_cols
            placeholders = ", ".join(["?"] * len(all_cols))
            sql = f"INSERT OR REPLACE INTO games ({', '.join(all_cols)}) VALUES ({placeholders})"
            conn.execute(
                sql,
                (
                    str(game_id),
                    game_date,
                    str_event,
                    home,
                    away,
                    home_score,
                    away_score,
                    *box_vals,
                ),
            )
            inserted += 1
        conn.commit()
        print(f"nba_api ingest: inserted={inserted}, skipped={skipped}, db={db_path.resolve()}")
        if print_season_summary:
            print_season_team_summary(conn)
    finally:
        conn.close()

    return inserted, skipped


if __name__ == "__main__":
    run_nba_api_pull(SEASON_MONTHS, DB_PATH)
