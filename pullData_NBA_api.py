#!/usr/bin/env python3
"""Date windows and multi-season ingest into one SQLite database.

- **`season` column** on each game: display id like `2020-2021`, `2024-2025` (set by `pull_nba_api`).

**Default __main__** writes **`nba_multiseason.sqlite3`** with regular seasons
**2020-21** through **2025-26** (six requests to stats.nba.com, one per year).

    python3 pull_nba_api_2425.py

Re-run to refresh: same DB path will upsert rows by `id_event` (GAME_ID).

For a **single** season into its own file, import a `months_*_window` and call
`run_nba_api_pull(...)` from `pull_nba_api` with your paths.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date, timedelta
from pathlib import Path

from pull_nba_api import run_nba_api_pull

# Default combined output (source for the viz with season selector).
DB_PATH = Path(__file__).resolve().parent / "nba_multiseason.sqlite3"

# (stats.nba.com `season` id, `season` column value, months factory)
SEASON_SPECS: list[tuple[str, str, Callable[[], dict[str, list[str]]]]] = [
    ("2020-21", "2020-2021", lambda: months_2020_21_window()),
    ("2021-22", "2021-2022", lambda: months_2021_22_window()),
    ("2022-23", "2022-2023", lambda: months_2022_23_window()),
    ("2023-24", "2023-2024", lambda: months_2023_24_window()),
    ("2024-25", "2024-2025", lambda: months_2024_25_window()),
    ("2025-26", "2025-2026", lambda: months_2025_26_window()),
]


def months_regular_season_window(start: date, end: date) -> dict[str, list[str]]:
    """Keys ``YYYY-MM``; values are two-digit day strings, for `allowed_game_dates`."""
    if end < start:
        raise ValueError(f"end {end} before start {start}")
    out: dict[str, list[str]] = {}
    d = start
    while d <= end:
        key = d.strftime("%Y-%m")
        out.setdefault(key, []).append(d.strftime("%d"))
        d += timedelta(days=1)
    return out


def months_2024_25_window() -> dict[str, list[str]]:
    """2024-25 RS: 2024-10-22 … 2025-04-13."""
    return months_regular_season_window(date(2024, 10, 22), date(2025, 4, 13))


def months_2023_24_window() -> dict[str, list[str]]:
    """2023-24 RS: 2023-10-24 … 2024-04-14."""
    return months_regular_season_window(date(2023, 10, 24), date(2024, 4, 14))


def months_2022_23_window() -> dict[str, list[str]]:
    """2022-23 RS: 2022-10-18 … 2023-04-09."""
    return months_regular_season_window(date(2022, 10, 18), date(2023, 4, 9))


def months_2021_22_window() -> dict[str, list[str]]:
    """2021-22 RS: 2021-10-19 … 2022-04-10."""
    return months_regular_season_window(date(2021, 10, 19), date(2022, 4, 10))


def months_2020_21_window() -> dict[str, list[str]]:
    """2020-21 RS (72 games): 2020-12-22 … 2021-05-16."""
    return months_regular_season_window(date(2020, 12, 22), date(2021, 5, 16))


def months_2025_26_window() -> dict[str, list[str]]:
    """2025-26 RS: aligned with `pull_one_game.months` (2025-10-21 … 2026-04-12)."""
    return months_regular_season_window(date(2025, 10, 21), date(2026, 4, 12))


def pull_all_seasons_to(
    out_db: Path,
    *,
    print_each_summary: bool = True,
) -> None:
    """Ingest every entry in `SEASON_SPECS` into `out_db` (one DB file, many seasons)."""
    for api, label, months_fn in SEASON_SPECS:
        print(f"\n>>> {label}  (api season={api})\n")
        run_nba_api_pull(
            months_fn(),
            out_db,
            season=api,
            season_id_label=label,
            print_season_summary=print_each_summary,
        )


if __name__ == "__main__":
    pull_all_seasons_to(DB_PATH, print_each_summary=True)
