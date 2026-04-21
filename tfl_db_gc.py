#!/usr/bin/env python3
"""
Garbage collector for TFL Elo databases.

Deletes rows older than RETENTION_DAYS (default 7) from all time-series tables,
then runs VACUUM to reclaim disk space.

Usage:
    python tfl_db_gc.py                          # defaults: both DBs, 7 days
    python tfl_db_gc.py --days 3                  # keep only 3 days
    python tfl_db_gc.py --db tfl_train_event_elo.db  # single DB

Intended to be run via cron, e.g.:
    0 3 * * * cd /opt/tfl-elo && /opt/tfl-elo/.venv/bin/python tfl_db_gc.py >> /var/log/tfl_gc.log 2>&1
"""

import argparse
import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone

RETENTION_DAYS_DEFAULT = 7

# (db_file, table, timestamp_column)
CLEANUP_TARGETS = [
    ("tfl_train_event_elo.db", "train_predictions_raw", "snapshot_ts_utc"),
    ("tfl_train_event_elo.db", "train_stop_events", "expected_arrival_utc"),
    ("tfl_train_event_elo.db", "line_elo_snapshots", "snapshot_utc"),
    ("tfl_line_elo.db", "line_status_snapshots", "fetched_at_utc"),
]


def purge_old_rows(db_path: str, table: str, ts_col: str, cutoff_iso: str) -> int:
    """Delete rows older than cutoff. Returns number of rows deleted."""
    if not os.path.isfile(db_path):
        return 0
    con = sqlite3.connect(db_path, timeout=30)
    try:
        con.execute("PRAGMA busy_timeout=15000")
        cur = con.execute(
            f"DELETE FROM {table} WHERE {ts_col} < ?",  # noqa: S608
            (cutoff_iso,),
        )
        deleted = cur.rowcount
        con.commit()
        return deleted
    except sqlite3.OperationalError as exc:
        print(f"[gc] ERROR purging {db_path}:{table} — {exc}", file=sys.stderr)
        return 0
    finally:
        con.close()


def vacuum_db(db_path: str) -> None:
    """Reclaim disk space after deletes."""
    if not os.path.isfile(db_path):
        return
    try:
        con = sqlite3.connect(db_path, timeout=60)
        con.execute("VACUUM")
        con.close()
        print(f"[gc] VACUUM {db_path} OK")
    except sqlite3.OperationalError as exc:
        print(f"[gc] VACUUM {db_path} failed — {exc}", file=sys.stderr)


def run_gc(retention_days: int = RETENTION_DAYS_DEFAULT, db_filter: str | None = None) -> None:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=retention_days)).isoformat()
    print(f"[gc] {datetime.now(timezone.utc).isoformat()} — purging data older than {retention_days}d (cutoff {cutoff})")

    dbs_vacuumed: set[str] = set()
    for db_path, table, ts_col in CLEANUP_TARGETS:
        if db_filter and db_path != db_filter:
            continue
        deleted = purge_old_rows(db_path, table, ts_col, cutoff)
        if deleted:
            print(f"[gc] {db_path}:{table} — deleted {deleted} rows")
            dbs_vacuumed.add(db_path)
        else:
            print(f"[gc] {db_path}:{table} — nothing to purge")

    for db_path in dbs_vacuumed:
        vacuum_db(db_path)

    print("[gc] done")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TFL DB garbage collector")
    parser.add_argument("--days", type=int, default=RETENTION_DAYS_DEFAULT,
                        help=f"Retention period in days (default {RETENTION_DAYS_DEFAULT})")
    parser.add_argument("--db", type=str, default=None,
                        help="Only clean this specific DB file")
    args = parser.parse_args()
    run_gc(retention_days=args.days, db_filter=args.db)
