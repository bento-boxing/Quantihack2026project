import argparse
import csv
import json
import os
import sqlite3
import time
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlencode
from urllib.request import Request, urlopen


DB_PATH_DEFAULT = "tfl_line_elo.db"
BASE_URL = "https://api.tfl.gov.uk"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def init_db(db_path: str) -> None:
    con = sqlite3.connect(db_path)
    try:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS line_status_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fetched_at_utc TEXT NOT NULL,
                line_id TEXT NOT NULL,
                line_name TEXT NOT NULL,
                mode_name TEXT NOT NULL,
                status_severity INTEGER,
                status_description TEXT,
                reason TEXT,
                disruption_category TEXT,
                raw_json TEXT NOT NULL
            )
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_line_time
            ON line_status_snapshots(line_id, fetched_at_utc)
            """
        )
        con.commit()
    finally:
        con.close()


def build_url(path: str, app_id: Optional[str], app_key: Optional[str]) -> str:
    params = {}
    if app_id:
        params["app_id"] = app_id
    if app_key:
        params["app_key"] = app_key
    if params:
        return f"{BASE_URL}{path}?{urlencode(params)}"
    return f"{BASE_URL}{path}"


def http_get_json(url: str, timeout: int = 30) -> List[dict]:
    req = Request(url, headers={"User-Agent": "tfl-line-elo/1.0"})
    with urlopen(req, timeout=timeout) as resp:
        data = resp.read().decode("utf-8")
    return json.loads(data)


def fetch_line_statuses(mode: str, app_id: Optional[str], app_key: Optional[str]) -> List[dict]:
    path = f"/Line/Mode/{mode}/Status"
    url = build_url(path, app_id, app_key)
    payload = http_get_json(url)
    if not isinstance(payload, list):
        raise ValueError("Unexpected response payload from TfL API")
    return payload


def flatten_status_row(snapshot_ts: str, line_obj: dict) -> dict:
    line_statuses = line_obj.get("lineStatuses") or []
    status = line_statuses[0] if line_statuses else {}
    disruption = status.get("disruption") or {}
    return {
        "fetched_at_utc": snapshot_ts,
        "line_id": (line_obj.get("id") or "").strip(),
        "line_name": (line_obj.get("name") or "").strip(),
        "mode_name": (line_obj.get("modeName") or "").strip(),
        "status_severity": status.get("statusSeverity"),
        "status_description": (status.get("statusSeverityDescription") or "").strip(),
        "reason": (status.get("reason") or "").strip(),
        "disruption_category": (disruption.get("category") or "").strip(),
        "raw_json": json.dumps(line_obj, ensure_ascii=True),
    }


def save_snapshot_rows(db_path: str, rows: Iterable[dict]) -> int:
    con = sqlite3.connect(db_path)
    inserted = 0
    try:
        for row in rows:
            con.execute(
                """
                INSERT INTO line_status_snapshots (
                    fetched_at_utc,
                    line_id,
                    line_name,
                    mode_name,
                    status_severity,
                    status_description,
                    reason,
                    disruption_category,
                    raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["fetched_at_utc"],
                    row["line_id"],
                    row["line_name"],
                    row["mode_name"],
                    row["status_severity"],
                    row["status_description"],
                    row["reason"],
                    row["disruption_category"],
                    row["raw_json"],
                ),
            )
            inserted += 1
        con.commit()
    finally:
        con.close()
    return inserted


def run_collect_once(db_path: str, mode: str, app_id: Optional[str], app_key: Optional[str]) -> None:
    statuses = fetch_line_statuses(mode=mode, app_id=app_id, app_key=app_key)
    fetched_at = utc_now_iso()
    rows = [flatten_status_row(fetched_at, line_obj) for line_obj in statuses]
    inserted = save_snapshot_rows(db_path, rows)
    print(f"[{fetched_at}] Saved {inserted} {mode} line status rows.")


def run_collect_loop(
    db_path: str,
    mode: str,
    app_id: Optional[str],
    app_key: Optional[str],
    interval_seconds: int,
    max_iterations: int,
) -> None:
    iteration = 0
    while True:
        iteration += 1
        run_collect_once(db_path=db_path, mode=mode, app_id=app_id, app_key=app_key)
        if max_iterations > 0 and iteration >= max_iterations:
            break
        time.sleep(interval_seconds)


def parse_iso_to_dt(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


def quality_from_severity(status_severity: Optional[int]) -> float:
    if status_severity is None:
        return 0.4
    clamped = max(0, min(10, int(status_severity)))
    return clamped / 10.0


def calculate_elo_for_line(events: List[dict], base_elo: float, k_factor: float) -> Tuple[float, Dict[str, int], float]:
    elo = base_elo
    good_streak = 0
    stats = {
        "snapshots": 0,
        "bad_snapshots": 0,
        "status_changes": 0,
        "cancellation_mentions": 0,
    }
    prev_severity = None

    for event in events:
        stats["snapshots"] += 1
        severity = event["status_severity"]
        reason = (event["reason"] or "").lower()
        quality = quality_from_severity(severity)

        expected_quality = 1.0 / (1.0 + pow(10.0, (base_elo - elo) / 400.0))
        elo += k_factor * (quality - expected_quality)

        if severity is not None and severity < 10:
            stats["bad_snapshots"] += 1
            good_streak = 0
            elo -= 0.6
        else:
            good_streak += 1
            elo += min(2.0, 0.1 * good_streak)

        if "cancel" in reason:
            stats["cancellation_mentions"] += 1
            elo -= 8.0

        if prev_severity is not None and severity != prev_severity:
            stats["status_changes"] += 1
            if severity is not None and prev_severity is not None:
                if severity > prev_severity:
                    elo += 3.0
                elif severity < prev_severity:
                    elo -= 3.0

        prev_severity = severity

    good_ratio = 0.0
    if stats["snapshots"] > 0:
        good_ratio = (stats["snapshots"] - stats["bad_snapshots"]) / stats["snapshots"]

    return elo, stats, good_ratio


def fetch_snapshots(db_path: str, mode: str, lookback_hours: int) -> List[sqlite3.Row]:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        if lookback_hours > 0:
            cutoff = datetime.now(timezone.utc).timestamp() - (lookback_hours * 3600)
            cutoff_iso = datetime.fromtimestamp(cutoff, tz=timezone.utc).isoformat()
            rows = con.execute(
                """
                SELECT fetched_at_utc, line_id, line_name, mode_name, status_severity,
                       status_description, reason, disruption_category
                FROM line_status_snapshots
                WHERE mode_name = ?
                  AND fetched_at_utc >= ?
                ORDER BY fetched_at_utc ASC
                """,
                (mode, cutoff_iso),
            ).fetchall()
        else:
            rows = con.execute(
                """
                SELECT fetched_at_utc, line_id, line_name, mode_name, status_severity,
                       status_description, reason, disruption_category
                FROM line_status_snapshots
                WHERE mode_name = ?
                ORDER BY fetched_at_utc ASC
                """,
                (mode,),
            ).fetchall()
        return rows
    finally:
        con.close()


def run_rankings(
    db_path: str,
    mode: str,
    lookback_hours: int,
    base_elo: float,
    k_factor: float,
    out_csv: str,
) -> None:
    rows = fetch_snapshots(db_path=db_path, mode=mode, lookback_hours=lookback_hours)
    if not rows:
        print("No snapshots found. Run collect first.")
        return

    grouped: Dict[str, List[dict]] = {}
    names: Dict[str, str] = {}
    for row in rows:
        line_id = row["line_id"]
        names[line_id] = row["line_name"]
        grouped.setdefault(line_id, []).append(
            {
                "fetched_at_utc": row["fetched_at_utc"],
                "status_severity": row["status_severity"],
                "status_description": row["status_description"],
                "reason": row["reason"],
            }
        )

    leaderboard = []
    for line_id, events in grouped.items():
        elo, stats, good_ratio = calculate_elo_for_line(
            events=events,
            base_elo=base_elo,
            k_factor=k_factor,
        )
        latest = events[-1]
        leaderboard.append(
            {
                "line_id": line_id,
                "line_name": names.get(line_id, line_id),
                "elo": round(elo, 2),
                "snapshots": stats["snapshots"],
                "bad_snapshots": stats["bad_snapshots"],
                "status_changes": stats["status_changes"],
                "cancellation_mentions": stats["cancellation_mentions"],
                "good_ratio": round(good_ratio, 4),
                "latest_status": latest.get("status_description") or "",
                "latest_timestamp": latest.get("fetched_at_utc") or "",
            }
        )

    leaderboard.sort(key=lambda x: x["elo"], reverse=True)

    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "line_id",
                "line_name",
                "elo",
                "snapshots",
                "bad_snapshots",
                "status_changes",
                "cancellation_mentions",
                "good_ratio",
                "latest_status",
                "latest_timestamp",
            ],
        )
        writer.writeheader()
        writer.writerows(leaderboard)

    print("Top line Elo rankings")
    print("=" * 72)
    for idx, row in enumerate(leaderboard, start=1):
        print(
            f"{idx:2d}. {row['line_name']:<20} Elo={row['elo']:>7} "
            f"good_ratio={row['good_ratio']:.2%} latest='{row['latest_status']}'"
        )
    print("=" * 72)
    print(f"Wrote leaderboard CSV: {out_csv}")


def get_cli_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TfL line reliability Elo tracker")
    parser.add_argument("--db", default=DB_PATH_DEFAULT, help="SQLite DB path")
    parser.add_argument("--mode", default="tube", help="TfL mode to monitor, e.g. tube")
    parser.add_argument("--app-id", default=os.getenv("TFL_APP_ID"), help="TfL app_id")
    parser.add_argument("--app-key", default=os.getenv("TFL_APP_KEY"), help="TfL app_key")

    sub = parser.add_subparsers(dest="command", required=True)

    collect = sub.add_parser("collect", help="Collect a single snapshot or run loop")
    collect.add_argument("--interval", type=int, default=60, help="Loop interval seconds")
    collect.add_argument(
        "--iterations",
        type=int,
        default=1,
        help="Number of collection iterations. Use 0 for infinite loop.",
    )

    rank = sub.add_parser("rank", help="Compute Elo leaderboard from snapshots")
    rank.add_argument("--lookback-hours", type=int, default=0, help="0 means full history")
    rank.add_argument("--base-elo", type=float, default=1500.0, help="Starting Elo")
    rank.add_argument("--k-factor", type=float, default=24.0, help="Elo K factor")
    rank.add_argument(
        "--out-csv",
        default="tfl_line_elo_leaderboard.csv",
        help="Output leaderboard CSV path",
    )

    return parser.parse_args()


def main() -> None:
    args = get_cli_args()
    init_db(args.db)

    if args.command == "collect":
        run_collect_loop(
            db_path=args.db,
            mode=args.mode,
            app_id=args.app_id,
            app_key=args.app_key,
            interval_seconds=args.interval,
            max_iterations=args.iterations,
        )
    elif args.command == "rank":
        run_rankings(
            db_path=args.db,
            mode=args.mode,
            lookback_hours=args.lookback_hours,
            base_elo=args.base_elo,
            k_factor=args.k_factor,
            out_csv=args.out_csv,
        )
    else:
        raise ValueError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    main()
