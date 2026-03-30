import argparse
import csv
import hashlib
import json
import math
import os
import sqlite3
import time
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Set
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from zoneinfo import ZoneInfo


BASE_URL = "https://api.tfl.gov.uk"
DB_PATH_DEFAULT = "tfl_train_event_elo.db"
LONDON_TZ = ZoneInfo("Europe/London")

BASE_ELO = 1200.0
REVERSION_CENTER_ELO = 1500.0
MIN_ELO = 1.0
MAX_ELO = 3500.0
LOW_EDGE_START = 800.0
HIGH_EDGE_START = 2500.0
LATE_GRACE_SECONDS_DEFAULT = 0
LATE_FULL_LOSS_SECONDS = 900
ARRIVAL_DETECTED_SECONDS = 180
CANCELLATION_GRACE_SECONDS_DEFAULT = 900
CANCELLATION_MIN_SIGHTINGS = 3

ALL_TUBE_LINES = [
    ("bakerloo", "Bakerloo"),
    ("central", "Central"),
    ("circle", "Circle"),
    ("district", "District"),
    ("hammersmith-city", "Hammersmith & City"),
    ("jubilee", "Jubilee"),
    ("metropolitan", "Metropolitan"),
    ("northern", "Northern"),
    ("piccadilly", "Piccadilly"),
    ("victoria", "Victoria"),
    ("waterloo-city", "Waterloo & City"),
]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_url(path: str, app_id: Optional[str], app_key: Optional[str]) -> str:
    params = {}
    if app_id:
        params["app_id"] = app_id
    if app_key:
        params["app_key"] = app_key
    if params:
        return f"{BASE_URL}{path}?{urlencode(params)}"
    return f"{BASE_URL}{path}"


def http_get_json(url: str, timeout: int = 30):
    req = Request(url, headers={"User-Agent": "tfl-train-event-elo/1.0"})
    last_err: Optional[Exception] = None
    for attempt in range(4):
        try:
            with urlopen(req, timeout=timeout) as resp:
                data = resp.read().decode("utf-8")
            return json.loads(data)
        except HTTPError as exc:
            last_err = exc
            if exc.code == 429:
                time.sleep(1.5 * (attempt + 1))
                continue
            raise
        except URLError as exc:
            last_err = exc
            time.sleep(0.8 * (attempt + 1))
            continue

    if last_err:
        raise last_err
    raise RuntimeError("Unexpected HTTP fetch failure")


def init_db(db_path: str) -> None:
    con = sqlite3.connect(db_path, timeout=20)
    try:
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA synchronous=NORMAL")
        con.execute("PRAGMA busy_timeout=10000")
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS train_predictions_raw (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_ts_utc TEXT NOT NULL,
                event_id TEXT NOT NULL,
                prediction_id TEXT,
                mode_name TEXT,
                line_id TEXT,
                line_name TEXT,
                vehicle_id TEXT,
                stop_id TEXT,
                station_name TEXT,
                platform_name TEXT,
                direction TEXT,
                destination_name TEXT,
                expected_arrival_utc TEXT,
                time_to_station INTEGER,
                current_location TEXT,
                raw_json TEXT NOT NULL
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS train_stop_events (
                event_id TEXT PRIMARY KEY,
                mode_name TEXT,
                line_id TEXT,
                line_name TEXT,
                vehicle_id TEXT,
                stop_id TEXT,
                station_name TEXT,
                platform_name TEXT,
                direction TEXT,
                destination_name TEXT,
                expected_arrival_utc TEXT,
                first_seen_utc TEXT,
                last_seen_utc TEXT,
                sightings INTEGER DEFAULT 0,
                min_time_to_station INTEGER,
                max_time_to_station INTEGER
            )
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_raw_snapshot
            ON train_predictions_raw(snapshot_ts_utc)
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_raw_event_snapshot
            ON train_predictions_raw(event_id, snapshot_ts_utc, id)
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_event_line
            ON train_stop_events(line_id, expected_arrival_utc)
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS line_elo_seed (
                line_id TEXT PRIMARY KEY,
                line_name TEXT NOT NULL,
                seed_elo REAL NOT NULL,
                source TEXT NOT NULL,
                updated_utc TEXT NOT NULL
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS line_elo_snapshots (
                snapshot_utc TEXT NOT NULL,
                line_id TEXT NOT NULL,
                line_name TEXT NOT NULL,
                elo INTEGER NOT NULL,
                on_time_arrivals INTEGER NOT NULL,
                late_arrivals INTEGER NOT NULL,
                cancelled_trains INTEGER NOT NULL,
                PRIMARY KEY (snapshot_utc, line_id)
            )
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_elo_snapshots_line_time
            ON line_elo_snapshots(line_id, snapshot_utc)
            """
        )
        con.commit()
    finally:
        con.close()


def hash_event_id(
    mode_name: str,
    line_id: str,
    vehicle_id: str,
    stop_id: str,
    direction: str,
    destination_name: str,
    expected_arrival_utc: str,
) -> str:
    key = "|".join(
        [
            mode_name or "",
            line_id or "",
            vehicle_id or "",
            stop_id or "",
            direction or "",
            destination_name or "",
            expected_arrival_utc or "",
        ]
    )
    return hashlib.sha1(key.encode("utf-8")).hexdigest()


def fetch_lines(mode: str, app_id: Optional[str], app_key: Optional[str]) -> List[dict]:
    url = build_url(f"/Line/Mode/{mode}", app_id, app_key)
    payload = http_get_json(url)
    if not isinstance(payload, list):
        return []
    return payload


def fetch_line_stop_ids(line_id: str, app_id: Optional[str], app_key: Optional[str]) -> Set[str]:
    url = build_url(f"/Line/{line_id}/StopPoints", app_id, app_key)
    payload = http_get_json(url)
    stop_ids: Set[str] = set()
    if isinstance(payload, list):
        for item in payload:
            sid = (item.get("naptanId") or item.get("id") or "").strip()
            if sid:
                stop_ids.add(sid)
    return stop_ids


def evenly_sample_stops(stops: List[str], sample_size: int) -> List[str]:
    if sample_size <= 0 or sample_size >= len(stops):
        return stops
    if sample_size == 1:
        return [stops[len(stops) // 2]]

    picked: List[str] = []
    last_idx = len(stops) - 1
    for i in range(sample_size):
        idx = round(i * last_idx / (sample_size - 1))
        picked.append(stops[idx])
    return picked


def rotating_sample_stops(stops: List[str], sample_size: int, cycle_index: int) -> List[str]:
    if sample_size <= 0 or sample_size >= len(stops):
        return stops
    if not stops:
        return stops

    offset = cycle_index % len(stops)
    rotated = stops[offset:] + stops[:offset]
    return evenly_sample_stops(rotated, sample_size)


def fetch_stop_arrivals(stop_id: str, app_id: Optional[str], app_key: Optional[str]) -> List[dict]:
    url = build_url(f"/StopPoint/{stop_id}/Arrivals", app_id, app_key)
    payload = http_get_json(url)
    if not isinstance(payload, list):
        return []
    return payload


def upsert_event(con: sqlite3.Connection, row: Dict[str, object]) -> None:
    con.execute(
        """
        INSERT INTO train_stop_events (
            event_id, mode_name, line_id, line_name, vehicle_id, stop_id,
            station_name, platform_name, direction, destination_name,
            expected_arrival_utc, first_seen_utc, last_seen_utc,
            sightings, min_time_to_station, max_time_to_station
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(event_id) DO UPDATE SET
            last_seen_utc=excluded.last_seen_utc,
            sightings=train_stop_events.sightings + 1,
            min_time_to_station=CASE
                WHEN excluded.min_time_to_station IS NULL THEN train_stop_events.min_time_to_station
                WHEN train_stop_events.min_time_to_station IS NULL THEN excluded.min_time_to_station
                WHEN excluded.min_time_to_station < train_stop_events.min_time_to_station THEN excluded.min_time_to_station
                ELSE train_stop_events.min_time_to_station
            END,
            max_time_to_station=CASE
                WHEN excluded.max_time_to_station IS NULL THEN train_stop_events.max_time_to_station
                WHEN train_stop_events.max_time_to_station IS NULL THEN excluded.max_time_to_station
                WHEN excluded.max_time_to_station > train_stop_events.max_time_to_station THEN excluded.max_time_to_station
                ELSE train_stop_events.max_time_to_station
            END
        """,
        (
            row["event_id"],
            row["mode_name"],
            row["line_id"],
            row["line_name"],
            row["vehicle_id"],
            row["stop_id"],
            row["station_name"],
            row["platform_name"],
            row["direction"],
            row["destination_name"],
            row["expected_arrival_utc"],
            row["snapshot_ts_utc"],
            row["snapshot_ts_utc"],
            1,
            row["time_to_station"],
            row["time_to_station"],
        ),
    )


def collect_once(
    db_path: str,
    mode: str,
    line_ids: List[str],
    max_stops_per_line: int,
    cycle_index: int,
    app_id: Optional[str],
    app_key: Optional[str],
) -> None:
    snapshot_ts = utc_now_iso()

    if not line_ids:
        lines = fetch_lines(mode=mode, app_id=app_id, app_key=app_key)
        line_ids = [str(x.get("id")) for x in lines if x.get("id")]

    target_stops: Set[str] = set()
    for line_id in line_ids:
        try:
            stops = sorted(fetch_line_stop_ids(line_id=line_id, app_id=app_id, app_key=app_key))
        except Exception:
            continue
        if max_stops_per_line > 0:
            stops = rotating_sample_stops(stops, max_stops_per_line, cycle_index)
        target_stops.update(stops)

    con = sqlite3.connect(db_path, timeout=20)
    con.execute("PRAGMA busy_timeout=10000")
    raw_rows = 0
    unique_events = 0
    try:
        for stop_id in sorted(target_stops):
            try:
                arrivals = fetch_stop_arrivals(stop_id=stop_id, app_id=app_id, app_key=app_key)
            except Exception:
                continue

            for p in arrivals:
                if p.get("modeName") != mode:
                    continue
                if line_ids and p.get("lineId") not in line_ids:
                    continue

                row = {
                    "snapshot_ts_utc": snapshot_ts,
                    "prediction_id": str(p.get("id") or ""),
                    "mode_name": str(p.get("modeName") or ""),
                    "line_id": str(p.get("lineId") or ""),
                    "line_name": str(p.get("lineName") or ""),
                    "vehicle_id": str(p.get("vehicleId") or ""),
                    "stop_id": str(p.get("naptanId") or stop_id),
                    "station_name": str(p.get("stationName") or ""),
                    "platform_name": str(p.get("platformName") or ""),
                    "direction": str(p.get("direction") or ""),
                    "destination_name": str(p.get("destinationName") or ""),
                    "expected_arrival_utc": str(p.get("expectedArrival") or ""),
                    "time_to_station": p.get("timeToStation"),
                    "current_location": str(p.get("currentLocation") or ""),
                    "raw_json": json.dumps(p, ensure_ascii=True),
                }
                row["event_id"] = hash_event_id(
                    mode_name=row["mode_name"],
                    line_id=row["line_id"],
                    vehicle_id=row["vehicle_id"],
                    stop_id=row["stop_id"],
                    direction=row["direction"],
                    destination_name=row["destination_name"],
                    expected_arrival_utc=row["expected_arrival_utc"],
                )

                con.execute(
                    """
                    INSERT INTO train_predictions_raw (
                        snapshot_ts_utc, event_id, prediction_id, mode_name, line_id,
                        line_name, vehicle_id, stop_id, station_name, platform_name,
                        direction, destination_name, expected_arrival_utc,
                        time_to_station, current_location, raw_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row["snapshot_ts_utc"],
                        row["event_id"],
                        row["prediction_id"],
                        row["mode_name"],
                        row["line_id"],
                        row["line_name"],
                        row["vehicle_id"],
                        row["stop_id"],
                        row["station_name"],
                        row["platform_name"],
                        row["direction"],
                        row["destination_name"],
                        row["expected_arrival_utc"],
                        row["time_to_station"],
                        row["current_location"],
                        row["raw_json"],
                    ),
                )
                raw_rows += 1

                before = con.total_changes
                upsert_event(con, row)
                after = con.total_changes
                if after > before:
                    unique_events += 1

        con.commit()
    finally:
        con.close()

    print(
        f"[{snapshot_ts}] mode={mode} lines={len(line_ids)} stops={len(target_stops)} "
        f"raw_rows={raw_rows} event_upserts={unique_events}"
    )


def run_collect_loop(
    db_path: str,
    mode: str,
    line_ids: List[str],
    max_stops_per_line: int,
    interval_seconds: int,
    iterations: int,
    app_id: Optional[str],
    app_key: Optional[str],
) -> None:
    i = 0
    while True:
        i += 1
        collect_once(
            db_path=db_path,
            mode=mode,
            line_ids=line_ids,
            max_stops_per_line=max_stops_per_line,
            cycle_index=i,
            app_id=app_id,
            app_key=app_key,
        )
        if iterations > 0 and i >= iterations:
            break
        time.sleep(interval_seconds)


def is_peak_london_time(expected_arrival_utc: str) -> bool:
    if not expected_arrival_utc:
        return False
    try:
        dt_utc = datetime.fromisoformat(expected_arrival_utc.replace("Z", "+00:00"))
    except Exception:
        return False

    dt_local = dt_utc.astimezone(LONDON_TZ)
    if dt_local.weekday() >= 5:
        return False

    minutes = dt_local.hour * 60 + dt_local.minute
    morning_peak = 7 * 60 <= minutes <= 10 * 60
    evening_peak = 16 * 60 <= minutes <= 19 * 60
    return morning_peak or evening_peak


def compute_event_delta(elo: float, outcome: str, peak: bool, lateness_seconds: float = 0.0) -> float:
    peak_mult = 1.65 if peak else 1.0
    loss_scale = max(0.15, min(1.15, (elo - 650.0) / 700.0))

    if outcome == "arrived":
        gain_scale = max(0.15, min(1.0, (1700.0 - elo) / 700.0))
        return 4.8 * peak_mult * gain_scale

    if outcome == "late":
        lateness_ratio = max(0.0, min(1.0, float(lateness_seconds) / float(LATE_FULL_LOSS_SECONDS)))
        return -6.0 * peak_mult * loss_scale * lateness_ratio

    return -6.0 * peak_mult * loss_scale


def apply_boundary_friction(current_elo: float, raw_delta: float) -> float:
    if raw_delta > 0:
        if current_elo <= HIGH_EDGE_START:
            return raw_delta

        above = current_elo - HIGH_EDGE_START
        drag = 1.0 + 0.0014 * (above ** 1.30)
        if current_elo > 2800.0:
            drag += 0.9 * math.exp((current_elo - 2800.0) / 160.0)
        if current_elo > 3000.0:
            drag += 1.6 * math.exp((current_elo - 3000.0) / 130.0)
        if current_elo > 3200.0:
            drag += 3.0 * math.exp((current_elo - 3200.0) / 95.0)
        if current_elo > 3400.0:
            drag += 5.0 * math.exp((current_elo - 3400.0) / 75.0)
        return raw_delta / drag

    if raw_delta < 0:
        if current_elo >= LOW_EDGE_START:
            return raw_delta

        below = LOW_EDGE_START - current_elo
        drag = 1.0 + 0.0016 * (below ** 1.30)
        if current_elo < 700.0:
            drag += 0.9 * math.exp((700.0 - current_elo) / 160.0)
        if current_elo < 600.0:
            drag += 1.8 * math.exp((600.0 - current_elo) / 130.0)
        if current_elo < 500.0:
            drag += 3.2 * math.exp((500.0 - current_elo) / 95.0)
        if current_elo < 400.0:
            drag += 5.0 * math.exp((400.0 - current_elo) / 75.0)
        return raw_delta / drag

    return 0.0


def compute_line_elo_update(base_elo: float, row: Dict[str, float | int | str]) -> float:
    w_arr = float(row["weighted_arrivals"])
    w_late = float(row["weighted_lates"])
    w_can = float(row["weighted_cancellations"])
    resolved_w = w_arr + w_late + w_can
    if resolved_w <= 0.0:
        return base_elo

    on_time_ratio = w_arr / resolved_w
    late_ratio = w_late / resolved_w
    cancel_ratio = w_can / resolved_w

    late_avg_minutes = float(row["late_minutes_total"]) / max(1.0, float(row["late_events"]))
    late_saturation = 1.0 - math.exp(-max(0.0, late_avg_minutes) / 5.0)

    peak_w = float(row["weighted_peak_events"])
    peak_fail_w = float(row["weighted_peak_failures"])
    peak_fail_ratio = peak_fail_w / max(1.0, peak_w)

    peak_presence = min(1.0, peak_w / 45.0)

    late_penalty = 0.40 * late_ratio * (0.35 + 0.65 * late_saturation)
    cancel_ratio_penalty = 3.10 * cancel_ratio
    peak_penalty = 0.42 * peak_fail_ratio * peak_presence
    cancel_shock = 0.44 * (1.0 - math.exp(-float(row["missed_events"]) / 6.0))
    late_shock = 0.04 * (1.0 - math.exp(-float(row["late_events"]) / 30.0))
    late_count_pressure = 0.013 * math.log1p(float(row["late_events"]))
    cancel_count_pressure = 0.060 * math.log1p(float(row["missed_events"]))
    on_time_reward = 0.30 * on_time_ratio

    performance = (
        on_time_ratio
        + on_time_reward
        + 0.28
        - late_penalty
        - cancel_ratio_penalty
        - peak_penalty
        - cancel_shock
        - late_shock
        - late_count_pressure
        - cancel_count_pressure
    )
    qty_bonus = 0.002 * math.log1p(resolved_w)
    raw_delta = 820.0 * ((performance - 0.56) + qty_bonus)

    confidence = min(1.0, math.sqrt(resolved_w) / 14.0)
    raw_delta *= confidence

    reversion_confidence = min(1.0, math.sqrt(resolved_w) / 28.0)
    reversion_strength = 0.18 * ((1.0 - reversion_confidence) ** 2.2)
    raw_delta += (REVERSION_CENTER_ELO - base_elo) * reversion_strength

    next_elo = base_elo + apply_boundary_friction(base_elo, raw_delta)
    next_elo = max(MIN_ELO, next_elo)
    next_elo = min(MAX_ELO, next_elo)
    return next_elo


def adaptive_base_elo(seed_elo: float, row: Dict[str, float | int | str]) -> float:
    resolved_w = float(row["weighted_arrivals"]) + float(row["weighted_lates"]) + float(row["weighted_cancellations"])
    if resolved_w <= 0.0:
        return seed_elo

    seed_weight = min(0.35, 40.0 / (40.0 + resolved_w))
    return (BASE_ELO * (1.0 - seed_weight)) + (seed_elo * seed_weight)


def load_seed_map(db_path: str) -> Dict[str, Dict[str, float | str]]:
    con = sqlite3.connect(db_path, timeout=20)
    con.execute("PRAGMA busy_timeout=10000")
    con.row_factory = sqlite3.Row
    try:
        rows = con.execute(
            """
            SELECT line_id, line_name, seed_elo
            FROM line_elo_seed
            """
        ).fetchall()
    finally:
        con.close()

    result: Dict[str, Dict[str, float | str]] = {}
    for r in rows:
        result[str(r["line_id"])] = {
            "line_name": str(r["line_name"]),
            "seed_elo": float(r["seed_elo"]),
        }
    return result


def reseed_ratings_from_history(
    db_path: str,
    mode: str,
    spread_min: float,
    spread_max: float,
    prior_strength: float,
    prior_success: float,
    late_grace_seconds: int,
    reset_events: bool,
) -> None:
    con = sqlite3.connect(db_path, timeout=20)
    con.execute("PRAGMA busy_timeout=10000")
    con.row_factory = sqlite3.Row
    try:
        now_ts = datetime.now(timezone.utc).timestamp()
        rows = con.execute(
            """
            SELECT
                line_id,
                COALESCE(MAX(line_name), line_id) AS line_name,
                SUM(CASE WHEN min_time_to_station IS NOT NULL AND min_time_to_station <= ? THEN 1 ELSE 0 END) AS arrived_events,
                SUM(CASE WHEN min_time_to_station IS NOT NULL
                          AND min_time_to_station <= ?
                          AND expected_arrival_utc IS NOT NULL
                          AND strftime('%s', replace(last_seen_utc, 'Z', '+00:00'))
                              > strftime('%s', replace(expected_arrival_utc, 'Z', '+00:00')) + ?
                         THEN 1 ELSE 0 END) AS late_events,
                SUM(CASE WHEN min_time_to_station IS NOT NULL
                          AND min_time_to_station <= ?
                          AND expected_arrival_utc IS NOT NULL
                          THEN
                              CASE
                                  WHEN strftime('%s', replace(last_seen_utc, 'Z', '+00:00'))
                                       <= strftime('%s', replace(expected_arrival_utc, 'Z', '+00:00')) + ?
                                  THEN 0.0
                                  WHEN strftime('%s', replace(last_seen_utc, 'Z', '+00:00'))
                                       >= strftime('%s', replace(expected_arrival_utc, 'Z', '+00:00')) + ? + ?
                                  THEN 1.0
                                  ELSE
                                       (strftime('%s', replace(last_seen_utc, 'Z', '+00:00'))
                                       - (strftime('%s', replace(expected_arrival_utc, 'Z', '+00:00')) + ?))
                                       * 1.0 / ?
                              END
                          ELSE 0.0 END) AS late_loss_equiv,
                SUM(CASE WHEN expected_arrival_utc IS NOT NULL
                          AND strftime('%s', replace(expected_arrival_utc, 'Z', '+00:00')) < ?
                          AND (min_time_to_station IS NULL OR min_time_to_station > ?)
                          AND sightings >= ?
                         THEN 1 ELSE 0 END) AS missed_events
            FROM train_stop_events
            WHERE mode_name = ?
            GROUP BY line_id
            """,
            (
                ARRIVAL_DETECTED_SECONDS,
                ARRIVAL_DETECTED_SECONDS,
                late_grace_seconds,
                ARRIVAL_DETECTED_SECONDS,
                late_grace_seconds,
                late_grace_seconds,
                LATE_FULL_LOSS_SECONDS,
                late_grace_seconds,
                LATE_FULL_LOSS_SECONDS,
                now_ts - CANCELLATION_GRACE_SECONDS_DEFAULT,
                ARRIVAL_DETECTED_SECONDS,
                CANCELLATION_MIN_SIGHTINGS,
                mode,
            ),
        ).fetchall()

        by_line: Dict[str, Dict[str, float | str]] = {
            str(r["line_id"]): {
                "line_name": str(r["line_name"]),
                "arrived": float(r["arrived_events"] or 0),
                "late": float(r["late_events"] or 0),
                "late_loss_equiv": float(r["late_loss_equiv"] or 0.0),
                "missed": float(r["missed_events"] or 0),
            }
            for r in rows
        }

        for line_id, line_name in ALL_TUBE_LINES:
            by_line.setdefault(
                line_id,
                {"line_name": line_name, "arrived": 0.0, "late": 0.0, "late_loss_equiv": 0.0, "missed": 0.0},
            )

        scored = []
        for line_id, entry in by_line.items():
            arrived = float(entry["arrived"])
            late = float(entry["late"])
            late_loss_equiv = float(entry["late_loss_equiv"])
            missed = float(entry["missed"])
            n = arrived + late + missed
            effective_success = max(0.0, arrived - late_loss_equiv)
            p = (effective_success + (prior_strength * prior_success)) / (n + prior_strength)
            confidence = min(1.0, n / 400.0)
            score = p + 0.08 * confidence + 0.015 * math.log1p(n)
            scored.append((line_id, str(entry["line_name"]), score, n, arrived, late, late_loss_equiv, missed))

        scored.sort(key=lambda x: x[2], reverse=True)
        total = len(scored)

        seeds = []
        for idx, (line_id, line_name, _score, n, arrived, late, late_loss_equiv, missed) in enumerate(scored):
            if total <= 1:
                elo = (spread_min + spread_max) / 2.0
            else:
                t = idx / (total - 1)
                elo = spread_max - t * (spread_max - spread_min)
            seeds.append((line_id, line_name, int(round(elo)), n, arrived, late, late_loss_equiv, missed))

        con.execute("DELETE FROM line_elo_seed")
        stamp = utc_now_iso()
        for line_id, line_name, elo, _n, _a, _l, _le, _m in seeds:
            con.execute(
                """
                INSERT INTO line_elo_seed (line_id, line_name, seed_elo, source, updated_utc)
                VALUES (?, ?, ?, ?, ?)
                """,
                (line_id, line_name, elo, "historical_event_agglomeration", stamp),
            )

        if reset_events:
            con.execute("DELETE FROM train_predictions_raw")
            con.execute("DELETE FROM train_stop_events")

        con.commit()
    finally:
        con.close()

    print("Seed ratings generated")
    print("=" * 72)
    for idx, (line_id, line_name, elo, n, arrived, late, late_loss_equiv, missed) in enumerate(seeds, start=1):
        print(
            f"{idx:2d}. {line_name:<20} seed={int(elo):>7d} "
            f"n={int(n):>5d} arrived={int(arrived):>4d} late={int(late):>4d} "
            f"lateEq={late_loss_equiv:>5.2f} missed={int(missed):>4d}"
        )
    print("=" * 72)
    if reset_events:
        print("Event history reset complete. New Elo starts from seeded baseline.")


def run_monitor_loop(
    db_path: str,
    mode: str,
    line_ids: List[str],
    max_stops_per_line: int,
    interval_seconds: int,
    iterations: int,
    app_id: Optional[str],
    app_key: Optional[str],
    out_csv: str,
    grace_seconds: int,
    late_grace_seconds: int,
    lookback_hours: int,
) -> None:
    i = 0
    while True:
        i += 1
        try:
            collect_once(
                db_path=db_path,
                mode=mode,
                line_ids=line_ids,
                max_stops_per_line=max_stops_per_line,
                cycle_index=i,
                app_id=app_id,
                app_key=app_key,
            )
            export_line_event_elo(
                db_path=db_path,
                out_csv=out_csv,
                mode=mode,
                grace_seconds=grace_seconds,
                late_grace_seconds=late_grace_seconds,
                lookback_hours=lookback_hours,
            )
        except sqlite3.OperationalError as exc:
            print(f"[warn] monitor cycle sqlite error: {exc}")
        except Exception as exc:
            print(f"[warn] monitor cycle error: {exc}")
        if iterations > 0 and i >= iterations:
            break
        time.sleep(interval_seconds)


def export_line_event_elo(
    db_path: str,
    out_csv: str,
    mode: str,
    grace_seconds: int,
    late_grace_seconds: int,
    lookback_hours: int,
) -> None:
    con = sqlite3.connect(db_path, timeout=20)
    con.execute("PRAGMA busy_timeout=10000")
    con.row_factory = sqlite3.Row
    try:
        if lookback_hours > 0:
            cutoff = datetime.now(timezone.utc).timestamp() - (lookback_hours * 3600)
            cutoff_iso = datetime.fromtimestamp(cutoff, tz=timezone.utc).isoformat()
            rows = con.execute(
                """
                SELECT line_id, line_name, expected_arrival_utc, last_seen_utc, min_time_to_station, sightings
                FROM train_stop_events
                WHERE mode_name = ?
                  AND expected_arrival_utc IS NOT NULL
                  AND expected_arrival_utc >= ?
                ORDER BY expected_arrival_utc ASC
                """,
                (mode, cutoff_iso),
            ).fetchall()
        else:
            rows = con.execute(
                """
                SELECT line_id, line_name, expected_arrival_utc, last_seen_utc, min_time_to_station, sightings
                FROM train_stop_events
                WHERE mode_name = ?
                ORDER BY expected_arrival_utc ASC
                """,
                (mode,),
            ).fetchall()
    finally:
        con.close()

    now_ts = datetime.now(timezone.utc).timestamp()
    seed_map = load_seed_map(db_path)
    scores: Dict[str, Dict[str, float | int | str]] = {}

    for line_id, line_name in ALL_TUBE_LINES:
        seed = seed_map.get(line_id, {})
        base_name = str(seed.get("line_name", line_name))
        base_elo = float(seed.get("seed_elo", BASE_ELO))
        scores[line_id] = {
            "line_id": line_id,
            "line_name": base_name,
            "elo": base_elo,
            "events": 0,
            "arrived_events": 0,
            "late_events": 0,
            "late_minutes_total": 0.0,
            "missed_events": 0,
            "pending_events": 0,
            "peak_events": 0,
            "weighted_arrivals": 0.0,
            "weighted_lates": 0.0,
            "weighted_cancellations": 0.0,
            "weighted_peak_events": 0.0,
            "weighted_peak_failures": 0.0,
        }

    for r in rows:
        line_id = r["line_id"] or ""
        line_name = r["line_name"] or line_id
        scores.setdefault(
            line_id,
            {
                "line_id": line_id,
                "line_name": line_name,
                "elo": BASE_ELO,
                "events": 0,
                "arrived_events": 0,
                "late_events": 0,
                "late_minutes_total": 0.0,
                "missed_events": 0,
                "pending_events": 0,
                "peak_events": 0,
                "weighted_arrivals": 0.0,
                "weighted_lates": 0.0,
                "weighted_cancellations": 0.0,
                "weighted_peak_events": 0.0,
                "weighted_peak_failures": 0.0,
            },
        )
        scores[line_id]["events"] = int(scores[line_id]["events"]) + 1

        expected = r["expected_arrival_utc"]
        min_tts = r["min_time_to_station"]
        arrived = (min_tts is not None) and (int(min_tts) <= ARRIVAL_DETECTED_SECONDS)

        overdue = False
        if expected:
            try:
                exp_ts = datetime.fromisoformat(expected.replace("Z", "+00:00")).timestamp()
                overdue = now_ts > (exp_ts + grace_seconds)
            except Exception:
                overdue = False

        peak = is_peak_london_time(expected)
        event_weight = 1.75 if peak else 1.0
        if peak:
            scores[line_id]["peak_events"] = int(scores[line_id]["peak_events"]) + 1
            scores[line_id]["weighted_peak_events"] = float(scores[line_id]["weighted_peak_events"]) + event_weight

        is_late = False
        if arrived and expected and r["last_seen_utc"]:
            try:
                exp_ts = datetime.fromisoformat(expected.replace("Z", "+00:00")).timestamp()
                last_seen_ts = datetime.fromisoformat(str(r["last_seen_utc"]).replace("Z", "+00:00")).timestamp()
                is_late = last_seen_ts > (exp_ts + late_grace_seconds)
                late_seconds = max(0.0, last_seen_ts - (exp_ts + late_grace_seconds))
            except Exception:
                is_late = False
                late_seconds = 0.0
        else:
            late_seconds = 0.0

        if arrived and not is_late:
            scores[line_id]["arrived_events"] = int(scores[line_id]["arrived_events"]) + 1
            scores[line_id]["weighted_arrivals"] = float(scores[line_id]["weighted_arrivals"]) + event_weight
        elif arrived and is_late:
            scores[line_id]["late_events"] = int(scores[line_id]["late_events"]) + 1
            scores[line_id]["late_minutes_total"] = float(scores[line_id]["late_minutes_total"]) + (late_seconds / 60.0)
            scores[line_id]["weighted_lates"] = float(scores[line_id]["weighted_lates"]) + event_weight
            if peak:
                scores[line_id]["weighted_peak_failures"] = float(scores[line_id]["weighted_peak_failures"]) + (0.8 * event_weight)
        elif overdue and int(r["sightings"] or 0) >= CANCELLATION_MIN_SIGHTINGS:
            scores[line_id]["missed_events"] = int(scores[line_id]["missed_events"]) + 1
            scores[line_id]["weighted_cancellations"] = float(scores[line_id]["weighted_cancellations"]) + event_weight
            if peak:
                scores[line_id]["weighted_peak_failures"] = float(scores[line_id]["weighted_peak_failures"]) + (1.3 * event_weight)
        else:
            scores[line_id]["pending_events"] = int(scores[line_id]["pending_events"]) + 1

    for line_id, row in scores.items():
        seed_elo = float(seed_map.get(line_id, {}).get("seed_elo", BASE_ELO))
        base_elo = adaptive_base_elo(seed_elo, row)
        row["elo"] = compute_line_elo_update(base_elo, row)

    leaderboard = sorted(scores.values(), key=lambda x: float(x["elo"]), reverse=True)

    rounded_leaderboard: List[Dict[str, float | int | str]] = []
    for row in leaderboard:
        r = dict(row)
        r["elo"] = int(round(float(r["elo"])))
        rounded_leaderboard.append(r)
    for row in rounded_leaderboard:
        row["late_minutes_total"] = round(float(row["late_minutes_total"]), 2)

    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        display_rows = []
        for row in rounded_leaderboard:
            display_rows.append(
                {
                    "line_id": row["line_id"],
                    "lines": row["line_name"],
                    "elo": row["elo"],
                    "on-time arrivals": row["arrived_events"],
                    "late arrivals": row["late_events"],
                    "cancelled trains": row["missed_events"],
                }
            )

        writer = csv.DictWriter(
            f,
            fieldnames=[
                "line_id",
                "lines",
                "elo",
                "on-time arrivals",
                "late arrivals",
                "cancelled trains",
            ],
        )
        writer.writeheader()
        writer.writerows(display_rows)

    snapshot_ts = utc_now_iso()
    con = sqlite3.connect(db_path, timeout=20)
    con.execute("PRAGMA busy_timeout=10000")
    try:
        for row in rounded_leaderboard:
            con.execute(
                """
                INSERT OR REPLACE INTO line_elo_snapshots (
                    snapshot_utc,
                    line_id,
                    line_name,
                    elo,
                    on_time_arrivals,
                    late_arrivals,
                    cancelled_trains
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot_ts,
                    str(row["line_id"]),
                    str(row["line_name"]),
                    int(row["elo"]),
                    int(row["arrived_events"]),
                    int(row["late_events"]),
                    int(row["missed_events"]),
                ),
            )
        con.commit()
    finally:
        con.close()

    print("Train-event Elo leaderboard")
    print("=" * 72)
    for i, row in enumerate(rounded_leaderboard, start=1):
        print(
            f"{i:2d}. {row['line_name']:<20} Elo={int(row['elo']):>7d} "
            f"arrivals={row['arrived_events']} lates={row['late_events']} "
            f"cancellations={row['missed_events']}"
        )
    print("=" * 72)
    print(f"Wrote: {out_csv}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="TfL train-event collector and Elo")
    p.add_argument("--db", default=DB_PATH_DEFAULT, help="SQLite DB path")
    p.add_argument("--mode", default="tube", help="TfL mode, e.g. tube")
    p.add_argument("--app-id", default=os.getenv("TFL_APP_ID"), help="TfL app_id")
    p.add_argument("--app-key", default=os.getenv("TFL_APP_KEY"), help="TfL app_key")

    sub = p.add_subparsers(dest="command", required=True)

    c = sub.add_parser("collect", help="Collect train-level arrivals as events")
    c.add_argument("--line-ids", default="", help="Comma-separated line ids, e.g. victoria,central")
    c.add_argument("--max-stops-per-line", type=int, default=0, help="0 means all stops")
    c.add_argument("--interval", type=int, default=60, help="Polling interval seconds")
    c.add_argument("--iterations", type=int, default=1, help="0 means infinite")

    r = sub.add_parser("rank", help="Rank lines based on train stop-call events")
    r.add_argument("--out-csv", default="tfl_train_event_elo_leaderboard.csv")
    r.add_argument("--grace-seconds", type=int, default=CANCELLATION_GRACE_SECONDS_DEFAULT, help="Cancellation grace seconds")
    r.add_argument("--late-grace-seconds", type=int, default=LATE_GRACE_SECONDS_DEFAULT, help="Seconds after expected arrival to count as late")
    r.add_argument("--lookback-hours", type=int, default=6, help="Scoring window in hours; 0 means full history")

    s = sub.add_parser("reseed", help="Reset and reseed line Elo baselines")
    s.add_argument("--spread-min", type=float, default=800.0)
    s.add_argument("--spread-max", type=float, default=2000.0)
    s.add_argument("--prior-strength", type=float, default=80.0)
    s.add_argument("--prior-success", type=float, default=0.55)
    s.add_argument("--late-grace-seconds", type=int, default=LATE_GRACE_SECONDS_DEFAULT)
    s.add_argument("--reset-events", action="store_true", help="Clear existing raw/events after seeding")

    m = sub.add_parser("monitor", help="Collect and rank continuously")
    m.add_argument("--line-ids", default="", help="Comma-separated line ids, e.g. victoria,central")
    m.add_argument("--max-stops-per-line", type=int, default=0, help="0 means all stops")
    m.add_argument("--interval", type=int, default=60, help="Polling interval seconds")
    m.add_argument("--iterations", type=int, default=0, help="0 means infinite")
    m.add_argument("--out-csv", default="tfl_train_event_elo_leaderboard.csv")
    m.add_argument("--grace-seconds", type=int, default=CANCELLATION_GRACE_SECONDS_DEFAULT, help="Cancellation grace seconds")
    m.add_argument("--late-grace-seconds", type=int, default=LATE_GRACE_SECONDS_DEFAULT, help="Seconds after expected arrival to count as late")
    m.add_argument("--lookback-hours", type=int, default=6, help="Scoring window in hours; 0 means full history")

    return p.parse_args()


def main() -> None:
    args = parse_args()
    init_db(args.db)

    if args.command == "collect":
        line_ids = [x.strip() for x in args.line_ids.split(",") if x.strip()]
        run_collect_loop(
            db_path=args.db,
            mode=args.mode,
            line_ids=line_ids,
            max_stops_per_line=args.max_stops_per_line,
            interval_seconds=args.interval,
            iterations=args.iterations,
            app_id=args.app_id,
            app_key=args.app_key,
        )
    elif args.command == "rank":
        export_line_event_elo(
            db_path=args.db,
            out_csv=args.out_csv,
            mode=args.mode,
            grace_seconds=args.grace_seconds,
            late_grace_seconds=args.late_grace_seconds,
            lookback_hours=args.lookback_hours,
        )
    elif args.command == "reseed":
        reseed_ratings_from_history(
            db_path=args.db,
            mode=args.mode,
            spread_min=args.spread_min,
            spread_max=args.spread_max,
            prior_strength=args.prior_strength,
            prior_success=args.prior_success,
            late_grace_seconds=args.late_grace_seconds,
            reset_events=args.reset_events,
        )
    elif args.command == "monitor":
        line_ids = [x.strip() for x in args.line_ids.split(",") if x.strip()]
        run_monitor_loop(
            db_path=args.db,
            mode=args.mode,
            line_ids=line_ids,
            max_stops_per_line=args.max_stops_per_line,
            interval_seconds=args.interval,
            iterations=args.iterations,
            app_id=args.app_id,
            app_key=args.app_key,
            out_csv=args.out_csv,
            grace_seconds=args.grace_seconds,
            late_grace_seconds=args.late_grace_seconds,
            lookback_hours=args.lookback_hours,
        )
    else:
        raise ValueError(f"Unknown command {args.command}")


if __name__ == "__main__":
    main()
