import sqlite3
import time
import math
import json
from html import escape
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo


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

LINE_BOX_COLORS: Dict[str, Dict[str, str]] = {
    "bakerloo": {"bg": "#b36305", "fg": "#ffffff"},
    "central": {"bg": "#e32017", "fg": "#ffffff"},
    "circle": {"bg": "#ffd300", "fg": "#111111"},
    "district": {"bg": "#00782a", "fg": "#ffffff"},
    "hammersmith-city": {"bg": "#f3a9bb", "fg": "#111111"},
    "jubilee": {"bg": "#a0a5a9", "fg": "#111111"},
    "metropolitan": {"bg": "#9b0056", "fg": "#ffffff"},
    "northern": {"bg": "#000000", "fg": "#ffffff"},
    "piccadilly": {"bg": "#003688", "fg": "#ffffff"},
    "victoria": {"bg": "#0098d4", "fg": "#ffffff"},
    "waterloo-city": {"bg": "#95cdba", "fg": "#111111"},
}

PODIUM_COLORS: Dict[int, Dict[str, str]] = {
    1: {"bg": "#ffd700", "fg": "#111111"},
    2: {"bg": "#c0c0c0", "fg": "#111111"},
    3: {"bg": "#cd7f32", "fg": "#ffffff"},
}


def is_sqlite_lock_error(exc: sqlite3.OperationalError) -> bool:
    msg = str(exc).lower()
    return ("database is locked" in msg) or ("database is busy" in msg)


def fetch_live_line_status(mode: str) -> Dict[str, Dict[str, str]]:
    url = f"https://api.tfl.gov.uk/Line/Mode/{mode}/Status"
    req = Request(url, headers={"User-Agent": "tfl-train-event-dashboard/1.0"})
    payload = None
    for _ in range(3):
        try:
            with urlopen(req, timeout=15) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
            break
        except Exception:
            time.sleep(0.6)
    if payload is None:
        return {}

    status_map: Dict[str, Dict[str, str]] = {}
    if not isinstance(payload, list):
        return status_map

    for line in payload:
        line_id = str(line.get("id") or "").strip().lower()
        statuses = line.get("lineStatuses") or []
        if not line_id or not statuses:
            continue

        worst = min(statuses, key=lambda s: int(s.get("statusSeverity") or 99))
        desc = str(worst.get("statusSeverityDescription") or "Unknown")
        low = desc.lower()

        if "cancel" in low or "severe" in low or "suspend" in low:
            color = "#ef4444"
        elif "delay" in low and "minor" not in low:
            color = "#f97316"
        elif "minor" in low:
            color = "#facc15"
        else:
            color = "#22c55e"

        status_map[line_id] = {"desc": desc, "color": color}

    return status_map


def fetch_rows(db_path: str, mode: str, lookback_hours: int):
    last_err = None
    for _ in range(15):
        con = sqlite3.connect(db_path, timeout=1)
        con.row_factory = sqlite3.Row
        try:
            con.execute("PRAGMA busy_timeout=800")
            if lookback_hours > 0:
                cutoff = datetime.now(timezone.utc).timestamp() - (lookback_hours * 3600)
                cutoff_iso = datetime.fromtimestamp(cutoff, tz=timezone.utc).isoformat()
                events = con.execute(
                    """
                    SELECT line_id, line_name, expected_arrival_utc, last_seen_utc, min_time_to_station, sightings
                    FROM train_stop_events
                    WHERE mode_name = ?
                      AND expected_arrival_utc IS NOT NULL
                      AND expected_arrival_utc >= ?
                    """,
                    (mode, cutoff_iso),
                ).fetchall()
            else:
                events = con.execute(
                    """
                    SELECT line_id, line_name, expected_arrival_utc, last_seen_utc, min_time_to_station, sightings
                    FROM train_stop_events
                    WHERE mode_name = ?
                    """,
                    (mode,),
                ).fetchall()
            seeds = con.execute(
                """
                SELECT line_id, line_name, seed_elo
                FROM line_elo_seed
                """
            ).fetchall()
            return events, seeds
        except sqlite3.OperationalError as exc:
            last_err = exc
            time.sleep(0.3)
        finally:
            con.close()

    raise sqlite3.OperationalError(f"DB locked after retries: {last_err}")


def ensure_snapshot_table(db_path: str) -> bool:
    last_err: Optional[Exception] = None
    for _ in range(3):
        con = sqlite3.connect(db_path, timeout=1)
        try:
            con.execute("PRAGMA busy_timeout=800")
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
            return True
        except sqlite3.OperationalError as exc:
            if not is_sqlite_lock_error(exc):
                if "no such table" in str(exc).lower():
                    return False
                raise
            last_err = exc
            time.sleep(0.1)
        finally:
            con.close()

    check_con = sqlite3.connect(db_path, timeout=5)
    try:
        exists_row = check_con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='line_elo_snapshots'"
        ).fetchone()
    finally:
        check_con.close()

    if exists_row:
        return True

    if last_err:
        print(f"[warn] snapshot table unavailable due to DB lock: {last_err}")
    return False


def persist_leaderboard_snapshot(db_path: str, leaderboard: List[Dict[str, float | int | str]]) -> None:
    if not leaderboard:
        return

    snapshot_ts = datetime.now(timezone.utc).isoformat()
    last_err: Optional[Exception] = None
    for _ in range(8):
        con = sqlite3.connect(db_path, timeout=1)
        try:
            con.execute("PRAGMA busy_timeout=800")
            for row in leaderboard:
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
            return
        except sqlite3.OperationalError as exc:
            if "no such table" in str(exc).lower():
                if not ensure_snapshot_table(db_path):
                    return
                continue
            if not is_sqlite_lock_error(exc):
                raise
            last_err = exc
            time.sleep(0.2)
        finally:
            con.close()

    if last_err:
        print(f"[warn] snapshot write skipped due to DB lock: {last_err}")


def fetch_elo_snapshots_for_lines(db_path: str, line_ids: List[str], lookback_hours: int) -> List[sqlite3.Row]:
    if not line_ids:
        return []

    last_err: Optional[Exception] = None
    for _ in range(8):
        con = sqlite3.connect(db_path, timeout=1)
        con.row_factory = sqlite3.Row
        try:
            con.execute("PRAGMA busy_timeout=800")
            placeholders = ",".join(["?"] * len(line_ids))
            params: List[object] = list(line_ids)
            sql = (
                "SELECT snapshot_utc, line_id, line_name, elo "
                "FROM line_elo_snapshots "
                f"WHERE line_id IN ({placeholders}) "
            )
            if lookback_hours > 0:
                cutoff = datetime.now(timezone.utc).timestamp() - (lookback_hours * 3600)
                cutoff_iso = datetime.fromtimestamp(cutoff, tz=timezone.utc).isoformat()
                sql += "AND snapshot_utc >= ? "
                params.append(cutoff_iso)
            sql += "ORDER BY snapshot_utc ASC"
            return con.execute(sql, params).fetchall()
        except sqlite3.OperationalError as exc:
            if "no such table" in str(exc).lower():
                return []
            if not is_sqlite_lock_error(exc):
                raise
            last_err = exc
            time.sleep(0.2)
        finally:
            con.close()

    if last_err:
        print(f"[warn] snapshot read failed due to DB lock: {last_err}")
    return []


def fetch_elo_snapshots_for_line(db_path: str, line_id: str, lookback_hours: int) -> List[sqlite3.Row]:
    return fetch_elo_snapshots_for_lines(db_path=db_path, line_ids=[line_id], lookback_hours=lookback_hours)


def compute_leaderboard(
    events: List[sqlite3.Row],
    seeds: List[sqlite3.Row],
    grace_seconds: int,
    late_grace_seconds: int,
) -> List[Dict[str, float | int | str]]:
    now_ts = datetime.now(timezone.utc).timestamp()
    scores: Dict[str, Dict[str, float | int | str]] = {}

    seed_map: Dict[str, Dict[str, float | str]] = {}
    for s in seeds:
        seed_map[str(s["line_id"])] = {
            "line_name": str(s["line_name"]),
            "seed_elo": float(s["seed_elo"]),
        }

    for line_id, line_name in ALL_TUBE_LINES:
        seed = seed_map.get(line_id, {})
        scores[line_id] = {
            "line_id": line_id,
            "line_name": str(seed.get("line_name", line_name)),
            "elo": float(seed.get("seed_elo", BASE_ELO)),
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

    for r in events:
        line_id = r["line_id"] or ""
        line_name = r["line_name"] or line_id
        if line_id not in scores:
            scores[line_id] = {
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
            }

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
    for row in leaderboard:
        row["elo"] = int(round(float(row["elo"])))
        row["late_minutes_total"] = round(float(row["late_minutes_total"]), 2)
    return leaderboard


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
    return (7 * 60 <= minutes <= 10 * 60) or (16 * 60 <= minutes <= 19 * 60)


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


def get_stat_int(stats: Optional[sqlite3.Row], key: str) -> int:
    if not stats:
        return 0
    value = stats[key]
    if value is None:
        return 0
    return int(value)


@__import__("streamlit").fragment(run_every="60s")
def render_live_leaderboard(
    db_path: str,
    mode: str,
    lookback_hours: int,
    grace_seconds: int,
    late_grace_seconds: int,
) -> None:
    st = __import__("streamlit")
    with st.spinner("Updating live data..."):
        live_status = fetch_live_line_status(mode)
        if live_status:
            st.session_state["_live_status_map"] = live_status
        else:
            live_status = st.session_state.get("_live_status_map", {})

        events: List[sqlite3.Row] = []
        seeds: List[sqlite3.Row] = []
        try:
            events, seeds = fetch_rows(db_path=db_path, mode=mode, lookback_hours=lookback_hours)
        except Exception as exc:
            st.error(f"Failed to read DB: {exc}")
            return

    leaderboard = compute_leaderboard(
        events=events,
        seeds=seeds,
        grace_seconds=grace_seconds,
        late_grace_seconds=late_grace_seconds,
    )
    try:
        persist_leaderboard_snapshot(db_path=db_path, leaderboard=leaderboard)
    except Exception:
        pass

    st.subheader("Elo leaderboard")
    display_rows = []
    for row in leaderboard:
        display_rows.append(
            {
                "line_id": row["line_id"],
                "line_name": row["line_name"],
                "elo": row["elo"],
                "on-time arrivals": row["arrived_events"],
                "late arrivals": row["late_events"],
                "cancelled trains": row["missed_events"],
            }
        )

    display_rows_with_rank = []
    for idx, row in enumerate(display_rows, start=1):
        r = dict(row)
        r["rank"] = idx
        display_rows_with_rank.append(r)

    ordered_rows = []
    for row in display_rows_with_rank:
        ordered_rows.append(
            {
                "rank": row["rank"],
                "line_id": row["line_id"],
                "line_name": row["line_name"],
                "elo": row["elo"],
                "on-time arrivals": row["on-time arrivals"],
                "late arrivals": row["late arrivals"],
                "cancelled trains": row["cancelled trains"],
            }
        )

    table_html = [
        "<table style='width:100%; border-collapse: collapse;'>",
        "<thead><tr>",
        "<th style='text-align:left; padding:6px;'>rank</th>",
        "<th style='text-align:left; padding:6px; width:170px;'>lines</th>",
        "<th style='text-align:right; padding:6px;'>elo</th>",
        "<th style='text-align:right; padding:6px;'>on-time arrivals</th>",
        "<th style='text-align:right; padding:6px;'>late arrivals</th>",
        "<th style='text-align:right; padding:6px;'>cancelled trains</th>",
        "</tr></thead><tbody>",
    ]

    for row in ordered_rows:
        line_key = str(row["line_id"]).strip().lower()
        status = live_status.get(line_key, {"desc": "Unknown", "color": "#9ca3af"})
        line_colors = LINE_BOX_COLORS.get(line_key, {"bg": "#1f2937", "fg": "#ffffff"})
        rank_colors = PODIUM_COLORS.get(int(row["rank"]), {"bg": "transparent", "fg": "#e5e7eb"})
        rank_cell = (
            f"<span style='display:inline-block; min-width:28px; text-align:center;"
            f" padding:2px 8px; border-radius:12px; background:{rank_colors['bg']};"
            f" color:{rank_colors['fg']}; font-weight:700;'>{int(row['rank'])}</span>"
        )
        line_cell = (
            "<div style='display:flex; align-items:center; justify-content:space-between; width:100%;'>"
            f"<span style='display:inline-block; padding:3px 8px; border-radius:6px;"
            f" background:{line_colors['bg']}; color:{line_colors['fg']}; font-weight:600;'>"
            f"{escape(str(row['line_name']))}</span>"
            f"<span title='{escape(str(status['desc']))}' style='color:{status['color']}; margin-left:10px;'>●</span>"
            "</div>"
        )
        table_html.append(
            "<tr>"
            f"<td style='padding:6px;'>{rank_cell}</td>"
            f"<td style='padding:6px; width:170px;'>{line_cell}</td>"
            f"<td style='padding:6px; text-align:right;'>{int(row['elo'])}</td>"
            f"<td style='padding:6px; text-align:right;'>{int(row['on-time arrivals'])}</td>"
            f"<td style='padding:6px; text-align:right;'>{int(row['late arrivals'])}</td>"
            f"<td style='padding:6px; text-align:right;'>{int(row['cancelled trains'])}</td>"
            "</tr>"
        )

    table_html.append("</tbody></table>")
    st.markdown("".join(table_html), unsafe_allow_html=True)

    st.markdown(
        "<div style='margin-top:10px; font-size:0.95rem;'>"
        "<span style='color:#22c55e;'>●</span> Good Service &nbsp; "
        "<span style='color:#facc15;'>●</span> Minor Delays &nbsp; "
        "<span style='color:#f97316;'>●</span> Delays &nbsp; "
        "<span style='color:#ef4444;'>●</span> Severe Delays / Cancellations"
        "</div>",
        unsafe_allow_html=True,
    )


def fetch_line_events_for_hours(db_path: str, mode: str, line_id: str, lookback_hours: int) -> List[sqlite3.Row]:
    con = sqlite3.connect(db_path, timeout=1)
    con.row_factory = sqlite3.Row
    try:
        con.execute("PRAGMA busy_timeout=800")
        cutoff = datetime.now(timezone.utc).timestamp() - (lookback_hours * 3600)
        cutoff_iso = datetime.fromtimestamp(cutoff, tz=timezone.utc).isoformat()
        rows = con.execute(
            """
            SELECT expected_arrival_utc, last_seen_utc, min_time_to_station, sightings
            FROM train_stop_events
            WHERE mode_name = ?
              AND line_id = ?
              AND expected_arrival_utc IS NOT NULL
              AND expected_arrival_utc >= ?
            ORDER BY expected_arrival_utc ASC
            """,
            (mode, line_id, cutoff_iso),
        ).fetchall()
        return rows
    finally:
        con.close()


def fetch_events_for_lines_hours(db_path: str, mode: str, line_ids: List[str], lookback_hours: int) -> List[sqlite3.Row]:
    if not line_ids:
        return []

    con = sqlite3.connect(db_path, timeout=1)
    con.row_factory = sqlite3.Row
    try:
        con.execute("PRAGMA busy_timeout=800")
        cutoff = datetime.now(timezone.utc).timestamp() - (lookback_hours * 3600)
        cutoff_iso = datetime.fromtimestamp(cutoff, tz=timezone.utc).isoformat()
        placeholders = ",".join(["?"] * len(line_ids))
        sql = (
            "SELECT line_id, expected_arrival_utc, last_seen_utc, min_time_to_station, sightings "
            "FROM train_stop_events "
            "WHERE mode_name = ? "
            f"AND line_id IN ({placeholders}) "
            "AND expected_arrival_utc IS NOT NULL "
            "AND expected_arrival_utc >= ? "
            "ORDER BY expected_arrival_utc ASC"
        )
        params: List[object] = [mode]
        params.extend(line_ids)
        params.append(cutoff_iso)
        return con.execute(sql, params).fetchall()
    finally:
        con.close()


def fetch_seed_map(db_path: str) -> Dict[str, float]:
    con = sqlite3.connect(db_path, timeout=1)
    con.row_factory = sqlite3.Row
    try:
        con.execute("PRAGMA busy_timeout=800")
        rows = con.execute("SELECT line_id, seed_elo FROM line_elo_seed").fetchall()
        return {str(r["line_id"]): float(r["seed_elo"]) for r in rows}
    finally:
        con.close()


def classify_event(row: sqlite3.Row, now_ts: float, grace_seconds: int, late_grace_seconds: int) -> str:
    expected = row["expected_arrival_utc"]
    min_tts = row["min_time_to_station"]
    arrived = (min_tts is not None) and (int(min_tts) <= ARRIVAL_DETECTED_SECONDS)

    overdue = False
    exp_ts = None
    if expected:
        try:
            exp_ts = datetime.fromisoformat(str(expected).replace("Z", "+00:00")).timestamp()
            overdue = now_ts > (exp_ts + grace_seconds)
        except Exception:
            overdue = False

    if arrived and expected and row["last_seen_utc"] and exp_ts is not None:
        try:
            last_seen_ts = datetime.fromisoformat(str(row["last_seen_utc"]).replace("Z", "+00:00")).timestamp()
            if last_seen_ts > (exp_ts + late_grace_seconds):
                return "late"
        except Exception:
            pass
        return "ontime"

    if overdue and int(row["sightings"] or 0) >= CANCELLATION_MIN_SIGHTINGS:
        return "cancelled"

    return "pending"


def render_line_chart_page(db_path: str, mode: str, grace_seconds: int, late_grace_seconds: int) -> None:
    st = __import__("streamlit")
    st.subheader("Line Chart")

    names = {line_id: line_name for line_id, line_name in ALL_TUBE_LINES}
    c1, c2 = st.columns([3, 1])
    selected_lines = c1.multiselect(
        "Choose line(s)",
        options=[x[0] for x in ALL_TUBE_LINES],
        default=["jubilee", "victoria", "central"],
        format_func=lambda x: names.get(x, x),
        key="line_chart_lines",
    )
    hours = c2.slider("Hours", min_value=1, max_value=48, value=6, step=1, key="line_chart_hours")

    if not selected_lines:
        st.info("Select at least one line to plot Elo over time.")
        return

    rows = fetch_elo_snapshots_for_lines(db_path=db_path, line_ids=selected_lines, lookback_hours=hours)
    tz_now = datetime.now(LONDON_TZ).tzname() or "UK"

    if not rows:
        st.info("No Elo trajectory data available yet for selected lines/time window.")
        return

    points = []
    elo_vals: List[float] = []
    for r in rows:
        try:
            dt_utc = datetime.fromisoformat(str(r["snapshot_utc"]).replace("Z", "+00:00"))
        except Exception:
            continue
        dt_local = dt_utc.astimezone(LONDON_TZ)
        elo = float(r["elo"])
        elo_vals.append(elo)
        points.append(
            {
                "time": dt_local.isoformat(),
                "line": names.get(str(r["line_id"]), str(r["line_id"])),
                "elo": round(elo, 2),
            }
        )

    if not points:
        st.info("No Elo trajectory data available yet for selected lines/time window.")
        return

    y_min = min(elo_vals)
    y_max = max(elo_vals)
    pad = max(10.0, (y_max - y_min) * 0.12)
    y_domain = [round(y_min - pad, 2), round(y_max + pad, 2)]
    window_end = datetime.now(LONDON_TZ)
    window_start = window_end - timedelta(hours=hours)
    x_domain = [window_start.isoformat(), window_end.isoformat()]

    spec = {
        "mark": {"type": "line", "point": True},
        "encoding": {
            "x": {
                "field": "time",
                "type": "temporal",
                "scale": {"domain": x_domain},
                "axis": {"title": f"Time ({tz_now})", "labelAngle": -45, "format": "%d %b %H:%M"},
            },
            "y": {
                "field": "elo",
                "type": "quantitative",
                "scale": {"domain": y_domain, "zero": False},
                "axis": {"title": "Elo Rating"},
            },
            "color": {"field": "line", "type": "nominal", "legend": {"title": "Line"}},
            "tooltip": [
                {"field": "line", "type": "nominal"},
                {"field": "time", "type": "temporal", "format": "%Y-%m-%d %H:%M"},
                {"field": "elo", "type": "quantitative"},
            ],
        },
        "height": 420,
    }
    st.vega_lite_chart(points, spec, use_container_width=True)
    st.caption(f"Elo trajectory over the last {hours}h in UK local time ({tz_now}).")


def render_candle_chart_page(db_path: str, mode: str, grace_seconds: int, late_grace_seconds: int) -> None:
    st = __import__("streamlit")
    st.subheader("Candle Chart")

    names = {line_id: line_name for line_id, line_name in ALL_TUBE_LINES}
    c1, c2 = st.columns([2, 1])
    line_id = c1.selectbox("Line", options=[x[0] for x in ALL_TUBE_LINES], format_func=lambda x: names.get(x, x), key="candle_line")
    hours = c2.slider("Hours", min_value=1, max_value=72, value=24, step=1, key="candle_hours")

    rows = fetch_elo_snapshots_for_line(db_path=db_path, line_id=line_id, lookback_hours=hours)
    if not rows:
        st.info("No Elo snapshot data available for this line/time window yet.")
        return

    buckets: Dict[datetime, List[tuple[datetime, float]]] = {}
    for r in rows:
        try:
            dt_utc = datetime.fromisoformat(str(r["snapshot_utc"]).replace("Z", "+00:00"))
        except Exception:
            continue
        dt_local = dt_utc.astimezone(LONDON_TZ)
        bucket = dt_local.replace(minute=0, second=0, microsecond=0)
        buckets.setdefault(bucket, []).append((dt_local, float(r["elo"])))

    if not buckets:
        st.info("No Elo snapshot data available for this line/time window yet.")
        return

    candles = []
    for bucket in sorted(buckets.keys()):
        points = sorted(buckets[bucket], key=lambda x: x[0])
        if not points:
            continue
        vals = [x[1] for x in points]
        candles.append(
            {
                "time": bucket.isoformat(),
                "open": round(vals[0], 2),
                "high": round(max(vals), 2),
                "low": round(min(vals), 2),
                "close": round(vals[-1], 2),
            }
        )

    if len(candles) < 2:
        st.info("Not enough Elo history yet for candlesticks. Keep the monitor running for more snapshots.")
        return

    lows = [float(c["low"]) for c in candles]
    highs = [float(c["high"]) for c in candles]
    y_min = min(lows)
    y_max = max(highs)
    span = y_max - y_min
    pad = max(8.0, span * 0.12)
    y_domain = [round(y_min - pad, 2), round(y_max + pad, 2)]
    first_bucket_local = datetime.fromisoformat(str(candles[0]["time"]))
    window_start = first_bucket_local
    window_end = window_start + timedelta(hours=hours)
    x_domain = [window_start.isoformat(), window_end.isoformat()]
    window_end = datetime.now(LONDON_TZ)
    window_start = window_end - timedelta(hours=hours)
    x_domain = [window_start.isoformat(), window_end.isoformat()]

    spec = {
        "layer": [
            {
                "mark": {"type": "rule", "color": "#9ca3af"},
                "encoding": {
                    "x": {
                        "field": "time",
                        "type": "temporal",
                        "scale": {"domain": x_domain},
                        "axis": {
                            "labelAngle": -45,
                            "title": f"Time ({datetime.now(LONDON_TZ).tzname() or 'UK'})",
                            "format": "%d %b %H:%M",
                        },
                    },
                    "y": {
                        "field": "low",
                        "type": "quantitative",
                        "scale": {"domain": y_domain, "zero": False},
                        "axis": {"title": "Elo Rating"},
                    },
                    "y2": {"field": "high"},
                },
            },
            {
                "mark": {"type": "bar"},
                "encoding": {
                    "x": {"field": "time", "type": "temporal", "scale": {"domain": x_domain}},
                    "y": {"field": "open", "type": "quantitative", "scale": {"domain": y_domain, "zero": False}},
                    "y2": {"field": "close"},
                    "tooltip": [
                        {"field": "time", "type": "temporal", "format": "%Y-%m-%d %H:%M"},
                        {"field": "open", "type": "quantitative"},
                        {"field": "high", "type": "quantitative"},
                        {"field": "low", "type": "quantitative"},
                        {"field": "close", "type": "quantitative"},
                    ],
                    "color": {
                        "condition": {"test": "datum.close >= datum.open", "value": "#22c55e"},
                        "value": "#ef4444",
                    },
                },
            },
        ],
        "height": 380,
    }
    st.vega_lite_chart(candles, spec, use_container_width=True)
    st.caption("Candles show real hourly Elo OHLC from persisted snapshots (green up, red down).")


def render_why_page() -> None:
    st = __import__("streamlit")
    st.subheader("Why?")
    st.markdown(
        """
This ranking emphasizes reliability under pressure:
- **On-time arrivals** push Elo up.
- **Late arrivals** reduce Elo with severity scaling.
- **Cancelled trains** are penalized heavily, especially repeated failures.
- **Peak-hour failures** are weighted higher than off-peak issues.
- **Quantity matters a little**, but proportions dominate.

The score is intentionally frictional: extreme ratings move slower as they get further from the center.
        """
    )


def main() -> None:
    import importlib

    st = importlib.import_module("streamlit")
    logo_bg = "#070b12"
    logo_blue = "#1015a8"

    st.set_page_config(page_title="TfL Train Event Elo", layout="wide", initial_sidebar_state="collapsed")
    st.markdown(
        """
        <style>
          .stApp,
          [data-testid="stAppViewContainer"] {
            background: #070b12 !important;
            color: #e5e7eb !important;
          }
          [data-testid="stToolbar"],
          [data-testid="stHeaderActionElements"],
          button[title="Deploy"] {
            display: none !important;
          }
          [data-testid="stStatusWidget"],
          [data-testid="collapsedControl"],
          [data-testid="stSidebar"] {
            display: none !important;
          }
        </style>
        """,
        unsafe_allow_html=True,
    )

    page_options = ["Elo leaderboard", "Line Chart", "Candle Chart", "Why?"]
    if "active_page" not in st.session_state:
        st.session_state["active_page"] = "Elo leaderboard"
    if "last_page_switch_ts" not in st.session_state:
        st.session_state["last_page_switch_ts"] = 0.0

    top_left, top_right = st.columns([1.8, 1.2])
    with top_left:
        logo_col, text_col = st.columns([1.0, 2.3])
        with logo_col:
            st.markdown(
                f"""
                <div style="width:220px; max-width:100%; background:{logo_bg}; padding:4px 0;">
                  <svg viewBox="0 0 820 420" width="100%" role="img" aria-label="TfL Elo logo">
                    <rect x="0" y="0" width="820" height="420" fill="{logo_bg}"/>
                    <ellipse cx="410" cy="210" rx="305" ry="155" fill="none" stroke="{logo_blue}" stroke-width="72"/>
                    <rect x="0" y="175" width="820" height="70" fill="{logo_blue}"/>
                    <text x="410" y="223" text-anchor="middle" fill="#ffffff" font-size="44" font-family="Arial, sans-serif" font-weight="700">TFL ELO</text>
                  </svg>
                </div>
                """,
                unsafe_allow_html=True,
            )
        with text_col:
            st.subheader("TfL Train Event ELO Dashboard")
    with top_right:
        selected = st.segmented_control(
            label="Views",
            options=page_options,
            default=st.session_state["active_page"],
            label_visibility="collapsed",
            key="top_nav_page_selector",
        )
    if selected is None:
        selected = st.session_state["active_page"]

    if selected != st.session_state["active_page"]:
        st.session_state["active_page"] = selected
        st.session_state["last_page_switch_ts"] = time.time()

    if time.time() - float(st.session_state["last_page_switch_ts"]) < 0.25:
        with st.spinner("Switching view..."):
            time.sleep(0.18)

    db_path = "tfl_train_event_elo.db"
    if "_snapshot_checked" not in st.session_state:
        st.session_state["_snapshot_checked"] = ensure_snapshot_table(db_path)
    mode = "tube"
    grace_seconds = CANCELLATION_GRACE_SECONDS_DEFAULT
    late_grace_seconds = LATE_GRACE_SECONDS_DEFAULT
    lookback_hours = 6

    active_page = st.session_state["active_page"]
    if active_page == "Elo leaderboard":
        render_live_leaderboard(
            db_path=db_path,
            mode=mode,
            lookback_hours=lookback_hours,
            grace_seconds=grace_seconds,
            late_grace_seconds=late_grace_seconds,
        )
    elif active_page == "Line Chart":
        render_line_chart_page(
            db_path=db_path,
            mode=mode,
            grace_seconds=grace_seconds,
            late_grace_seconds=late_grace_seconds,
        )
    elif active_page == "Candle Chart":
        render_candle_chart_page(
            db_path=db_path,
            mode=mode,
            grace_seconds=grace_seconds,
            late_grace_seconds=late_grace_seconds,
        )
    else:
        render_why_page()


if __name__ == "__main__":
    main()
