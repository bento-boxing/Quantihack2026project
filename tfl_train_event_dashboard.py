import sqlite3
import time
import math
from html import escape
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo


LONDON_TZ = ZoneInfo("Europe/London")
BASE_ELO = 1200.0
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


def auto_refresh(components: Any, seconds: int) -> None:
    ms = max(5, seconds) * 1000
    components.html(
        f"""
        <script>
        setTimeout(function() {{
            window.parent.location.reload();
        }}, {ms});
        </script>
        """,
        height=0,
    )


def fetch_rows(db_path: str, mode: str, lookback_hours: int):
    last_err = None
    for _ in range(15):
        con = sqlite3.connect(db_path, timeout=10)
        con.row_factory = sqlite3.Row
        try:
            con.execute("PRAGMA busy_timeout=10000")
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
        elif overdue and int(r["sightings"] or 0) >= CANCELLATION_MIN_SIGHTINGS:
            scores[line_id]["missed_events"] = int(scores[line_id]["missed_events"]) + 1
            scores[line_id]["weighted_cancellations"] = float(scores[line_id]["weighted_cancellations"]) + event_weight
        else:
            scores[line_id]["pending_events"] = int(scores[line_id]["pending_events"]) + 1

    for line_id, row in scores.items():
        base_elo = float(seed_map.get(line_id, {}).get("seed_elo", BASE_ELO))
        row["elo"] = compute_line_elo_update(base_elo, row)

    leaderboard = sorted(scores.values(), key=lambda x: float(x["elo"]), reverse=True)
    for row in leaderboard:
        row["elo"] = int(round(float(row["elo"])))
        row["late_minutes_total"] = round(float(row["late_minutes_total"]), 2)
    return leaderboard


def apply_boundary_friction(current_elo: float, raw_delta: float) -> float:
    if raw_delta > 0:
        drag = 1.0 + max(0.0, (current_elo - 1400.0) / 450.0)
        drag += max(0.0, (current_elo - 2000.0) / 180.0) * 2.0
        return raw_delta / drag

    if raw_delta < 0:
        drag = 1.0 + max(0.0, (900.0 - current_elo) / 350.0)
        drag += max(0.0, (800.0 - current_elo) / 180.0) * 2.0
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
    late_severity = min(1.0, late_avg_minutes / 15.0)

    performance = on_time_ratio - (0.55 * late_ratio * (1.0 + late_severity)) - (1.35 * cancel_ratio)
    qty_bonus = 0.03 * math.log1p(resolved_w)
    raw_delta = 420.0 * ((performance - 0.45) + qty_bonus)
    confidence = min(1.0, math.sqrt(resolved_w) / 25.0)
    raw_delta *= confidence

    return base_elo + apply_boundary_friction(base_elo, raw_delta)


def get_stat_int(stats: Optional[sqlite3.Row], key: str) -> int:
    if not stats:
        return 0
    value = stats[key]
    if value is None:
        return 0
    return int(value)


def main() -> None:
    import importlib

    st = importlib.import_module("streamlit")
    components = importlib.import_module("streamlit.components.v1")

    st.set_page_config(page_title="TfL Train Event Elo", layout="wide")
    st.title("TfL Train Event Elo Dashboard")

    with st.sidebar:
        st.header("Settings")
        db_path = st.text_input("DB path", value="tfl_train_event_elo.db")
        mode = st.text_input("Mode", value="tube")
        refresh_seconds = st.number_input("Refresh interval (s)", min_value=10, max_value=300, value=60, step=10)
        grace_seconds = st.number_input("Cancellation grace (s)", min_value=120, max_value=3600, value=CANCELLATION_GRACE_SECONDS_DEFAULT, step=60)
        late_grace_seconds = st.number_input("Late grace (s)", min_value=0, max_value=600, value=LATE_GRACE_SECONDS_DEFAULT, step=15)
        lookback_hours = st.number_input("Scoring window (hours)", min_value=1, max_value=72, value=6, step=1)

    auto_refresh(components, int(refresh_seconds))

    events: List[sqlite3.Row] = []
    seeds: List[sqlite3.Row] = []
    try:
        events, seeds = fetch_rows(db_path=db_path, mode=mode, lookback_hours=int(lookback_hours))
    except Exception as exc:
        st.error(f"Failed to read DB: {exc}")
        st.stop()

    leaderboard = compute_leaderboard(
        events=events,
        seeds=seeds,
        grace_seconds=int(grace_seconds),
        late_grace_seconds=int(late_grace_seconds),
    )

    st.subheader("Elo leaderboard (event-level)")
    display_rows = []
    for row in leaderboard:
        display_rows.append(
            {
                "line_name": row["line_name"],
                "elo": row["elo"],
                "arrivals": row["arrived_events"],
                "lates": row["late_events"],
                "cancellations": row["missed_events"],
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
                "line_name": row["line_name"],
                "elo": row["elo"],
                "arrivals": row["arrivals"],
                "lates": row["lates"],
                "cancellations": row["cancellations"],
            }
        )

    table_html = [
        "<table style='width:100%; border-collapse: collapse;'>",
        "<thead><tr>",
        "<th style='text-align:left; padding:6px;'>rank</th>",
        "<th style='text-align:left; padding:6px;'>line_name</th>",
        "<th style='text-align:right; padding:6px;'>elo</th>",
        "<th style='text-align:right; padding:6px;'>arrivals</th>",
        "<th style='text-align:right; padding:6px;'>lates</th>",
        "<th style='text-align:right; padding:6px;'>cancellations</th>",
        "</tr></thead><tbody>",
    ]

    for row in ordered_rows:
        table_html.append(
            "<tr>"
            f"<td style='padding:6px;'>{int(row['rank'])}</td>"
            f"<td style='padding:6px;'>{escape(str(row['line_name']))}</td>"
            f"<td style='padding:6px; text-align:right;'>{int(row['elo'])}</td>"
            f"<td style='padding:6px; text-align:right;'>{int(row['arrivals'])}</td>"
            f"<td style='padding:6px; text-align:right;'>{int(row['lates'])}</td>"
            f"<td style='padding:6px; text-align:right;'>{int(row['cancellations'])}</td>"
            "</tr>"
        )

    table_html.append("</tbody></table>")
    st.markdown("".join(table_html), unsafe_allow_html=True)


if __name__ == "__main__":
    main()
