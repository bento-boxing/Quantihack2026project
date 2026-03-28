TfL Line Elo Tracker

This script tracks TfL line status snapshots and computes Elo-style reliability ratings.

Files
- `tfl_line_elo.py`: collector + Elo ranking CLI
- `tfl_line_elo.db`: SQLite database (created on first run)
- `tfl_line_elo_leaderboard.csv`: ranking output (after `rank` command)

Quick start

1) Collect one snapshot:

```bash
python tfl_line_elo.py collect --iterations 1
```

2) Compute rankings:

```bash
python tfl_line_elo.py rank --lookback-hours 24
```

3) Run continuous collection (every minute):

```bash
python tfl_line_elo.py collect --interval 60 --iterations 0
```

Optional TfL credentials

Set credentials if you have them:

```bash
set TFL_APP_ID=your_app_id
set TFL_APP_KEY=your_app_key
```

Then run commands normally.

Notes
- Elo is line-level and event-driven from status snapshots.
- This is an Elo-style reliability score, not official TfL punctuality metrics.
- You can tune ranking behavior with `--base-elo` and `--k-factor`.
