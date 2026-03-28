TfL Train Event Elo (Per-Event, Not Broad Status)

This tracker logs train-level stop-call predictions and computes line Elo from those events.

Files
- `tfl_train_event_elo.py`: collector + monitor + ranking CLI
- `tfl_train_event_dashboard.py`: Streamlit website dashboard
- `tfl_train_event_elo.db`: SQLite database (created automatically)
- `tfl_train_event_elo_leaderboard.csv`: latest ranking snapshot

Core commands

Collect one snapshot:

```bash
python tfl_train_event_elo.py collect --line-ids victoria --max-stops-per-line 8 --iterations 1
```

Rank from collected events:

```bash
python tfl_train_event_elo.py rank --out-csv tfl_train_event_elo_leaderboard.csv
```

Reset and reseed all line Elo baselines with wide spread:

```bash
python tfl_train_event_elo.py reseed --spread-min 800 --spread-max 2000 --prior-strength 120 --prior-success 0.55 --reset-events
```

Continuous monitor loop (collect + rank every minute):

```bash
python tfl_train_event_elo.py monitor --line-ids victoria,central,northern --max-stops-per-line 0 --interval 60 --iterations 0 --out-csv tfl_train_event_elo_leaderboard.csv
```

Run the website dashboard:

```bash
python -m streamlit run tfl_train_event_dashboard.py
```

One-click local starters (Windows)

```bash
start_tfl_monitor.bat
start_tfl_dashboard.bat
```

Optional TfL credentials

```bash
set TFL_APP_ID=your_app_id
set TFL_APP_KEY=your_app_key
```

Then run the commands as normal.

Notes
- This is event-level from live polling onward (vehicle + stop + expected arrival).
- For full coverage, keep monitor loop running continuously.
- `--max-stops-per-line 0` means all stops for selected lines.

Run permanently on Windows startup (Task Scheduler)

1) Open Task Scheduler -> Create Task.
2) Trigger: At log on.
3) Action: Start a program.
4) Program/script:

```text
C:\Windows\System32\cmd.exe
```

5) Add arguments:

```text
/c "C:\Users\AtulS\Documents\Python Scripts\start_tfl_monitor.bat"
```

Create a second task for dashboard using:

```text
/c "C:\Users\AtulS\Documents\Python Scripts\start_tfl_dashboard.bat"
```
