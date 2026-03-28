@echo off
setlocal

cd /d "%~dp0"

REM Optional TfL credentials (uncomment and set)
REM set TFL_APP_ID=your_app_id
REM set TFL_APP_KEY=your_app_key

python "tfl_train_event_elo.py" monitor --line-ids bakerloo,central,circle,district,hammersmith-city,jubilee,metropolitan,northern,piccadilly,victoria,waterloo-city --max-stops-per-line 8 --interval 60 --iterations 0 --lookback-hours 6 --out-csv "tfl_train_event_elo_leaderboard.csv"

endlocal
