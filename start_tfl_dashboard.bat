@echo off
setlocal

cd /d "%~dp0"

python -m streamlit run "tfl_train_event_dashboard.py" --server.headless true --server.port 8501

endlocal
