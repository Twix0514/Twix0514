@echo off
title TWIX WATCHDOG
cd /d "%~dp0"
echo Starting watchdog...
python watchdog.py
pause
