@echo off
cd /D "C:\Users\ojeku\OneDrive\Documents\GitHub\Twix0514"
:loop
C:\Python314\python.exe bot.py >> bot_stderr.log 2>&1
echo [%date% %time%] Bot exited — restarting in 5s >> bot_stderr.log
timeout /t 5 /nobreak >nul
goto loop
