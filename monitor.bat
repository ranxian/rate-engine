@echo off
:loop

taskkill /F /IM worker.exe /T
echo "killed workers"
start /B worker.exe
timeout /T 21600
echo "started workers"

goto loop