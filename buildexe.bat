cxfreeze monitor.py
cxfreeze worker.py
copy rate_run.pyd dist
copy dist/worker.exe worker.exe 
copy dist/monitor.exe monitor.exe
pause

