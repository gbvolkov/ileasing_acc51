call .\.venv\Scripts\activate.bat
.\.venv\Scripts\python.exe preanalysis_51.py --data=../FullData_ini --logfile=./data/preanalysis51_full_ini.csv > ./data/preanalysis_log_full_ini.log 2> ./data/preanalysis_err_full_ini.err
call .\.venv\Scripts\deactivate.bat