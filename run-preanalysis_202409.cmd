call .\.venv\Scripts\activate.bat
.\.venv\Scripts\python.exe preanalysis_51.py --data=../Data --logfile=./data/preanalysis51_full_202409.csv > ./data/preanalysis_log_full_202409.log 2> ./data/preanalysis_err_full_202409.err
call .\.venv\Scripts\deactivate.bat