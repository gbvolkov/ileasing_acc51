call .\.venv\Scripts\activate.bat
.\.venv\Scripts\python.exe extracttablefromxls.py --data=./data/preanalysis51_full_ini.csv --done=../Done --output=./data/parsed_full --no-split --pdf --excel --logfile=./data/extract_full_log.txt --types="карточка счёта 51,карточка счета 51,выписка" > ./data/extract_full_log.log 2> ./data/extract_full_log.err
call .\.venv\Scripts\deactivate.bat