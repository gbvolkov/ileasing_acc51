call .\.venv\Scripts\activate.bat
.\.venv\Scripts\python.exe extracttablefromxls.py --data=./data/preanalysis51_full_ini.csv --done=../Done --output=./data/parsed_full --no-split --pdf --excel --logfile=./data/extract_full_log.txt --types="����窠 ���� 51,����窠 ��� 51,�믨᪠" > ./data/extract_full_log.log 2> ./data/extract_full_log.err
call .\.venv\Scripts\deactivate.bat