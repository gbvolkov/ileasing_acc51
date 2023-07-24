from enum import Enum
from typing import Any
import pandas as pd
from pathlib import Path


header1=pd.DataFrame([
    "Дата документа|Номер документа|Счёт организации|Организация|ИНН организации|Счёт контрагента|Контрагент|ИНН контрагента|Назначение платежа|Поступление|Списание|Остаток входящий|Остаток исходящий|Код дебитора|Тип документа"
	])

#res = header1[0].str.lower().replace('\n', '').replace(r'\s+', '', regex=True).replace(r'ё', 'е', regex=True).fillna("").astype(str)[0]
#res = res.replace("ё", "е")
header1 = header1[0].str.lower().replace('\n', '').replace(r'\s+', '', regex=True).replace(r'ё', 'е', regex=True).fillna("").astype(str)
with open("./data/experiments.out", "w", encoding='utf-8', buffering=1) as logf:
    for row in header1:
        logf.write(row + "\n")
logf.close()
