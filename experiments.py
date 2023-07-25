from enum import Enum
from typing import Any
import pandas as pd
from pathlib import Path


header1=pd.DataFrame([
    "номердокумента|дат-адокумента|датаоперации|счет|контрагент|иннконтрагента|бикбанкаконтрагента|корр.счетбанкаконтрагента|наименованиебанкаконтрагента|счётконтрагента|списание|зачисление|назначениеплатежа"
    ])

#res = header1[0].str.lower().replace('\n', '').replace(r'\s+', '', regex=True).replace(r'ё', 'е', regex=True).fillna("").astype(str)[0]
#res = res.replace("ё", "е")
header1 = header1[0].str.lower().replace(r'[\n\.\,\(\)\/\-]', '', regex=True).replace(r'№', 'n', regex=True).replace(r'\s+', '', regex=True).replace(r'ё', 'е', regex=True)
with open("./data/experiments.out", "w", encoding='utf-8', buffering=1) as logf:
    for row in header1:
        logf.write(row + "\n")
logf.close()
