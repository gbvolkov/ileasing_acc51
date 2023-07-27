from enum import Enum
from typing import Any
import pandas as pd
from pathlib import Path
import re

hdr = "17.11.2022|Сбе|1111111111111|рБизнес 41.018.02_0002|Филиал Публичного акционерного общества Сбербанк России Адыгейское отделение № 8620|ПАО Сбербанк|Дата формирования выписки 17.11.2022 в 16:11:27|ВЫПИСКА ОПЕРАЦИЙ ПО ЛИЦЕВОМУ СЧЕТУ 40802810601000005388|Глава крестьянского (фермерского) хозяйства КРАМАРЕНКО ВИТАЛИЙ ПЕТРОВИЧ|за период с|21 июня 2021 г.|по|21 декабря 2021 г.|Российский рубль|Дата предыдущей операции по счету 12 марта 2021 г.|Дата|проводки|Счет|Сумма по дебету Сумма по кредиту № документа ВО Банк (БИК и наименов"
acc = re.search(r"\b\d{20}\b", hdr)
inn = re.search(r"(\b\d{10}\b)|(\b\d{12}\b)", hdr)

df = pd.DataFrame([
    {"c1": None, "c2" : "v2"}
    ,{"c1" : "v3", "c2" : "v4"}
    ,{"c2": "v6"}
    ]).dropna(axis=0,how='all')

#print(df)
res = ".".join(df[:].apply(
        lambda x: '.'.join(x.dropna()), axis=1
    ))

input_str = "Входящий остаток 2 33 34234 45.23 ратабор 34566.233"
regex_str = r"(?:(?:[0-9]{1,3}){1}(?:\s*[0-9]{3})*\s*[0-9]{1,3})[.\-,][0-9]{0,2}"
#regex_str = r"[-+]?(([0-9]{0,3}\s?[0-9]{3})*[0-9]{,3}[\.\-\,][0-9]+|[0-9]+)"
amount = re.search(regex_str, input_str)
print(amount.group()) if amount else print("NO AMOUNT")

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
