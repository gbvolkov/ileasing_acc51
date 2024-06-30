from enum import Enum
from typing import Any
import pandas as pd
from pathlib import Path
import re
import shutil
import os
from pdfminer.high_level import extract_pages
from pdfminer.layout import LTTextBoxHorizontal, LTTextLineHorizontal
import PyPDF2

def get_head_lines_pdf_2(pdfname: str):
    result: list[str] = []
    with open(pdfname,'rb') as f:
        pdf_reader = PyPDF2.PdfReader(f)
        txt = pdf_reader.pages[0].extract_text()
        lines = [x.strip() for x in txt.split("\n")]
        result.extend(line for line in lines if len(line) > 0)
    return result

def pdf_get_y_value(elem):
    return (elem.y0, -1*elem.x0)

def get_head_lines_pdf(pdf_path: str, num_lines: int = 3) -> list[str]:
    lines = []
    for page_layout in extract_pages(pdf_path, maxpages=1):
        for text_box in sorted(
            (elem for elem in page_layout if isinstance(elem, LTTextBoxHorizontal)),
            key=lambda elem: (elem.y0, -1*elem.x0),
            reverse=True,
        ):
            text = text_box.get_text()
            lines.extend(line.strip() for line in text.split("\n") if line.strip())
            if len(lines) >= num_lines:
                return lines[:num_lines]
    return lines

result = get_head_lines_pdf("../FullData\\9729289098_772901001\\[1]выписка 30.03.2021.pdf", 30)
print(result)
result = get_head_lines_pdf_2("../FullData\\9729289098_772901001\\[1]выписка 30.03.2021.pdf")
print(result)


def move2folder(fname: str, done_folder: str):
    outdir = f"{done_folder}/{os.path.split(os.path.dirname(fname))[1]}"
    outname = f"{outdir}/{os.path.basename(fname)}"
    if not os.path.isdir(outdir):
        os.mkdir(outdir)
    shutil.move(fname, outname)
    
inname = "../Data/027409994519_/1.txt"
doneFolder = "../Done"
clientid = "027409994519_"
filename = "1.txt"

move2folder(inname, doneFolder)


if not os.path.isdir(f"{doneFolder}/{clientid}/"):
    os.mkdir(f"{doneFolder}/{clientid}/")
shutil.move(inname, f"{doneFolder}/{clientid}/{filename}")
#Path(inname).rename(f"{doneFolder}/{clientid}/{filename}")

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
regex_str = r"\d{1,3}(?:\s*\d{3})*\s*\d{1,3}[.\-,]\d{0,2}"
amount = re.search(regex_str, input_str)
print(amount.group()) if amount else print("NO AMOUNT")

header1=pd.DataFrame([
    "номердокумента|дат-адокумента|датаоперации|счет|контрагент|иннконтрагента|бикбанкаконтрагента|корр.счетбанкаконтрагента|наименованиебанкаконтрагента|счётконтрагента|списание|зачисление|назначениеплатежа"
    ])

header1 = header1[0].str.lower().replace(r'[\n\.\,\(\)\/\-]', '', regex=True).replace(r'№', 'n', regex=True).replace(r'\s+', '', regex=True).replace(r'ё', 'е', regex=True)
with open("./data/experiments.out", "w", encoding='utf-8', buffering=1) as logf:
    for row in header1:
        logf.write(row + "\n")
logf.close()
