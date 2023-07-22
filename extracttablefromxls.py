from enum import Enum
from typing import Any
import pandas as pd
from pathlib import Path
import re
from datetime import datetime
import locale
import shutil
import os
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter, BooleanOptionalAction
import PyPDF2
import sys
from io import TextIOWrapper
import numpy as np
import csv

#using this (type: ignore) since camelot does not have stubs
import camelot # type: ignore
from pdfminer.high_level import extract_pages
from pdfminer.layout import LTTextBoxHorizontal, LTTextLineHorizontal

from const import COLUMNS
from BankStatement_1_process import BankStatement_1_process
from BankStatement_2_process import BankStatement_2_process
from BankStatement_3_process import BankStatement_3_process
from BankStatement_4_process import BankStatement_4_process
from BankStatement_5_process import BankStatement_5_process
from BankStatement_6_process import BankStatement_6_process
from BankStatement_7_process import BankStatement_7_process
from BankStatement_8_process import BankStatement_8_process
from BankStatement_9_process import BankStatement_9_process
from BankStatement_10_process import BankStatement_10_process
from BankStatement_11_process import BankStatement_11_process
from BankStatement_12_process import BankStatement_12_process
from BankStatement_13_process import BankStatement_13_process
from BankStatement_14_process import BankStatement_14_process
from BankStatement_15_process import BankStatement_15_process
from BankStatement_16_process import BankStatement_16_process
from BankStatement_17_process import BankStatement_17_process
from BankStatement_18_process import BankStatement_18_process
from BankStatement_19_process import BankStatement_19_process
from BankStatement_20_process import BankStatement_20_process
from BankStatement_21_process import BankStatement_21_process
from BankStatement_22_process import BankStatement_22_process
from BankStatement_23_process import BankStatement_23_process
from BankStatement_24_process import BankStatement_24_process
from BankStatement_25_process import BankStatement_25_process


def NoneHDR_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    datatype = "|".join(data.columns).replace('\n', ' ')
    DATATYPES.append(datatype)
    print(f"Datatype: {datatype} NOT FOUND")

    logstr = f"{datetime.now()}:NOT IMPLEMENTED:{clientid}:{os.path.basename(inname)}:{sheet}:0:\"{datatype}\"\n"
    logf.write(logstr)

    return pd.DataFrame(columns = COLUMNS)

def IgnoreHDR_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    return pd.DataFrame(columns = COLUMNS)

def TestHDR_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    NoneHDR_process(header, data, footer, inname, clientid, sheet, logf)
    
    nameparts = os.path.split(inname)
    fname = os.path.splitext(nameparts[1])[0]

    headerfname = os.path.join(nameparts[0], f"{fname}_{sheet}_header.csv") # type: ignore
    datafname = os.path.join(nameparts[0], f"{fname}_{sheet}_data.csv") # type: ignore
    footerfname = os.path.join(nameparts[0], f"{fname}_{sheet}_footer.csv") # type: ignore

    header.to_csv(headerfname, mode="w", header=not Path(headerfname).is_file(), index=False)
    data.to_csv(datafname, mode="w", header=not Path(datafname).is_file(), index=False)
    footer.to_csv(footerfname, mode="w", header=not Path(footerfname).is_file(), index=False)

    return pd.DataFrame(columns = COLUMNS)


HDRSIGNATURES = [{"Дата документа|Дата операции|№|БИК|Счет|Контрагент|ИНН контрагента|БИК банка контрагента|Корр.счет банка контрагента|Наименование банка контрагента|Счет контрагента|Списание|Зачисление|Назначение платежа|Код": BankStatement_1_process},
                 {"Дата документа|Дата операции|№|БИК|Счет|Контрагент|ИНН контрагента|БИК банка контрагента|Корр.счет банка контрагента|Наименование банка контрагента|Счет контрагента|Списание|Зачисление|Назначение платежа|Код|Показатель статуса (101)|Код дохода/бюджетной классификации (104)|Код ОКТМО (105)|Показатель основания платежа (106)|Показатель налогового периода/код таможенного органа (107)|Показатель номера документа (108)|Показатель даты документа (109)|Показатель типа платежа (110)": BankStatement_1_process},
                 {"Дата|Вид (шифр) операции (ВО)|Номер документа Банка|Номер документа|БИК банка корреспондента|Корреспондирующий счет|Сумма по дебету|Сумма по кредиту": BankStatement_2_process},
                 {"Дата операции|Номер документа|Дебет|Кредит|Контрагент.Наименование |Контрагент.ИНН |Контрагент.КПП |Контрагент.Счет |Контрагент.БИК |Контрагент.Наименование банка |Назначение платежа|Тип документа": BankStatement_3_process},
                 {"Дата операции|Номер документа|Дебет|Кредит|Контрагент.Наименование |Контрагент.ИНН |Контрагент.КПП |Контрагент.Счет |Контрагент.БИК |Контрагент.Наименование банка |Назначение платежа|Код дебитора|Тип документа": BankStatement_3_process},
                 {"№ док|Дата документа|Дата операции|Реквизиты корреспондента.Наименование|Реквизиты корреспондента.Счет|Реквизиты корреспондента.ИНН Контрагента|Реквизиты корреспондента.Банк|Дебет Сумма/Сумма в НП|Кредит Сумма/Сумма в НП|Курс ЦБ на дату операции|Основание операции (назначение платежа)": BankStatement_4_process},
                 {"Дата|Номер документа|Дебет|Кредит|Контрагент.Наименование|Контрагент.ИНН|Контрагент.КПП|Контрагент.БИК|Контрагент.Наименование банка|Назначение платежа|Тип документа": BankStatement_5_process},
                 {"Дата|Номер документа|Дебет|Кредит|Контрагент.Наименование|Контрагент.ИНН|Контрагент.КПП|Контрагент.БИК|Контрагент.Наименование банка|Назначение платежа|Код дебитора|Тип документа": BankStatement_5_process},
                 {"Дата|Номер документа|Дебет|Кредит|Контрагент.Наименование|Контрагент.ИНН|Контрагент.КПП|Контрагент.Счет|Контрагент.БИК|Контрагент.Наименование банка|Назначение платежа|Код дебитора|Тип документа": BankStatement_5_process},
                 {"Дата|Номер документа|Дебет|Кредит|Контрагент.Наименование|Контрагент.ИНН|Контрагент.КПП|Контрагент.Счёт|Контрагент.БИК|Контрагент.Наименование банка|Назначение платежа|Код дебитора|Тип документа": BankStatement_5_process},                 
                 {"Номер документа|Дата документа|Дата операции|Счёт|Контрагент|ИНН контрагента|БИК банка контрагента|Корр.счёт банка контрагента|Наименование банка контрагента|Счёт контрагента|Списание|Зачисление|Назначение платежа": BankStatement_6_process},
                 {"Template Code|repStatementsRurExcel.xls": IgnoreHDR_process},
                 {"Номер|Контрагент|Реквизиты контрагента|Назначение платежа|Дебет|Кредит": IgnoreHDR_process},
                 {"XDO_?accountNumber?|<?/data/ean?>|<?/data/ean?>": IgnoreHDR_process},
                 {"Дата проводки|Счет.Дебет|Счет.Кредит|Сумма по дебету|Сумма по кредиту|№ документа|ВО|Банк (БИК и наименование)|Назначение платежа": BankStatement_7_process},
                 #{"Дата проводки|Дата проводки|Счет.Дебет|Счет|Счет.Кредит|Сумма по дебету|Сумма по дебету|Сумма по кредиту|№ документа|ВО|Банк (БИК и наименование)|Банк (БИК и наименование)|Назначение платежа": BankStatement_7_process},
                 {"Дата опер.|КО|Номер докум.|Дата докум.|Дебет|Кредит|Рублевое покрытие|Контрагент.ИНН|Контрагент.КПП|Контрагент.Наименование|Контрагент.Счет|Контрагент.БИК|Контрагент.Коррсчет|Контрагент.Банк|Клиент.ИНН|Клиент.Наименование|Клиент.Счет|Клиент.КПП|Клиент.БИК|Клиент.Коррсчет|Клиент.Банк|Код|Назначение платежа|Очер. платежа|Бюджетный платеж.Статус сост.|Бюджетный платеж.КБК|Бюджетный платеж.ОКТМО|Бюджетный платеж.Основание|Бюджетный платеж.Налог. период|Бюджетный платеж.Номер док.|ID опер.": BankStatement_8_process},
                 {"Дата опер.|КО|Номер докум.|Дата докум.|Дебет|Кредит|Рублевое покрытие|Контрагент.ИНН|Контрагент.КПП|Контрагент.Наименование|Контрагент.Счет|Контрагент.БИК|Контрагент.Коррсчет|Контрагент.Банк|Клиент.ИНН|Клиент.Наименование|Клиент.Счет|Клиент.КПП|Клиент.БИК|Клиент.Коррсчет|Клиент.Банк|Назначение платежа|Очер. платежа|ID опер.": BankStatement_8_process},
                 {"Дата опер.|КО|Номер докум.|Дата докум.|Дебет|Кредит|Рублевое покрытие|Контрагент.ИНН|Контрагент.КПП|Контрагент.Наименование|Контрагент.Счет|Контрагент.БИК|Контрагент.Коррсчет|Контрагент.Банк|Клиент.ИНН|Клиент.Наименование|Клиент.Счет|Клиент.КПП|Клиент.БИК|Клиент.Коррсчет|Клиент.Банк|Рез. Поле|Код|Код выплат|Назначение платежа|Очер. платежа|Вид условия оплаты|Основание для списания|Бюджетный платеж.Статус сост.|Бюджетный платеж.КБК|Бюджетный платеж.ОКТМО|Бюджетный платеж.Основание|Бюджетный платеж.Налог. период|Бюджетный платеж.Номер док.|Бюджетный платеж.Дата док.|ID опер.": BankStatement_8_process},
                 {"Дата опер.|КО|Номер докум.|Дата докум.|Дебет|Кредит|Рублевое покрытие|Контрагент.ИНН|Контрагент.КПП|Контрагент.Наименование|Контрагент.Счет|Контрагент.БИК|Контрагент.Коррсчет|Контрагент.Банк|Клиент.ИНН|Клиент.Наименование|Клиент.Счет|Клиент.КПП|Клиент.БИК|Клиент.Коррсчет|Клиент.Банк|Рез. Поле|Код|Код выплат|Назначение платежа|Очер. платежа|Вид условия оплаты|Основание для списания|Бюджетный платеж.Статус сост.|Бюджетный платеж.КБК|Бюджетный платеж.ОКТМО|Бюджетный платеж.Основание|Бюджетный платеж.Налог. период|Бюджетный платеж.Номер док.|Бюджетный платеж.Дата док.|ID опер.|ID докум.": BankStatement_8_process},
                 {"Дата операции|№ док.|Вид операции|Контрагент|ИНН контрагента|БИК банка контрагента|Лицевой счет|Дебет|Кредит|Назначение": BankStatement_9_process},
                 {"Дата операции|№ док.|Вид операции|Контрагент|ИНН контрагента|БИК банка контрагента|Лицевой счет|Дебет|Кредит|Назначение|Сумма в нац. покрытии|Курс": BankStatement_9_process},
                 {"Дата|Вид опер.|№ док.|БИК|Банк контрагента|Контрагент|ИНН контрагента|Счёт контрагента|Дебет (RUB)|Кредит (RUB)|Операция": BankStatement_10_process},
                 {"Дата|Номер|Вид операции|Контрагент|ИНН контрагента|БИК банка контрагента|Счет контрагента|Дебет, RUR|Кредит, RUR|Назначение": BankStatement_11_process},
                 {"Дата|РО|Док.|КБ|Внеш.счет|Счет|Дебет|Кредит|Назначение|Контрагент|Контр. ИНН": BankStatement_12_process},
                 {"Документ|Дата операции|Корреспондент.Наименование|Корреспондент.ИНН|Корреспондент.КПП|Корреспондент.Счет|Корреспондент.БИК|Вх.остаток|Оборот Дт|Оборот Кт|Назначение платежа": BankStatement_13_process},
                 {"Тип|Дата|Номер|Вид операции|Сумма|Валюта|Основание платежа|БИК Банка получателя|Счет Получателя|Наименование Получателя": BankStatement_14_process},
                 {"№ П/П|Дата операции / Posting date|Дата валютир. / Value|Вид опер. / Op. type|Номер документа / Document number|Реквизиты корреспондента /Counter party details.Наименование / Name|Реквизиты корреспондента /Counter party details.Счет / Account|Реквизиты корреспондента /Counter party details.Банк / Bank|Дебет / Debit|Кредит / Credit|Основание операции (назначение платежа) / Payment details": BankStatement_15_process},
                 {"№ документа|Дата|БИК|№ Счёта|Деб. оборот|Кред. оборот|ИНН и наименование получателя|Назначение платежа": BankStatement_16_process},
                 {"Дата|№ док.|ВО|Банк контрагента|Контрагент|Счет контрагента|Дебет|Кредит|Назначение платежа": BankStatement_17_process},
                 {"№ п/п|Дата совершения операции (дд.мм.гг)|Реквизиты документа, на основании которого была совершена операция по счету (специальному банковскому счету).вид (шифр)|Реквизиты документа, на основании которого была совершена операция по счету (специальному банковскому счету).номер|Реквизиты документа, на основании которого была совершена операция по счету (специальному банковскому счету).дата|Реквизиты банка плательщика/получателя денежных средств.номер корреспондентского счета|Реквизиты банка плательщика/получателя денежных средств.наименование|Реквизиты банка плательщика/получателя денежных средств.БИК|Реквизиты плательщика/получателя денежных средств.наименование/Ф.И.О.|Реквизиты плательщика/получателя денежных средств.ИНН/КИО|Реквизиты плательщика/получателя денежных средств.КПП|Реквизиты плательщика/получателя денежных средств.номер счета (специального банковского счета)|Сумма операции по счету (специальному банковскому счету).по дебету|Сумма операции по счету (специальному банковскому счету).по кредиту|Назначение платежа": BankStatement_18_process},
                 {"Номер|Номер счёта|Дата|Контрагент cчёт|Контрагент|Поступление|Валюта|Списание|Валюта|Назначение": BankStatement_19_process},
                 {"№ п.п|№ док.|Дата операции|БИК/SWIFT банка плат.|Наименование Банка плательщика|Наименование плательщика|ИНН плательщика|№ счета плательщика|БИК/SWIFT банка получ.|Наименование банка получателя|Наименование получателя|ИНН получателя|№ счета получателя|Сальдо входящее|Дебет|Кредит|Сальдо исходящее|Назначение платежа": BankStatement_20_process},
                 {"Дата док.|№ док.|Дата операции|ВО|Название корр.|ИНН корр.|БИК банка корр.|Счет корр.|Дебет|Кредит|Назначение": BankStatement_21_process},
                 {"|№ п.п|№ док.|Дата операции|БИК/SWIFT банка плат.|Наименование Банка плательщика|Наименование плательщика|ИНН плательщика|№ счета плательщика|БИК/SWIFT банка получ.|Наименование банка получателя|Наименование получателя|ИНН получателя|№ счета получателя|Сальдо входящее|Дебет|Кредит|Сальдо исходящее|Назначение платежа": BankStatement_20_process},
                 {"Дата|№ док.|ВО|Название корр.|ИНН контрагента|БИК банка корр.|Лицевой счет|Дебет|Кредит|Назначение": BankStatement_22_process},
                 {"Номер строки|Дата проводки|Вид операции|Номер документа|Счет плательщика/получателя|Реквизиты плательщика/получателя денежных средств.Наименование/ФИО|Реквизиты плательщика/получателя денежных средств.ИНН/КИО|Реквизиты плательщика/получателя денежных средств.КПП|Сумма Дебет|Сумма Кредит|Назначение платежа": BankStatement_23_process},
                 {"Дата|№|Клиент.ИНН|Клиент.Наименование|Клиент.Счет|Корреспондент.БИК|Корреспондент.Счет|Корреспондент.Наименование|В О|Содержание|Обороты.Дебет|Обороты.Кредит": BankStatement_24_process},
                 {"Дата и время проводки|Счет корреспондента|Дебет|Кредит|Исходящий остаток|Наименование корреспондента|ИНН корреспондента|Назначение платежа": BankStatement_25_process},
                 ]

"""
Берём первые пятдесят строк
Сливаем каждую строку со следующей
В результирующем датасете ищем первую строку с минимальным количеством нулов
"""
def findHeaderRow(df: pd.DataFrame) -> tuple[int, int, list[int]]:
    df = df.iloc[:50].fillna("").astype(str)
    result = pd.DataFrame(columns=["_idx", "_cnas", "_header"])
    axis=0
    # Delete rows containing either 60% or more than 60% NaN Values
    perc = 50.0 
    df = df.replace('\n', '').replace(r'\s+', '', regex=True).replace(r'\d+\.?\d*', '', regex=True).fillna("").astype(str)
    maxnotna = df.mask(df == '').notna().sum(axis=1).max()
    min_count =  int((perc*maxnotna/100) + 1)
    #df = df.dropna( axis=0, thresh=min_count)

    for idx in range(len(df.index)-1):
        
        header1 = df.iloc[idx].replace('\n', '').replace(r'\s+', '', regex=True).replace(r'\d+\.?\d*', '', regex=True).fillna("").astype(str)
        if header1.mask(header1 == '').notna().sum() >= min_count:
            header2 = df.iloc[idx+1].fillna("").astype(str)
            header = pd.concat([header1, header2], axis=1).apply(
                lambda x: '.'.join([y for y in x if y]), axis=1)
            cnas = header.mask(header == '').isna().sum()
            rowidx = df.iloc[idx:idx+1].index[0]
            #newline = pd.DataFrame()
            #result = pd.concat([result, pd.DataFrame([{"_idx": rowidx, "_cnas": cnas, "_header": "|".join(header)}])])
            result = pd.concat([result, pd.DataFrame([{"_idx": rowidx, "_cnas": cnas, "_header": header}])])
    result = result[result._idx==result[result._cnas==result._cnas.min()]._idx.min()]
    header = result._header[0]
    return result[result._cnas==result._cnas.min()]._idx.min(), header.mask(header=='').notna().sum(), header.mask(header=='').dropna().index.to_list()


def getTableRange(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    firstrow = 0
    lastrow = len(df.index)-1
    ncols = len(df.columns)
    footer = pd.DataFrame()

    firstrowidx, nheadercols, headercols = findHeaderRow(df)

    #Удаляем из хвоста все столбцы, где больше 90% значений NaN
    partialColumns = (df.isnull().sum() > lastrow * 0.9)
    for idx in range(len(partialColumns.index)-1, 0, -1):
        if not partialColumns.iloc[idx]:
            break
        ncols-=1
    dfFilled = df.iloc[:,:ncols]


    #for idx in range(len(df.index)):
    #    cnavalues = dfFilled.iloc[idx].isnull().sum()
    #    if cnavalues*100/ncols < 53 and not all(dfFilled.iloc[idx][ncols-3:].isnull()):
    #        firstrow = idx
    #        break
    
    #firstrowidx = df.dropna(subset=[df.columns[-3], df.columns[-2], df.columns[-1]], how='all').isna().sum(axis=1).idxmin()
    lastrpowidx = firstrowidx + 1 # type: ignore

    for idx in range(len(df.index)-1, 0, -1):
        cnavalues = dfFilled.iloc[idx].isnull().sum()
        if (ncols-cnavalues)*100/nheadercols > 47 and not all(dfFilled.iloc[idx][ncols-3:].isnull()):
            lastrow = idx
            lastrpowidx = df.iloc[lastrow:lastrow+1].index[0]
            break
    

    #header = df.iloc[:firstrow].dropna(axis=1,how='all').dropna(axis=0,how='all')
    #footer = df.iloc[lastrow + 1 : ].dropna(axis=1,how='all').dropna(axis=0,how='all')
    header = df.loc[:firstrowidx-1].dropna(axis=1,how='all').dropna(axis=0,how='all') # type: ignore
    footer = df.loc[lastrpowidx+1 : ].dropna(axis=1,how='all').dropna(axis=0,how='all') # type: ignore


    #df = df.iloc[firstrow : lastrow + 1]
    df = df.loc[firstrowidx : lastrpowidx, headercols]
    #Удаляем из головы все столбцы, где больше 40% значений NaN
    #lastrow = len(df.index)
    #scol = 0
    #partialColumns = (df.isnull().sum() > lastrow * 0.7)
    #for idx in range(len(partialColumns.index)-1):
    #    if not partialColumns.iloc[idx]:
    #        break
    #    scol+=1
    #df = df.iloc[:,scol:]

    #data = df.iloc[firstrow : lastrow + 1].dropna(axis=1,how='all').dropna(axis=0,how='all')
    data = df.dropna(axis=1,how='all').dropna(axis=0,how='all')
    return (
		header,
		data,
		footer,
	)

def setDataColumns(df) -> pd.DataFrame:
    header1 = df.iloc[0]
    header1 = header1.fillna(method='ffill').fillna("")
    datastart = 1
    #Здеесь возможно надо проверять не на null, а на naп - intger или дата (через regex)
    if df.iloc[1].isnull().iloc[0]:
        header2 = df.iloc[1].fillna("")
        header = pd.concat([header1, header2], axis=1).apply(
            lambda x: '.'.join([y for y in x if y]), axis=1)
        datastart = 2
    else:
        header = header1
    #header = header.drop_duplicates()
    df = df[datastart:]
    df.columns = header.str.replace('\n', ' ').replace(r'\s+', ' ', regex=True).fillna("column")
    ncols = len(df.columns)
    return df[df[list(df.columns)].isnull().sum(axis=1) < ncols * 0.8].dropna(
        axis=0, how='all'
    )

DATATYPES: list[str] = []

#removes rows, containint 1, 2, 3, 4, 5, ... (assuming that is just rows with columns numbers, which should be ignored)
def cleanupRawData(df: pd.DataFrame) -> pd.DataFrame:
    ncols = len(df.columns)
    row2del = "_".join([str(x) for x in range(1, ncols+1)])

    df["__rowval"] = pd.Series(df.fillna("").replace(r'\s+', '', regex=True).values.tolist()).str.join('_').values
    df = df[df.__rowval != row2del]

    return df.drop("__rowval", axis=1)

#sets to_ignore to True, if entrDate is empty
def cleanupProcessedData(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df.entryDate.notna()]
    return df

def processExcel(inname: str, clientid: str, logf: TextIOWrapper) -> tuple[pd.DataFrame, int, bool]:
    berror = False
    df = pd.DataFrame()
    data = pd.DataFrame()
    result = pd.DataFrame()
    nameparts = os.path.split(inname)
    #fname = os.path.splitext(nameparts[1])[0]

    sheets = pd.read_excel(inname, header=None, sheet_name=None)
    if len(sheets) > 1 :
        print(f"{datetime.now()}:{inname}:WARNING:{len(sheets)} sheets found")
    for sheet in sheets:
        try:
            df = sheets[sheet]
            df = df.dropna(axis=1,how='all')
            if not df.empty:
                header, data, footer = getTableRange(df)

                data = setDataColumns(data)
                data = cleanupRawData(data)
                signature = "|".join(data.columns).replace('\n', ' ')
                funcs = list(filter(lambda item: item is not None, [sig.get(signature) for sig in HDRSIGNATURES]))
                func = funcs[0] if funcs else NoneHDR_process
                outdata = func(header, data, footer, inname, clientid, sheet, logf) # type: ignore
                outdata = cleanupProcessedData(outdata)
                result = pd.concat([result, outdata])

        except Exception as err:
            berror = True   
            print(f"{datetime.now()}:{inname}_{sheet}:ERROR:{err}")
            logstr = f"{datetime.now()}:ERROR:{clientid}:{os.path.basename(inname)}:{sheet}:0:{type(err).__name__} {str(err)}\n"
            logf.write(logstr)
    return (result, len(sheets), berror)

def processOther(inname: str, clientid: str, logf: TextIOWrapper) -> tuple[pd.DataFrame, int, bool]:
    return (pd.DataFrame(), 0, True)

def process(inname: str, clientid: str, logf: TextIOWrapper) -> tuple[pd.DataFrame, int, bool]:
    processFunc = processOther

    if inname.lower().endswith('.xls') or inname.lower().endswith('.xlsx'):
        processFunc = processExcel
        
    df, pages, berror = processFunc(inname, clientid, logf)
    return (df, pages, berror)


def runParsing(clientid, outname, inname, doneFolder, logf) -> int:
    filename = os.path.basename(inname)
    print(f"{datetime.now()}:START: {clientid}: {filename}")


    df, pages, berror = process(inname, clientid, logf)
    if not berror:
        if not df.empty:
            df.to_csv(outname, mode="a+", header=not Path(outname).is_file(), index=False)
            logstr = f"{datetime.now()}:PROCESSED: {clientid}:{filename}:{pages}:{str(df.shape[0])}:{outname}\n"
            #shutil.move(inname, doneFolder + clientid + '_' + filename)
        else: 
            berror = True
            logstr = f"{datetime.now()}:EMPTY: {clientid}:{filename}:{pages}:0:{outname}\n"
        logf.write(logstr)
    print(f"{datetime.now()}:DONE: {clientid}: {filename}")
    return not berror

def getFilesList(log: str) -> list[str]:
    df = pd.read_csv(log, on_bad_lines='skip', names=['status', 'clientid', 'filename', 'sheets', 'doctype', 'filetype', 'error'], delimiter='|')
    filelist = df['filename'][(df['status']=='PROCESSED') & (df['doctype'] == 'выписка')]
    return pd.Series(filelist).to_list()

def main():
    sys.stdout.reconfigure(encoding="utf-8") # type: ignore

    preanalysislog, logname, outbasename, bSplit, maxFiles, doneFolder, FILEEXT = getParameters()
    cnt = 0
    outname = outbasename + ".csv"
    DIRPATH = '../Data'
    fileslist = getFilesList(preanalysislog)
    with open(logname, "w", encoding='utf-8', buffering=1) as logf:

        sys.stdout.reconfigure(encoding="utf-8", line_buffering=True) # type: ignore
        print(f"START:{datetime.now()}\ninput:{DIRPATH}\nlog:{logname}\noutput:{outname}\nsplit:{bSplit}\nmaxinput:{maxFiles}\ndone:{doneFolder}\nextensions:{FILEEXT}")

  
        #for file in fileslist:
        for fname in filter(lambda file: any(ext for ext in FILEEXT if (file.lower().endswith(ext))), fileslist):
            if Path(fname).is_file():
                parts = os.path.split(os.path.dirname(fname))
                clientid = parts[1]
                inname = fname
                try:
                    pages = 0
                    if bSplit and cnt % maxFiles == 0:
                        outname = outbasename + str(cnt) + ".csv"
                    try :
                        cnt += runParsing(clientid, outname, inname, doneFolder, logf)
                    except Exception as err:
                        logf.write(f"{datetime.now()}:FILE_ERROR:{clientid}:{os.path.basename(inname)}:{pages}::{type(err).__name__} {str(err)}\n")
                        print(f"{datetime.now()}:{inname}:ERROR:{err}")
                except Exception as err:
                    print(f"{datetime.now()}:{clientid}:!!!CRITICAL ERROR!!! {err}")
                    logf.write(f"{datetime.now()}:CRITICAL ERROR:{clientid}:ND:ND:ERROR\n")

    #with open('datatypes.csv', 'w', encoding='utf-8') as f:
    df = pd.DataFrame(np.unique(DATATYPES))
    df.to_csv("./data/datatypes.csv", mode="w", index = False)

def getFileExtList(isExcel, isPDF) -> list[str]:
    FILEEXT = []
    if isExcel:
        FILEEXT += ['.xls', '.xlsx']
    if isPDF:
        FILEEXT += ['.pdf']
    return FILEEXT

def getArguments():
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument("-d", "--data", default="./data/test_preanalysis.csv", help="Data folder")
    parser.add_argument("-r", "--done", default="./data/Done", help="Done folder")
    parser.add_argument("-l", "--logfile", default="./data/test_parsing_log.log.txt", help="Log file")
    parser.add_argument("-o", "--output", default="./data/test_parsed_statements", help="Resulting file name (no extension)")
    parser.add_argument("--split", default=True, action=BooleanOptionalAction, help="Weather splitting resulting file required (--no-spilt opposite option)")
    parser.add_argument("-m", "--maxinput", default=500, type=int, help="Maximum files sored in one resulting file")
    parser.add_argument("--pdf", default=False, action=BooleanOptionalAction, help="Weather to include pdf (--no-pdf opposite option)")
    parser.add_argument("--excel", default=True, action=BooleanOptionalAction, help="Weather to include excel files (--no-excel opposite option)")
    return vars(parser.parse_args())

def getParameters():
    args = getArguments()

    preanalysislog = args["data"]
    logname = args["logfile"]
    outbasename = args["output"]
    bSplit = args["split"]
    maxFiles = args["maxinput"]
    doneFolder = args["done"] + "/"
    FILEEXT = getFileExtList(args["excel"], args["pdf"])
    return preanalysislog,logname,outbasename,bSplit,maxFiles,doneFolder,FILEEXT

if __name__ == "__main__":
    DATATYPES = []
    main()
