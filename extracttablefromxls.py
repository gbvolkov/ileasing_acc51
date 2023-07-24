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

from const import COLUMNS, DOCTYPES
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
from BankStatement_26_process import BankStatement_26_process
from BankStatement_27_process import BankStatement_27_process
from BankStatement_28_process import BankStatement_28_process
from BankStatement_29_process import BankStatement_29_process
from BankStatement_30_process import BankStatement_30_process
from BankStatement_31_process import BankStatement_31_process
from BankStatement_32_process import BankStatement_32_process
from BankStatement_33_process import BankStatement_33_process
from BankStatement_34_process import BankStatement_34_process
from BankStatement_35_process import BankStatement_35_process
from BankStatement_36_process import BankStatement_36_process


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

    header.to_csv(headerfname, mode="w", header=True, index=False)
    data.to_csv(datafname, mode="w", header=True, index=False)
    footer.to_csv(footerfname, mode="w", header=True, index=False)

    return pd.DataFrame(columns = COLUMNS)


HDRSIGNATURES = [{"датадокумента|датаоперации|№|бик|счет|контрагент|иннконтрагента|бикбанкаконтрагента|корр.счетбанкаконтрагента|наименованиебанкаконтрагента|счетконтрагента|списание|зачисление|назначениеплатежа|код": BankStatement_1_process},
                 {"датадокумента|датаоперации|№|бик|счет|контрагент|иннконтрагента|бикбанкаконтрагента|корр.счетбанкаконтрагента|наименованиебанкаконтрагента|счетконтрагента|списание|зачисление|назначениеплатежа|код|показательстатуса(101)|коддохода/бюджетнойклассификации(104)|кодоктмо(105)|показательоснованияплатежа(106)|показательналоговогопериода/кодтаможенногооргана(107)|показательномерадокумента(108)|показательдатыдокумента(109)|показательтипаплатежа(110)": BankStatement_1_process},
                 {"дата|вид(шифр)операции(во)|номердокументабанка|номердокумента|бикбанкакорреспондента|корреспондирующийсчет|суммаподебету|суммапокредиту": BankStatement_2_process},
                 {"датаоперации|номердокумента|дебет|кредит|контрагент.наименование|контрагент.инн|контрагент.кпп|контрагент.бик|контрагент.наименованиебанка|назначениеплатежа|коддебитора|типдокумента": BankStatement_3_process},
                 {"датаоперации|номердокумента|дебет|кредит|контрагент.наименование|контрагент.инн|контрагент.кпп|контрагент.счет|контрагент.бик|контрагент.наименованиебанка|назначениеплатежа|коддебитора|типдокумента": BankStatement_3_process},
                 {"№док|датадокумента|датаоперации|реквизитыкорреспондента.наименование|реквизитыкорреспондента.счет|реквизитыкорреспондента.иннконтрагента|реквизитыкорреспондента.банк|дебетсумма/суммавнп|кредитсумма/суммавнп|курсцбнадатуоперации|основаниеоперации(назначениеплатежа)": BankStatement_4_process},
                 {"дата|номердокумента|дебет|кредит|контрагент.наименование|контрагент.инн|контрагент.кпп|контрагент.бик|контрагент.наименованиебанка|назначениеплатежа|типдокумента": BankStatement_5_process},
                 {"дата|номердокумента|дебет|кредит|контрагент.наименование|контрагент.инн|контрагент.кпп|контрагент.бик|контрагент.наименованиебанка|назначениеплатежа|коддебитора|типдокумента": BankStatement_5_process},
                 {"дата|номердокумента|дебет|кредит|контрагент.наименование|контрагент.инн|контрагент.кпп|контрагент.счет|контрагент.бик|контрагент.наименованиебанка|назначениеплатежа|коддебитора|типдокумента": BankStatement_5_process},
                 {"номердокумента|датадокумента|датаоперации|счет|контрагент|иннконтрагента|бикбанкаконтрагента|корр.счетбанкаконтрагента|наименованиебанкаконтрагента|счётконтрагента|списание|зачисление|назначениеплатежа": BankStatement_6_process},
                 {"templatecode|repstatementsrurexcel.xls": IgnoreHDR_process},
                 {"номер|контрагент|реквизитыконтрагента|назначениеплатежа|дебет|кредит": IgnoreHDR_process},
                 {"xdo_?accountnumber?|<?/data/ean?>|<?/data/ean?>": IgnoreHDR_process},
                 {"датапроводки|счет.дебет|счет.кредит|суммаподебету|суммапокредиту|№документа|во|банк(бикинаименование)|назначениеплатежа": BankStatement_7_process},
                 {"датаопер.|ко|номердокум.|датадокум.|дебет|кредит|рублевоепокрытие|контрагент.инн|контрагент.кпп|контрагент.наименование|контрагент.счет|контрагент.бик|контрагент.коррсчет|контрагент.банк|клиент.инн|клиент.наименование|клиент.счет|клиент.кпп|клиент.бин|клиент.коррсчет|клиент.банк|код|назначениеплатежа|очер.платежа|бюджетныйплатеж.статуссост.|бюджетныйплатеж.кбк|бюджетныйплатеж.октмо|бюджетныйплатеж.основание|бюджетныйплатеж.налог.период|бюджетныйплатеж.номердок.|idопер.": BankStatement_8_process},
                 {"датаопер.|ко|номердокум.|датадокум.|дебет|кредит|рублевоепокрытие|контрагент.инн|контрагент.кпп|контрагент.наименование|контрагент.счет|контрагент.бик|контрагент.коррсчет|контрагент.банк|клиент.инн|клиент.наименование|клиент.счет|клиент.кпп|клиент.бин|клиент.коррсчет|клиент.банк|назначениеплатежа|очер.платежа|idопер.": BankStatement_8_process},
                 {"датаопер.|ко|номердокум.|датадокум.|дебет|кредит|рублевоепокрытие|контрагент.инн|контрагент.кпп|контрагент.наименование|контрагент.счет|контрагент.бик|контрагент.коррсчет|контрагент.банк|клиент.инн|клиент.наименование|клиент.счет|клиент.кпп|клиент.бик|клиент.коррсчет|клиент.банк|рез.поле|код|кодвыплат|назначениеплатежа|очер.платежа|видусловияоплаты|основаниедлясписания|бюджетныйплатеж.статуссост.|бюджетныйплатеж.кбк|бюджетныйплатеж.октмо|бюджетныйплатеж.основание|бюджетныйплатеж.налог.период|бюджетныйплатеж.номердок.|бюджетныйплатеж.датадок.|idопер.": BankStatement_8_process},
                 {"датаопер.|ко|номердокум.|датадокум.|дебет|кредит|рублевоепокрытие|контрагент.инн|контрагент.кпп|контрагент.наименование|контрагент.счет|контрагент.бик|контрагент.коррсчет|контрагент.банк|клиент.инн|клиент.наименование|клиент.счет|клиент.кпп|клиент.бик|клиент.коррсчет|клиент.банк|рез.поле|код|кодвыплат|назначениеплатежа|очер.платежа|видусловияоплаты|основаниедлясписания|бюджетныйплатеж.статуссост.|бюджетныйплатеж.кбк|бюджетныйплатеж.октмо|бюджетныйплатеж.основание|бюджетныйплатеж.налог.период|бюджетныйплатеж.номердок.|бюджетныйплатеж.датадок.|idопер.|idдокум.": BankStatement_8_process},
                 {"датаопер.|ко|номердокум.|датадокум.|дебет|кредит|рублевоепокрытие|контрагент.инн|контрагент.кпп|контрагент.наименование|контрагент.счет|контрагент.бик|контрагент.коррсчет|контрагент.банк|клиент.инн|клиент.наименование|клиент.счет|клиент.кпп|клиент.бик|клиент.коррсчет|клиент.банк|рез.поле|код|кодвыплат|назначениеплатежа|очер.платежа|видусловияоплаты|бюджетныйплатеж.статуссост.|бюджетныйплатеж.кбк|бюджетныйплатеж.октмо|бюджетныйплатеж.основание|бюджетныйплатеж.налог.период|бюджетныйплатеж.номердок.|бюджетныйплатеж.датадок.|idопер.|idдокум.": BankStatement_8_process},
                 {"датаоперации|№док.|видоперации|контрагент|иннконтрагента|бикбанкаконтрагента|лицевойсчет|дебет|кредит|назначение": BankStatement_9_process},
                 {"датаоперации|№док.|видоперации|контрагент|иннконтрагента|бикбанкаконтрагента|лицевойсчет|дебет|кредит|назначение|суммавнац.покрытии|курс": BankStatement_9_process},
                 {"дата|видопер.|№док.|бик|банкконтрагента|контрагент|иннконтрагента|счетконтрагента|дебет(rub)|кредит(rub)|операция": BankStatement_10_process},
                 {"дата|номер|видоперации|контрагент|иннконтрагента|бикбанкаконтрагента|счетконтрагента|дебет,rur|кредит,rur|назначение": BankStatement_11_process},
                 {"дата|ро|док.|кб|внеш.счет|счет|дебет|кредит|назначение|контрагент|контр.инн": BankStatement_12_process},
                 {"документ|датаоперации|корреспондент.наименование|корреспондент.инн|корреспондент.кпп|корреспондент.счет|корреспондент.бик|вх.остаток|оборотдт|обороткт|назначениеплатежа": BankStatement_13_process},
                 {"тип|дата|номер|видоперации|сумма|валюта|основаниеплатежа|бикбанкаполучателя|счетполучателя|наименованиеполучателя": BankStatement_14_process},
                 {"№п/п|датаоперации/postingdate|датавалютир./value|видопер./op.type|номердокумента/documentnumber|реквизитыкорреспондента/counterpartydetails.наименование/name|реквизитыкорреспондента/counterpartydetails.счет/account|реквизитыкорреспондента/counterpartydetails.банк/bank|дебет/debit|кредит/credit|основаниеоперации(назначениеплатежа)/paymentdetails": BankStatement_15_process},
                 {"№документа|дата|бик|№счета|деб.оборот|кред.оборот|иннинаименованиеполучателя|назначениеплатежа": BankStatement_16_process},
                 {"дата|№док.|во|банкконтрагента|контрагент|счетконтрагента|дебет|кредит|назначениеплатежа": BankStatement_17_process},
                 {"№п/п|датасовершенияоперации(дд.мм.гг)|реквизитыдокумента,наоснованиикоторогобыласовершенаоперацияпосчету(специальномубанковскомусчету).вид(шифр)|реквизитыдокумента,наоснованиикоторогобыласовершенаоперацияпосчету(специальномубанковскомусчету).номер|реквизитыдокумента,наоснованиикоторогобыласовершенаоперацияпосчету(специальномубанковскомусчету).дата|реквизитыбанкаплательщика/получателяденежныхсредств.номеркорреспондентскогосчета|реквизитыбанкаплательщика/получателяденежныхсредств.наименование|реквизитыбанкаплательщика/получателяденежныхсредств.бик|реквизитыплательщика/получателяденежныхсредств.наименование/ф.и.о.|реквизитыплательщика/получателяденежныхсредств.инн/кио|реквизитыплательщика/получателяденежныхсредств.кпп|реквизитыплательщика/получателяденежныхсредств.номерсчета(специальногобанковскогосчета)|суммаоперациипосчету(специальномубанковскомусчету).подебету|суммаоперациипосчету(специальномубанковскомусчету).покредиту|назначениеплатежа": BankStatement_18_process},
                 {"номер|номерсчета|дата|контрагентcчет|контрагент|поступление|валюта|списание|валюта|назначение": BankStatement_19_process},
                 {"№п.п|№док.|датаоперации|бик/swiftбанкаплат.|наименованиебанкаплательщика|наименованиеплательщика|иннплательщика|№счетаплательщика|бик/swiftбанкаполуч.|наименованиебанкаполучателя|наименованиеполучателя|иннполучателя|№счетаполучателя|сальдовходящее|дебет|кредит|сальдоисходящее|назначениеплатежа": BankStatement_20_process},
                 {"|№п.п|№док.|датаоперации|бик/swiftбанкаплат.|наименованиебанкаплательщика|наименованиеплательщика|иннплательщика|№счетаплательщика|бик/swiftбанкаполуч.|наименованиебанкаполучателя|наименованиеполучателя|иннполучателя|№счетаполучателя|сальдовходящее|дебет|кредит|сальдоисходящее|назначениеплатежа": BankStatement_20_process},
                 {"датадок.|№док.|датаоперации|во|названиекорр.|иннкорр.|бикбанкакорр.|счеткорр.|дебет|кредит|назначение": BankStatement_21_process},
                 {"дата|№док.|во|названиекорр.|иннконтрагента|бикбанкакорр.|лицевойсчет|дебет|кредит|назначение": BankStatement_22_process},
                 {"дата|№док.|во|названиекорр.|бикбанкакорр.|лицевойсчет|дебет|кредит|назначение": BankStatement_22_process},
                 {"номерстроки|датапроводки|видоперации|номердокумента|счетплательщика/получателя|реквизитыплательщика/получателяденежныхсредств.наименование/фио|реквизитыплательщика/получателяденежныхсредств.инн/кио|реквизитыплательщика/получателяденежныхсредств.кпп|суммадебет|суммакредит|назначениеплатежа": BankStatement_23_process},
                 {"номерстроки|датапроводки|видоперации|датадокумента|номердокумента|счетплательщика/получателя|реквизитыплательщика/получателяденежныхсредств.наименование/фио|реквизитыплательщика/получателяденежныхсредств.инн/кио|реквизитыплательщика/получателяденежныхсредств.кпп|суммадебет|суммакредит|назначениеплатежа": BankStatement_23_process},
                 {"дата|№|клиент.инн|клиент.наименование|клиент.счет|корреспондент.бик|корреспондент.счет|корреспондент.наименование|во|содержание|обороты.дебет|обороты.кредит": BankStatement_24_process},
                 {"датаивремяпроводки|счеткорреспондента|дебет|кредит|исходящийостаток|наименованиекорреспондента|иннкорреспондента|назначениеплатежа": BankStatement_25_process},
                 {"датаоперации|номердокумента|дебет|кредит|контрагент.наименование|контрагент.инн|контрагент.кпп|контрагент.бик|контрагент.наименованиебанка|назначениеплатежа|коддебитора|типдокумента": BankStatement_26_process},
                 {"номерстроки|датапроводки|видоперации|номердокументаклиента|номердокументабанка(номердокументавсмфр)|счетплательщика/получателя|суммадебет|суммакредит|назначениеплатежа": BankStatement_27_process},
                 {"номерстроки|датапроводки|видоперации|номердокументаклиента|номердокументабанка(номердокументавсмфр)|счетплательщика/получателя|наименованиекорреспондирующегосчета|суммадебет|суммакредит|назначениеплатежа": BankStatement_27_process},
                 {"датаивремяпроводки|входостаток|дебет|кредит|исходящийостаток|док.|наименованиекорреспондента|иннкорреспондента|назначениеплатежа": BankStatement_28_process},
                 {"№п/п|датасовершенияоперации(дд.мм.гг)|реквизитыдокумента,наоснованиикоторогобыласовершенаоперацияпосчету(специальномубанковскомусчету).вид(шифр)|реквизитыдокумента,наоснованиикоторогобыласовершенаоперацияпосчету(специальномубанковскомусчету).номер|реквизитыдокумента,наоснованиикоторогобыласовершенаоперацияпосчету(специальномубанковскомусчету).дата|реквизитыбанкаплательщика/получателяденежныхсредств.номеркорреспондентскогосчета|реквизитыбанкаплательщика/получателяденежныхсредств.наименование|реквизитыбанкаплательщика/получателяденежныхсредств.бик|реквизитыплательщика/получателяденежныхсредств.наименование/ф.и.о.|реквизитыплательщика/получателяденежныхсредств.инн/кио|реквизитыплательщика/получателяденежныхсредств.кпп|реквизитыплательщика/получателяденежныхсредств.номерсчета|суммаоперациипосчету.подебету|суммаоперациипосчету.покредиту|назначениеплатежа": BankStatement_29_process},
                 {"датапроводки|во|nдок|банккорр.|корреспондент|счетконтрагента|дебет|кредит|назначениеплатежа": BankStatement_30_process},
                 {"документ|датаоперации|корреспондент.наименование|корреспондент.инн|корреспондент.счет|корреспондент.бик|вх.остаток|оборотдт|обороткт|назначениеплатежа": BankStatement_31_process},
                 {"дата|№док.|во|бикбанкаконтрагента|банкконтрагента|иннконтрагента|контрагент|счетконтрагента|дебет|кредит|назначениеплатежа": BankStatement_32_process},
                 {"датадокумента|номердокумента|поступление|списание|счеторганизации|организация|иннорганизации|счетконтрагента|контрагент|иннконтрагента|назначениеплатежа|коддебитора|типдокумента": BankStatement_33_process},
                 {"датадокумента|номердокумента|счеторганизации|организация|иннорганизации|счетконтрагента|контрагент|иннконтрагента|назначениеплатежа|поступление|списание|остатоквходящий|остатокисходящий|коддебитора|типдокумента": BankStatement_33_process},
                 {"датадок-та|номердок-та|корреспондент.банк|корреспондент.счет|корреспондент.наименование|видопер.|обороты.подебету|обороты.покредиту|назначениеплатежа": BankStatement_34_process},
                 {"датапроводки|№документа|клиент.инн|клиент.наименование|клиент.счет|корреспондент.бик|корреспондент.банк|корреспондент.счет|корреспондент.инн|корреспондент.наименование|во|назначениеплатежа|обороты.дебет|обороты.кредит|референспроводки": BankStatement_35_process},
                 {"номерсчета|идентификатортранзакции|типоперации(пополнение/списание)|категорияоперации|статус|датасозданияоперации|датаавторизации|дататранзакции|идентификатороригинальнойоперации|суммаоперацииввалютеоперации|валютаоперации|суммаввалютесчета|контрагент|иннконтрагента|кппконтрагента|счетконтрагента|бикбанкаконтрагента|корр.счетбанкаконтрагента|наименованиебанкаконтрагента|назначениеплатежа|номерплатежа|очередность|код(уин)|номеркарты|mcc|местосовершения(город)|местосовершения(страна)|адресорганизации|банк|статуссоставителярасчетногодокумента|кбк-кодбюджетнойклассификации|кодоктмо|основаниеналоговогоплатежа|налоговыйпериод/кодтаможенногооргана|номерналоговогодокумента|датаналоговогодокумента|типналоговогоплатежа": BankStatement_36_process},
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
    header1 = header1.mask(header1 == '').fillna(method='ffill').fillna("").astype(str)
    #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    header1 = header1.str.lower().replace('\n', '').replace(r'\s+', '', regex=True).replace(r'ё', 'е', regex=True).fillna("").astype(str)
    #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    #header1 = header1.replace('\n', '').replace(r'\s+', '', regex=True).replace(r'\d+\.?\d*', '', regex=True).fillna("").astype(str)
    datastart = 1
    #Здесь возможно надо проверять не на null, а на naп - integer или дата (через regex)

    if len(df.axes[0]) > 1 and df.iloc[1].mask(df.iloc[1]=='').isnull().iloc[0]:
        header2 = df.iloc[1].fillna("").astype(str)
        header2 = header2.str.lower().replace('\n', '').replace(r'\s+', '', regex=True).replace(r'ё', 'е', regex=True).replace(r'\d+\.?\d*', '', regex=True).fillna("").astype(str)
        header = pd.concat([header1, header2], axis=1).apply(
            lambda x: '.'.join([y for y in x if y]), axis=1)
        datastart = 2
    else:
        header = header1
    #header = header.drop_duplicates()
    df = df[datastart:]
    df.columns = header.str.lower().replace('\n', '').replace(r'\s+', '', regex=True).replace(r'ё', 'е', regex=True).fillna("column")
    ncols = len(df.columns)
    return df[df[list(df.columns)].isnull().sum(axis=1) < ncols * 0.8].dropna(
        axis=0, how='all'
    )

DATATYPES: list[str] = []

#removes rows, containing 1, 2, 3, 4, 5, ... (assuming that is just rows with columns numbers, which should be ignored)
def cleanupRawData(df: pd.DataFrame) -> pd.DataFrame:
    ncols = len(df.columns)
    row2del = "_".join([str(x) for x in range(1, ncols+1)])

    df["__rowval"] = pd.Series(df.fillna("").replace(r'\s+', '', regex=True).values.tolist()).str.join('_').values
    df = df[df.__rowval != row2del]

    return df.drop("__rowval", axis=1)

import warnings
warnings.filterwarnings("ignore", 'This pattern has match groups')

#sets to_ignore to True, if entrDate is empty
def cleanupProcessedData(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df.entryDate.notna()]
    df = df[df.entryDate.astype(str).str.contains(r"^(\d{1,2}[\,\.\-\/]\d{1,2}[\,\.\-\/]\d{2,4}|\d{2,4}[\,\.\-\/]\d{1,2}[\,\.\-\/]\d{1,2}[\w ]*)", regex=True)]
    return df


def getHeadLinesEXCEL(data, nlines: int = 3):
    idx = 0
    result = []
    while (idx < data.shape[0] and idx < nlines):
        row = data.iloc[[idx][0]]
        result.append(row.dropna(how='all'))
        idx += 1
    return result

def getExcelSheetKind(df):
    kinds = []
    headers = getHeadLinesEXCEL(df, 10)
    if suitable := [
        kind
        for kind in DOCTYPES
        if len(
            [
                row
                for row in headers
                if any(
                    row.astype(str)
                    .str.contains(kind, case=False)
                    .dropna(how='all')
                )
            ]
        )
        > 0
    ]:
        kinds.append(suitable[0])
    return kinds[0] if kinds else "UNDEFINED"

def processExcel(inname: str, clientid: str, logf: TextIOWrapper) -> tuple[pd.DataFrame, int, bool]:
    berror = False
    df = pd.DataFrame()
    data = pd.DataFrame()
    result = pd.DataFrame()

    sheets = pd.read_excel(inname, header=None, sheet_name=None)
    if len(sheets) > 1 :
        print(f"{datetime.now()}:{inname}:WARNING:{len(sheets)} sheets found")
    for sheet in sheets:
        try:
            df = sheets[sheet]
            df = df.dropna(axis=1,how='all')
            if not df.empty:
                kind = getExcelSheetKind(df)
                if kind == "выписка":
                    header, data, footer = getTableRange(df)

                    data = setDataColumns(data)
                    data = cleanupRawData(data)
                    signature = "|".join(data.columns).replace('\n', ' ')
                    funcs = list(filter(lambda item: item is not None, [sig.get(signature) for sig in HDRSIGNATURES]))
                    func = funcs[0] if funcs else NoneHDR_process
                    outdata = func(header, data, footer, inname, clientid, sheet, logf) # type: ignore
                    outdata = cleanupProcessedData(outdata)
                    result = pd.concat([result, outdata])
                else: 
                    logstr = f"{datetime.now()}:PASSED:{clientid}:{os.path.basename(inname)}:{sheet}:0:{kind}\n"
                    logf.write(logstr)
        except Exception as err:
            berror = True   
            print(f"{datetime.now()}:{inname}_{sheet}:ERROR:{err}")
            logstr = f"{datetime.now()}:ERROR:{clientid}:{os.path.basename(inname)}:{sheet}:0:{type(err).__name__} {str(err)}\n"
            logf.write(logstr)
    return (result, len(sheets), berror)

def pdfPagesCount(pdfname)->int:
    with open(pdfname,'rb') as f:
        pdfReader = PyPDF2.PdfFileReader(f)
        return pdfReader.getNumPages()

def getPDFData(inname: str) -> pd.DataFrame:
    npages = pdfPagesCount(inname)
    spage = 1
    cchunk = 100
    df = pd.DataFrame()

    if npages >= 500 :
        print(datetime.now(), ":", inname, ':WARINING: huge pdf:', npages, " pages")
        sys.stdout.flush()
    while spage <= npages :
        lpage = min(spage + cchunk - 1, npages)
        tables = camelot.read_pdf(inname, pages=f'{spage}-{lpage}', line_scale = 100, shift_text=['l', 't'], backend="poppler", layout_kwargs = {"char_margin": 0.1, "line_margin": 0.1, "boxes_flow": None})
        for tbl in tables :
            df = pd.concat([df, tbl.df])
        if npages >= 500 :
            print(datetime.now(), ":", inname, f':PROCESSED: {spage}-{lpage} of ', npages, " pages")
            sys.stdout.flush()
        spage = lpage + 1
    return df.reset_index(drop=True).dropna(axis=1,how='all')


def processPDF(inname: str, clientid: str, logf: TextIOWrapper) -> tuple[pd.DataFrame, int, bool]:
    berror = False
    data = pd.DataFrame()
    result = pd.DataFrame()

    try:
        df = getPDFData(inname)
        if not df.empty:
            header, data, footer = getTableRange(df)

            data = setDataColumns(data)
            data = cleanupRawData(data)
            signature = "|".join(data.columns).replace('\n', ' ')
            funcs = list(filter(lambda item: item is not None, [sig.get(signature) for sig in HDRSIGNATURES]))
            func = funcs[0] if funcs else NoneHDR_process
            outdata = func(header, data, footer, inname, clientid, "pdf", logf) # type: ignore
            outdata = cleanupProcessedData(outdata)
            result = pd.concat([result, outdata])
    
    except Exception as err:
        berror = True   
        print(f"{datetime.now()}:{inname}:ERROR:{err}")
        logstr = f"{datetime.now()}:ERROR:{clientid}:{os.path.basename(inname)}:pdf:0:{type(err).__name__} {str(err)}\n"
        logf.write(logstr)
    return (result, 1, berror)

def processOther(inname: str, clientid: str, logf: TextIOWrapper) -> tuple[pd.DataFrame, int, bool]:
    return (pd.DataFrame(), 0, True)

def process(inname: str, clientid: str, logf: TextIOWrapper) -> tuple[pd.DataFrame, int, bool]:
    processFunc = processOther

    if inname.lower().endswith('.xls') or inname.lower().endswith('.xlsx'):
        processFunc = processExcel
    elif inname.lower().endswith('.pdf'):
        processFunc = processPDF


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
