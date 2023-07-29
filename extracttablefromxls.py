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
import re

#using this (type: ignore) since camelot does not have stubs
import camelot # type: ignore
from pdfminer.high_level import extract_pages
from pdfminer.layout import LTTextBoxHorizontal, LTTextLineHorizontal
from BankStatement_51_process import BankStatement_51_process
from BankStatement_52_process import BankStatement_52_process
from BankStatement_53_process import BankStatement_53_process
from BankStatement_54_process import BankStatement_54_process
from BankStatement_55_process import BankStatement_55_process
from const import COLUMNS, DOCTYPES, REGEX_ACCOUNT, REGEX_AMOUNT, REGEX_BIC, REGEX_INN
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
from BankStatement_37_process import BankStatement_37_process
from BankStatement_38_process import BankStatement_38_process
from BankStatement_39_process import BankStatement_39_process
from BankStatement_40_process import BankStatement_40_process
from BankStatement_41_process import BankStatement_41_process
from BankStatement_42_process import BankStatement_42_process
from BankStatement_43_process import BankStatement_43_process
from BankStatement_44_process import BankStatement_44_process
from BankStatement_45_process import BankStatement_45_process
from BankStatement_46_process import BankStatement_46_process
from BankStatement_47_process import BankStatement_47_process
from BankStatement_48_process import BankStatement_48_process
from BankStatement_49_process import BankStatement_49_process
from BankStatement_50_process import BankStatement_50_process

def NoneHDR_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, params: dict, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    datatype = "|".join(data.columns).replace('\n', ' ')
    DATATYPES.append(datatype)
    print(f"Datatype: {datatype} NOT FOUND")

    logstr = f"{datetime.now()}:NOT IMPLEMENTED:{clientid}:{os.path.basename(inname)}:{sheet}:0:\"{datatype}\"\n"
    logf.write(logstr)

    return pd.DataFrame(columns = COLUMNS)

def IgnoreHDR_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, params: dict, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    return pd.DataFrame(columns = COLUMNS)

def TestHDR_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, params: dict, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    NoneHDR_process(header, data, footer, inname, clientid, params, sheet, logf)
    
    nameparts = os.path.split(inname)
    fname = os.path.splitext(nameparts[1])[0]

    headerfname = os.path.join(nameparts[0], f"{fname}_{sheet}_header.csv") # type: ignore
    datafname = os.path.join(nameparts[0], f"{fname}_{sheet}_data.csv") # type: ignore
    footerfname = os.path.join(nameparts[0], f"{fname}_{sheet}_footer.csv") # type: ignore

    header.to_csv(headerfname, mode="w", header=True, index=False)
    data.to_csv(datafname, mode="w", header=True, index=False)
    footer.to_csv(footerfname, mode="w", header=True, index=False)

    return pd.DataFrame(columns = COLUMNS)


HDRSIGNATURES = [{"датадокумента|датаоперации|n|бик|счет|контрагент|иннконтрагента|бикбанкаконтрагента|коррсчетбанкаконтрагента|наименованиебанкаконтрагента|счетконтрагента|списание|зачисление|назначениеплатежа|код": BankStatement_1_process},
                 {"датадокумента|датаоперации|n|бик|счет|контрагент|иннконтрагента|бикбанкаконтрагента|коррсчетбанкаконтрагента|наименованиебанкаконтрагента|счетконтрагента|списание|зачисление|назначениеплатежа|код|показательстатуса101|коддоходабюджетнойклассификации104|кодоктмо105|показательоснованияплатежа106|показательналоговогопериодакодтаможенногооргана107|показательномерадокумента108|показательдатыдокумента109|показательтипаплатежа110": BankStatement_1_process},
                 {"дата|видшифроперацииво|номердокументабанка|номердокумента|бикбанкакорреспондента|корреспондирующийсчет|суммаподебету|суммапокредиту": BankStatement_2_process},
                 {"датаоперации|номердокумента|дебет|кредит|контрагентнаименование|контрагентинн|контрагенткпп|контрагентбик|контрагентнаименованиебанка|назначениеплатежа|коддебитора|типдокумента": BankStatement_3_process},
                 {"датаоперации|номердокумента|дебет|кредит|контрагентнаименование|контрагентинн|контрагенткпп|контрагентсчет|контрагентбик|контрагентнаименованиебанка|назначениеплатежа|коддебитора|типдокумента": BankStatement_3_process},
                 {"nдок|датадокумента|датаоперации|реквизитыкорреспондентанаименование|реквизитыкорреспондентасчет|реквизитыкорреспондентаиннконтрагента|реквизитыкорреспондентабанк|дебетсуммасуммавнп|кредитсуммасуммавнп|курсцбнадатуоперации|основаниеоперацииназначениеплатежа": BankStatement_4_process},
                 {"дата|номердокумента|дебет|кредит|контрагентнаименование|контрагентинн|контрагенткпп|контрагентбик|контрагентнаименованиебанка|назначениеплатежа|типдокумента": BankStatement_5_process},
                 {"дата|номердокумента|дебет|кредит|контрагентнаименование|контрагентинн|контрагенткпп|контрагентбик|контрагентнаименованиебанка|назначениеплатежа|коддебитора|типдокумента": BankStatement_5_process},
                 {"дата|номердокумента|дебет|кредит|контрагентнаименование|контрагентинн|контрагенткпп|контрагентсчет|контрагентбик|контрагентнаименованиебанка|назначениеплатежа|коддебитора|типдокумента": BankStatement_5_process},
                 {"номердокумента|датадокумента|датаоперации|счет|контрагент|иннконтрагента|бикбанкаконтрагента|коррсчетбанкаконтрагента|наименованиебанкаконтрагента|счетконтрагента|списание|зачисление|назначениеплатежа": BankStatement_6_process},
                 {"templatecode|repstatementsrurexcelxls": IgnoreHDR_process},
                 {"номер|контрагент|реквизитыконтрагента|назначениеплатежа|дебет|кредит": IgnoreHDR_process},
                 {"xdo_?accountnumber?|<?dataean?>|<?dataean?>": IgnoreHDR_process},
                 {"датапроводки|счетдебет|счеткредит|суммаподебету|суммапокредиту|nдокумента|во|банкбикинаименование|назначениеплатежа": BankStatement_7_process},
                 {"датаопер|ко|номердокум|датадокум|дебет|кредит|рублевоепокрытие|контрагентинн|контрагенткпп|контрагентнаименование|контрагентсчет|контрагентбик|контрагенткоррсчет|контрагентбанк|клиентинн|клиентнаименование|клиентсчет|клиенткпп|клиентбин|клиенткоррсчет|клиентбанк|код|назначениеплатежа|очерплатежа|бюджетныйплатежстатуссост|бюджетныйплатежкбк|бюджетныйплатежоктмо|бюджетныйплатежоснование|бюджетныйплатежналогпериод|бюджетныйплатежномердок|idопер": BankStatement_8_process},
                 {"датаопер|ко|номердокум|датадокум|дебет|кредит|рублевоепокрытие|контрагентинн|контрагенткпп|контрагентнаименование|контрагентсчет|контрагентбик|контрагенткоррсчет|контрагентбанк|клиентинн|клиентнаименование|клиентсчет|клиенткпп|клиентбин|клиенткоррсчет|клиентбанк|назначениеплатежа|очерплатежа|idопер": BankStatement_8_process},
                 {"датаопер|ко|номердокум|датадокум|дебет|кредит|рублевоепокрытие|контрагентинн|контрагенткпп|контрагентнаименование|контрагентсчет|контрагентбик|контрагенткоррсчет|контрагентбанк|клиентинн|клиентнаименование|клиентсчет|клиенткпп|клиентбик|клиенткоррсчет|клиентбанк|резполе|код|кодвыплат|назначениеплатежа|очерплатежа|видусловияоплаты|основаниедлясписания|бюджетныйплатежстатуссост|бюджетныйплатежкбк|бюджетныйплатежоктмо|бюджетныйплатежоснование|бюджетныйплатежналогпериод|бюджетныйплатежномердок|бюджетныйплатеждатадок|idопер": BankStatement_8_process},
                 {"датаопер|ко|номердокум|датадокум|дебет|кредит|рублевоепокрытие|контрагентинн|контрагенткпп|контрагентнаименование|контрагентсчет|контрагентбик|контрагенткоррсчет|контрагентбанк|клиентинн|клиентнаименование|клиентсчет|клиенткпп|клиентбик|клиенткоррсчет|клиентбанк|резполе|код|кодвыплат|назначениеплатежа|очерплатежа|видусловияоплаты|основаниедлясписания|бюджетныйплатежстатуссост|бюджетныйплатежкбк|бюджетныйплатежоктмо|бюджетныйплатежоснование|бюджетныйплатежналогпериод|бюджетныйплатежномердок|бюджетныйплатеждатадок|idопер|idдокум": BankStatement_8_process},
                 {"датаопер|ко|номердокум|датадокум|дебет|кредит|рублевоепокрытие|контрагентинн|контрагенткпп|контрагентнаименование|контрагентсчет|контрагентбик|контрагенткоррсчет|контрагентбанк|клиентинн|клиентнаименование|клиентсчет|клиенткпп|клиентбик|клиенткоррсчет|клиентбанк|резполе|код|кодвыплат|назначениеплатежа|очерплатежа|видусловияоплаты|бюджетныйплатежстатуссост|бюджетныйплатежкбк|бюджетныйплатежоктмо|бюджетныйплатежоснование|бюджетныйплатежналогпериод|бюджетныйплатежномердок|бюджетныйплатеждатадок|idопер|idдокум": BankStatement_8_process},
                 {"датаопер|ко|номердокум|датадокум|дебет|кредит|рублевоепокрытие|контрагентинн|контрагенткпп|контрагентнаименование|контрагентсчет|контрагентбик|контрагенткоррсчет|контрагентбанк|клиентинн|клиентнаименование|клиентсчет|клиенткпп|клиентбик|клиенткоррсчет|клиентбанк|резполе|код|кодвыплат|назначениеплатежа|очерплатежа|видусловияоплаты|основаниедлясписания|бюджетныйплатежстатуссост|бюджетныйплатежкбк|бюджетныйплатежоктмо|бюджетныйплатежоснование|бюджетныйплатежналогпериод|бюджетныйплатежномердок|бюджетныйплатеждатадок|idопер|idдокум|кодвидадохода": BankStatement_8_process},
                 {"датаоперации|nдок|видоперации|контрагент|иннконтрагента|бикбанкаконтрагента|лицевойсчет|дебет|кредит|назначение": BankStatement_9_process},
                 {"датаоперации|nдок|видоперации|контрагент|иннконтрагента|бикбанкаконтрагента|лицевойсчет|дебет|кредит|назначение|суммавнацпокрытии|курс": BankStatement_9_process},
                 {"дата|видопер|nдок|бик|банкконтрагента|контрагент|иннконтрагента|счетконтрагента|дебетrub|кредитrub|операция": BankStatement_10_process},
                 {"дата|номер|видоперации|контрагент|иннконтрагента|бикбанкаконтрагента|счетконтрагента|дебетrur|кредитrur|назначение": BankStatement_11_process},
                 {"дата|ро|док|кб|внешсчет|счет|дебет|кредит|назначение|контрагент|контринн": BankStatement_12_process},
                 {"документ|датаоперации|корреспондентнаименование|корреспондентинн|корреспонденткпп|корреспондентсчет|корреспондентбик|вхостаток|оборотдт|обороткт|назначениеплатежа": BankStatement_13_process},
                 {"тип|дата|номер|видоперации|сумма|валюта|основаниеплатежа|бикбанкаполучателя|счетполучателя|наименованиеполучателя": BankStatement_14_process},
                 {"nпп|датаоперацииpostingdate|датавалютирvalue|видоперoptype|номердокументаdocumentnumber|реквизитыкорреспондентаcounterpartydetailsнаименованиеname|реквизитыкорреспондентаcounterpartydetailsсчетaccount|реквизитыкорреспондентаcounterpartydetailsбанкbank|дебетdebit|кредитcredit|основаниеоперацииназначениеплатежаpaymentdetails": BankStatement_15_process},
                 {"nпп|датаоперацииpostingdate|датавалютирvaluedate|видоперoptype|номердокументаdocumentnumber|реквизитыкорреспондентаcounterpartydetailsнаименованиеname|реквизитыкорреспондентаcounterpartydetailsсчетaccount|реквизитыкорреспондентаcounterpartydetailsбанкbank|дебетdebit|кредитcredit|основаниеоперацииназначениеплатежаpaymentdetails": BankStatement_15_process},
                 {"nдокумента|дата|бик|nсчета|дебоборот|кредоборот|иннинаименованиеполучателя|назначениеплатежа": BankStatement_16_process},
                 {"дата|nдок|во|банкконтрагента|контрагент|счетконтрагента|дебет|кредит|назначениеплатежа": BankStatement_17_process},
                 {"дата|no|во|бик|банкконтрагента|контрагент|иннконтрагента|счетконтрагента|дебет|кредит|назначениеплатежа": BankStatement_17_process},
                 {"nпп|датасовершенияоперацииддммгг|реквизитыдокументанаоснованиикоторогобыласовершенаоперацияпосчетуспециальномубанковскомусчетувидшифр|реквизитыдокументанаоснованиикоторогобыласовершенаоперацияпосчетуспециальномубанковскомусчетуномер|реквизитыдокументанаоснованиикоторогобыласовершенаоперацияпосчетуспециальномубанковскомусчетудата|реквизитыбанкаплательщикаполучателяденежныхсредствномеркорреспондентскогосчета|реквизитыбанкаплательщикаполучателяденежныхсредствнаименование|реквизитыбанкаплательщикаполучателяденежныхсредствбик|реквизитыплательщикаполучателяденежныхсредствнаименованиефио|реквизитыплательщикаполучателяденежныхсредствиннкио|реквизитыплательщикаполучателяденежныхсредствкпп|реквизитыплательщикаполучателяденежныхсредствномерсчетаспециальногобанковскогосчета|суммаоперациипосчетуспециальномубанковскомусчетуподебету|суммаоперациипосчетуспециальномубанковскомусчетупокредиту|назначениеплатежа": BankStatement_18_process},
                 {"номер|номерсчета|дата|контрагентcчет|контрагент|поступление|валюта|списание|валюта|назначение": BankStatement_19_process},
                 {"nпп|nдок|датаоперации|бикswiftбанкаплат|наименованиебанкаплательщика|наименованиеплательщика|иннплательщика|nсчетаплательщика|бикswiftбанкаполуч|наименованиебанкаполучателя|наименованиеполучателя|иннполучателя|nсчетаполучателя|сальдовходящее|дебет|кредит|сальдоисходящее|назначениеплатежа": BankStatement_20_process},
                 {"column|nпп|nдок|датаоперации|бикswiftбанкаплат|наименованиебанкаплательщика|наименованиеплательщика|иннплательщика|nсчетаплательщика|бикswiftбанкаполуч|наименованиебанкаполучателя|наименованиеполучателя|иннполучателя|nсчетаполучателя|сальдовходящее|дебет|кредит|сальдоисходящее|назначениеплатежа": BankStatement_20_process},
                 {"датадок|nдок|датаоперации|во|названиекорр|иннкорр|бикбанкакорр|счеткорр|дебет|кредит|назначение": BankStatement_21_process},
                 {"дата|nдок|во|названиекорр|иннконтрагента|бикбанкакорр|лицевойсчет|дебет|кредит|назначение": BankStatement_22_process},
                 {"дата|nдок|во|названиекорр|бикбанкакорр|лицевойсчет|дебет|кредит|назначение": BankStatement_22_process},
                 {"дата|nдок|во|бикбанкакорр|названиекорр|лицевойсчет|дебет|кредит|назначение": BankStatement_22_process},
                 {"номерстроки|датапроводки|видоперации|номердокумента|счетплательщикаполучателя|реквизитыплательщикаполучателяденежныхсредствнаименованиефио|реквизитыплательщикаполучателяденежныхсредствиннкио|реквизитыплательщикаполучателяденежныхсредствкпп|суммадебет|суммакредит|назначениеплатежа": BankStatement_23_process},
                 {"номерстроки|датапроводки|видоперации|датадокумента|номердокумента|счетплательщикаполучателя|реквизитыплательщикаполучателяденежныхсредствнаименованиефио|реквизитыплательщикаполучателяденежныхсредствиннкио|реквизитыплательщикаполучателяденежныхсредствкпп|суммадебет|суммакредит|назначениеплатежа": BankStatement_23_process},
                 {"номерстроки|датапроводки|видоперации|номердокумента|счетплательщикаполучателя|суммадебет|суммакредит|назначениеплатежа": BankStatement_23_process},
                 {"дата|n|клиентинн|клиентнаименование|клиентсчет|корреспондентбик|корреспондентсчет|корреспондентнаименование|во|содержание|оборотыдебет|оборотыкредит": BankStatement_24_process},
                 {"датаивремяпроводки|счеткорреспондента|дебет|кредит|исходящийостаток|наименованиекорреспондента|иннкорреспондента|назначениеплатежа": BankStatement_25_process},
                 {"датаоперации|номердокумента|дебет|кредит|контрагентнаименование|контрагентинн|контрагенткпп|контрагентбик|контрагентнаименованиебанка|назначениеплатежа|коддебитора|типдокумента": BankStatement_26_process},
                 {"номерстроки|датапроводки|видоперации|номердокументаклиента|номердокументабанканомердокументавсмфр|счетплательщикаполучателя|суммадебет|суммакредит|назначениеплатежа": BankStatement_27_process},
                 {"номерстроки|датапроводки|видоперации|номердокументаклиента|номердокументабанканомердокументавсмфр|счетплательщикаполучателя|наименованиекорреспондирующегосчета|суммадебет|суммакредит|назначениеплатежа": BankStatement_27_process},
                 {"датаивремяпроводки|входостаток|дебет|кредит|исходящийостаток|док|наименованиекорреспондента|иннкорреспондента|назначениеплатежа": BankStatement_28_process},
                 {"nпп|датасовершенияоперацииддммгг|реквизитыдокументанаоснованиикоторогобыласовершенаоперацияпосчетуспециальномубанковскомусчетувидшифр|реквизитыдокументанаоснованиикоторогобыласовершенаоперацияпосчетуспециальномубанковскомусчетуномер|реквизитыдокументанаоснованиикоторогобыласовершенаоперацияпосчетуспециальномубанковскомусчетудата|реквизитыбанкаплательщикаполучателяденежныхсредствномеркорреспондентскогосчета|реквизитыбанкаплательщикаполучателяденежныхсредствнаименование|реквизитыбанкаплательщикаполучателяденежныхсредствбик|реквизитыплательщикаполучателяденежныхсредствнаименованиефио|реквизитыплательщикаполучателяденежныхсредствиннкио|реквизитыплательщикаполучателяденежныхсредствкпп|реквизитыплательщикаполучателяденежныхсредствномерсчета|суммаоперациипосчетуподебету|суммаоперациипосчетупокредиту|назначениеплатежа": BankStatement_29_process},
                 {"датапроводки|во|nдок|банккорр|корреспондент|счетконтрагента|дебет|кредит|назначениеплатежа": BankStatement_30_process},
                 {"документ|датаоперации|корреспондентнаименование|корреспондентинн|корреспондентсчет|корреспондентбик|вхостаток|оборотдт|обороткт|назначениеплатежа": BankStatement_31_process},
                 {"дата|nдок|во|бикбанкаконтрагента|банкконтрагента|иннконтрагента|контрагент|счетконтрагента|дебет|кредит|назначениеплатежа": BankStatement_32_process},
                 {"датадокумента|номердокумента|поступление|списание|счеторганизации|организация|иннорганизации|счетконтрагента|контрагент|иннконтрагента|назначениеплатежа|коддебитора|типдокумента": BankStatement_33_process},
                 {"датадокумента|номердокумента|счеторганизации|организация|иннорганизации|счетконтрагента|контрагент|иннконтрагента|назначениеплатежа|поступление|списание|остатоквходящий|остатокисходящий|коддебитора|типдокумента": BankStatement_33_process},
                 {"датадокта|номердокта|корреспондентбанк|корреспондентсчет|корреспондентнаименование|видопер|оборотыподебету|оборотыпокредиту|назначениеплатежа": BankStatement_34_process},
                 {"датапроводки|nдокумента|клиентинн|клиентнаименование|клиентсчет|корреспондентбик|корреспондентбанк|корреспондентсчет|корреспондентинн|корреспондентнаименование|во|назначениеплатежа|оборотыдебет|оборотыкредит|референспроводки": BankStatement_35_process},
                 {"датапроводки|nдокумента|клиентинн|клиентнаименование|клиентсчет|корреспондентбик|корреспондентбанк|корреспондентсчет|корреспондентнаименование|во|назначениеплатежа|оборотыдебет|оборотыкредит|референспроводки": BankStatement_35_process},
                 {"номерсчета|идентификатортранзакции|типоперациипополнениесписание|категорияоперации|статус|датасозданияоперации|датаавторизации|дататранзакции|идентификатороригинальнойоперации|суммаоперацииввалютеоперации|валютаоперации|суммаввалютесчета|контрагент|иннконтрагента|кппконтрагента|счетконтрагента|бикбанкаконтрагента|коррсчетбанкаконтрагента|наименованиебанкаконтрагента|назначениеплатежа|номерплатежа|очередность|кодуин|номеркарты|mcc|местосовершениягород|местосовершениястрана|адресорганизации|банк|статуссоставителярасчетногодокумента|кбккодбюджетнойклассификации|кодоктмо|основаниеналоговогоплатежа|налоговыйпериодкодтаможенногооргана|номерналоговогодокумента|датаналоговогодокумента|типналоговогоплатежа": BankStatement_36_process},
                 {"номердокумента|ко|датаоперации|дебет|кредит|реквизитыконтрагентабик|реквизитыконтрагентанаименование|основаниеоперации": BankStatement_37_process},
                 {"nпп|датадокументаdocumentdate|датавалютирvaluedate|видоперoptype|реквизитыкорреспондентаcounterpartydetailsбикbic|реквизитыкорреспондентаcounterpartydetailsсчетaccount|реквизитыкорреспондентаcounterpartydetailsиннinn|реквизитыкорреспондентаcounterpartydetailsкппkpp|реквизитыкорреспондентаcounterpartydetailsнаименованиеname|дебетdebit|кредитcredit|основаниеоперацииназначениеплатежаpaymentdetails": BankStatement_38_process},
                 {"датаоперации|номердокумента|суммаподебету|суммапокредиту|контрагентсчетинннаименование|контрагентбанкбикнаименование|назначениеплатежа|коддебитора|типдокумента": BankStatement_39_process},
                 {"nдок|датадокумента|датаоперации|реквизитыкорреспондентанаименование|реквизитыкорреспондентасчет|реквизитыкорреспондентаиннконтрагента|реквизитыкорреспондентабанк|дебетсуммасуммавнп|кредитсуммасуммавнп|курсцбнадатуоперации|основаниеоперацииназначениеплатежа": BankStatement_40_process},
                 {"nn|датапров|во|номдок|бик|счеткорреспондент|дебет|кредит|основание|основание": BankStatement_41_process},
                 {"n|дата|счеткорреспондент|оборотдебет|обороткредит|примечание": BankStatement_42_process},
                 {"дата|номер|дебет|кредит|контрагентнаименованиеиннкппсчет|контрагентбанкбикнаименование|назначениеплатежа|коддебитор|документ": BankStatement_43_process},
                 {"дата|номер|дебет|кредит|контрагентнаименованиеиннкппсчет|контрагентбанкбикнаименование|назначениеплатежа|коддебитора|документ": BankStatement_43_process},
                 {"номердокумента|ко|датаоперации|дебет|кредит|реквизитыкорреспондентабик|реквизитыкорреспондентанаименование|основаниеоперации": BankStatement_44_process},
                 {"column|no|контрагент|иннконтрагента|счетконтрагента|дебет|кредит|назначениеплатежа": BankStatement_45_process},
                 {"no|датаоперации|nдокумента|шифрдокумента|бикбанкакорреспондента|наименованиекорреспондента|nсчетакорреспондента|дебет|кредит|суммавнацпокрытии|назначениеплатежа": BankStatement_46_process},
                 {"дата|nдок|во|названиекорр|иннкорр|бикбанкакорр|счеткорр|дебет|кредит|назначение": BankStatement_47_process},
                 {"датаоперации|датадокумента|во|nдокта|коррсчет|бик|наименованиебанка|счет|иннинаименованиекорреспондента|дебет|кредит|назначениеплатежа": BankStatement_48_process},
                 {"номерстроки|датапроводки|видоперации|номердокументаклиента|номердокументабанканомердокументавсмфр|счетплательщикаполучателя|дебет|кредит|остаток|назначениеплатежа": BankStatement_49_process},
                 {"датаоперации|номердокумента|корреспондентнаименованиеинн|корреспондентномерсчета|корреспондентнаименованиебанкабик|дебет|кредит|назначениеплатежа": BankStatement_50_process},
                 {"дата|n|во|контрагентинн|контрагентбикбанка|контрагентсчет|контрагентнаименование|оборотыrurдебет|оборотыrurкредит|назначение": BankStatement_51_process},
                 {"датапроводки|счетдебет|счеткредит|сумма|nдок|вид|во|банккоррбикинаименование|назначениеплатежа": BankStatement_52_process},
                 {"дата|n|иннплательщика|иннполучателя|корреспондентбик|корреспондентсчет|корреспондентнаименование|во|содержание|оборотыrurдебет|оборотыrurкредит": BankStatement_53_process},
                 {"документ|датаоперации|корреспондент|оборотдт|обороткт|назначениеплатежа": BankStatement_54_process},
                 {"датапроводки|во|номдок|банккорр|названиекорреспондента|счетплательщика|счетполучателя|дебет|кредит|назначениеплатежа": BankStatement_55_process},
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

    for idx in range(len(df.index)-1):
        
        header1 = df.iloc[idx].replace('\n', '').replace(r'\s+', '', regex=True).replace(r'\d+\.?\d*', '', regex=True).fillna("").astype(str)
        if header1.mask(header1 == '').notna().sum() >= min_count:
            header2 = df.iloc[idx+1].fillna("").astype(str)
            header = pd.concat([header1, header2], axis=1).apply(
                lambda x: '.'.join([y for y in x if y]), axis=1)
            cnas = header.mask(header == '').isna().sum()
            rowidx = df.iloc[idx:idx+1].index[0]
            result = pd.concat([result, pd.DataFrame([{"_idx": rowidx, "_cnas": cnas, "_header": header}])])
    result = result[result._idx==result[result._cnas==result._cnas.min()]._idx.min()]
    if result._header.size > 0:
        header = result._header[0]
        ncols = header.mask(header=='').notna().sum()
    else:
        header = pd.DataFrame()
        ncols = 0
    return result[result._cnas==result._cnas.min()]._idx.min(), ncols, header.mask(header=='').dropna().index.to_list()


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

    lastrpowidx = firstrowidx + 1 # type: ignore

    for idx in range(len(df.index)-1, 0, -1):
        cnavalues = dfFilled.iloc[idx].isnull().sum()
        if (ncols-cnavalues)*100/nheadercols > 47 and not all(dfFilled.iloc[idx][ncols-3:].isnull()):
            lastrow = idx
            lastrpowidx = df.iloc[lastrow:lastrow+1].index[0]
            break
    header = df.loc[:firstrowidx-1].dropna(axis=1,how='all').dropna(axis=0,how='all') # type: ignore
    footer = df.loc[lastrpowidx+1 : ].dropna(axis=1,how='all').dropna(axis=0,how='all') # type: ignore
    #df = df.loc[firstrowidx : lastrpowidx, headercols]
    df = df.loc[firstrowidx : , headercols]

    data = df.dropna(axis=1,how='all').dropna(axis=0,how='all')
    return (
		header,
		data,
		footer,
	)

def setDataColumns(df) -> pd.DataFrame:
    header1 = df.iloc[0]
    header1 = header1.mask(header1 == '').fillna(method='ffill').fillna("").astype(str)
    header1 = header1.str.lower().replace('\n', '').replace(r'\s+', '', regex=True).replace(r'ё', 'е', regex=True).fillna("").astype(str)
    datastart = 1

    if len(df.axes[0]) > 1 and df.iloc[1].mask(df.iloc[1]=='').isnull().iloc[0]:
        header2 = df.iloc[1].fillna("").astype(str)
        header2 = header2.str.lower().replace('\n', '').replace(r'\s+', '', regex=True).replace(r'ё', 'е', regex=True).replace(r'\d+\.?\d*', '', regex=True).fillna("").astype(str)
        header = pd.concat([header1, header2], axis=1).apply(
            lambda x: '.'.join([y for y in x if y]), axis=1)
        datastart = 2
    else:
        header = header1
    df = df[datastart:]
    df.columns = header.str.lower().replace(r'[\n\.\,\(\)\/\-]', '', regex=True).replace(r'№', 'n', regex=True).replace(r'\s+', '', regex=True).replace(r'ё', 'е', regex=True).mask(header1 == '').fillna("column")
    cols=pd.Series(df.columns)
    for dup in cols[cols.duplicated()].unique(): 
        cols[cols[cols == dup].index.values.tolist()] = [dup + '.' + str(i) if i != 0 else dup for i in range(sum(cols == dup))]
    df.columns=cols
    ncols = len(df.columns)
    return df[df[list(df.columns)].isnull().sum(axis=1) < ncols * 0.8].dropna(
        axis=0, how='all'
    )

DATATYPES: list[str] = []

#removes rows, containing 1, 2, 3, 4, 5, ... (assuming that is just rows with columns numbers, which should be ignored)
def cleanupRawData(df: pd.DataFrame) -> pd.DataFrame:
    ncols = len(df.columns)
    row2del = "_".join([str(x) for x in range(1, ncols+1)])

    df["__rowval"] = pd.Series(df.fillna("").astype(str).replace(r'\s+', '', regex=True).values.tolist()).str.join('_').values
    df = df[df.__rowval != row2del]

    return df.drop("__rowval", axis=1)

import warnings
warnings.filterwarnings("ignore", 'This pattern has match groups')

#sets to_ignore to True, if entrDate is empty
def cleanupAndEnreachProcessedData(df: pd.DataFrame, inname: str, clientid: str, params: dict, sheet: str) -> pd.DataFrame:

    #cleanup
    df = df[df.entryDate.notna()]
    df.entryDate = df.entryDate.astype(str).replace(r'[\s\n]', '', regex=True)
    #df = df[df.entryDate.astype(str).str.contains(r"^(?:\d{1,2}[\,\.\-\/]\d{1,2}[\,\.\-\/]\d{2,4}|\d{2,4}[\,\.\-\/]\d{1,2}[\,\.\-\/]\d{1,2}[\w ]*)", regex=True)]
    df.loc[df.entryDate.astype(str).str.contains(r"^(?:\d{1,2}[\,\.\-\/]\d{1,2}[\,\.\-\/]\d{2,4}|\d{2,4}[\,\.\-\/]\d{1,2}[\,\.\-\/]\d{1,2}[\w ]*)", regex=True, na=False) == False, "result"] = 1

    #enreach
    df["__hdrcpTaxCode"] = params["inn"]
    df["__hdrclientBIC"] = params["bic"]
    df["__hdrclientAcc"] = params["account"]
    df["__hdropenBalance"] = params["amount"]
    df["__header"] = params["header"]

    df["clientID"] = clientid
    df["filename"] = f"{inname}_{sheet}"
    df["processdate"] = datetime.now()

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
            df = sheets[sheet].dropna(axis=1,how='all')
            if not df.empty:
                kind = getExcelSheetKind(df)
                if kind == "выписка":
                    header, data, footer = getTableRange(df)
                    if not data.empty:
                        data = setDataColumns(data)
                        data = cleanupRawData(data)
                        signature = "|".join(data.columns).replace('\n', ' ')
                        
                        params = getHeaderValues("|".join(header[:].apply(lambda x: '|'.join(x.dropna().astype(str)), axis=1)), signature)

                        funcs = list(filter(lambda item: item is not None, [sig.get(signature) for sig in HDRSIGNATURES]))
                        func = funcs[0] if funcs else NoneHDR_process
                        outdata = func(header, data, footer, inname, clientid, params, sheet, logf) # type: ignore
                        if not outdata.empty:
                            outdata = cleanupAndEnreachProcessedData(outdata, inname, clientid, params, str(sheet))
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

def getHeadLinesPDF(pdfname: str, nlines: int = 3) :
    result = []
    for page_layout in extract_pages(pdfname, maxpages=1) :
        for element in page_layout :
            if isinstance(element, LTTextBoxHorizontal) :
                txt = element.get_text()
                lines = [x.strip() for x in txt.split("\n")]
                for line in lines :
                    if len(line) > 0 :
                        result.append(line)
                        if len(result) >= nlines :
                            return result
    return result

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


def getHeaderValues(header: str, signature: str) -> dict:
    
    hdr = re.sub(r"[\n\.\,\(\)\/\-\|]", '',  re.sub(r'№', 'n', re.sub(r'\s+', '', re.sub(r'ё', 'е', header)))).lower()
    eoh = hdr.find(signature[:8].replace('|', ''))
    if eoh != -1:
        header = header[:int(len(header)*eoh/len(hdr))]
    account = re.search(REGEX_ACCOUNT, header)
    account = account.group() if account else ""
    bic = re.search(REGEX_BIC, header)
    bic = bic.group() if bic else ""
    inn = re.search(REGEX_INN, header)
    inn = inn.group() if inn else ""
    amount = re.search(REGEX_AMOUNT, header)
    amount = amount.group() if amount else ""
    return {"header": header, "bic": bic, "account": account, "inn": inn, "amount": amount}

def processPDF(inname: str, clientid: str, logf: TextIOWrapper) -> tuple[pd.DataFrame, int, bool]:
    berror = False
    data = pd.DataFrame()
    result = pd.DataFrame()

    try:
        df = getPDFData(inname)
        if not df.empty:
            header, data, footer = getTableRange(df)
            if not data.empty:
                data = setDataColumns(data)
                data = cleanupRawData(data)
                signature = "|".join(data.columns).replace('\n', ' ')
                
                params = getHeaderValues('|'.join(getHeadLinesPDF(inname, 50)), signature)

                funcs = list(filter(lambda item: item is not None, [sig.get(signature) for sig in HDRSIGNATURES]))
                func = funcs[0] if funcs else NoneHDR_process
                outdata = func(header, data, footer, inname, clientid, params, "pdf", logf) # type: ignore
                outdata = cleanupAndEnreachProcessedData(outdata, inname, clientid, params, "pdf")
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
    parser.add_argument("--pdf", default=True, action=BooleanOptionalAction, help="Weather to include pdf (--no-pdf opposite option)")
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
