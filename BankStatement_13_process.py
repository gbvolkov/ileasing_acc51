from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#Документ|Дата операции|Корреспондент.Наименование|Корреспондент.ИНН|Корреспондент.КПП|Корреспондент.Счет|Корреспондент.БИК|Вх.остаток|Оборот Дт|Оборот Кт|Назначение платежа
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_13_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    # sourcery skip: extract-method
    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["Дата операции"]
    df["cpBIC"] = data["Корреспондент.БИК"]
    #df["cpBank"] = data["Банк контрагента"]
    df["cpAcc"] = data["Корреспондент.Счет"]
    df["cpTaxCode"] = data["Корреспондент.ИНН"]
    df["cpName"] = data["Корреспондент.Наименование"]
    df["Debet"] = data["Оборот Дт"]
    df["Credit"] = data["Оборот Кт"]
    df["Comment"] = data["Назначение платежа"]

    #header: За период,c 22.06.2020 по 22.06.2021,,,,

    if len(header.axes[0]) >= 1:
        acc = header[header.iloc[:,0] == 'Выписка по счету:'].dropna(axis=1,how='all')
        if acc.size > 1:
            df["clientAcc"] = acc.iloc[:,1].values[0]
        bank = header[header.iloc[:,0] == 'Банк:'].dropna(axis=1,how='all')
        if bank.size > 1:
            df["clientBank"] = bank.iloc[:,1].values[0]
        cname = header[header.iloc[:,0] == 'Владелец:'].dropna(axis=1,how='all')
        if cname.size > 1:
            df["clientName"] = cname.iloc[:,1].values[0]

    df["clientID"] = clientid
    df["filename"] = f"{inname}_{sheet}"
    df['processdate'] = datetime.now()
    
    return df
