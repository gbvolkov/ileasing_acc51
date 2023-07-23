from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#Дата и время проводки|Счет корреспондента|Дебет|Кредит|Исходящий остаток|Наименование корреспондента|ИНН корреспондента|Назначение платежа
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_25_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["Дата и время проводки"]
    #df["cpBIC"] = data["Корреспондент.БИК"]
    #df["cpBank"] = data["Банк контрагента"]
    df["cpAcc"] = data["Счет корреспондента"]
    df["cpTaxCode"] = data["ИНН корреспондента"]
    df["cpName"] = data["Наименование корреспондента"]
    df["Debet"] = data['Дебет']
    df["Credit"] = data['Кредит']
    df["Comment"] = data["Назначение платежа"]

    #df["clientBIC"] = header.iloc[1,0]
    if len(header.axes[0]) >= 1:
        df["clientBank"] = header.iloc[0,0]
    #df["clientAcc"] = header.iloc[0,0]
    #df["clientName"] = header.iloc[0,0]

    df["closingBalance"] = data["Исходящий остаток"].iloc[len(data)-1]

    df["clientID"] = clientid
    df["filename"] = f"{inname}_{sheet}"
    df['processdate'] = datetime.now()

    return df
