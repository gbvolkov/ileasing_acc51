from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#Дата и время проводки|Вход остаток|Дебет|Кредит|Исходящий остаток|Док.|Наименование корреспондента|ИНН корреспондента|Назначение платежа
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_28_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["Дата и время проводки"]
    #df["cpBIC"] = data["Корреспондент.БИК"]
    #df["cpBank"] = data["Банк контрагента"]
    #df["cpAcc"] = data["Счет корреспондента"]
    df["cpTaxCode"] = data["ИНН корреспондента"]
    df["cpName"] = data["Наименование корреспондента"]
    df["Debet"] = data['Дебет']
    df["Credit"] = data['Кредит']
    df["Comment"] = data["Назначение платежа"]

    #df["clientBIC"] = header.iloc[1,0]
    df["clientBank"] = header.iloc[0,0]
    #df["clientAcc"] = header.iloc[0,0]
    #df["clientName"] = header.iloc[0,0]

    df["openBalance"] = data["Вход остаток"].iloc[0]
    df["closingBalance"] = data["Исходящий остаток"].iloc[len(data)-1]

    df["clientID"] = clientid
    df["filename"] = f"{inname}_{sheet}"
    df['processdate'] = datetime.now()
    
    return df
