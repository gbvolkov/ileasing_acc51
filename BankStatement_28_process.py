from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#Дата и время проводки|Вход остаток|Дебет|Кредит|Исходящий остаток|Док.|Наименование корреспондента|ИНН корреспондента|Назначение платежа
#датаивремяпроводки|входостаток|дебет|кредит|исходящийостаток|док.|наименованиекорреспондента|иннкорреспондента|назначениеплатежа
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_28_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["датаивремяпроводки"]
    #df["cpBIC"] = data["Корреспондент.БИК"]
    #df["cpBank"] = data["Банк контрагента"]
    #df["cpAcc"] = data["Счет корреспондента"]
    df["cpTaxCode"] = data["иннкорреспондента"]
    df["cpName"] = data["наименованиекорреспондента"]
    df["Debet"] = data['дебет']
    df["Credit"] = data['кредит']
    df["Comment"] = data["назначениеплатежа"]

    #df["clientBIC"] = header.iloc[1,0]
    df["clientBank"] = header.iloc[0,0]
    #df["clientAcc"] = header.iloc[0,0]
    #df["clientName"] = header.iloc[0,0]

    df["openBalance"] = data["входостаток"].iloc[0]
    df["closingBalance"] = data["исходящийостаток"].iloc[len(data)-1]

    df["clientID"] = clientid
    df["filename"] = f"{inname}_{sheet}"
    df['processdate'] = datetime.now()
    
    return df
