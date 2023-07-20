from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#Дата операции|№ док.|Вид операции|Контрагент|ИНН контрагента|БИК банка контрагента|Лицевой счет|Дебет|Кредит|Назначение
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def HDR9_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["Дата операции"]
    df["cpBIC"] = data["БИК банка контрагента"]
    #df["cpBank"] = data["Контрагент.Банк"]
    df["cpAcc"] = data["Лицевой счет"]
    df["cpTaxCode"] = data["ИНН контрагента"]
    df["cpName"] = data["Контрагент"]
    df["Debet"] = data["Дебет"]
    df["Credit"] = data["Кредит"]
    df["Comment"] = data["Назначение"]

    #header: За период,c 22.06.2020 по 22.06.2021,,,,
    df["clientName"] = header.iloc[4:,0]
    #df["clientBIC"] = data["Клиент.БИК"]
    df["clientBank"] = header.iloc[0:,0]
    df["clientAcc"] = header.iloc[3,0]
    
    obalance = header[header.iloc[:,0].str.startswith('ВХОДЯЩИЙ ОСТАТОК')].dropna(axis=1,how='all')
    if obalance.size > 1:
        df["openBalance"] = obalance.iloc[:,1].values[0]
    cbalance = footer[header.iloc[:,0] == 'ИСХОДЯЩИЙ ОСТАТОК'].dropna(axis=1,how='all')
    if cbalance.size > 1:
        df["closingBalance"] = cbalance.iloc[:,1].values[0]

    df["clientID"] = clientid
    df["filename"] = f"{inname}_{sheet}"
    
    return df
