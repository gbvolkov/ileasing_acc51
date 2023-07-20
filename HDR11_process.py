from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#Дата|Номер|Вид операции|Контрагент|ИНН контрагента|БИК банка контрагента|Счет контрагента|Дебет, RUR|Кредит, RUR|Назначение
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def HDR11_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["Дата"]
    df["cpBIC"] = data["БИК банка контрагента"]
    #df["cpBank"] = data["Банк контрагента"]
    df["cpAcc"] = data["Счет контрагента"]
    df["cpTaxCode"] = data["ИНН контрагента"]
    df["cpName"] = data["Контрагент"]
    df["Debet"] = data["Дебет, RUR"]
    df["Credit"] = data["Кредит, RUR"]
    df["Comment"] = data["Назначение"]

    #header: За период,c 22.06.2020 по 22.06.2021,,,,
    clientinfo = header[header.iloc[:,0] == 'Номер счета:'].dropna(axis=1,how='all')
    if clientinfo.size > 1:
        df["clientAcc"] = clientinfo.iloc[:,1].values[0]
    if clientinfo.size > 5:
        df["clientName"] = clientinfo.iloc[:,5].values[0]
    #df["clientBIC"] = data["Клиент.БИК"]
    #df["clientBank"] = header.iloc[0,0]
    
    balances = header[header.iloc[:,0] == 'Входящий остаток: '].dropna(axis=1,how='all')
    if balances.size > 1:
        df["openBalance"] = balances.iloc[:,1].values[0]
    if balances.size > 3:
        df["closingBalance"] = balances.iloc[:,3].values[0]

    turnovers = footer[footer.iloc[:,0] == 'ИТОГО:'].dropna(axis=1,how='all')
    if turnovers.size > 1:
        df["totalDebet"] = turnovers.iloc[:,1].values[0]
    if turnovers.size > 2:
        df["totalCredit"] = turnovers.iloc[:,2].values[0]

    df["clientID"] = clientid
    df["filename"] = f"{inname}_{sheet}"
    
    return df
