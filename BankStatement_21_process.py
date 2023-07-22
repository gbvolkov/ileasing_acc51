from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#Дата док.|№ док.|Дата операции|ВО|Название корр.|ИНН корр.|БИК банка корр.|Счет корр.|Дебет|Кредит|Назначение
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_21_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["Дата операции"]

    df["cpBIC"] = data["БИК банка корр."]
    #df["cpBank"] = data.apply(lambda row: row['Наименование Банка плательщика'] if pd.isna(row['Дебет']) else row['Наименование банка получателя'], axis=1)
    df["cpAcc"] = data["Счет корр."]
    df["cpTaxCode"] = data["ИНН корр."]
    df["cpName"] = data["Название корр."]
    df["Debet"] = data['Дебет']
    df["Credit"] = data['Кредит']
    df["Comment"] = data["Назначение"]

    df["clientAcc"] = header.iloc[3,0]
    client = header[header.iloc[:,1] == 'Владелец счета'].dropna(axis=1,how='all')
    if client.size > 1:
        df["clientName"] = client.iloc[:,1].values[0]
    df["clientBank"] = header.iloc[0,0]

    obalance = header[header.iloc[:,0].str.startswith('ВХОДЯЩИЙ ОСТАТОК')].dropna(axis=1,how='all')
    if obalance.size > 1:
        df["openBalance"] = obalance.iloc[:,1].values[0]
    cbalance = footer[footer.iloc[:,0] == 'ИСХОДЯЩИЙ ОСТАТОК '].dropna(axis=1,how='all')
    if cbalance.size > 1:
        df["closingBalance"] = cbalance.iloc[:,1].values[0]
    turnovers = footer[footer.iloc[:,0] == 'ИТОГО ОБОРОТЫ'].dropna(axis=1,how='all')
    if turnovers.size > 1:
        df["totalDebet"] = turnovers.iloc[:,1].values[0]
    if turnovers.size > 2:
        df["totalCredit"] = turnovers.iloc[:,2].values[0]

    df["clientID"] = clientid
    df["filename"] = f"{inname}_{sheet}"
    
    return df
