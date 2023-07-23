from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#Дата|№ док.|ВО|Название корр.|ИНН контрагента|БИК банка корр.|Лицевой счет|Дебет|Кредит|Назначение
#Дата|№ док.|ВО|Название корр.|БИК банка корр.|Лицевой счет|Дебет|Кредит|Назначение
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_22_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["Дата"]
    df["cpBIC"] = data["БИК банка корр."]
    #df["cpBank"] = data["Банк контрагента"]
    df["cpAcc"] = data["Лицевой счет"]
    if "ИНН контрагента" in data.columns:
        df["cpTaxCode"] = data["ИНН контрагента"]
    df["cpName"] = data["Название корр."]
    df["Debet"] = data['Дебет']
    df["Credit"] = data['Кредит']
    df["Comment"] = data["Назначение"]

    df["clientAcc"] = header.iloc[3,0]
    df["clientName"] = header.iloc[4,0]
    df["clientBank"] = header.iloc[0,0]

    obalance = header[header.iloc[:,0].fillna("").str.startswith('ВХОДЯЩИЙ ОСТАТОК')].dropna(axis=1,how='all')
    if obalance.size > 1:
        df["openBalance"] = obalance.iloc[:,1].values[0]
    cbalance = footer[footer.iloc[:,0].fillna("").str.startswith('ИСХОДЯЩИЙ ОСТАТОК')].dropna(axis=1,how='all')
    if cbalance.size > 1:
        df["closingBalance"] = cbalance.iloc[:,1].values[0]
    turnovers = footer[footer.iloc[:,0].isin(['Обороты по дебету/ кредиту', 'Итого'])].dropna(axis=1,how='all')
    if turnovers.size > 1:
        df["totalDebet"] = turnovers.iloc[:,1].values[0]
    if turnovers.size > 2:
        df["totalCredit"] = turnovers.iloc[:,2].values[0]

    df["clientID"] = clientid
    df["filename"] = f"{inname}_{sheet}"
    df['processdate'] = datetime.now()
    
    return df
