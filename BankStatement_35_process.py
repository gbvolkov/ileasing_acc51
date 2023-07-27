from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#датапроводки|nдокумента|клиентинн|клиентнаименование|клиентсчет|корреспондентбик|корреспондентбанк|корреспондентсчет|корреспондентинн|корреспондентнаименование|во|назначениеплатежа|оборотыдебет|оборотыкредит|референспроводки
#датапроводки|nдокумента|клиентинн|клиентнаименование|клиентсчет|корреспондентбик|корреспондентбанк|корреспондентсчет|корреспондентнаименование|во|назначениеплатежа|оборотыдебет|оборотыкредит|референспроводки
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_35_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, params: dict, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["датапроводки"]
    df["cpBIC"] = data["корреспондентбик"]
    df["cpBank"] = data["корреспондентбанк"]
    df["cpAcc"] = data["корреспондентсчет"]
    if "корреспондентинн" in data.columns:
        df["cpTaxCode"] = data["корреспондентинн"]
    df["cpName"] = data["корреспондентнаименование"]
    df["Debet"] = data['оборотыдебет']
    df["Credit"] = data['оборотыкредит']
    df["Comment"] = data["назначениеплатежа"]

    #df["clientBIC"] = header.iloc[1,0]
    #df["clientBank"] = header.iloc[1,0]
    df["clientAcc"] = data["клиентсчет"]
    df["clientName"] = data["клиентнаименование"]
    df["clientTaxCode"] = data["клиентинн"]

    if len(header.axes[0]) >= 1:
        obalance = header[header.iloc[:,0].fillna("").str.startswith('Входящий остаток')].dropna(axis=1,how='all')
        if obalance.size > 1:
            df["openBalance"] = obalance.iloc[:,1].values[0]
    if len(footer.axes[0]) >= 1:
        cbalance = footer[footer.iloc[:,0].str.startswith('Исходящий остаток')].dropna(axis=1,how='all')
        if cbalance.size > 1:
            df["closingBalance"] = cbalance.iloc[:,1].values[0]
        turnovers = footer[footer.iloc[:,0] == 'Итого'].dropna(axis=1,how='all')
        if turnovers.size > 1:
            df["totalDebet"] = turnovers.iloc[:,1].values[0]
        if turnovers.size > 2:
            df["totalCredit"] = turnovers.iloc[:,2].values[0]

    
    return df
