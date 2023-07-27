from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#дата|nдок|во|банкконтрагента|контрагент|счетконтрагента|дебет|кредит|назначениеплатежа
#дата|no|во|бик|банкконтрагента|контрагент|иннконтрагента|счетконтрагента|дебет|кредит|назначениеплатежа
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_17_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, params: dict, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["дата"]
    if "бик" in data.columns:
        df["cpBIC"] = data["бик"]
    df["cpBank"] = data["банкконтрагента"]
    df["cpAcc"] = data["счетконтрагента"]
    if "иннконтрагента" in data.columns:
        df["cpTaxCode"] = data["иннконтрагента"]
    df["cpName"] = data["контрагент"]
    df["Debet"] = data['дебет']
    df["Credit"] = data['кредит']
    df["Comment"] = data["назначениеплатежа"]

    if len(header.axes[0]) >= 1:
        acc = header[header.iloc[:,1] == 'Счет'].dropna(axis=1,how='all')
        if acc.size > 1:
            df["clientAcc"] = acc.iloc[:,1].values[0]
        client = header[header.iloc[:,1] == 'Владелец счета'].dropna(axis=1,how='all')
        if client.size > 1:
            df["clientName"] = client.iloc[:,1].values[0]
        df["clientBank"] = header.iloc[0,1]

        obalance = header[header.iloc[:,0] == 'ВХОДЯЩИЙ ОСТАТОК'].dropna(axis=1,how='all')
        if obalance.size > 1:
            df["openBalance"] = obalance.iloc[:,2].values[0]
    if len(footer.axes[0]) >= 1:
        cbalance = footer[footer.iloc[:,0] == 'ИСХОДЯЩИЙ ОСТАТОК'].dropna(axis=1,how='all')
        if cbalance.size > 1:
            df["closingBalance"] = cbalance.iloc[:,1].values[0]
        turnovers = footer[footer.iloc[:,0] == 'ИТОГО ОБОРОТЫ'].dropna(axis=1,how='all')
        if turnovers.size > 1:
            df["totalDebet"] = turnovers.iloc[:,1].values[0]
        if turnovers.size > 2:
            df["totalCredit"] = turnovers.iloc[:,2].values[0]

    return df
