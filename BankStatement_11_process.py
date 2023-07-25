from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#дата|номер|видоперации|контрагент|иннконтрагента|бикбанкаконтрагента|счетконтрагента|дебетrur|кредитrur|назначение
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_11_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["дата"]
    df["cpBIC"] = data["бикбанкаконтрагента"]
    #df["cpBank"] = data["Банк контрагента"]
    df["cpAcc"] = data["счетконтрагента"]
    df["cpTaxCode"] = data["иннконтрагента"]
    df["cpName"] = data["контрагент"]
    df["Debet"] = data["дебетrur"]
    df["Credit"] = data["кредитrur"]
    df["Comment"] = data["назначение"]

    #header: За период,c 22.06.2020 по 22.06.2021,,,,
    if len(header.axes[0]) >= 1:
        clientinfo = header[header.iloc[:,0] == 'Номер счета:'].dropna(axis=1,how='all')
        if clientinfo.size > 1:
            df["clientAcc"] = clientinfo.iloc[:,1].values[0]
        if clientinfo.size > 5:
            df["clientName"] = clientinfo.iloc[:,5].values[0]
        
        balances = header[header.iloc[:,0] == 'Входящий остаток: '].dropna(axis=1,how='all')
        if balances.size > 1:
            df["openBalance"] = balances.iloc[:,1].values[0]
        if balances.size > 3:
            df["closingBalance"] = balances.iloc[:,3].values[0]

    if len(footer.axes[0]) >= 1:
        turnovers = footer[footer.iloc[:,0] == 'ИТОГО:'].dropna(axis=1,how='all')
        if turnovers.size > 1:
            df["totalDebet"] = turnovers.iloc[:,1].values[0]
        if turnovers.size > 2:
            df["totalCredit"] = turnovers.iloc[:,2].values[0]

    df["clientID"] = clientid
    df["filename"] = f"{inname}_{sheet}"
    df['processdate'] = datetime.now()
    
    return df
