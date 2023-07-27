from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#датадок|nдок|датаоперации|во|названиекорр|иннкорр|бикбанкакорр|счеткорр|дебет|кредит|назначение
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_21_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, params: dict, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["датаоперации"]

    df["cpBIC"] = data["бикбанкакорр"]
    #df["cpBank"] = data.apply(lambda row: row['Наименование Банка плательщика'] if pd.isna(row['Дебет']) else row['Наименование банка получателя'], axis=1)
    df["cpAcc"] = data["счеткорр"]
    df["cpTaxCode"] = data["иннкорр"]
    df["cpName"] = data["названиекорр"]
    df["Debet"] = data['дебет']
    df["Credit"] = data['кредит']
    df["Comment"] = data["назначение"]

    if len(header.axes[0]) >= 4:
        df["clientAcc"] = header.iloc[3,0]
        client = header[header.iloc[:,1] == 'Владелец счета'].dropna(axis=1,how='all')
        if client.size > 1:
            df["clientName"] = client.iloc[:,1].values[0]
        df["clientBank"] = header.iloc[0,0]

        obalance = header[header.iloc[:,0].fillna("").str.startswith('ВХОДЯЩИЙ ОСТАТОК')].dropna(axis=1,how='all')
        if obalance.size > 1:
            df["openBalance"] = obalance.iloc[:,1].values[0]
    if len(footer.axes[0]) >= 1:
        cbalance = footer[footer.iloc[:,0] == 'ИСХОДЯЩИЙ ОСТАТОК '].dropna(axis=1,how='all')
        if cbalance.size > 1:
            df["closingBalance"] = cbalance.iloc[:,1].values[0]
        turnovers = footer[footer.iloc[:,0] == 'ИТОГО ОБОРОТЫ'].dropna(axis=1,how='all')
        if turnovers.size > 1:
            df["totalDebet"] = turnovers.iloc[:,1].values[0]
        if turnovers.size > 2:
            df["totalCredit"] = turnovers.iloc[:,2].values[0]

    return df
