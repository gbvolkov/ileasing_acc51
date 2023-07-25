from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#дата|nдок|во|названиекорр|иннконтрагента|бикбанкакорр|лицевойсчет|дебет|кредит|назначение
#дата|nдок|во|названиекорр|бикбанкакорр|лицевойсчет|дебет|кредит|назначение
#дата|nдок|во|бикбанкакорр|названиекорр|лицевойсчет|дебет|кредит|назначение
# COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_22_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["дата"]
    df["cpBIC"] = data["бикбанкакорр"]
    #df["cpBank"] = data["Банк контрагента"]
    df["cpAcc"] = data["лицевойсчет"]
    if "иннконтрагента" in data.columns:
        df["cpTaxCode"] = data["иннконтрагента"]
    df["cpName"] = data["названиекорр"]
    df["Debet"] = data['дебет']
    df["Credit"] = data['кредит']
    df["Comment"] = data["назначение"]

    if len(header.axes[0]) >= 5:
        df["clientAcc"] = header.iloc[3,0]
        df["clientName"] = header.iloc[4,0]
        df["clientBank"] = header.iloc[0,0]

        obalance = header[header.iloc[:,0].fillna("").str.startswith('ВХОДЯЩИЙ ОСТАТОК')].dropna(axis=1,how='all')
        if obalance.size > 1:
            df["openBalance"] = obalance.iloc[:,1].values[0]
    if len(footer.axes[0]) >= 1:
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
