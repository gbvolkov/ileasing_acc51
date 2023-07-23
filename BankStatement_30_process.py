from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#Дата проводки|ВО|N док|Банк корр.|Корреспондент|Счет контрагента|Дебет|Кредит|Назначение платежа
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_30_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["Дата проводки"]
    #df["cpBIC"] = data["БИК банка корр."]
    df["cpBank"] = data["Банк корр."]
    df["cpAcc"] = data["Счет контрагента"]
    #df["cpTaxCode"] = data["Корреспондент.ИНН"]
    df["cpName"] = data["Корреспондент"]
    df["Debet"] = data['Дебет']
    df["Credit"] = data['Кредит']
    df["Comment"] = data["Назначение платежа"]

    acc = header[header.iloc[:,0] == 'СЧЕТ'].dropna(axis=1,how='all')
    if acc.size > 1:
        df["clientAcc"] = acc.iloc[:,1].values[0]
    cname = header[header.iloc[:,0] == 'НАЗВАНИЕ'].dropna(axis=1,how='all')
    if cname.size > 1:
        df["clientName"] = cname.iloc[:,1].values[0]
    cbic = footer[footer.iloc[:,0] == 'БИК'].dropna(axis=1,how='all')
    if cbic.size > 1:
        df["clientBIC"] = cbic.iloc[:,1].values[0]
    df["clientBank"] = header.iloc[0,0]

    obalance = header[header.iloc[:,0] == 'Входящий остаток'].dropna(axis=1,how='all')
    if obalance.size > 2:
        df["openBalance"] = obalance.iloc[:,2].values[0]
    cbalance = footer[footer.iloc[:,0] == 'Исходящий остаток'].dropna(axis=1,how='all')
    if cbalance.size > 2:
        df["closingBalance"] = cbalance.iloc[:,2].values[0]
    turnovers = footer[footer.iloc[:,0] == 'Итого обороты'].dropna(axis=1,how='all')
    if turnovers.size > 1:
        df["totalDebet"] = turnovers.iloc[:,1].values[0]
    if turnovers.size > 2:
        df["totalCredit"] = turnovers.iloc[:,2].values[0]

    df["clientID"] = clientid
    df["filename"] = f"{inname}_{sheet}"
    df['processdate'] = datetime.now()
    
    return df
