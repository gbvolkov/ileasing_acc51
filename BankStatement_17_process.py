from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#Дата|№ док.|ВО|Банк контрагента|Контрагент|Счет контрагента|Дебет|Кредит|Назначение платежа
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_17_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["Дата"]
    #df["cpBIC"] = data["Реквизиты корреспондента /Counter party details.Банк / Bank"]
    df["cpBank"] = data["Банк контрагента"]
    df["cpAcc"] = data["Счет контрагента"]
    #df["cpTaxCode"] = data["Корреспондент.ИНН"]
    df["cpName"] = data["Контрагент"]
    df["Debet"] = data['Дебет']
    df["Credit"] = data['Кредит']
    df["Comment"] = data["Назначение платежа"]

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
    cbalance = footer[footer.iloc[:,0] == 'ИСХОДЯЩИЙ ОСТАТОК'].dropna(axis=1,how='all')
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
