from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#Дата|Вид опер.|№ док.|БИК|Банк контрагента|Контрагент|ИНН контрагента|Счёт контрагента|Дебет (RUB)|Кредит (RUB)|Операция
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_10_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["Дата"]
    df["cpBIC"] = data["БИК"]
    df["cpBank"] = data["Банк контрагента"]
    df["cpAcc"] = data["Счёт контрагента"]
    df["cpTaxCode"] = data["ИНН контрагента"]
    df["cpName"] = data["Контрагент"]
    df["Debet"] = data["Дебет (RUB)"]
    df["Credit"] = data["Кредит (RUB)"]
    df["Comment"] = data["Операция"]

    #header: За период,c 22.06.2020 по 22.06.2021,,,,
    if len(header.axes[0]) >= 1:
        clientName = header[header.iloc[:,0] == 'Владелец счета'].dropna(axis=1,how='all')
        if clientName.size > 1:
            df["clientName"] = clientName.iloc[:,1].values[0]
        #df["clientBIC"] = data["Клиент.БИК"]
        df["clientBank"] = header.iloc[0,0]
        clientAcc = header[header.iloc[:,0] == 'Счет'].dropna(axis=1,how='all')
        if clientAcc.size > 1:
            df["clientAcc"] = clientAcc.iloc[:,1].values[0]
        
        obalance = header[header.iloc[:,0] == 'Остаток на конец предыдущего периода:'].dropna(axis=1,how='all')
        if obalance.size > 1:
            df["openBalance"] = obalance.iloc[:,1].values[0]

    if len(footer.axes[0]) >= 1:
        cbalance = footer[footer.iloc[:,0].fillna("").str.startswith('Исходящий остаток')].dropna(axis=1,how='all')
        if cbalance.size > 1:
            df["closingBalance"] = cbalance.iloc[:,1].values[0]
        turnovers = footer[footer.iloc[:,0] == 'ИТОГО ОБОРОТЫ'].dropna(axis=1,how='all')
        if turnovers.size > 1:
            df["totalDebet"] = turnovers.iloc[:,1].values[0]
        if turnovers.size > 2:
            df["totalCredit"] = turnovers.iloc[:,2].values[0]

    df["clientID"] = clientid
    df["filename"] = f"{inname}_{sheet}"
    df['processdate'] = datetime.now()
    
    return df
