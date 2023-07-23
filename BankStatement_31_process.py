from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#Документ|Дата операции|Корреспондент.Наименование|Корреспондент.ИНН|Корреспондент.Счет|Корреспондент.БИК|Вх.остаток|Оборот Дт|Оборот Кт|Назначение платежа
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_31_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["Дата операции"]
    df["cpBIC"] = data["Корреспондент.БИК"]
    #df["cpBank"] = data["Банк корр."]
    df["cpAcc"] = data["Корреспондент.Счет"]
    df["cpTaxCode"] = data["Корреспондент.ИНН"]
    df["cpName"] = data["Корреспондент.Наименование"]
    df["Debet"] = data['Оборот Дт']
    df["Credit"] = data['Оборот Кт']
    df["Comment"] = data["Назначение платежа"]

    if len(header.axes[0]) >= 3:
        acc = header[header.iloc[:,0].fillna("").str.startswith('Выписка по счету')].dropna(axis=1,how='all')
        if acc.size > 0:
            df["clientAcc"] = acc.iloc[:,0].values[0]
        df["clientName"] = header.iloc[2,0]
        df["clientBank"] = header.iloc[1,0]

        obalance = header[header.iloc[:,1] == 'Входящий остаток:'].dropna(axis=1,how='all')
        if obalance.size > 1:
            df["openBalance"] = obalance.iloc[:,1].values[0]
    if len(footer.axes[0]) >= 1:
        cbalance = footer[footer.iloc[:,1] == 'Исходящий остаток:'].dropna(axis=1,how='all')
        if cbalance.size > 1:
            df["closingBalance"] = cbalance.iloc[:,1].values[0]
        turnovers = footer[footer.iloc[:,0] == 'ИТОГО:'].dropna(axis=1,how='all')
        if turnovers.size > 1:
            df["totalDebet"] = turnovers.iloc[:,1].values[0]
        if turnovers.size > 2:
            df["totalCredit"] = turnovers.iloc[:,2].values[0]

    df["clientID"] = clientid
    df["filename"] = f"{inname}_{sheet}"
    df['processdate'] = datetime.now()
    
    return df
