from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#Номер|Номер счёта|Дата|Контрагент cчёт|Контрагент|Поступление|Валюта|Списание|Валюта|Назначение
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_19_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:
    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["Дата"]
    #df["cpBIC"] = data["Реквизиты банка плательщика/получателя денежных средств.БИК"]
    #df["cpBank"] = data["Реквизиты банка плательщика/получателя денежных средств.наименование"]
    df["cpAcc"] = data["Контрагент cчёт"]
    #df["cpTaxCode"] = data["Реквизиты плательщика/получателя денежных средств.ИНН/КИО"]
    df["cpName"] = data["Контрагент"]
    df["Debet"] = data['Списание']
    df["Credit"] = data['Поступление']
    df["Comment"] = data["Назначение"]

    if len(header.axes[0]) >= 1:
        df["clientName"] = header.iloc[0,0]

    if len(footer.axes[0]) >= 1:
        obalance = footer[footer.iloc[:,0] == 'Входящий остаток'].dropna(axis=1,how='all')
        if obalance.size > 2:
            df["openBalance"] = obalance.iloc[:,2].values[0]
        cbalance = footer[footer.iloc[:,0] == 'Исходящий остаток'].dropna(axis=1,how='all')
        if cbalance.size > 2:
            df["closingBalance"] = cbalance.iloc[:,2].values[0]
        turnovers = footer[footer.iloc[:,0] == 'Итого оборотов'].dropna(axis=1,how='all')
        if turnovers.size > 1:
            df["totalDebet"] = turnovers.iloc[:,1].values[0]
        if turnovers.size > 2:
            df["totalCredit"] = turnovers.iloc[:,2].values[0]

    df["clientID"] = clientid
    df["filename"] = f"{inname}_{sheet}"
    df['processdate'] = datetime.now()
    
    return df
