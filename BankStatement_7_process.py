from datetime import datetime
from io import TextIOWrapper
import pandas as pd
from const import COLUMNS

#датапроводки|счетдебет|счеткредит|суммаподебету|суммапокредиту|nдокумента|во|банкбикинаименование|назначениеплатежа
#COLUMNS = ["clientID", "clientBIC", "clientBank", "clientAcc", "clientName", "stmtDate", "stmtFrom", "stmtTo", "openBalance", "totalDebet", "totalCredit", "closingBalance",
#           "entryDate", "cpBIC", "cpBank", "cpAcc", "cpTaxCode", "cpName", "Debet", "Credit", "Comment",
#           "filename"]
def BankStatement_7_process(header: pd.DataFrame, data: pd.DataFrame, footer: pd.DataFrame, inname: str, clientid: str, sheet: str, logf: TextIOWrapper) -> pd.DataFrame:

    df = pd.DataFrame(columns = COLUMNS)

    df["entryDate"] = data["датапроводки"]
    df["cpBank"] = data["банкбикинаименование"]
    df["cpAcc"] = data.apply(lambda row: row['счетдебет'] if pd.isna(row['суммаподебету']) else row['счеткредит'], axis=1)
    #df["cpTaxCode"] = data["ИНН контрагента"]
    #df["cpName"] = data["Контрагент"]
    df["Debet"] = data["суммаподебету"]
    df["Credit"] = data["суммапокредиту"]
    df["Comment"] = data["назначениеплатежа"]

    #header: За период,c 22.06.2020 по 22.06.2021,,,,
    if len(header.axes[0]) >= 1:
        df["clientBank"] = header.iloc[1,0]
        acc = header[header.iloc[:,0] == 'ВЫПИСКА ОПЕРАЦИЙ ПО ЛИЦЕВОМУ СЧЕТУ'].dropna(axis=1,how='all')
        if acc.size > 1:
            df["clientAcc"] = acc.iloc[:,1].values[0]
    if len(footer.axes[0]) >= 1:
        obalance = footer[footer.iloc[:,0] == 'Входящий остаток'].dropna(axis=1,how='all')
        if obalance.size > 2:
            df["openBalance"] = obalance.iloc[:,2].values[0]
        turnovers = footer[footer.iloc[:,0] == 'Итого оборотов'].dropna(axis=1,how='all')
        if turnovers.size > 1:
            df["totalDebet"] = turnovers.iloc[:,1].values[0]
        if turnovers.size > 2:
            df["totalCredit"] = turnovers.iloc[:,2].values[0]
        cbalance = footer[footer.iloc[:,0] == 'Исходящий остаток'].dropna(axis=1,how='all')
        if cbalance.size > 2:
            df["closingBalance"] = cbalance.iloc[:,2].values[0]

    df["clientID"] = clientid
    df["filename"] = f"{inname}_{sheet}"
    df['processdate'] = datetime.now()

    return df
